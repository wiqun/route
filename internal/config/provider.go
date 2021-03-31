package config

import (
	"fmt"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"os"
	"reflect"
	"strings"
)

type Provider interface {
	ProvideByFlag([]string)
	ProvideByEnv()
	unmarshal() error
}

type Provide struct {
	config     interface{}
	tags       map[string]bool
	values     map[string]string
	ignoreCase bool
	prefix     string
}

func New(config interface{}, ignoreCase bool, prefix string) Provider {
	tags := make(map[string]bool)
	getAllTags(reflect.ValueOf(config), "", tags)
	provide := &Provide{
		config:     config,
		ignoreCase: ignoreCase,
		tags:       tags,
		values:     map[string]string{},
		prefix:     prefix,
	}
	return provide
}

/**
只适用于基础类型切片和Map
*/
func getAllTags(v reflect.Value, tag string, tags map[string]bool) {
	switch v.Kind() {
	case reflect.Slice, reflect.Array:
		tags[strings.TrimPrefix(tag, "_")] = true
	case reflect.Struct:
		t := v.Type()
		for i := 0; i < v.NumField(); i++ {
			getAllTags(v.Field(i), fmt.Sprintf("%s_%s", tag, t.Field(i).Tag.Get("yaml")), tags) //为每一级添加下划线做分割
		}
	case reflect.Map:
		var s string
		for _, key := range v.MapKeys() {
			s += fmt.Sprintf("%v: %v,", key, v.MapIndex(key)) //暂时没有使用到的Map解析
		}
		tags[strings.TrimPrefix(tag, "_")] = false
	case reflect.Ptr:
		if v.IsNil() {
			tags = nil
			return
		} else {
			getAllTags(v.Elem(), tag, tags) //将指针指向的值递归
		}
	case reflect.Interface:
		if v.IsNil() {
			tags = nil
			return
		} else {
			getAllTags(v.Elem(), tag, tags) //递归接口的值
		}
	default:
		tags[strings.TrimPrefix(tag, "_")] = false //将每个tag对应的值添加进map
	}
}

func (r *Provide) ProvideByFlag(keyAndValue []string) {
	r.provideByFlag(keyAndValue) //keyAndValue是一个已经解析出的命令行参数数组
}

func (r *Provide) provideByFlag(keyAndValue []string) {
	if keyAndValue != nil {
		kv := make([]string, 2)
		for _, v := range keyAndValue {
			kv = strings.Split(v, "=")
			if r.deletePrefix(kv) {
				r.updateValue(kv)
			}
		}
	}
}

/*删除命令行参数用于区分所需命令的前缀,若没有前缀代表不是所需的命令行参数，跳过*/
func (r *Provide) deletePrefix(kv []string) bool {
	if strings.HasPrefix(kv[0], r.prefix) {
		kv[0] = strings.TrimPrefix(kv[0], r.prefix+"_")
		return true
	}
	return false
}

func (r *Provide) ProvideByEnv() {
	r.provideByEnv()
}

/*
获取所有的环境变量,当环境变量对应所需要的参数时则将其参数的value修改为环境变量配置的value
*/
func (r *Provide) provideByEnv() {
	kv := make([]string, 2)
	for _, v := range os.Environ() {
		kv = strings.Split(v, "=")
		if r.deletePrefix(kv) {
			r.updateValue(kv)
		}
	}
}

/*
根据传递过来的key-value数组修改将其暂时存放到values
*/
func (r *Provide) updateValue(kv []string) {
	if r.ignoreCase {
		for key, _ := range r.tags {
			if strings.ToLower(key) == strings.ToLower(kv[0]) {
				r.values[key] = kv[1]
			}
		}
	} else {
		for key, _ := range r.tags {
			if key == kv[0] {
				r.values[key] = kv[1]
			}
		}
	}
}

/*
解析获取的map配置传给config结构体
*/
func (r *Provide) unmarshal() error {
	configMap := make(map[string]interface{}) //新建一个ConfigMap用于保存嵌套map结构
	for k, v := range r.values {              //遍历该结构解析成树形嵌套结构
		ks := strings.Split(k, "_")
		var value = configMap[ks[0]]
		nextValue := make(map[string]interface{})
		var temValue map[string]interface{}
		var hasValueCount int
		for index := 0; index < len(ks); index++ {
			if value == nil { //判断是否已经赋值，防止覆盖
				if len(ks) == 1 { //判断是否是基本数据类型
					value = r.checkSlice(k, value, v)
					break
				} else {
					temValue = make(map[string]interface{})
					value = temValue
					continue
				}
			} else if reflect.TypeOf(value) == reflect.TypeOf(nextValue) { //若已赋值且为map[string]interface类型则将其地址赋值给temValue
				temValue = value.(map[string]interface{})
				hasValueCount++
			} else {
				break
			}
			if temValue[ks[index]] == nil { //判断其子结构的参数是否赋值
				if index == len(ks)-1 { //判断子结构的参数是否是基本类型
					temValue[ks[index]] = r.checkSlice(k, temValue[ks[index]], v)
				} else if index >= hasValueCount {
					nextValue = make(map[string]interface{})
					temValue[ks[index]] = nextValue
					temValue = nextValue
				}
			}
		}
		configMap[ks[0]] = value //将修改后的value重新赋值给map
	}
	data, err := yaml.Marshal(configMap) //解析该map获取data
	if err != nil {
		return err
	}
	s := strings.ReplaceAll(string(data), "'", "") //删除字符串中单引号
	s = strings.ReplaceAll(s, "\"", "")            //删除字符串中双引号
	err = yaml.Unmarshal([]byte(s), r.config)      //解析获取的byte数组赋值给config//s:=r.Marshal(reflect.ValueOf(configMap))
	if err != nil {
		return err
	}
	return nil
}

func (r *Provide) checkSlice(k string, value interface{}, v string) interface{} {
	if r.tags[k] {
		value = strings.Split(v, ",")
	} else {
		value = v
	}
	return value
}
func FromYaml(config interface{}, configPath string) error {
	data, err := ioutil.ReadFile(configPath)
	if err != nil {
		return err
	}
	err = yaml.Unmarshal(data, config)
	if err != nil {
		return err
	}
	return nil
}

func FromEnv(config interface{}, ignoreCase bool, prefix string) error {
	provide := New(config, ignoreCase, prefix)
	provide.ProvideByEnv()
	return provide.unmarshal()
}

func FromFlag(config interface{}, ignoreCase bool, prefix string, keyAndValue []string) error {
	provide := New(config, ignoreCase, prefix)
	provide.ProvideByFlag(keyAndValue)
	return provide.unmarshal()
}
