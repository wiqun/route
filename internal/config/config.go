package config

import (
	"fmt"
	"os"
	"time"
)

type Config struct {
	Cluster ClusterConfig `yaml:"cluster"`

	Broker BrokerConfig `yaml:"broker"`

	Chan ChanConfig `yaml:"chan"`

	Service ServiceConfig `yaml:"service"`

	//支持: Trace,Debug,Information,Warning,Error,Critical,None
	LogLevel string `yaml:"logLevel"`
}

type BrokerConfig struct {
	//websocket监听的端口
	//格式:
	// 127.0.0.1:4000
	// :4000
	//4000
	ListenAddr string `yaml:"listenAddr"`

	///写入超时时间
	WriteTimeout time.Duration `yaml:"writeTimeout"`

	//是否启用pprof
	//url: ListenAddr/debug/pprof
	Pprof bool `yaml:"pprof"`

	//是否启用获取集群状态快照
	//url: ListenAddr/debug/cluster/snapshot
	//警告! 生产环境最好不要启用,因为在集群数据量比较大的时候获取快照会比较耗时
	ClusterSnapshot bool `yaml:"clusterSnapshot"`
}

type ServiceConfig struct {
	//每个核心分配的io线程数
	//默认为1
	IOGoCoroutineCoreNums int `yaml:"ioGoCoroutineCoreNums"`

	//内部io管道的大小
	//默认为2000
	IOChanSize int `yaml:"ioGoCoroutineNums"`
}

//集群参数配置
type ClusterConfig struct {

	//本节点的名称,在集群必需唯一,默认为硬件地址
	NodeName string `yaml:"nodeName"`

	//本节点监听的端口
	//格式:
	// 127.0.0.1:4001
	// :4001
	//默认为4001端口
	ListenAddr string `yaml:"listenAddr"`

	//gossip回调的间隔
	//默认为30s
	GossipInterval time.Duration `yaml:"gossipInterval"`

	//Crdt状态回收检查间隔
	//默认为1h
	GcInterval time.Duration `yaml:"gcInterval"`

	//检查其他节点是否需要连接的间隔
	//默认为15s
	GossipCheckInterval time.Duration `yaml:"gossipCheckInterval"`

	//delTime的过期时长
	//默认为1d
	DelTimeExpired time.Duration `yaml:"delTimeExpired"`

	//初始的集群地址
	Seed []string `yaml:"seed"`
}

//管道参数配置
//有Core后缀的最后都要乘与核心的数量
type ChanConfig struct {
	//Local 对应着LocalMsgNotifier
	LocalBatchTopicOpChanCoreSize int `yaml:"localBatchTopicOpChanCoreSize"`
	QueryRequestChanCoreSize      int `yaml:"queryRequestChanCoreSize"`
	LocalPubMessageChanCoreSize   int `yaml:"localPubMessageChanCoreSize"`
	RemotePubMessageChanCoreSize  int `yaml:"remotePubMessageChanCoreSize"`

	//Remote 对应着RemoteMsgNotifier
	RemoteBatchTopicOpChanCoreSize int `yaml:"remoteBatchTopicOpChanCoreSize"`
}

func NewConfig(configPath string, keyAndValue []string) (*Config, *ClusterConfig, *BrokerConfig, *ChanConfig, *ServiceConfig) {
	c := newDefaultConfig()
	err := FromYaml(c, configPath)

	if err != nil {
		if _, ok := err.(*os.PathError); ok {
			fmt.Fprintln(os.Stderr, "yaml配置文件找不到 path:", configPath, err)
		} else {
			panic(err)
		}
	}
	err = FromEnv(c, true, "route")
	if err != nil {
		panic(err)
	}

	err = FromFlag(c, true, "route", keyAndValue)
	if err != nil {
		panic(err)
	}
	return c, &c.Cluster, &c.Broker, &c.Chan, &c.Service
}

func newDefaultConfig() *Config {

	c := &Config{
		Cluster: ClusterConfig{
			ListenAddr:          ":4001",
			GossipInterval:      time.Second * 30,
			GossipCheckInterval: time.Second * 15,
			DelTimeExpired:      time.Hour * 24,
			GcInterval:          time.Hour,
		},
		Broker: BrokerConfig{
			ListenAddr:      ":4000",
			Pprof:           true,
			ClusterSnapshot: false,
			WriteTimeout:    time.Second * 10,
		},
		Service: ServiceConfig{
			IOChanSize:            2000,
			IOGoCoroutineCoreNums: 1,
		},
		Chan: ChanConfig{
			LocalBatchTopicOpChanCoreSize: 1000,
			QueryRequestChanCoreSize:      2000,
			LocalPubMessageChanCoreSize:   4000, //推送消息管道大一点
			RemotePubMessageChanCoreSize:  4000, //推送消息管道大一点

			RemoteBatchTopicOpChanCoreSize: 1000,
		},
		LogLevel: "Information",
	}

	return c
}
