### 简介
Route是一个高性能的发布订阅中间件
- 去中心化(任何节点都是对等的)
- 弹性伸缩,高可用和分区容错性(AP),最终一致性
- 高订阅数,高性能(见下面性能测试)
- 支持批量化操作
- ...

### 什么地方适合使用Route?
Route适合在需要高性能高订阅数,但是又允许消息丢失的场景下使用。(route不保证消息一定送达)

### 入门
#### 源码运行
```
$ go run main.go
```
#### docker运行
```
$ docker run -d --name route -p 4000:4000 --restart=unless-stopped routeio/route
```
#### k8s部署集群
集群将会默认暴露出30400端口用于访问,如果不需要此端口则需要自行修改[k8s.yaml](k8s.yaml)文件
```
$ kubectl.exe apply -f .\k8s.yaml
```

### 使用例子
假设route服务器监听的端口为4000。

和route服务器通信需要使用[route-go](https://github.com/wiqun/route-go)SDK(暂时还没有其他语言的SDK)。
```shell
$ go get -u github.com/wiqun/route-go
```
```go
import . "github.com/wiqun/route-go"
func main() {
	c, err := NewClient(NewDefaultOption("ws://127.0.0.1:4000"), nil)
	if err != nil {
		panic(err)
	}

	//消息回调
	c.SetOnMessageHandler(func(messages []*message.Message) {
		for _, m := range messages {
			fmt.Println(string(m.Payload))
		}
	})

	//连接
	err = c.Connect()
	if err != nil {
		panic(err)
	}

	//订阅主题
	err = c.BatchSubscribe([]string{"hello"})
	if err != nil {
		panic(err)
	}
	time.Sleep(time.Second*2)


	//发布消息
	c.BatchPublish([]*message.PubRequest{{Topic: "hello", Payload: []byte("hello world")}})

	time.Sleep(time.Second*10)
}
//hello world
```
更多例子请看[route-go example](https://github.com/wiqun/route-go/tree/master/example)目录

### 配置选项
支持从环境变量,命令行参数,配置文件进行配置



配置文件例子:
```yaml
broker:
  listenAddr: :4000
cluster:
  gossipCheckInterval: 1m
  gossipInterval: 10s
  listenAddr: :4001
  nodeName: "00:00:00:00:00:01"
  seed: ["example.com"]
logLevel: Debug
```

命令行例子:
```
$ go run main.go -e route_broker_listenAddr=:4002 -e route_logLevel=Debug
```

重要的配置选项:

|  配置名   |默认值| 说明 |   
|  ----  | ----  | ----  |
| route_broker_listenAddr  | 4000 | 服务监听的端口 | 
| route_broker_writeTimeout  | 10s | 写入超时时间 | 
| route_broker_pprof  | false | 是否启用pprof | 
| route_chan_clusterSnapshot  | false | 是否启用获取集群状态快照Api | 
| route_logLevel  | Information | 日志级别(Trace,Debug,Information,Warning,Error,Critical,None) | 
| route_cluster_nodeName  |硬件地址| 本节点的名称,在集群必需唯一 | 
| route_cluster_listenAddr  |4001| 集群端口,格式:127.0.0.1:4001或者:4001 | 
| route_cluster_gossipInterval  | 30s | gossip回调的间隔 | 
| route_cluster_gcIntervall  | 1h | Crdt状态回收检查间隔 | 
| route_cluster_gossipCheckInterval  | 15s | 检查其他节点是否需要连接的间隔 | 
| route_cluster_delTimeExpired  | 1d | delTime的过期时长 | 
| route_cluster_seed  | 无 | ip或者域名,用于与其他节点进行连接 

<br>



### 性能测试

- Intel(R) Xeon(R) X5650
- cpu限制为1核
- 打压机与被打压机处于不同网段(数据传输需要经过网关)  
- 每条连接定时发布一条消息到对应的主题并且持续压测10分钟

|  连接数(等同于订阅数)   | TPS | CPU使用率  | 平均耗时(受网关的性能影响)  |
|  ----  | ----  |----  |----  |
| 3000  | 1W1+ | 60~70% | 100ms+ |
| 10000  | 1W+ | 70+% | 300ms+ |

如果切换为批量操作的模式,性能将会显著提升。

这里CPU使用率不能达到100%可能是由于网关的的处理速度达不到要求导致的。

压测例子请见: [route-go](https://github.com/wiqun/route-go/tree/master/example/pressure_test)


### 为什么这么快?
1. 只专注于高性能发布订阅场景,舍弃了一定的消息可靠性(万有一失的设计理念)
2. 1+N的协程模式(1个主协程+N个IO协程)。主协程负责所有订阅树操作,从而避免引入锁(锁带来的性能问题)。
3. 批量化操作。批量化能减少IO的次数,也能提高带宽的利用效率


### 设计文档
[设计文档](docs/设计文档.md)


