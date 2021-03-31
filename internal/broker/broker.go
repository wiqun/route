package broker

import (
	"context"
	"github.com/gorilla/websocket"
	"github.com/wiqun/route/internal/cluster"
	"github.com/wiqun/route/internal/common"
	"github.com/wiqun/route/internal/config"
	"github.com/wiqun/route/internal/generate"
	"github.com/wiqun/route/internal/log"
	"github.com/wiqun/route/internal/message"
	"net/http"
	"net/http/pprof"
	"sync"
)

//此类主要功能是实现对客户端的连接的处理,将各种请求转发到Service,
type Broker interface {
	common.Runnable
}

type broker struct {
	config           *config.BrokerConfig
	log              log.Logger
	localNotifier    message.LocalMsgNotifier
	localIdGenerator generate.LocalIdGenerator
	swarm            cluster.Swarm
}

var upgrader = websocket.Upgrader{}

func NewBroker(config *config.BrokerConfig, logFactoryer log.LogFactoryer,
	localNotifier message.LocalMsgNotifier, localIdGenerator generate.LocalIdGenerator, swarm cluster.Swarm) Broker {
	l := logFactoryer.CreateLogger("broker")

	b := &broker{
		config:           config,
		log:              l,
		localNotifier:    localNotifier,
		localIdGenerator: localIdGenerator,
		swarm:            swarm,
	}

	return b
}

func (b *broker) Run(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()

	s := http.NewServeMux()

	s.HandleFunc("/", b.connect)

	if b.config.Pprof {
		s.HandleFunc("/debug/pprof/", pprof.Index)
		s.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
		s.HandleFunc("/debug/pprof/profile", pprof.Profile)
		s.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
		s.HandleFunc("/debug/pprof/trace", pprof.Trace)
	}

	if b.config.ClusterSnapshot {
		s.HandleFunc("/debug/cluster/snapshot/", b.clusterSnapshot)
	}

	go func() {
		b.log.Println("运行端口: ", b.config.ListenAddr)
		err := http.ListenAndServe(b.config.ListenAddr, s)
		if err != nil {
			panic(err)
		}
	}()

}

func (b *broker) clusterSnapshot(writer http.ResponseWriter, r *http.Request) {
	writer.Header().Set("Content-Type", "application/json; charset=utf-8")
	json := b.swarm.JsonSnapshot()
	writer.Write(json)
}

func (b *broker) connect(writer http.ResponseWriter, request *http.Request) {
	c, err := upgrader.Upgrade(writer, request, nil)
	if err != nil {
		b.log.Printf("upgrade: path: %s err:%v", request.RequestURI, err)
		return
	}

	b.log.PrintlnDebug("有新的连接")

	conn := b.newConn(c)
	go conn.process()

}
