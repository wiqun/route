package cluster

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/weaveworks/mesh"
	"github.com/wiqun/route/internal/common"
	"github.com/wiqun/route/internal/config"
	. "github.com/wiqun/route/internal/crdt"
	. "github.com/wiqun/route/internal/log"
	"github.com/wiqun/route/internal/message"
	"net"
	"strconv"
	"sync"
	"time"
)

//此类主要处理集群数据的同步问题
type Swarm interface {
	common.Runnable

	JsonSnapshot() []byte
}

type swarm struct {
	name           mesh.PeerName
	log            Logger
	state          *CrdtState
	router         *mesh.Router
	gossip         mesh.Gossip
	localNotifier  message.LocalMsgNotifier
	remoteReceiver message.RemoteMsgChanReceiver
	config         *config.ClusterConfig
	localIpv4      string //不含端口
	peers          map[string]struct{}
}

func NewSwarm(logFactoryer LogFactoryer, config *config.ClusterConfig, localNotifier message.LocalMsgNotifier, remoteReceiver message.RemoteMsgChanReceiver) Swarm {

	log := logFactoryer.CreateLogger("swarm")

	name, err := resolvePeerName(config.NodeName)
	if err != nil {
		log.Fatalf("获取硬件地址失败   %s : %v", config.NodeName, err)
	}

	host, portStr, err := net.SplitHostPort(config.ListenAddr)
	if err != nil {
		log.Fatalf("解析监听地址失败 %s : %v", config.ListenAddr, err)
	}

	localIpv4, err := getLocalIP()
	log.Println("本机ip: ", localIpv4)
	if err != nil {
		log.Fatalf("getLocalIPv4失败  %v", err)
	}
	//是否删除端口号,让peer自动查找端口? 问题请看: https://github.com/prometheus/alertmanager/issues/879#issuecomment-362553627
	nickName := net.JoinHostPort(localIpv4, portStr)

	port, err := strconv.Atoi(portStr)
	if err != nil {
		log.Fatalf("解析端口错误")
	}
	log.Println("集群端口:", port)
	swarm := &swarm{
		log:            log,
		state:          NewCrdtState(),
		name:           name,
		localNotifier:  localNotifier,
		remoteReceiver: remoteReceiver,
		config:         config,
		localIpv4:      localIpv4,
		peers:          map[string]struct{}{},
	}

	router, err := mesh.NewRouter(mesh.Config{
		Host:               host,
		Port:               port,
		ProtocolMinVersion: mesh.ProtocolMaxVersion,
		ConnLimit:          64,
		GossipInterval:     &config.GossipInterval,
		PeerDiscovery:      true,
		TrustedSubnets:     []*net.IPNet{},
	}, name, nickName, mesh.NullOverlay{}, log)
	if err != nil {
		log.Fatalln(err)
	}

	gossip, err := router.NewGossip("swarm", swarm)
	if err != nil {
		log.Fatalln(err)
	}
	swarm.gossip = gossip
	swarm.router = router
	swarm.router.Peers.OnGC(swarm.peerGc)

	return swarm
}

func (s *swarm) JsonSnapshot() []byte {
	snapshot, err := json.Marshal(s.state)
	if err != nil {
		s.log.PrintfError("JsonSnapshot 序列化为json出错:", err)
		return nil
	} else {
		return snapshot
	}
}

func (s *swarm) Run(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()

	wg.Add(1)
	go s.listen(ctx, wg)

	wg.Add(1)
	go s.processLocalBroadcast(ctx, wg)

	wg.Add(1)
	go s.processStateGc(ctx, wg)
}

func (s *swarm) listen(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()

	s.router.Start()

	errs := s.join(s.config.Seed...)
	if len(errs) != 0 {
		s.log.Fatalln("listen: 尝试加入集群错误")
	}

	wg.Add(1)
	go s.processJoin(ctx, wg)

	<-ctx.Done()
	s.log.Println("listen收到取消事件,退出协程!")

	err := s.router.Stop()
	if err != nil {
		s.log.PrintfError("router Stop错误 :%v", err)
	}
}

func (s *swarm) processStateGc(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()

	timer := time.NewTicker(s.config.GcInterval)

	for {
		select {
		case <-ctx.Done():
			s.log.Println("processStateGc收到取消事件,退出协程!")
			timer.Stop()
			return
		case <-timer.C:
			s.log.Println("processStateGc: StateGcing.....")
			s.state.Gc(s.config.DelTimeExpired)
		}
	}
}

//定期检查集群里的其他节点是否需要尝试重新连接
func (s *swarm) processJoin(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()

	timer := time.NewTicker(s.config.GossipCheckInterval)

	for {
		select {
		case <-ctx.Done():
			s.log.Println("processJoin收到取消事件,退出协程!")
			timer.Stop()
			return
		case <-timer.C:

			s.join(s.config.Seed...)

			//desc := s.router.Peers.Descriptions()
			//for _, peer := range desc {
			//	if peer.Self {
			//		continue
			//	}
			//	if peer.NumConnections < (len(desc) - 1) {
			//		s.log.Println("processJoin 发现需要加入的节点:", peer.NickName)
			//		s.join(peer.NickName)
			//	}
			//}

		}
	}

}

//处理从本地来的Sub/Unsub消息
func (s *swarm) processLocalBroadcast(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()
	myselfPeerName := uint64(s.name)
	for {
		select {
		case <-ctx.Done():
			s.log.Println("processLocalBroadcast收到取消事件,退出协程!")
			return
		case batchOp := <-s.remoteReceiver.RemoteBatchTopicOpChan():
			if len(batchOp.Topic) == 0 {
				continue
			}
			if batchOp.IsSubOp {
				delta := s.state.BatchAdd(myselfPeerName, batchOp.Topic)
				s.gossip.GossipBroadcast(NewCrdtStateWithOther(delta))
			} else {
				delta := s.state.BatchDel(myselfPeerName, batchOp.Topic)
				s.gossip.GossipBroadcast(NewCrdtStateWithOther(delta))
			}
		}
	}
}

//某个节点断线或者挂掉了会触发此函数
func (s *swarm) peerGc(peer *mesh.Peer) {
	s.log.Println("peerGcing...")

	//todo 是否需要广播给其他节点?
	delList := s.state.DelPeerAllTopic(uint64(peer.Name))
	if len(delList) == 0 {
		return
	}

	s.localNotifier.NotifierUnsubscribe(&message.LocalBatchUnsubMsg{SubscriberId: peer.Name.String(), Topic: delList})

}

//加入集群
func (s *swarm) join(peers ...string) []error {
	var addrs []string
	for _, h := range peers {
		ips, err := net.LookupHost(h)
		if err == nil {
			for _, addr := range ips {
				if addr != s.localIpv4 {
					addrs = append(addrs, addr)
				}
			}
			continue
		}

		_, _, err = net.SplitHostPort(h)
		if err == nil {
			if h != s.localIpv4 {
				addrs = append(addrs, h)
			}
			continue
		}
	}

	if len(addrs) == 0 {
		s.log.PrintlnDebug("join: 可加入集群ip为空")
		return nil
	}

	s.log.PrintlnDebug("try joining:", addrs)

	errs := s.router.ConnectionMaker.InitiateConnections(addrs, false)
	if len(errs) != 0 {
		s.log.PrintfError("join 调用InitiateConnections错误: %v", errs)
	}

	newPeers := map[string]struct{}{}
	for _, addr := range addrs {
		newPeers[addr] = struct{}{}
	}

	var delPeers []string
	for peer := range s.peers {
		_, found := newPeers[peer]
		if !found {
			delPeers = append(delPeers, peer)
		}
	}

	s.peers = newPeers
	if len(delPeers) != 0 {
		//当addrs记录减少时需要忘记已存在的连接
		s.log.PrintfDebug("忘记Peer: %v", delPeers)
		s.router.ConnectionMaker.ForgetConnections(delPeers)
	}

	return errs
}

//只用于pub消息
func (s *swarm) OnGossipUnicast(src mesh.PeerName, msg []byte) error {

	m := &message.ServerMessage{}
	err := m.Unmarshal(msg)
	if err != nil {
		s.log.PrintfError("OnGossipUnicast PubMessage反序列化失败: %v", err)
		return err
	}
	if m.Type != message.ServerType_MessagesType {
		s.log.PrintfError("OnGossipUnicast 不支持的类型: %s", message.ServerType_name[int32(m.Type)])
		return err
	}

	s.localNotifier.NotifierRemotePubMessage(m.Messages)

	return nil
}

//只用于sub/unsub消息
func (s *swarm) OnGossipBroadcast(src mesh.PeerName, update []byte) (mesh.GossipData, error) {
	if src == s.name {
		return nil, nil
	}
	return s.merge(update)
}

func (s *swarm) Gossip() (complete mesh.GossipData) {
	return s.state
}

func (s *swarm) OnGossip(msg []byte) (mesh.GossipData, error) {
	return s.merge(msg)
}

//在断线到重连的过程中有其他节点可能已经触发了onPeerGc方法导致本节点自己的订阅被强制覆盖位del状态
//因此要利用自己的节点是一定的最新的信息,
//将自己的订阅强制修正位Add状态(例如某个订阅xTopic,在本地是Add状态但是远程状态是Del,则强制把时间戳更新位最新,并且设置标志位位Add)并广播给其他节点
func (s *swarm) merge(msg []byte) (mesh.GossipData, error) {
	other, err := DecodeCrdtState(msg)
	if err != nil {
		return nil, err
	}

	updateCount, myselfDelta := s.state.MergeDelta(other, uint64(s.name), s.config.DelTimeExpired)

	if myselfDelta != nil {
		s.gossip.GossipBroadcast(NewCrdtStateWithOther(myselfDelta))
	}

	if updateCount == 0 {
		return nil, nil
	}

	s.emitLocalSubscriptions(other)

	return other, nil
}

//从变化的内容来触发相关事件
func (s *swarm) emitLocalSubscriptions(other *CrdtState) {
	other.ForEach(func(peerName uint64, topicMap *TopicMap) {
		if peerName == uint64(s.name) {
			return
		}
		if len(topicMap.Map) == 0 {
			return
		}
		peer := newPeer(mesh.PeerName(peerName), s.gossip)
		var subList []string
		var unSubList []string

		for topic, value := range topicMap.Map {
			if FlagValue(value).IsAdd() {
				subList = append(subList, topic)
			} else {
				unSubList = append(unSubList, topic)
			}
		}
		if len(subList) != 0 {
			s.localNotifier.NotifierSubscribe(&message.LocalBatchSubMsg{Subscriber: peer, Topic: subList})
		}
		if len(unSubList) != 0 {
			s.localNotifier.NotifierUnsubscribe(&message.LocalBatchUnsubMsg{SubscriberId: peer.ID(), Topic: unSubList})
		}
	})
}

func resolvePeerName(nodeName string) (mesh.PeerName, error) {
	if nodeName != "" {
		return mesh.PeerNameFromString(nodeName)
	} else {
		return mesh.PeerNameFromString(mustHardwareAddr())
	}
}

func mustHardwareAddr() string {
	ifaces, err := net.Interfaces()
	if err != nil {
		panic(err)
	}
	for _, iface := range ifaces {
		if s := iface.HardwareAddr.String(); s != "" {
			return s
		}
	}
	panic("no valid network interfaces")
}

func getLocalIP() (string, error) {
	ifaces, err := net.Interfaces()
	if err != nil {
		return "", err
	}
	for _, iface := range ifaces {
		if iface.Flags&net.FlagUp == 0 {
			continue // interface down
		}
		if iface.Flags&net.FlagLoopback != 0 {
			continue // loopback interface
		}
		addrs, err := iface.Addrs()
		if err != nil {
			return "", err
		}
		for _, addr := range addrs {
			var ip net.IP
			switch v := addr.(type) {
			case *net.IPNet:
				ip = v.IP
			case *net.IPAddr:
				ip = v.IP
			}
			if ip == nil || ip.IsLoopback() {
				continue
			}
			ip = ip.To4()
			if ip == nil {
				continue // not an ipv4 address
			}
			return ip.String(), nil
		}
	}
	return "", errors.New("获取ipv4地址失败")
}
