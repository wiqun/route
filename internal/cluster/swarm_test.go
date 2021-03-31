package cluster

import (
	"context"
	"github.com/stretchr/testify/assert"
	"github.com/weaveworks/mesh"
	"route/internal/config"
	"route/internal/crdt"
	"route/internal/log"
	"route/internal/message"
	"strconv"
	"sync"
	"testing"
	"time"
)

var myselfPeerName = uint64(1)
var otherPeerName = uint64(2)

type gossipMock struct {
	unicastOut map[mesh.PeerName][][]*message.Message
	out        []*crdt.CrdtState
}

func newGossipMock() *gossipMock {
	return &gossipMock{
		unicastOut: map[mesh.PeerName][][]*message.Message{},
	}
}

func (g *gossipMock) GossipUnicast(dst mesh.PeerName, msg []byte) error {
	b := &message.ServerMessage{}
	err := b.Unmarshal(msg)
	if err != nil {
		panic(err)
	}
	g.unicastOut[dst] = append(g.unicastOut[dst], b.Messages)
	return nil
}

func (g *gossipMock) GossipBroadcast(update mesh.GossipData) {
	g.out = append(g.out, update.(*crdt.CrdtState))
}

func (g *gossipMock) GossipNeighbourSubset(update mesh.GossipData) {
	g.out = append(g.out, update.(*crdt.CrdtState))
}

func newSwarm(t *testing.T) (*swarm, message.LocalMsgChanReceiver, message.RemoteMsgNotifier, *gossipMock) {
	c := &config.ChanConfig{
		LocalBatchTopicOpChanCoreSize:  100,
		QueryRequestChanCoreSize:       100,
		LocalPubMessageChanCoreSize:    100,
		RemotePubMessageChanCoreSize:   100,
		RemoteBatchTopicOpChanCoreSize: 100,
	}

	n, r := message.NewLocalMsgNotifierChanReceiver(c)
	rn, rr := message.NewRemoteMsgNotifierChanReceiver(c)

	factory := log.NewLogFactoryerMock(t)

	g := newGossipMock()

	s := &swarm{
		name:           mesh.PeerName(myselfPeerName),
		log:            factory.CreateLogger("swarm"),
		state:          crdt.NewCrdtState(),
		localNotifier:  n,
		remoteReceiver: rr,
		gossip:         g,
		config: &config.ClusterConfig{
			GossipInterval:      time.Minute * 10,
			GossipCheckInterval: time.Minute * 10,
			DelTimeExpired:      time.Hour,
		},
	}
	return s, r, rn, g
}

func newFlagValue(time int64, isAdd bool) int64 {
	if isAdd {
		return (time << 1) | 0x01
	} else {
		return time << 1
	}
}

func toSliceTopicOp(ch <-chan message.LocalBatchTopicOp) []message.LocalBatchTopicOp {

	r := make([]message.LocalBatchTopicOp, 0, len(ch))
	for {
		select {
		case i := <-ch:
			r = append(r, i)
		default:
			return r
		}
	}
}

func toSlice(ch <-chan []*message.Message) [][]*message.Message {
	var r [][]*message.Message
	for len(ch) != 0 {
		r = append(r, <-ch)
	}
	return r
}

func Test_swarm_processLocalBroadcast(t *testing.T) {
	t.Run("订阅", func(t *testing.T) {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		wg := &sync.WaitGroup{}
		wg.Add(1)

		s, _, rn, g := newSwarm(t)

		go s.processLocalBroadcast(ctx, wg)

		topicList := []string{"topic1", "topic2"}

		rn.NotifierSubscribe(topicList)

		time.Sleep(time.Millisecond * 100)

		assert.Len(t, g.out, 1)
		assert.Len(t, g.out[0].State.Set, 1)
		assert.Len(t, g.out[0].State.Set[myselfPeerName].Map, 2)
		assert.True(t, crdt.FlagValue(g.out[0].State.Set[myselfPeerName].Map[topicList[0]]).IsAdd())
		assert.True(t, crdt.FlagValue(g.out[0].State.Set[myselfPeerName].Map[topicList[1]]).IsAdd())
	})
	t.Run("取消订阅", func(t *testing.T) {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		wg := &sync.WaitGroup{}
		wg.Add(1)

		s, _, rn, g := newSwarm(t)
		go s.processLocalBroadcast(ctx, wg)

		topicList := []string{"topic1", "topic2"}

		rn.NotifierUnsubscribe(topicList)

		time.Sleep(time.Millisecond * 100)

		assert.Len(t, g.out, 1)
		assert.Len(t, g.out[0].State.Set, 1)
		assert.Len(t, g.out[0].State.Set[myselfPeerName].Map, 2)
		assert.True(t, crdt.FlagValue(g.out[0].State.Set[myselfPeerName].Map[topicList[0]]).IsRemove())
		assert.True(t, crdt.FlagValue(g.out[0].State.Set[myselfPeerName].Map[topicList[1]]).IsRemove())

	})
}

func Test_swarm_OnGossipUnicast(t *testing.T) {

	t.Run("发布多个", func(t *testing.T) {
		s, r, _, g := newSwarm(t)

		pubMsg := &message.ServerMessage{
			Type: message.ServerType_MessagesType,
			Messages: []*message.Message{
				{Topic: "topic1", Payload: []byte{1}},
				{Topic: "topic2", Payload: []byte{2}},
			},
		}

		date, err := pubMsg.Marshal()

		assert.NoError(t, err)
		err = s.OnGossipUnicast(mesh.PeerName(otherPeerName), date)
		assert.NoError(t, err)

		slice := toSlice(r.RemotePubMessageChan())
		assert.Len(t, g.out, 0)
		assert.Len(t, g.unicastOut, 0)
		assert.Len(t, slice, 1)
		assert.Len(t, slice[0], 2)
		assert.EqualValues(t, slice[0][0].Topic, "topic1")
		assert.EqualValues(t, slice[0][1].Topic, "topic2")
	})
}

func Test_swarm_merge(t *testing.T) {

	t.Run("订阅及取消订阅(自己的订阅信息)  (无,ADD) (无,Del) (无,Del)无效时间", func(t *testing.T) {
		s, r, _, g := newSwarm(t)
		s.config.DelTimeExpired = time.Second * 10

		topicList := []string{"topic1", "topic2", "topic3", "topic4"}

		now := (time.Now().Unix() - 2) * 1000
		expiredNow := (time.Now().Unix() - 100) * 1000

		pubMsg := &crdt.State{
			Set: map[uint64]*crdt.TopicMap{
				myselfPeerName: {Map: map[string]int64{
					topicList[0]: newFlagValue(now, true),
					topicList[1]: newFlagValue(expiredNow, true),
					topicList[2]: newFlagValue(now, false),
					topicList[3]: newFlagValue(expiredNow, false),
				}},
			},
		}

		date, err := pubMsg.Marshal()
		assert.NoError(t, err)

		d, err := s.OnGossip(date)
		assert.NoError(t, err)
		assert.Nil(t, d)

		topicOpSlice := toSliceTopicOp(r.LocalBatchTopicOpChan())
		assert.Len(t, topicOpSlice, 0)
		assert.Len(t, s.state.State.Set, 1)
		assert.Len(t, s.state.State.Set[myselfPeerName].Map, 3)
		assert.True(t, crdt.FlagValue(s.state.State.Set[myselfPeerName].Map[topicList[0]]).IsRemove())
		assert.True(t, crdt.FlagValue(s.state.State.Set[myselfPeerName].Map[topicList[0]]).Time() != now)
		assert.True(t, crdt.FlagValue(s.state.State.Set[myselfPeerName].Map[topicList[1]]).IsRemove())
		assert.True(t, crdt.FlagValue(s.state.State.Set[myselfPeerName].Map[topicList[1]]).Time() != now)
		assert.True(t, crdt.FlagValue(s.state.State.Set[myselfPeerName].Map[topicList[2]]).IsRemove())
		assert.True(t, crdt.FlagValue(s.state.State.Set[myselfPeerName].Map[topicList[2]]).Time() == now)

		assert.Len(t, g.out, 1)
		assert.Len(t, g.out[0].State.Set, 1)
		assert.Len(t, g.out[0].State.Set[myselfPeerName].Map, 3)
		assert.True(t, crdt.FlagValue(g.out[0].State.Set[myselfPeerName].Map[topicList[0]]).IsRemove())
		assert.True(t, crdt.FlagValue(g.out[0].State.Set[myselfPeerName].Map[topicList[0]]).Time() != now)
		assert.True(t, crdt.FlagValue(g.out[0].State.Set[myselfPeerName].Map[topicList[1]]).IsRemove())
		assert.True(t, crdt.FlagValue(g.out[0].State.Set[myselfPeerName].Map[topicList[1]]).Time() != now)
		assert.True(t, crdt.FlagValue(g.out[0].State.Set[myselfPeerName].Map[topicList[2]]).IsRemove())
		assert.True(t, crdt.FlagValue(g.out[0].State.Set[myselfPeerName].Map[topicList[2]]).Time() == now)
	})

	t.Run("订阅及取消订阅(自己的订阅信息)  (ADD,ADD) (ADD,Del)", func(t *testing.T) {
		s, r, _, g := newSwarm(t)
		s.config.DelTimeExpired = time.Second * 10

		topicList := make([]string, 0, 100)
		for i := 1; i < 100; i++ {
			topicList = append(topicList, "topic"+strconv.Itoa(i))
		}

		now := (time.Now().Unix() - 2) * 1000

		s.state.State.Set = map[uint64]*crdt.TopicMap{
			myselfPeerName: {Map: map[string]int64{
				topicList[0]: newFlagValue(now, true),
				topicList[1]: newFlagValue(now, true),
				topicList[2]: newFlagValue(now, true),

				topicList[3]: newFlagValue(now, true),
				topicList[4]: newFlagValue(now, true),
				topicList[5]: newFlagValue(now, true),
			}},
		}

		pubMsg := &crdt.State{
			Set: map[uint64]*crdt.TopicMap{
				myselfPeerName: {Map: map[string]int64{
					topicList[0]: newFlagValue(now+1, true),
					topicList[1]: newFlagValue(now, true),
					topicList[2]: newFlagValue(now-1, true),

					topicList[3]: newFlagValue(now+1, false),
					topicList[4]: newFlagValue(now, false),
					topicList[5]: newFlagValue(now-1, false),
				}},
			},
		}

		date, err := pubMsg.Marshal()
		assert.NoError(t, err)

		d, err := s.OnGossip(date)
		assert.NoError(t, err)
		assert.Nil(t, d)

		topicOpSlice := toSliceTopicOp(r.LocalBatchTopicOpChan())
		assert.Len(t, topicOpSlice, 0)
		assert.Len(t, s.state.State.Set, 1)
		assert.Len(t, s.state.State.Set[myselfPeerName].Map, 6)
		assert.True(t, crdt.FlagValue(s.state.State.Set[myselfPeerName].Map[topicList[0]]).IsAdd())
		assert.True(t, crdt.FlagValue(s.state.State.Set[myselfPeerName].Map[topicList[0]]).Time() == now)
		assert.True(t, crdt.FlagValue(s.state.State.Set[myselfPeerName].Map[topicList[1]]).IsAdd())
		assert.True(t, crdt.FlagValue(s.state.State.Set[myselfPeerName].Map[topicList[1]]).Time() == now)
		assert.True(t, crdt.FlagValue(s.state.State.Set[myselfPeerName].Map[topicList[2]]).IsAdd())
		assert.True(t, crdt.FlagValue(s.state.State.Set[myselfPeerName].Map[topicList[2]]).Time() == now)

		assert.True(t, crdt.FlagValue(s.state.State.Set[myselfPeerName].Map[topicList[3]]).IsAdd())
		assert.True(t, crdt.FlagValue(s.state.State.Set[myselfPeerName].Map[topicList[3]]).Time() > now+1)
		assert.True(t, crdt.FlagValue(s.state.State.Set[myselfPeerName].Map[topicList[4]]).IsAdd())
		assert.True(t, crdt.FlagValue(s.state.State.Set[myselfPeerName].Map[topicList[4]]).Time() > now)
		assert.True(t, crdt.FlagValue(s.state.State.Set[myselfPeerName].Map[topicList[5]]).IsAdd())
		assert.True(t, crdt.FlagValue(s.state.State.Set[myselfPeerName].Map[topicList[5]]).Time() == now)

		assert.Len(t, g.out, 1)
		assert.Len(t, g.out[0].State.Set, 1)
		assert.Len(t, g.out[0].State.Set[myselfPeerName].Map, 2)
		assert.True(t, crdt.FlagValue(g.out[0].State.Set[myselfPeerName].Map[topicList[3]]).IsAdd())
		assert.True(t, crdt.FlagValue(g.out[0].State.Set[myselfPeerName].Map[topicList[3]]).Time() > now+1)
		assert.True(t, crdt.FlagValue(g.out[0].State.Set[myselfPeerName].Map[topicList[3]]).IsAdd())
		assert.True(t, crdt.FlagValue(g.out[0].State.Set[myselfPeerName].Map[topicList[4]]).Time() > now)
	})

	t.Run("订阅及取消订阅(自己的订阅信息)  (Del,Add) (Del,Del)", func(t *testing.T) {
		s, r, _, g := newSwarm(t)
		s.config.DelTimeExpired = time.Second * 10

		topicList := make([]string, 0, 100)
		for i := 1; i < 100; i++ {
			topicList = append(topicList, "topic"+strconv.Itoa(i))
		}

		now := (time.Now().Unix() - 2) * 1000

		s.state.State.Set = map[uint64]*crdt.TopicMap{
			myselfPeerName: {Map: map[string]int64{
				topicList[0]: newFlagValue(now, false),
				topicList[1]: newFlagValue(now, false),
				topicList[2]: newFlagValue(now, false),

				topicList[3]: newFlagValue(now, false),
				topicList[4]: newFlagValue(now, false),
				topicList[5]: newFlagValue(now, false),
			}},
		}

		pubMsg := &crdt.State{
			Set: map[uint64]*crdt.TopicMap{
				myselfPeerName: {Map: map[string]int64{
					topicList[0]: newFlagValue(now+1, true),
					topicList[1]: newFlagValue(now, true),
					topicList[2]: newFlagValue(now-1, true),

					topicList[3]: newFlagValue(now+1, false),
					topicList[4]: newFlagValue(now, false),
					topicList[5]: newFlagValue(now-1, false),
				}},
			},
		}

		date, err := pubMsg.Marshal()
		assert.NoError(t, err)

		d, err := s.OnGossip(date)
		assert.NoError(t, err)
		assert.Nil(t, d)

		topicOpSlice := toSliceTopicOp(r.LocalBatchTopicOpChan())
		assert.Len(t, topicOpSlice, 0)
		assert.Len(t, s.state.State.Set, 1)
		assert.Len(t, s.state.State.Set[myselfPeerName].Map, 6)
		assert.True(t, crdt.FlagValue(s.state.State.Set[myselfPeerName].Map[topicList[0]]).IsRemove())
		assert.True(t, crdt.FlagValue(s.state.State.Set[myselfPeerName].Map[topicList[0]]).Time() == now)
		assert.True(t, crdt.FlagValue(s.state.State.Set[myselfPeerName].Map[topicList[1]]).IsRemove())
		assert.True(t, crdt.FlagValue(s.state.State.Set[myselfPeerName].Map[topicList[1]]).Time() == now)
		assert.True(t, crdt.FlagValue(s.state.State.Set[myselfPeerName].Map[topicList[2]]).IsRemove())
		assert.True(t, crdt.FlagValue(s.state.State.Set[myselfPeerName].Map[topicList[2]]).Time() == now)

		assert.True(t, crdt.FlagValue(s.state.State.Set[myselfPeerName].Map[topicList[3]]).IsRemove())
		assert.True(t, crdt.FlagValue(s.state.State.Set[myselfPeerName].Map[topicList[3]]).Time() == now+1)
		assert.True(t, crdt.FlagValue(s.state.State.Set[myselfPeerName].Map[topicList[4]]).IsRemove())
		assert.True(t, crdt.FlagValue(s.state.State.Set[myselfPeerName].Map[topicList[4]]).Time() == now)
		assert.True(t, crdt.FlagValue(s.state.State.Set[myselfPeerName].Map[topicList[5]]).IsRemove())
		assert.True(t, crdt.FlagValue(s.state.State.Set[myselfPeerName].Map[topicList[5]]).Time() == now)

		assert.Len(t, g.out, 1)
		assert.Len(t, g.out[0].State.Set, 1)
		assert.Len(t, g.out[0].State.Set[myselfPeerName].Map, 1)
		assert.True(t, crdt.FlagValue(g.out[0].State.Set[myselfPeerName].Map[topicList[3]]).IsRemove())
		assert.True(t, crdt.FlagValue(g.out[0].State.Set[myselfPeerName].Map[topicList[3]]).Time() == now+1)
	})

	t.Run("订阅及取消订阅(不包括自己的订阅信息)", func(t *testing.T) {

		s, r, _, _ := newSwarm(t)

		topicList := []string{"topic1", "topic2", "topic3", "topic4"}

		now := time.Now().Unix() * 1000

		pubMsg := &crdt.State{
			Set: map[uint64]*crdt.TopicMap{
				otherPeerName: {Map: map[string]int64{
					topicList[0]: newFlagValue(now, true),
					topicList[1]: newFlagValue(now, true),
					topicList[2]: newFlagValue(now, false),
					topicList[3]: newFlagValue(now, false),
				}},
			},
		}
		date, err := pubMsg.Marshal()
		assert.NoError(t, err)

		d, err := s.OnGossip(date)
		delta := d.(*crdt.CrdtState)
		assert.NoError(t, err)

		topicOpSlice := toSliceTopicOp(r.LocalBatchTopicOpChan())
		assert.Len(t, topicOpSlice, 2)
		assert.NotNil(t, topicOpSlice[0].Sub)
		assert.NotNil(t, topicOpSlice[1].Unsub)
		assert.ElementsMatch(t, topicOpSlice[0].Sub.Topic, topicList[:2])
		assert.ElementsMatch(t, topicOpSlice[1].Unsub.Topic, topicList[2:])
		assert.Len(t, delta.State.Set, 1)
		assert.Len(t, s.state.State.Set, 1)
		assert.Len(t, s.state.State.Set[otherPeerName].Map, 4)
		assert.EqualValues(t, s.state.State.Set[otherPeerName].Map[topicList[0]], newFlagValue(now, true))
		assert.EqualValues(t, s.state.State.Set[otherPeerName].Map[topicList[1]], newFlagValue(now, true))
		assert.EqualValues(t, s.state.State.Set[otherPeerName].Map[topicList[2]], newFlagValue(now, false))
		assert.EqualValues(t, s.state.State.Set[otherPeerName].Map[topicList[3]], newFlagValue(now, false))

	})

	t.Run("订阅及取消订阅(不包括自己的订阅信息) 无效时间", func(t *testing.T) {
		s, r, _, _ := newSwarm(t)

		topicList := []string{"topic1", "topic2", "topic3", "topic4"}

		now := time.Now().Unix() * 1000

		pubMsg := &crdt.State{
			Set: map[uint64]*crdt.TopicMap{
				otherPeerName: {Map: map[string]int64{
					topicList[0]: newFlagValue(now, true),
					topicList[1]: newFlagValue(now, true),
					topicList[2]: newFlagValue(now, false),
					topicList[3]: newFlagValue(now, false),
				}},
			},
		}
		date, err := pubMsg.Marshal()
		assert.NoError(t, err)

		d, err := s.OnGossip(date)
		delta := d.(*crdt.CrdtState)
		assert.NoError(t, err)

		topicOpSlice := toSliceTopicOp(r.LocalBatchTopicOpChan())
		assert.Len(t, topicOpSlice, 2)
		assert.NotNil(t, topicOpSlice[0].Sub)
		assert.NotNil(t, topicOpSlice[1].Unsub)
		assert.ElementsMatch(t, topicOpSlice[0].Sub.Topic, topicList[:2])
		assert.ElementsMatch(t, topicOpSlice[1].Unsub.Topic, topicList[2:])
		assert.Len(t, delta.State.Set, 1)
		assert.Len(t, s.state.State.Set, 1)
		assert.Len(t, s.state.State.Set[otherPeerName].Map, 4)
		assert.EqualValues(t, s.state.State.Set[otherPeerName].Map[topicList[0]], newFlagValue(now, true))
		assert.EqualValues(t, s.state.State.Set[otherPeerName].Map[topicList[1]], newFlagValue(now, true))
		assert.EqualValues(t, s.state.State.Set[otherPeerName].Map[topicList[2]], newFlagValue(now, false))
		assert.EqualValues(t, s.state.State.Set[otherPeerName].Map[topicList[3]], newFlagValue(now, false))

		pubMsg = &crdt.State{
			Set: map[uint64]*crdt.TopicMap{
				otherPeerName: {Map: map[string]int64{
					topicList[0]: newFlagValue(now-1, true),
					topicList[1]: newFlagValue(now+1, true),
					topicList[2]: newFlagValue(now-1, false),
					topicList[3]: newFlagValue(now+1, false),
				}},
			},
		}
		date, err = pubMsg.Marshal()
		assert.NoError(t, err)

		d, err = s.OnGossip(date)
		delta = d.(*crdt.CrdtState)
		assert.NoError(t, err)

		topicOpSlice = toSliceTopicOp(r.LocalBatchTopicOpChan())
		assert.Len(t, topicOpSlice, 2)
		assert.NotNil(t, topicOpSlice[0].Sub)
		assert.NotNil(t, topicOpSlice[1].Unsub)
		assert.ElementsMatch(t, topicOpSlice[0].Sub.Topic, []string{"topic2"})
		assert.ElementsMatch(t, topicOpSlice[1].Unsub.Topic, []string{"topic4"})
		assert.Len(t, delta.State.Set, 1)
		assert.Len(t, s.state.State.Set, 1)
		assert.Len(t, s.state.State.Set[otherPeerName].Map, 4)
		assert.EqualValues(t, s.state.State.Set[otherPeerName].Map[topicList[0]], newFlagValue(now, true))
		assert.EqualValues(t, s.state.State.Set[otherPeerName].Map[topicList[1]], newFlagValue(now+1, true))
		assert.EqualValues(t, s.state.State.Set[otherPeerName].Map[topicList[2]], newFlagValue(now, false))
		assert.EqualValues(t, s.state.State.Set[otherPeerName].Map[topicList[3]], newFlagValue(now+1, false))

	})
}

func Test_swarm_peerGc(t *testing.T) {
	s, r, _, _ := newSwarm(t)

	topicList := []string{"topic1", "topic2", "topic3", "topic4"}

	s.state.State = &crdt.State{
		Set: map[uint64]*crdt.TopicMap{
			otherPeerName: {Map: map[string]int64{
				topicList[0]: newFlagValue(10, true),
				topicList[1]: newFlagValue(10, true),
				topicList[2]: newFlagValue(10, false),
				topicList[3]: newFlagValue(10, false),
			}},
		},
	}

	s.peerGc(&mesh.Peer{Name: mesh.PeerName(otherPeerName)})

	assert.True(t, crdt.FlagValue(s.state.State.Set[otherPeerName].Map[topicList[0]]).IsRemove())
	assert.True(t, crdt.FlagValue(s.state.State.Set[otherPeerName].Map[topicList[0]]).Time() > 10)
	assert.True(t, crdt.FlagValue(s.state.State.Set[otherPeerName].Map[topicList[1]]).IsRemove())
	assert.True(t, crdt.FlagValue(s.state.State.Set[otherPeerName].Map[topicList[1]]).Time() > 10)
	assert.True(t, crdt.FlagValue(s.state.State.Set[otherPeerName].Map[topicList[2]]).IsRemove())
	assert.True(t, crdt.FlagValue(s.state.State.Set[otherPeerName].Map[topicList[2]]).Time() > 10)
	assert.True(t, crdt.FlagValue(s.state.State.Set[otherPeerName].Map[topicList[3]]).IsRemove())
	assert.True(t, crdt.FlagValue(s.state.State.Set[otherPeerName].Map[topicList[3]]).Time() > 10)

	topicOpSlice := toSliceTopicOp(r.LocalBatchTopicOpChan())
	assert.Len(t, topicOpSlice, 1)
	assert.NotNil(t, topicOpSlice[0].Unsub, 1)
	assert.ElementsMatch(t, topicOpSlice[0].Unsub.Topic, topicList)
	assert.EqualValues(t, topicOpSlice[0].Unsub.SubscriberId, mesh.PeerName(otherPeerName).String())

}
