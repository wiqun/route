package service

import (
	"context"
	"github.com/stretchr/testify/assert"
	"github.com/wiqun/route/internal/config"
	"github.com/wiqun/route/internal/log"
	"github.com/wiqun/route/internal/message"
	"math/rand"
	"strconv"
	"sync"
	"testing"
	"time"
)

type queryPersonMock struct {
	out chan *message.QueryResponse
	id  uint64
}

func newQueryPersonMock() *queryPersonMock {
	return &queryPersonMock{
		id:  uint64(rand.Int63()),
		out: make(chan *message.QueryResponse, 100),
	}
}

func (q *queryPersonMock) ID() string {
	return strconv.FormatUint(q.id, 10)
}

func (q *queryPersonMock) ConcurrentId() uint64 {
	return q.id
}

func (q *queryPersonMock) SendQueryResult(result *message.QueryResponse) error {
	q.out <- result
	return nil
}

func wait() {
	time.Sleep(time.Millisecond * 10)
}

type localSubscriberMock struct {
	out     chan []*message.Message
	id      uint64
	subType message.LocalSubscriberType
}

func newLocalSubscriberMock(id uint64, subType message.LocalSubscriberType) *localSubscriberMock {
	return &localSubscriberMock{
		id:      id,
		subType: subType,
		out:     make(chan []*message.Message, 100),
	}
}

func (l *localSubscriberMock) toSlice() [][]*message.Message {
	var r [][]*message.Message
	for len(l.out) != 0 {
		r = append(r, <-l.out)
	}
	return r
}

func (l *localSubscriberMock) ID() string {
	return strconv.FormatUint(l.id, 10)
}

func (l *localSubscriberMock) ConcurrentId() uint64 {
	return l.id
}

func (l *localSubscriberMock) Type() message.LocalSubscriberType {
	return l.subType
}

func (l *localSubscriberMock) SendMessages(messages []*message.Message) error {
	l.out <- messages
	return nil
}

func newService(t *testing.T) (*service, message.LocalMsgNotifier, message.RemoteMsgChanReceiver) {

	c := &config.ChanConfig{
		LocalBatchTopicOpChanCoreSize:  100,
		QueryRequestChanCoreSize:       100,
		LocalPubMessageChanCoreSize:    100,
		RemotePubMessageChanCoreSize:   100,
		RemoteBatchTopicOpChanCoreSize: 100,
	}
	cs := &config.ServiceConfig{
		IOGoCoroutineCoreNums: 1,
		IOChanSize:            0,
	}

	n, r := message.NewLocalMsgNotifierChanReceiver(c)
	rn, rr := message.NewRemoteMsgNotifierChanReceiver(c)

	s := NewService(log.NewLogFactoryerMock(t),
		r,
		rn, cs)

	s.Run(context.Background(), &sync.WaitGroup{})

	time.Sleep(time.Millisecond * 100)
	return s.(*service), n, rr
}

func remoteBatchTopicOpToSlice(c <-chan message.RemoteBatchTopicOp) []message.RemoteBatchTopicOp {
	r := make([]message.RemoteBatchTopicOp, 0, len(c))
	for {
		select {
		case i := <-c:
			r = append(r, i)
		default:
			return r
		}
	}
}

type PubResultRecipientMock struct {
	id           uint64
	notFoundList [][]byte
}

func newPubResultRecipientMock(id uint64) *PubResultRecipientMock {
	return &PubResultRecipientMock{
		id: id,
	}
}

func (l *PubResultRecipientMock) ID() string {
	return strconv.FormatUint(l.id, 10)
}

func (l *PubResultRecipientMock) ConcurrentId() uint64 {
	return l.id
}

func (p *PubResultRecipientMock) SendPubNotFoundResult(notFound []byte) error {
	p.notFoundList = append(p.notFoundList, notFound)
	return nil
}

func Test_service_handleSubMsg(t *testing.T) {

	t.Run("订阅一个", func(t *testing.T) {

		topicMap := make(map[string]*message.SubCounter)
		s, _, remoteRecv := newService(t)
		suber := newLocalSubscriberMock(1, message.LocalSubscriberDirect)
		topic := "topic"
		s.handleSubMsg(&message.LocalBatchSubMsg{Subscriber: suber, Topic: []string{topic}}, topicMap)

		assert.Len(t, topicMap, 1)
		assert.Len(t, topicMap[topic].Subs, 1)
		assert.EqualValues(t, topicMap[topic].DirectCount, 1)
		r := remoteBatchTopicOpToSlice(remoteRecv.RemoteBatchTopicOpChan())
		assert.Len(t, r, 1)
		assert.True(t, r[0].IsSubOp)
	})
	t.Run("重复订阅一个", func(t *testing.T) {
		topicMap := make(map[string]*message.SubCounter)
		s, _, remoteRecv := newService(t)
		suber := newLocalSubscriberMock(1, message.LocalSubscriberDirect)
		topic := "topic"
		s.handleSubMsg(&message.LocalBatchSubMsg{Subscriber: suber, Topic: []string{topic}}, topicMap)

		assert.Len(t, topicMap, 1)
		assert.Len(t, topicMap[topic].Subs, 1)
		assert.EqualValues(t, topicMap[topic].DirectCount, 1)
		r := remoteBatchTopicOpToSlice(remoteRecv.RemoteBatchTopicOpChan())
		assert.Len(t, r, 1)
		assert.True(t, r[0].IsSubOp)

		s.handleSubMsg(&message.LocalBatchSubMsg{Subscriber: suber, Topic: []string{topic}}, topicMap)

		assert.Len(t, topicMap, 1)
		assert.Len(t, topicMap[topic].Subs, 1)
		assert.EqualValues(t, topicMap[topic].DirectCount, 1)
		r = remoteBatchTopicOpToSlice(remoteRecv.RemoteBatchTopicOpChan())
		assert.Len(t, r, 0)
	})

	t.Run("订阅一个 不同的Local订阅者", func(t *testing.T) {
		topicMap := make(map[string]*message.SubCounter)
		s, _, remoteRecv := newService(t)
		suberOne := newLocalSubscriberMock(1, message.LocalSubscriberDirect)
		suberTwo := newLocalSubscriberMock(2, message.LocalSubscriberDirect)
		topic := "topic"
		s.handleSubMsg(&message.LocalBatchSubMsg{Subscriber: suberOne, Topic: []string{topic}}, topicMap)
		s.handleSubMsg(&message.LocalBatchSubMsg{Subscriber: suberTwo, Topic: []string{topic}}, topicMap)

		assert.Len(t, topicMap, 1)
		assert.Len(t, topicMap[topic].Subs, 2)
		assert.EqualValues(t, topicMap[topic].DirectCount, 2)
		r := remoteBatchTopicOpToSlice(remoteRecv.RemoteBatchTopicOpChan())
		assert.Len(t, r, 1)
		assert.True(t, r[0].IsSubOp)
	})

	t.Run("订阅多个", func(t *testing.T) {
		topicMap := make(map[string]*message.SubCounter)
		s, _, remoteRecv := newService(t)
		suber := newLocalSubscriberMock(1, message.LocalSubscriberDirect)
		topicList := []string{"topic1", "topic2"}
		s.handleSubMsg(&message.LocalBatchSubMsg{Subscriber: suber, Topic: topicList}, topicMap)

		assert.Len(t, topicMap, 2)
		assert.Len(t, topicMap[topicList[0]].Subs, 1)
		assert.EqualValues(t, topicMap[topicList[0]].DirectCount, 1)
		assert.Len(t, topicMap[topicList[1]].Subs, 1)
		assert.EqualValues(t, topicMap[topicList[1]].DirectCount, 1)

		r := remoteBatchTopicOpToSlice(remoteRecv.RemoteBatchTopicOpChan())
		assert.Len(t, r, 1)
		assert.True(t, r[0].IsSubOp)
		assert.ElementsMatch(t, r[0].Topic, topicList)
	})
	t.Run("订阅多个 不同的订阅者", func(t *testing.T) {
		topicMap := make(map[string]*message.SubCounter)
		s, _, remoteRecv := newService(t)
		suber := newLocalSubscriberMock(1, message.LocalSubscriberDirect)
		remoteSuber := newLocalSubscriberMock(2, message.LocalSubscriberRemote)
		topicList := []string{"topic1", "topic2"}
		s.handleSubMsg(&message.LocalBatchSubMsg{Subscriber: suber, Topic: topicList}, topicMap)
		s.handleSubMsg(&message.LocalBatchSubMsg{Subscriber: remoteSuber, Topic: append(topicList, "topic3")}, topicMap)

		assert.Len(t, topicMap, 3)
		assert.Len(t, topicMap[topicList[0]].Subs, 2)
		assert.EqualValues(t, topicMap[topicList[0]].DirectCount, 1)

		assert.Len(t, topicMap[topicList[1]].Subs, 2)
		assert.EqualValues(t, topicMap[topicList[1]].DirectCount, 1)

		r := remoteBatchTopicOpToSlice(remoteRecv.RemoteBatchTopicOpChan())
		assert.Len(t, r, 1)
		assert.True(t, r[0].IsSubOp)
		assert.ElementsMatch(t, r[0].Topic, topicList)

	})
}

func Test_service_handleUnSubMsg(t *testing.T) {
	t.Run("取消订阅一个", func(t *testing.T) {
		topicMap := make(map[string]*message.SubCounter)
		s, _, remoteRecv := newService(t)
		suber := newLocalSubscriberMock(1, message.LocalSubscriberDirect)
		topic := "topic"
		s.handleSubMsg(&message.LocalBatchSubMsg{Subscriber: suber, Topic: []string{topic}}, topicMap)

		assert.Len(t, topicMap, 1)
		assert.Len(t, remoteBatchTopicOpToSlice(remoteRecv.RemoteBatchTopicOpChan()), 1)

		s.handleUnSubMsg(&message.LocalBatchUnsubMsg{SubscriberId: suber.ID(), Topic: []string{topic}}, topicMap)

		assert.Len(t, topicMap, 0)
		r := remoteBatchTopicOpToSlice(remoteRecv.RemoteBatchTopicOpChan())
		assert.Len(t, r, 1)
		assert.False(t, r[0].IsSubOp)
		assert.ElementsMatch(t, r[0].Topic, []string{topic})
	})
	t.Run("取消订阅不存在的主题", func(t *testing.T) {
		topicMap := make(map[string]*message.SubCounter)
		s, _, remoteRecv := newService(t)
		suber := newLocalSubscriberMock(1, message.LocalSubscriberDirect)
		topic := "topic"
		s.handleSubMsg(&message.LocalBatchSubMsg{Subscriber: suber, Topic: []string{topic}}, topicMap)

		assert.Len(t, topicMap, 1)
		assert.Len(t, remoteBatchTopicOpToSlice(remoteRecv.RemoteBatchTopicOpChan()), 1)

		s.handleUnSubMsg(&message.LocalBatchUnsubMsg{SubscriberId: suber.ID(), Topic: []string{"Test"}}, topicMap)

		assert.Len(t, topicMap, 1)
		assert.Len(t, remoteBatchTopicOpToSlice(remoteRecv.RemoteBatchTopicOpChan()), 0)

	})

	t.Run("重复取消订阅一个", func(t *testing.T) {
		topicMap := make(map[string]*message.SubCounter)
		s, _, remoteRecv := newService(t)
		suber := newLocalSubscriberMock(1, message.LocalSubscriberDirect)
		topic := "topic"
		s.handleSubMsg(&message.LocalBatchSubMsg{Subscriber: suber, Topic: []string{topic}}, topicMap)

		assert.Len(t, topicMap, 1)
		assert.Len(t, remoteBatchTopicOpToSlice(remoteRecv.RemoteBatchTopicOpChan()), 1)

		s.handleUnSubMsg(&message.LocalBatchUnsubMsg{SubscriberId: suber.ID(), Topic: []string{topic}}, topicMap)

		assert.Len(t, topicMap, 0)
		assert.Len(t, remoteBatchTopicOpToSlice(remoteRecv.RemoteBatchTopicOpChan()), 1)

		s.handleUnSubMsg(&message.LocalBatchUnsubMsg{SubscriberId: suber.ID(), Topic: []string{topic}}, topicMap)
		assert.Len(t, topicMap, 0)
		assert.Len(t, remoteBatchTopicOpToSlice(remoteRecv.RemoteBatchTopicOpChan()), 0)

	})

	t.Run("取消订阅一个 不同的Local订阅者", func(t *testing.T) {
		topicMap := make(map[string]*message.SubCounter)
		s, _, remoteRecv := newService(t)
		suberOne := newLocalSubscriberMock(1, message.LocalSubscriberDirect)
		suberTwo := newLocalSubscriberMock(2, message.LocalSubscriberDirect)
		topic := "topic"
		s.handleSubMsg(&message.LocalBatchSubMsg{Subscriber: suberOne, Topic: []string{topic}}, topicMap)
		s.handleSubMsg(&message.LocalBatchSubMsg{Subscriber: suberTwo, Topic: []string{topic}}, topicMap)

		assert.Len(t, topicMap, 1)
		assert.Len(t, topicMap[topic].Subs, 2)
		assert.EqualValues(t, topicMap[topic].DirectCount, 2)
		assert.Len(t, remoteBatchTopicOpToSlice(remoteRecv.RemoteBatchTopicOpChan()), 1)

		s.handleUnSubMsg(&message.LocalBatchUnsubMsg{SubscriberId: suberOne.ID(), Topic: []string{topic}}, topicMap)

		assert.Len(t, topicMap, 1)
		assert.Len(t, topicMap[topic].Subs, 1)
		assert.EqualValues(t, topicMap[topic].DirectCount, 1)
		assert.Len(t, remoteBatchTopicOpToSlice(remoteRecv.RemoteBatchTopicOpChan()), 0)

		s.handleUnSubMsg(&message.LocalBatchUnsubMsg{SubscriberId: suberTwo.ID(), Topic: []string{topic}}, topicMap)

		assert.Len(t, topicMap, 0)
		r := remoteBatchTopicOpToSlice(remoteRecv.RemoteBatchTopicOpChan())
		assert.Len(t, r, 1)
		assert.ElementsMatch(t, r[0].Topic, []string{topic})

	})

	t.Run("取消订阅多个", func(t *testing.T) {
		topicMap := make(map[string]*message.SubCounter)
		s, _, remoteRecv := newService(t)
		suber := newLocalSubscriberMock(1, message.LocalSubscriberDirect)
		topicList := []string{"topic1", "topic2"}
		s.handleSubMsg(&message.LocalBatchSubMsg{Subscriber: suber, Topic: topicList}, topicMap)

		assert.Len(t, topicMap, 2)
		assert.Len(t, remoteBatchTopicOpToSlice(remoteRecv.RemoteBatchTopicOpChan()), 1)

		s.handleUnSubMsg(&message.LocalBatchUnsubMsg{SubscriberId: suber.ID(), Topic: topicList}, topicMap)

		assert.Len(t, topicMap, 0)

		r := remoteBatchTopicOpToSlice(remoteRecv.RemoteBatchTopicOpChan())
		assert.Len(t, r, 1)
		assert.ElementsMatch(t, r[0].Topic, topicList)
		assert.False(t, r[0].IsSubOp)

	})
	t.Run("取消订阅多个 不同订阅者", func(t *testing.T) {
		topicMap := make(map[string]*message.SubCounter)
		s, _, remoteRecv := newService(t)
		suber := newLocalSubscriberMock(1, message.LocalSubscriberDirect)
		remoteSuber := newLocalSubscriberMock(2, message.LocalSubscriberRemote)
		topicList := []string{"topic1", "topic2"}
		remoteTopicList := []string{"topic1", "topic2", "topic3"}
		s.handleSubMsg(&message.LocalBatchSubMsg{Subscriber: suber, Topic: topicList}, topicMap)
		s.handleSubMsg(&message.LocalBatchSubMsg{Subscriber: remoteSuber, Topic: remoteTopicList}, topicMap)

		assert.Len(t, topicMap, 3)
		assert.Len(t, remoteBatchTopicOpToSlice(remoteRecv.RemoteBatchTopicOpChan()), 1)

		s.handleUnSubMsg(&message.LocalBatchUnsubMsg{SubscriberId: suber.ID(), Topic: topicList}, topicMap)

		assert.Len(t, topicMap, 3)
		assert.EqualValues(t, topicMap["topic1"].DirectCount, 0)
		assert.Len(t, topicMap["topic1"].Subs, 1)
		assert.EqualValues(t, topicMap["topic2"].DirectCount, 0)
		assert.Len(t, topicMap["topic2"].Subs, 1)
		assert.EqualValues(t, topicMap["topic3"].DirectCount, 0)
		assert.Len(t, topicMap["topic3"].Subs, 1)

		r := remoteBatchTopicOpToSlice(remoteRecv.RemoteBatchTopicOpChan())
		assert.Len(t, r, 1)
		assert.ElementsMatch(t, r[0].Topic, topicList)
		assert.False(t, r[0].IsSubOp)

		s.handleUnSubMsg(&message.LocalBatchUnsubMsg{SubscriberId: remoteSuber.ID(), Topic: remoteTopicList}, topicMap)

		r = remoteBatchTopicOpToSlice(remoteRecv.RemoteBatchTopicOpChan())
		assert.Len(t, r, 0)
	})
}

func Test_service_handleQueryRequest(t *testing.T) {
	t.Run("查询", func(t *testing.T) {
		topicMap := make(map[string]*message.SubCounter)
		s, _, _ := newService(t)
		suber := newLocalSubscriberMock(1, message.LocalSubscriberDirect)
		queryPerson := newQueryPersonMock()
		topicList := []string{"topic1", "topic2"}
		s.handleSubMsg(&message.LocalBatchSubMsg{Subscriber: suber, Topic: topicList}, topicMap)

		assert.Len(t, topicMap, 2)

		s.handleQueryRequest(&message.LocalQuery{
			Recipient:    queryPerson,
			QueryRequest: message.QueryRequest{CustomData: []byte{1}, TopicList: []string{"topic1"}}}, topicMap)

		wait()
		assert.Len(t, queryPerson.out, 1)
		r := <-queryPerson.out
		assert.EqualValues(t, r.TopicList, []string{"topic1"})
		assert.EqualValues(t, r.CustomData, []byte{1})

		s.handleQueryRequest(&message.LocalQuery{
			Recipient:    queryPerson,
			QueryRequest: message.QueryRequest{CustomData: []byte{2}, TopicList: []string{"topic2", "topic4"}}}, topicMap)

		wait()
		assert.Len(t, queryPerson.out, 1)
		r = <-queryPerson.out
		assert.EqualValues(t, r.TopicList, []string{"topic2"})
		assert.EqualValues(t, r.CustomData, []byte{2})

		s.handleQueryRequest(&message.LocalQuery{
			Recipient:    queryPerson,
			QueryRequest: message.QueryRequest{CustomData: []byte{3}, TopicList: []string{}}}, topicMap)

		wait()
		assert.Len(t, queryPerson.out, 1)
		r = <-queryPerson.out
		assert.Nil(t, r.TopicList)
		assert.EqualValues(t, r.CustomData, []byte{3})

		s.handleQueryRequest(&message.LocalQuery{
			Recipient:    queryPerson,
			QueryRequest: message.QueryRequest{CustomData: []byte{4}, TopicList: []string{"topic1", "topic2", "topic3"}}}, topicMap)

		wait()
		assert.Len(t, queryPerson.out, 1)
		r = <-queryPerson.out
		assert.EqualValues(t, r.TopicList, []string{"topic1", "topic2"})
		assert.EqualValues(t, r.CustomData, []byte{4})
	})
}

func Test_service_handleLocalPub(t *testing.T) {

	t.Run("一次推送一个", func(t *testing.T) {
		topicMap := make(map[string]*message.SubCounter)
		s, _, _ := newService(t)
		suber := newLocalSubscriberMock(1, message.LocalSubscriberDirect)
		remoteSuber := newLocalSubscriberMock(2, message.LocalSubscriberRemote)
		topicList := []string{"topic1", "topic2", "topic3"}

		s.handleSubMsg(&message.LocalBatchSubMsg{Subscriber: suber, Topic: topicList[:2]}, topicMap)
		s.handleSubMsg(&message.LocalBatchSubMsg{Subscriber: remoteSuber, Topic: topicList[1:]}, topicMap)

		assert.Len(t, topicMap, 3)

		s.handleLocalPub(message.LocalPub{
			PubRequest: &message.BatchPubRequest{Batch: []*message.PubRequest{
				{
					Topic:   topicList[0],
					Payload: []byte(topicList[0]),
				},
			}},
		}, topicMap)

		wait()
		assert.Len(t, suber.out, 1)
		assert.Len(t, remoteSuber.out, 0)

		s.handleLocalPub(message.LocalPub{
			PubRequest: &message.BatchPubRequest{Batch: []*message.PubRequest{
				{
					Topic:   topicList[1],
					Payload: []byte(topicList[1]),
				},
			}},
		}, topicMap)
		wait()

		assert.Len(t, suber.out, 2)
		assert.Len(t, remoteSuber.out, 1)

		s.handleLocalPub(message.LocalPub{
			PubRequest: &message.BatchPubRequest{Batch: []*message.PubRequest{
				{
					Topic:   topicList[2],
					Payload: []byte(topicList[2]),
				},
			}},
		}, topicMap)

		wait()
		assert.Len(t, suber.out, 2)
		assert.Len(t, remoteSuber.out, 2)
		r := suber.toSlice()

		assert.Len(t, r, 2)
		assert.Len(t, r[0], 1)
		assert.EqualValues(t, r[0][0].Topic, topicList[0])
		assert.EqualValues(t, r[0][0].Payload, topicList[0])

		assert.Len(t, r[1], 1)
		assert.EqualValues(t, r[1][0].Topic, topicList[1])
		assert.EqualValues(t, r[1][0].Payload, topicList[1])

		r = remoteSuber.toSlice()

		assert.Len(t, r, 2)
		assert.Len(t, r[0], 1)
		assert.EqualValues(t, r[0][0].Topic, topicList[1])
		assert.EqualValues(t, r[0][0].Payload, topicList[1])

		assert.Len(t, r[1], 1)
		assert.EqualValues(t, r[1][0].Topic, topicList[2])
		assert.EqualValues(t, r[1][0].Payload, topicList[2])
	})

	t.Run("一次推送多个", func(t *testing.T) {
		topicMap := make(map[string]*message.SubCounter)
		s, _, _ := newService(t)
		suber := newLocalSubscriberMock(1, message.LocalSubscriberDirect)
		remoteSuber := newLocalSubscriberMock(2, message.LocalSubscriberRemote)
		topicList := []string{"topic1", "topic2", "topic3"}

		s.handleSubMsg(&message.LocalBatchSubMsg{Subscriber: suber, Topic: topicList[:2]}, topicMap)
		s.handleSubMsg(&message.LocalBatchSubMsg{Subscriber: remoteSuber, Topic: topicList[1:]}, topicMap)

		assert.Len(t, topicMap, 3)

		s.handleLocalPub(message.LocalPub{
			PubRequest: &message.BatchPubRequest{Batch: []*message.PubRequest{
				{
					Topic:   topicList[0],
					Payload: []byte(topicList[0]),
				},
				{
					Topic:   topicList[1],
					Payload: []byte(topicList[1]),
				},
				{
					Topic:   topicList[2],
					Payload: []byte(topicList[2]),
				},
			}},
		}, topicMap)
		wait()

		assert.Len(t, suber.out, 1)
		assert.Len(t, remoteSuber.out, 1)

		r := suber.toSlice()
		assert.Len(t, r, 1)
		assert.Len(t, r[0], 2)
		assert.EqualValues(t, r[0][0].Topic, topicList[0])
		assert.EqualValues(t, r[0][0].Payload, topicList[0])
		assert.EqualValues(t, r[0][1].Topic, topicList[1])
		assert.EqualValues(t, r[0][1].Payload, topicList[1])

		r = remoteSuber.toSlice()
		assert.Len(t, r, 1)
		assert.Len(t, r[0], 2)
		assert.EqualValues(t, r[0][0].Topic, topicList[1])
		assert.EqualValues(t, r[0][0].Payload, topicList[1])
		assert.EqualValues(t, r[0][1].Topic, topicList[2])
		assert.EqualValues(t, r[0][1].Payload, topicList[2])
	})

	t.Run("推送不存在的主题", func(t *testing.T) {
		topicMap := make(map[string]*message.SubCounter)
		s, _, _ := newService(t)
		suber := newLocalSubscriberMock(1, message.LocalSubscriberDirect)
		remoteSuber := newLocalSubscriberMock(2, message.LocalSubscriberRemote)
		topicList := []string{"topic1", "topic2", "topic3"}
		recip := newPubResultRecipientMock(3)

		s.handleSubMsg(&message.LocalBatchSubMsg{Subscriber: suber, Topic: topicList[:2]}, topicMap)
		s.handleSubMsg(&message.LocalBatchSubMsg{Subscriber: remoteSuber, Topic: topicList[1:]}, topicMap)

		assert.Len(t, topicMap, 3)

		s.handleLocalPub(message.LocalPub{
			Recipient: recip,
			PubRequest: &message.BatchPubRequest{Batch: []*message.PubRequest{
				{
					Topic:    "not_found1",
					Payload:  []byte{1},
					NotFound: []byte{1},
				},
				{
					Topic:    "not_found2",
					Payload:  []byte{2},
					NotFound: []byte{2},
				},
				{
					Topic:   "not_found3",
					Payload: []byte{3},
				},
			}},
		}, topicMap)
		wait()

		assert.Len(t, recip.notFoundList, 2)

		assert.EqualValues(t, recip.notFoundList[0], []byte{1})
		assert.EqualValues(t, recip.notFoundList[1], []byte{2})

	})
}

func Test_service_handleRemotePub(t *testing.T) {
	t.Run("一次推送一个", func(t *testing.T) {
		topicMap := make(map[string]*message.SubCounter)
		s, _, _ := newService(t)
		suber := newLocalSubscriberMock(1, message.LocalSubscriberDirect)
		remoteSuber := newLocalSubscriberMock(2, message.LocalSubscriberRemote)
		topicList := []string{"topic1", "topic2", "topic3"}

		s.handleSubMsg(&message.LocalBatchSubMsg{Subscriber: suber, Topic: topicList[:2]}, topicMap)
		s.handleSubMsg(&message.LocalBatchSubMsg{Subscriber: remoteSuber, Topic: topicList[1:]}, topicMap)

		assert.Len(t, topicMap, 3)

		s.handleRemotePub([]*message.Message{
			{
				Topic:   topicList[0],
				Payload: []byte(topicList[0]),
			},
		}, topicMap)

		wait()
		assert.Len(t, suber.out, 1)
		assert.Len(t, remoteSuber.out, 0)

		s.handleRemotePub([]*message.Message{
			{
				Topic:   topicList[1],
				Payload: []byte(topicList[1]),
			},
		}, topicMap)

		wait()
		assert.Len(t, suber.out, 2)
		assert.Len(t, remoteSuber.out, 0)

		s.handleRemotePub([]*message.Message{
			{
				Topic:   topicList[2],
				Payload: []byte(topicList[2]),
			},
		}, topicMap)

		assert.Len(t, suber.out, 2)
		assert.Len(t, remoteSuber.out, 0)
		r := suber.toSlice()

		assert.Len(t, r, 2)
		assert.Len(t, r[0], 1)
		assert.EqualValues(t, r[0][0].Topic, topicList[0])
		assert.EqualValues(t, r[0][0].Payload, topicList[0])

		assert.Len(t, r[1], 1)
		assert.EqualValues(t, r[1][0].Topic, topicList[1])
		assert.EqualValues(t, r[1][0].Payload, topicList[1])

	})

	t.Run("一次推送多个", func(t *testing.T) {
		topicMap := make(map[string]*message.SubCounter)
		s, _, _ := newService(t)
		suber := newLocalSubscriberMock(1, message.LocalSubscriberDirect)
		remoteSuber := newLocalSubscriberMock(2, message.LocalSubscriberRemote)
		topicList := []string{"topic1", "topic2", "topic3"}

		s.handleSubMsg(&message.LocalBatchSubMsg{Subscriber: suber, Topic: topicList[:2]}, topicMap)
		s.handleSubMsg(&message.LocalBatchSubMsg{Subscriber: remoteSuber, Topic: topicList[1:]}, topicMap)

		assert.Len(t, topicMap, 3)

		s.handleRemotePub([]*message.Message{
			{
				Topic:   topicList[0],
				Payload: []byte(topicList[0]),
			},
			{
				Topic:   topicList[1],
				Payload: []byte(topicList[1]),
			},
			{
				Topic:   topicList[2],
				Payload: []byte(topicList[2]),
			},
		}, topicMap)

		wait()
		assert.Len(t, suber.out, 1)
		assert.Len(t, remoteSuber.out, 0)

		r := suber.toSlice()
		assert.Len(t, r, 1)
		assert.Len(t, r[0], 2)
		assert.EqualValues(t, r[0][0].Topic, topicList[0])
		assert.EqualValues(t, r[0][0].Payload, topicList[0])
		assert.EqualValues(t, r[0][1].Topic, topicList[1])
		assert.EqualValues(t, r[0][1].Payload, topicList[1])

	})
}
