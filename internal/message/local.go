package message

import (
	"fmt"
	"route/internal/config"
	"runtime"
)

type LocalBatchSubMsg struct {
	Subscriber LocalSubscriber
	Topic      []string
}

type LocalBatchUnsubMsg struct {
	SubscriberId string
	Topic        []string
}

type LocalBatchTopicOp struct {
	Sub   *LocalBatchSubMsg
	Unsub *LocalBatchUnsubMsg
}

type LocalQuery struct {
	QueryRequest QueryRequest

	Recipient QueryResultRecipient
}

type LocalPub struct {
	PubRequest *BatchPubRequest

	Recipient PubResultRecipient
}

type Identity interface {
	//需要保证唯一性
	ID() string

	//不需要保证唯一性,只要符合下面的特性即可
	//如果 a.ID()==b.ID() 则需要保证 a.ConcurrentId()==b.ConcurrentId()
	//如果 a.ConcurrentId()==b.ConcurrentId() 不要需要保证a.ID()==b.ID(),两者id可以不相同
	ConcurrentId() uint64
}

type QueryResultRecipient interface {
	Identity

	//todo 当service的处理逻辑单线程变成多线程则需要保证并发安全
	//此方法的调用者会确保此方法是串行调用,因此方法实现者可以认为此方法先线程安全的
	//前提是同一时间只有一种调用者
	SendQueryResult(result *QueryResponse) error
}

type PubResultRecipient interface {
	Identity

	//todo 当service的处理逻辑单线程变成多线程则需要保证并发安全
	//此方法的调用者会确保此方法是串行调用,因此方法实现者可以认为此方法先线程安全的
	//前提是同一时间只有一种调用者
	SendPubNotFoundResult(notFound []byte) error
}

type LocalSubscriberType uint8

const (
	LocalSubscriberDirect = LocalSubscriberType(iota)
	LocalSubscriberRemote
)

type LocalSubscriber interface {
	Identity

	Type() LocalSubscriberType

	//todo 当service的处理逻辑单线程变成多线程则需要保证并发安全
	//此方法的调用者会确保此方法是串行调用,因此方法实现者可以认为此方法先线程安全的
	//前提是同一时间只有一种调用者
	SendMessages(messages []*Message) error
}

type LocalMsgNotifier interface {
	//发送订阅消息
	NotifierSubscribe(*LocalBatchSubMsg)

	//发送取消订阅消息
	NotifierUnsubscribe(*LocalBatchUnsubMsg)

	//发送查询请求
	NotifierQueryRequest(*LocalQuery)

	//本地连接发送的推送消息
	//NotifierLocalPubMessage 和 NotifierRemotePubMessage 实际处理逻辑会有不同
	//NotifierLocalPubMessage 来的推送消息会遍历所有 LocalSubscriber 并调用 SendPubMessage ,不会区分 LocalSubscriberType
	//NotifierRemotePubMessage 来的推送消息只会遍历 LocalSubscriberType == LocalSubscriberDirect 的 LocalSubscriber
	NotifierLocalPubMessage(LocalPub)

	//远程节点发送的推送消息
	// NotifierLocalPubMessage 和 NotifierRemotePubMessage 实际处理逻辑会有不同
	NotifierRemotePubMessage(message []*Message)
}

type LocalMsgChanReceiver interface {

	//Sub/Unsub管道需要保证FIFO的特性,因此需要使用同一个管道
	LocalBatchTopicOpChan() <-chan LocalBatchTopicOp

	QueryRequestChan() <-chan *LocalQuery

	LocalPubMessageChan() <-chan LocalPub

	RemotePubMessageChan() <-chan []*Message
}

type localMsgNotifierChanReceiver struct {
	localBatchTopicOpChan chan LocalBatchTopicOp
	queryRequestChan      chan *LocalQuery
	localPubMessageChan   chan LocalPub
	remotePubMessageChan  chan []*Message
}

func NewLocalMsgNotifierChanReceiver(config *config.ChanConfig) (LocalMsgNotifier, LocalMsgChanReceiver) {
	coreNum := runtime.NumCPU()
	if coreNum < 1 {
		coreNum = 1
	}

	l := &localMsgNotifierChanReceiver{
		localBatchTopicOpChan: make(chan LocalBatchTopicOp, config.LocalBatchTopicOpChanCoreSize*coreNum),
		queryRequestChan:      make(chan *LocalQuery, config.QueryRequestChanCoreSize*coreNum),
		localPubMessageChan:   make(chan LocalPub, config.LocalPubMessageChanCoreSize*coreNum),
		remotePubMessageChan:  make(chan []*Message, config.RemotePubMessageChanCoreSize*coreNum),
	}

	fmt.Printf("localBatchTopicOpChan:%d queryRequestChan:%d localPubMessageChan:%d remotePubMessageChan:%d \n",
		cap(l.localBatchTopicOpChan), cap(l.queryRequestChan), cap(l.localPubMessageChan), cap(l.remotePubMessageChan))
	return l, l
}

func (l *localMsgNotifierChanReceiver) NotifierSubscribe(msg *LocalBatchSubMsg) {
	l.localBatchTopicOpChan <- LocalBatchTopicOp{Sub: msg}
}

func (l *localMsgNotifierChanReceiver) NotifierUnsubscribe(msg *LocalBatchUnsubMsg) {
	l.localBatchTopicOpChan <- LocalBatchTopicOp{Unsub: msg}
}

func (l *localMsgNotifierChanReceiver) NotifierQueryRequest(query *LocalQuery) {
	l.queryRequestChan <- query
}

func (l *localMsgNotifierChanReceiver) NotifierLocalPubMessage(message LocalPub) {
	l.localPubMessageChan <- message
}

func (l *localMsgNotifierChanReceiver) NotifierRemotePubMessage(messages []*Message) {
	l.remotePubMessageChan <- messages
}

func (l *localMsgNotifierChanReceiver) LocalBatchTopicOpChan() <-chan LocalBatchTopicOp {
	return l.localBatchTopicOpChan
}

func (l *localMsgNotifierChanReceiver) QueryRequestChan() <-chan *LocalQuery {
	return l.queryRequestChan
}

func (l *localMsgNotifierChanReceiver) LocalPubMessageChan() <-chan LocalPub {
	return l.localPubMessageChan
}

func (l *localMsgNotifierChanReceiver) RemotePubMessageChan() <-chan []*Message {
	return l.remotePubMessageChan
}
