package message

import (
	"fmt"
	"github.com/wiqun/route/internal/config"
	"runtime"
)

//代表着主题相关的操作
type RemoteBatchTopicOp struct {
	//是否是订阅主题
	IsSubOp bool
	Topic   []string
}

type RemoteMsgNotifier interface {
	NotifierSubscribe(topicList []string)
	NotifierUnsubscribe(topicList []string)
}

type RemoteMsgChanReceiver interface {
	RemoteBatchTopicOpChan() <-chan RemoteBatchTopicOp
}

type remoteMsgNotifierChanReceiver struct {
	batchTopicOpChan chan RemoteBatchTopicOp
}

func NewRemoteMsgNotifierChanReceiver(config *config.ChanConfig) (RemoteMsgNotifier, RemoteMsgChanReceiver) {
	coreNum := runtime.NumCPU()
	if coreNum < 1 {
		coreNum = 1
	}

	r := &remoteMsgNotifierChanReceiver{
		batchTopicOpChan: make(chan RemoteBatchTopicOp, config.RemoteBatchTopicOpChanCoreSize*coreNum),
	}

	fmt.Printf("batchTopicOpChan: %d \n", cap(r.batchTopicOpChan))

	return r, r
}

func (r *remoteMsgNotifierChanReceiver) NotifierSubscribe(topicList []string) {
	r.batchTopicOpChan <- RemoteBatchTopicOp{IsSubOp: true, Topic: topicList}
}

func (r *remoteMsgNotifierChanReceiver) NotifierUnsubscribe(topicList []string) {
	r.batchTopicOpChan <- RemoteBatchTopicOp{IsSubOp: false, Topic: topicList}
}

func (r *remoteMsgNotifierChanReceiver) RemoteBatchTopicOpChan() <-chan RemoteBatchTopicOp {
	return r.batchTopicOpChan
}
