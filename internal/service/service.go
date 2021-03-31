package service

import (
	"context"
	"route/internal/common"
	"route/internal/config"
	. "route/internal/log"
	"route/internal/message"
	"runtime"
	"sync"
)

//此service为核心类,主要处理sub,unsub,pub,query请求
type Service interface {
	common.Runnable
}

type service struct {
	log            Logger
	localMsgRecv   message.LocalMsgChanReceiver
	remoteNotifier message.RemoteMsgNotifier

	pubRequestResultIOChans []chan pubNotFoundIOTask
	pubIOChans              []chan *localSubscriberBatchPub
	queryIOChans            []chan queryIOTask
	ioNumbs                 int
}

type queryIOTask struct {
	queryPerson message.QueryResultRecipient
	result      *message.QueryResponse
}

type pubNotFoundIOTask struct {
	pubTopic message.PubResultRecipient
	notFound []byte
}

func NewService(logFactoryer LogFactoryer, localMsgRecv message.LocalMsgChanReceiver,
	remoteNotifier message.RemoteMsgNotifier, config *config.ServiceConfig) Service {
	log := logFactoryer.CreateLogger("service")

	s := &service{
		log:            log,
		localMsgRecv:   localMsgRecv,
		remoteNotifier: remoteNotifier,
	}

	s.ioNumbs = config.IOGoCoroutineCoreNums * runtime.NumCPU()
	if s.ioNumbs < 1 {
		s.ioNumbs = 1
	}
	log.Println("io协程数为:", s.ioNumbs)

	for i := 0; i < s.ioNumbs; i++ {
		s.pubRequestResultIOChans = append(s.pubRequestResultIOChans, make(chan pubNotFoundIOTask, config.IOChanSize))
		s.pubIOChans = append(s.pubIOChans, make(chan *localSubscriberBatchPub, config.IOChanSize))
		s.queryIOChans = append(s.queryIOChans, make(chan queryIOTask, config.IOChanSize))
	}
	return s
}

func (s *service) Run(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()

	wg.Add(1)
	go s.processLocalMsg(ctx, wg)

	wg.Add(s.ioNumbs)
	for i := 0; i < s.ioNumbs; i++ {
		go s.processIO(ctx, wg, s.pubIOChans[i], s.queryIOChans[i], s.pubRequestResultIOChans[i])
	}

}

func (s *service) processIO(ctx context.Context, wg *sync.WaitGroup,
	pubIOChan chan *localSubscriberBatchPub, queryIOChan chan queryIOTask, pubRequestResultChan chan pubNotFoundIOTask) {
	defer wg.Done()
	for {
		select {
		case <-ctx.Done():
			s.log.Println("processIO收到取消事件,退出协程!")
			return
		case result := <-pubRequestResultChan:
			err := result.pubTopic.SendPubNotFoundResult(result.notFound)
			if err != nil {
				s.log.PrintlnError("processIO: SendSearchResponse错误:", err)
			}
		case pub := <-pubIOChan:
			err := pub.flush()
			if err != nil {
				s.log.PrintlnError("processIO: flush错误:", err)
			}
		case query := <-queryIOChan:
			err := query.queryPerson.SendQueryResult(query.result)
			if err != nil {
				s.log.PrintlnError("processIO: SendQueryResult错误:", err)
			}
		}
	}
}

func (s *service) processLocalMsg(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()

	topicMap := make(map[string]*message.SubCounter)

	for {
		select {
		case <-ctx.Done():
			s.log.Println("processLocalMsg收到取消事件,退出协程!")
			return
		case batchOp := <-s.localMsgRecv.LocalBatchTopicOpChan():

			//不同的订阅顺序可能会出现的结果,因此Sub,Unsub同时只能拥有一个字段
			//例如先订阅A再取消A , 最终A被取消了
			//先取消A再订阅A, A最终还是订阅
			if batchOp.Sub != nil {
				s.handleSubMsg(batchOp.Sub, topicMap)
			} else {
				s.handleUnSubMsg(batchOp.Unsub, topicMap)
			}

		case queryRequest := <-s.localMsgRecv.QueryRequestChan():
			s.handleQueryRequest(queryRequest, topicMap)
		case localPub := <-s.localMsgRecv.LocalPubMessageChan():
			s.handleLocalPub(localPub, topicMap)
		case remotePub := <-s.localMsgRecv.RemotePubMessageChan():
			s.handleRemotePub(remotePub, topicMap)
		}
	}
}

func (s *service) handleSubMsg(subMsg *message.LocalBatchSubMsg, topicMap map[string]*message.SubCounter) {
	subscriber := subMsg.Subscriber
	var subList []string
	for _, topic := range subMsg.Topic {
		if _, ok := topicMap[topic]; !ok {
			topicMap[topic] = &message.SubCounter{
				Subs: make(map[string]message.LocalSubscriber),
			}
		}

		subCounter := topicMap[topic]

		if _, exist := subCounter.Subs[subscriber.ID()]; exist {
			continue
		}

		subCounter.Subs[subscriber.ID()] = subscriber
		if subscriber.Type() == message.LocalSubscriberDirect {
			//LocalSubscriberDirect才可能引起NotifierSubscribe
			subCounter.DirectCount++
			if subCounter.DirectCount == 1 {
				subList = append(subList, topic)
			}
		}

	}

	if len(subList) != 0 {
		s.remoteNotifier.NotifierSubscribe(subList)
	}
}

func (s *service) handleUnSubMsg(unsubMsg *message.LocalBatchUnsubMsg, topicMap map[string]*message.SubCounter) {
	subscriberId := unsubMsg.SubscriberId
	var unsubList []string
	for _, topic := range unsubMsg.Topic {
		subCounter, ok := topicMap[topic]
		if !ok || subCounter.Subs == nil {
			continue
		}

		if old, exist := subCounter.Subs[subscriberId]; exist {
			delete(subCounter.Subs, subscriberId)
			if old.Type() == message.LocalSubscriberDirect {
				//LocalSubscriberDirect才可能引起NotifierUnsubscribe
				subCounter.DirectCount--
				if subCounter.DirectCount == 0 {
					unsubList = append(unsubList, topic)
				}
			}

			//回收,防止空map一直在内存里
			if len(subCounter.Subs) == 0 {
				delete(topicMap, topic)
			}
		}
	}
	if len(unsubList) != 0 {
		s.remoteNotifier.NotifierUnsubscribe(unsubList)
	}

}

func (s *service) handleQueryRequest(query *message.LocalQuery, topicMap map[string]*message.SubCounter) {
	if len(query.QueryRequest.TopicList) == 0 {
		err := query.Recipient.SendQueryResult(&message.QueryResponse{CustomData: query.QueryRequest.CustomData})
		if err != nil {
			s.log.PrintlnError("handleQueryRequest: SendQueryResult错误:", err)
		}
		return
	}

	var haveSubscriber []string
	for _, topic := range query.QueryRequest.TopicList {

		subCounter, ok := topicMap[topic]
		if !ok || (len(subCounter.Subs) == 0) {
			continue
		}

		haveSubscriber = append(haveSubscriber, topic)

	}

	s.queryIOChans[query.Recipient.ConcurrentId()%uint64(s.ioNumbs)] <- queryIOTask{
		queryPerson: query.Recipient,
		result:      &message.QueryResponse{CustomData: query.QueryRequest.CustomData, TopicList: haveSubscriber},
	}
	return
}

type localSubscriberBatchPub struct {
	needSend   []*message.Message
	subscriber message.LocalSubscriber
}

func (l *localSubscriberBatchPub) commitRequest(request *message.PubRequest) {
	l.needSend = append(l.needSend, &message.Message{
		Topic:   request.Topic,
		Payload: request.Payload,
	})
}
func (l *localSubscriberBatchPub) commitMessage(request *message.Message) {
	l.needSend = append(l.needSend, request)
}

func (l *localSubscriberBatchPub) flush() error {
	return l.subscriber.SendMessages(l.needSend)
}
func (l *localSubscriberBatchPub) concurrentId() uint64 {
	return l.subscriber.ConcurrentId()
}

func (s *service) handleLocalPub(pubMsg message.LocalPub, topicMap map[string]*message.SubCounter) {
	if len(pubMsg.PubRequest.Batch) == 0 {
		return
	}

	sendMap := make(map[string]*localSubscriberBatchPub)
	for _, item := range pubMsg.PubRequest.Batch {
		subCounter, ok := topicMap[item.Topic]
		if !ok || (len(subCounter.Subs) == 0) {
			if item.NotFound != nil {
				s.pubRequestResultIOChans[pubMsg.Recipient.ConcurrentId()%uint64(s.ioNumbs)] <- pubNotFoundIOTask{
					pubTopic: pubMsg.Recipient,
					notFound: item.NotFound,
				}
			}
			continue
		}

		for _, subscriber := range subCounter.Subs {
			l, ok := sendMap[subscriber.ID()]
			if ok {
				l.commitRequest(item)
			} else {
				l = &localSubscriberBatchPub{}
				l.subscriber = subscriber
				l.commitRequest(item)
				sendMap[subscriber.ID()] = l
			}
		}
	}

	for _, l := range sendMap {
		s.pubIOChans[l.concurrentId()%uint64(s.ioNumbs)] <- l
	}
}

//会跳过Type==message.LocalSubscriberDirect的订阅者,防止循环pub
func (s *service) handleRemotePub(batch []*message.Message, topicMap map[string]*message.SubCounter) {
	if len(batch) == 0 {
		return
	}

	sendMap := make(map[string]*localSubscriberBatchPub)
	for _, item := range batch {
		subCounter, ok := topicMap[item.Topic]
		if !ok || subCounter.DirectCount == 0 {
			continue
		}

		for _, subscriber := range subCounter.Subs {
			if subscriber.Type() != message.LocalSubscriberDirect {
				continue
			}
			l, ok := sendMap[subscriber.ID()]
			if ok {
				l.commitMessage(item)
			} else {
				l = &localSubscriberBatchPub{}
				l.subscriber = subscriber
				l.commitMessage(item)
				sendMap[subscriber.ID()] = l
			}
		}
	}
	for _, l := range sendMap {
		s.pubIOChans[l.concurrentId()%uint64(s.ioNumbs)] <- l
	}
}
