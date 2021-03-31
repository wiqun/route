package broker

import (
	"bytes"
	"github.com/gorilla/websocket"
	"io"
	"route/internal/config"
	"route/internal/log"
	"route/internal/message"
	"time"
)

//服务器正常情况不会主动断开
//服务器是否也需要一个ReadTimeout?
type conn struct {
	socket        *websocket.Conn
	log           log.Logger
	localNotifier message.LocalMsgNotifier
	id            string
	concurrentId  uint64
	config        *config.BrokerConfig
	writeBuf      []byte
	readBuf       bytes.Buffer
}

func (b *broker) newConn(socket *websocket.Conn) *conn {
	return &conn{
		socket:        socket,
		config:        b.config,
		log:           b.log,
		localNotifier: b.localNotifier,
		id:            b.localIdGenerator.Next(),
		concurrentId:  b.localIdGenerator.NextUInt64(),
	}
}

func (c *conn) ID() string {
	return c.id
}

func (c *conn) ConcurrentId() uint64 {
	return c.concurrentId
}

func (c *conn) Type() message.LocalSubscriberType {
	return message.LocalSubscriberDirect
}

func (c *conn) writeMessage(r message.ServerMessage) error {
	size := r.Size()
	if cap(c.writeBuf) < size {
		c.writeBuf = make([]byte, 0, size)
	}
	c.writeBuf = c.writeBuf[:size]

	_, err := r.MarshalTo(c.writeBuf)
	if err != nil {
		return err
	}

	c.socket.SetWriteDeadline(time.Now().Add(c.config.WriteTimeout))
	err = c.socket.WriteMessage(websocket.BinaryMessage, c.writeBuf)
	if err != nil {
		//Close 将会导致c.socket.ReadMessage()返回异常,从而调用c.close
		c.socket.Close()
		return err
	}
	return nil
}

func (c conn) SendPubNotFoundResult(notFound []byte) error {
	r := message.ServerMessage{
		Type:        message.ServerType_PubNotFoundType,
		PubNotFound: notFound,
	}
	return c.writeMessage(r)
}

func (c *conn) SendMessages(messages []*message.Message) error {

	r := message.ServerMessage{
		Type:     message.ServerType_MessagesType,
		Messages: messages,
	}

	return c.writeMessage(r)

}

func (c *conn) SendQueryResult(result *message.QueryResponse) error {
	r := message.ServerMessage{
		Type:  message.ServerType_QueryResponseType,
		Query: result,
	}

	return c.writeMessage(r)
}

func (c *conn) readFrom(r io.Reader) (data []byte, err error) {
	c.readBuf.Reset()
	// If the buffer overflows, we will get bytes.ErrTooLarge.
	// Return that as an error. Any other panic remains.
	defer func() {
		e := recover()
		if e == nil {
			return
		}
		if panicErr, ok := e.(error); ok && panicErr == bytes.ErrTooLarge {
			err = panicErr
		} else {
			panic(e)
		}
	}()
	_, err = c.readBuf.ReadFrom(r)
	return c.readBuf.Bytes(), err
}

func (c *conn) process() {

	subTopic := make(map[string]struct{})

	defer c.close(subTopic)

	for {

		messageType, r, err := c.socket.NextReader()
		if err != nil {
			c.log.Println("NextReader :", err)
			break
		}

		data, err := c.readFrom(r)

		if err != nil {
			c.log.Println("ReadFrom :", err)
			break
		}

		if messageType != websocket.BinaryMessage {
			continue
		}

		msg := &message.ClientMessage{}
		err = msg.Unmarshal(data)
		if err != nil {
			c.log.Println("process 解码二进制消息失败")
			continue
		}

		c.onReceive(msg, subTopic)
	}

	return
}

func (c *conn) close(subTopic map[string]struct{}) {
	defer c.socket.Close()

	if len(subTopic) == 0 {
		return
	}

	unsubList := make([]string, 0, len(subTopic))
	for key, _ := range subTopic {
		unsubList = append(unsubList, key)
	}

	c.localNotifier.NotifierUnsubscribe(&message.LocalBatchUnsubMsg{
		SubscriberId: c.ID(),
		Topic:        unsubList,
	})
}

func (c *conn) onReceive(msg *message.ClientMessage, subTopic map[string]struct{}) {

	c.log.PrintfDebug("收到请求 Type: %d", msg.Type)

	switch msg.Type {
	case message.ClientType_BatchSubType:
		var subList []string
		for _, topic := range msg.BatchSub.TopicList {
			if _, ok := subTopic[topic]; !ok {
				subList = append(subList, topic)
				subTopic[topic] = struct{}{}
			}
		}

		if len(subList) == 0 {
			return
		}

		c.localNotifier.NotifierSubscribe(&message.LocalBatchSubMsg{
			Subscriber: c,
			Topic:      subList,
		})
	case message.ClientType_BatchUnSubType:
		var unsubList []string
		for _, topic := range msg.BatchUnsub.TopicList {
			if _, ok := subTopic[topic]; ok {
				unsubList = append(unsubList, topic)
				delete(subTopic, topic)
			}
		}

		if len(unsubList) == 0 {
			return
		}

		c.localNotifier.NotifierUnsubscribe(&message.LocalBatchUnsubMsg{
			SubscriberId: c.ID(),
			Topic:        unsubList,
		})
	case message.ClientType_BatchPubType:
		c.localNotifier.NotifierLocalPubMessage(message.LocalPub{
			PubRequest: msg.BatchPub,
			Recipient:  c,
		})

	case message.ClientType_QueryType:
		c.localNotifier.NotifierQueryRequest(&message.LocalQuery{
			QueryRequest: *msg.Query,
			Recipient:    c,
		})

	}
}
