package message

type SubCounter struct {
	Subs map[string]LocalSubscriber

	//订阅类型为LocalSubscriberDirect的total
	DirectCount int
}
