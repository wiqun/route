package cluster

import (
	"github.com/weaveworks/mesh"
	"route/internal/message"
)

type Peer struct {
	sender mesh.Gossip
	name   mesh.PeerName
}

func newPeer(name mesh.PeerName, sender mesh.Gossip) *Peer {
	return &Peer{
		name:   name,
		sender: sender,
	}
}

func (p *Peer) ID() string {
	return p.name.String()
}

func (p *Peer) ConcurrentId() uint64 {
	return uint64(p.name)
}

func (p *Peer) Type() message.LocalSubscriberType {
	return message.LocalSubscriberRemote
}

func (p *Peer) SendMessages(messages []*message.Message) error {

	m := &message.ServerMessage{Messages: messages, Type: message.ServerType_MessagesType}

	data, err := m.Marshal()
	if err != nil {
		return err
	}
	return p.sender.GossipUnicast(p.name, data)
}
