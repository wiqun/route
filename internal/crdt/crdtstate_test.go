package crdt

import (
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestCrdtState_BatchAdd(t *testing.T) {
	peer1 := uint64(1)
	peer2 := uint64(2)
	topicList := []string{"topic1", "topic2"}
	s := NewCrdtState()
	r1 := s.BatchAdd(peer1, topicList)
	r2 := s.BatchAdd(peer2, topicList)

	assert.Len(t, s.State.Set, 2)

	assert.Len(t, s.State.Set[peer1].Map, 2)
	assert.True(t, FlagValue(s.State.Set[peer1].Map[topicList[0]]).IsAdd())
	assert.True(t, FlagValue(s.State.Set[peer1].Map[topicList[1]]).IsAdd())

	assert.Len(t, s.State.Set[peer2].Map, 2)
	assert.True(t, FlagValue(s.State.Set[peer2].Map[topicList[0]]).IsAdd())
	assert.True(t, FlagValue(s.State.Set[peer2].Map[topicList[1]]).IsAdd())

	assert.Len(t, r1.Set[peer1].Map, 2)
	assert.True(t, FlagValue(r1.Set[peer1].Map[topicList[0]]).IsAdd())
	assert.True(t, FlagValue(r1.Set[peer1].Map[topicList[1]]).IsAdd())

	assert.Len(t, r2.Set[peer2].Map, 2)
	assert.True(t, FlagValue(r2.Set[peer2].Map[topicList[0]]).IsAdd())
	assert.True(t, FlagValue(r2.Set[peer2].Map[topicList[1]]).IsAdd())
}

func TestCrdtState_BatchDel(t *testing.T) {
	peer1 := uint64(1)
	peer2 := uint64(2)
	topicList := []string{"topic1", "topic2"}
	s := NewCrdtState()
	r1 := s.BatchDel(peer1, topicList)
	r2 := s.BatchDel(peer2, topicList)

	assert.Len(t, s.State.Set, 2)
	assert.Len(t, s.State.Set[peer1].Map, 2)
	assert.True(t, FlagValue(s.State.Set[peer1].Map[topicList[0]]).IsRemove())
	assert.True(t, FlagValue(s.State.Set[peer1].Map[topicList[1]]).IsRemove())
	assert.Len(t, s.State.Set[peer2].Map, 2)
	assert.True(t, FlagValue(s.State.Set[peer2].Map[topicList[0]]).IsRemove())
	assert.True(t, FlagValue(s.State.Set[peer2].Map[topicList[1]]).IsRemove())

	assert.Len(t, r1.Set[peer1].Map, 2)
	assert.True(t, FlagValue(r1.Set[peer1].Map[topicList[0]]).IsRemove())
	assert.True(t, FlagValue(r1.Set[peer1].Map[topicList[1]]).IsRemove())
	assert.Len(t, r2.Set[peer2].Map, 2)
	assert.True(t, FlagValue(r2.Set[peer2].Map[topicList[0]]).IsRemove())
	assert.True(t, FlagValue(r2.Set[peer2].Map[topicList[1]]).IsRemove())
}

func TestCrdtState_Merge(t *testing.T) {
	s := NewCrdtState()
	other := NewCrdtState()

	peer1 := uint64(1)
	peer2 := uint64(2)
	topicList := []string{"topic1", "topic2"}

	s.BatchAdd(peer1, topicList)
	other.BatchAdd(peer2, topicList)

	s.Merge(other)

	assert.Len(t, s.State.Set, 2)
	assert.Len(t, s.State.Set[peer1].Map, 2)
	assert.True(t, FlagValue(s.State.Set[peer1].Map[topicList[0]]).IsAdd())
	assert.True(t, FlagValue(s.State.Set[peer1].Map[topicList[1]]).IsAdd())
	assert.Len(t, s.State.Set[peer2].Map, 2)
	assert.True(t, FlagValue(s.State.Set[peer2].Map[topicList[0]]).IsAdd())
	assert.True(t, FlagValue(s.State.Set[peer2].Map[topicList[1]]).IsAdd())

	other.BatchDel(peer1, topicList)
	other.BatchDel(peer2, topicList)

	s.Merge(other)

	assert.Len(t, s.State.Set, 2)
	assert.Len(t, s.State.Set[peer1].Map, 2)
	assert.True(t, FlagValue(s.State.Set[peer1].Map[topicList[0]]).IsRemove())
	assert.True(t, FlagValue(s.State.Set[peer1].Map[topicList[1]]).IsRemove())
	assert.Len(t, s.State.Set[peer2].Map, 2)
	assert.True(t, FlagValue(s.State.Set[peer2].Map[topicList[0]]).IsRemove())
	assert.True(t, FlagValue(s.State.Set[peer2].Map[topicList[1]]).IsRemove())

}

func TestCrdtState_MergeDelta(t *testing.T) {
	s := NewCrdtState()
	other1 := NewCrdtState()

	peer1 := uint64(1)
	peer2 := uint64(2)
	topicList := []string{"topic1", "topic2"}

	s.BatchAdd(peer1, topicList)
	other1.BatchAdd(peer2, topicList)

	count, delta := s.MergeDelta(other1, uint64(300), time.Hour)

	assert.Nil(t, delta)
	assert.EqualValues(t, count, 2)
	assert.Len(t, other1.State.Set, 1)

	assert.Len(t, s.State.Set, 2)
	assert.Len(t, s.State.Set[peer1].Map, 2)
	assert.True(t, FlagValue(s.State.Set[peer1].Map[topicList[0]]).IsAdd())
	assert.True(t, FlagValue(s.State.Set[peer1].Map[topicList[1]]).IsAdd())
	assert.Len(t, s.State.Set[peer2].Map, 2)
	assert.True(t, FlagValue(s.State.Set[peer2].Map[topicList[0]]).IsAdd())
	assert.True(t, FlagValue(s.State.Set[peer2].Map[topicList[1]]).IsAdd())

	count, delta = s.MergeDelta(other1, uint64(300), time.Hour)
	assert.Nil(t, delta)
	assert.EqualValues(t, count, 0)
	assert.Len(t, other1.State.Set, 0)

	other2 := NewCrdtState()
	other2.BatchDel(peer1, topicList)
	other2.BatchDel(peer2, topicList)

	count, delta = s.MergeDelta(other2, uint64(300), time.Hour)

	assert.Nil(t, delta)
	assert.EqualValues(t, count, 4)
	assert.Len(t, other2.State.Set, 2)

	assert.Len(t, s.State.Set, 2)
	assert.Len(t, s.State.Set[peer1].Map, 2)
	assert.True(t, FlagValue(s.State.Set[peer1].Map[topicList[0]]).IsRemove())
	assert.True(t, FlagValue(s.State.Set[peer1].Map[topicList[1]]).IsRemove())
	assert.Len(t, s.State.Set[peer2].Map, 2)
	assert.True(t, FlagValue(s.State.Set[peer2].Map[topicList[0]]).IsRemove())
	assert.True(t, FlagValue(s.State.Set[peer2].Map[topicList[1]]).IsRemove())

	count, delta = s.MergeDelta(other2, uint64(300), time.Hour)
	assert.Nil(t, delta)
	assert.EqualValues(t, count, 0)
	assert.Len(t, other2.State.Set, 0)
}

func TestCrdtState_DelPeerAllTopic(t *testing.T) {
	peer2 := uint64(2)
	topicList := []string{"topic1", "topic2"}
	s := NewCrdtState()
	s.BatchAdd(peer2, topicList)

	assert.Len(t, s.State.Set, 1)
	assert.Len(t, s.State.Set[peer2].Map, 2)
	assert.True(t, FlagValue(s.State.Set[peer2].Map[topicList[0]]).IsAdd())
	assert.True(t, FlagValue(s.State.Set[peer2].Map[topicList[1]]).IsAdd())

	r := s.DelPeerAllTopic(peer2)

	assert.Len(t, s.State.Set, 1)
	assert.Len(t, s.State.Set[peer2].Map, 2)
	assert.True(t, FlagValue(s.State.Set[peer2].Map[topicList[0]]).IsRemove())
	assert.True(t, FlagValue(s.State.Set[peer2].Map[topicList[1]]).IsRemove())
	assert.ElementsMatch(t, r, topicList)
}
