package crdt

import (
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestState_mergeOtherDelta(t *testing.T) {
	t.Run("(无,Add)", func(t *testing.T) {
		peerName := uint64(1)
		topic := "topic"

		s := &State{}
		o := &State{
			Set: map[uint64]*TopicMap{
				peerName: {Map: map[string]int64{
					topic: int64(newFlagValue(10, true)),
				}},
			},
		}
		count := s.mergeOtherDelta(o, time.Hour)
		assert.EqualValues(t, count, 1)
		assert.Len(t, s.Set, 1)
		assert.Len(t, s.Set[peerName].Map, 1)
		assert.EqualValues(t, s.Set[peerName].Map[topic], newFlagValue(10, true))
		assert.Len(t, o.Set, 1)
		assert.Len(t, o.Set[peerName].Map, 1)
		assert.EqualValues(t, o.Set[peerName].Map[topic], newFlagValue(10, true))
	})

	t.Run("(无,Del) delTime已过期", func(t *testing.T) {
		peerName := uint64(1)
		topic := "topic"

		now := (time.Now().Unix() - 2) * 1000

		s := &State{}
		o := &State{
			Set: map[uint64]*TopicMap{
				peerName: {Map: map[string]int64{
					topic: int64(newFlagValue(now, false)),
				}},
			},
		}
		count := s.mergeOtherDelta(o, time.Millisecond*10)
		assert.EqualValues(t, count, 0)
		assert.Len(t, s.Set, 1)
		assert.Len(t, s.Set[peerName].Map, 0)
		assert.Len(t, o.Set, 0)

	})
	t.Run("(无,Del) delTime未过期", func(t *testing.T) {
		peerName := uint64(1)
		topic := "topic"

		now := time.Now().Unix() * 1000

		s := &State{}
		o := &State{
			Set: map[uint64]*TopicMap{
				peerName: {Map: map[string]int64{
					topic: int64(newFlagValue(now, false)),
				}},
			},
		}
		count := s.mergeOtherDelta(o, time.Hour)
		assert.EqualValues(t, count, 1)
		assert.Len(t, s.Set, 1)
		assert.Len(t, s.Set[peerName].Map, 1)
		assert.EqualValues(t, s.Set[peerName].Map[topic], newFlagValue(now, false))
		assert.Len(t, o.Set, 1)
		assert.Len(t, o.Set[peerName].Map, 1)
		assert.EqualValues(t, o.Set[peerName].Map[topic], newFlagValue(now, false))

	})

	t.Run("(Add,Add)", func(t *testing.T) {
		peerName := uint64(1)
		topic1 := "topic1"
		topic2 := "topic2"

		s := &State{
			Set: map[uint64]*TopicMap{
				peerName: {Map: map[string]int64{
					topic1: int64(newFlagValue(9, true)),
					topic2: int64(newFlagValue(11, true)),
				}},
			},
		}
		o := &State{
			Set: map[uint64]*TopicMap{
				peerName: {Map: map[string]int64{
					topic1: int64(newFlagValue(11, true)),
					topic2: int64(newFlagValue(9, true)),
				}},
			},
		}
		count := s.mergeOtherDelta(o, time.Hour)
		assert.EqualValues(t, count, 1)
		assert.Len(t, s.Set, 1)
		assert.Len(t, s.Set[peerName].Map, 2)
		assert.EqualValues(t, s.Set[peerName].Map[topic1], newFlagValue(11, true))
		assert.EqualValues(t, s.Set[peerName].Map[topic2], newFlagValue(11, true))

		assert.Len(t, o.Set, 1)
		assert.Len(t, o.Set[peerName].Map, 1)
		assert.EqualValues(t, s.Set[peerName].Map[topic1], newFlagValue(11, true))

	})

	t.Run("(Add,Del)", func(t *testing.T) {
		peerName := uint64(1)
		topic1 := "topic1"
		topic2 := "topic2"

		s := &State{
			Set: map[uint64]*TopicMap{
				peerName: {Map: map[string]int64{
					topic1: int64(newFlagValue(9, true)),
					topic2: int64(newFlagValue(11, true)),
				}},
			},
		}
		o := &State{
			Set: map[uint64]*TopicMap{
				peerName: {Map: map[string]int64{
					topic1: int64(newFlagValue(11, false)),
					topic2: int64(newFlagValue(9, false)),
				}},
			},
		}
		count := s.mergeOtherDelta(o, time.Hour)
		assert.EqualValues(t, count, 1)
		assert.Len(t, s.Set, 1)
		assert.Len(t, s.Set[peerName].Map, 2)
		assert.EqualValues(t, s.Set[peerName].Map[topic1], newFlagValue(11, false))
		assert.EqualValues(t, s.Set[peerName].Map[topic2], newFlagValue(11, true))

		assert.Len(t, o.Set, 1)
		assert.Len(t, o.Set[peerName].Map, 1)
		assert.EqualValues(t, s.Set[peerName].Map[topic1], newFlagValue(11, false))

	})

	t.Run("(Del,Del)", func(t *testing.T) {
		peerName := uint64(1)
		topic1 := "topic1"
		topic2 := "topic2"

		s := &State{
			Set: map[uint64]*TopicMap{
				peerName: {Map: map[string]int64{
					topic1: int64(newFlagValue(9, false)),
					topic2: int64(newFlagValue(11, false)),
				}},
			},
		}
		o := &State{
			Set: map[uint64]*TopicMap{
				peerName: {Map: map[string]int64{
					topic1: int64(newFlagValue(11, false)),
					topic2: int64(newFlagValue(9, false)),
				}},
			},
		}
		count := s.mergeOtherDelta(o, time.Hour)
		assert.EqualValues(t, count, 1)
		assert.Len(t, s.Set, 1)
		assert.Len(t, s.Set[peerName].Map, 2)
		assert.EqualValues(t, s.Set[peerName].Map[topic1], newFlagValue(11, false))
		assert.EqualValues(t, s.Set[peerName].Map[topic2], newFlagValue(11, false))

		assert.Len(t, o.Set, 1)
		assert.Len(t, o.Set[peerName].Map, 1)
		assert.EqualValues(t, s.Set[peerName].Map[topic1], newFlagValue(11, false))

	})

	t.Run("(Del,Add)", func(t *testing.T) {
		peerName := uint64(1)
		topic1 := "topic1"
		topic2 := "topic2"

		s := &State{
			Set: map[uint64]*TopicMap{
				peerName: {Map: map[string]int64{
					topic1: int64(newFlagValue(9, false)),
					topic2: int64(newFlagValue(11, false)),
				}},
			},
		}
		o := &State{
			Set: map[uint64]*TopicMap{
				peerName: {Map: map[string]int64{
					topic1: int64(newFlagValue(11, true)),
					topic2: int64(newFlagValue(9, true)),
				}},
			},
		}
		count := s.mergeOtherDelta(o, time.Hour)
		assert.EqualValues(t, count, 1)
		assert.Len(t, s.Set, 1)
		assert.Len(t, s.Set[peerName].Map, 2)
		assert.EqualValues(t, s.Set[peerName].Map[topic1], newFlagValue(11, true))
		assert.EqualValues(t, s.Set[peerName].Map[topic2], newFlagValue(11, false))

		assert.Len(t, o.Set, 1)
		assert.Len(t, o.Set[peerName].Map, 1)
		assert.EqualValues(t, s.Set[peerName].Map[topic1], newFlagValue(11, true))

	})
}

func TestState_mergeMyselfDelta(t *testing.T) {
	t.Run("(无,Add)", func(t *testing.T) {
		peerName := uint64(1)
		topic := "topic"

		s := &State{}
		o := &TopicMap{
			Map: map[string]int64{
				topic: int64(newFlagValue(10, true)),
			},
		}
		delta := s.mergeMyselfDelta(o, peerName, time.Hour)
		assert.Len(t, s.Set, 1)
		assert.Len(t, s.Set[peerName].Map, 1)
		assert.True(t, FlagValue(s.Set[peerName].Map[topic]).IsRemove())
		assert.True(t, FlagValue(s.Set[peerName].Map[topic]).Time() > 10)

		assert.Len(t, delta.Set, 1)
		assert.Len(t, delta.Set[peerName].Map, 1)
		assert.True(t, FlagValue(delta.Set[peerName].Map[topic]).IsRemove())
		assert.True(t, FlagValue(delta.Set[peerName].Map[topic]).Time() > 10)
	})

	t.Run("(无,Del) delTime过期", func(t *testing.T) {
		peerName := uint64(1)
		topic := "topic"

		s := &State{}
		o := &TopicMap{
			Map: map[string]int64{
				topic: int64(newFlagValue((time.Now().Unix()-2)*1000, false)),
			},
		}
		delta := s.mergeMyselfDelta(o, peerName, time.Millisecond*10)
		assert.Len(t, s.Set, 1)
		assert.Len(t, s.Set[peerName].Map, 0)

		assert.Nil(t, delta)
	})

	t.Run("(无,Del) delTime未过期", func(t *testing.T) {
		peerName := uint64(1)
		topic := "topic"

		now := (time.Now().Unix() - 2) * 1000

		s := &State{}
		o := &TopicMap{
			Map: map[string]int64{
				topic: int64(newFlagValue(now, false)),
			},
		}

		delta := s.mergeMyselfDelta(o, peerName, time.Hour)
		assert.Len(t, s.Set, 1)
		assert.Len(t, s.Set[peerName].Map, 1)
		assert.EqualValues(t, s.Set[peerName].Map[topic], newFlagValue(now, false))

		assert.Len(t, delta.Set, 1)
		assert.Len(t, delta.Set[peerName].Map, 1)
		assert.EqualValues(t, delta.Set[peerName].Map[topic], newFlagValue(now, false))
	})

	t.Run("(Add,Add)", func(t *testing.T) {
		peerName := uint64(1)
		topic1 := "topic1"
		topic2 := "topic2"
		topic3 := "topic3"

		s := &State{
			Set: map[uint64]*TopicMap{
				peerName: {Map: map[string]int64{
					topic1: int64(newFlagValue(9, true)),
					topic2: int64(newFlagValue(10, true)),
					topic3: int64(newFlagValue(11, true)),
				}},
			},
		}
		o := &TopicMap{
			Map: map[string]int64{
				topic1: int64(newFlagValue(8, true)),
				topic2: int64(newFlagValue(10, true)),
				topic3: int64(newFlagValue(12, true)),
			},
		}
		delta := s.mergeMyselfDelta(o, peerName, time.Hour)
		assert.Len(t, s.Set, 1)
		assert.Len(t, s.Set[peerName].Map, 3)
		assert.EqualValues(t, s.Set[peerName].Map[topic1], newFlagValue(9, true))
		assert.EqualValues(t, s.Set[peerName].Map[topic2], newFlagValue(10, true))
		assert.EqualValues(t, s.Set[peerName].Map[topic3], newFlagValue(11, true))

		assert.Nil(t, delta)
	})

	t.Run("(Add,Del)", func(t *testing.T) {
		peerName := uint64(1)
		topic1 := "topic1"
		topic2 := "topic2"
		topic3 := "topic3"

		s := &State{
			Set: map[uint64]*TopicMap{
				peerName: {Map: map[string]int64{
					topic1: int64(newFlagValue(9, true)),
					topic2: int64(newFlagValue(10, true)),
					topic3: int64(newFlagValue(11, true)),
				}},
			},
		}
		o := &TopicMap{
			Map: map[string]int64{
				topic1: int64(newFlagValue(8, false)),
				topic2: int64(newFlagValue(10, false)),
				topic3: int64(newFlagValue(12, false)),
			},
		}
		delta := s.mergeMyselfDelta(o, peerName, time.Hour)
		assert.Len(t, s.Set, 1)
		assert.Len(t, s.Set[peerName].Map, 3)
		assert.EqualValues(t, s.Set[peerName].Map[topic1], newFlagValue(9, true))
		assert.True(t, FlagValue(s.Set[peerName].Map[topic2]).IsAdd())
		assert.True(t, FlagValue(s.Set[peerName].Map[topic2]).Time() > 10)
		assert.True(t, FlagValue(s.Set[peerName].Map[topic3]).IsAdd())
		assert.True(t, FlagValue(s.Set[peerName].Map[topic3]).Time() > 12)

		assert.Len(t, delta.Set, 1)
		assert.Len(t, delta.Set[peerName].Map, 2)
		assert.True(t, FlagValue(delta.Set[peerName].Map[topic2]).IsAdd())
		assert.True(t, FlagValue(delta.Set[peerName].Map[topic2]).Time() > 10)
		assert.True(t, FlagValue(delta.Set[peerName].Map[topic3]).IsAdd())
		assert.True(t, FlagValue(delta.Set[peerName].Map[topic3]).Time() > 12)

	})

	t.Run("(Del,Add)", func(t *testing.T) {
		peerName := uint64(1)
		topic1 := "topic1"
		topic2 := "topic2"
		topic3 := "topic3"

		s := &State{
			Set: map[uint64]*TopicMap{
				peerName: {Map: map[string]int64{
					topic1: int64(newFlagValue(9, false)),
					topic2: int64(newFlagValue(10, false)),
					topic3: int64(newFlagValue(11, false)),
				}},
			},
		}
		o := &TopicMap{
			Map: map[string]int64{
				topic1: int64(newFlagValue(8, true)),
				topic2: int64(newFlagValue(10, true)),
				topic3: int64(newFlagValue(12, true)),
			},
		}
		delta := s.mergeMyselfDelta(o, peerName, time.Hour)
		assert.Len(t, s.Set, 1)
		assert.Len(t, s.Set[peerName].Map, 3)
		assert.EqualValues(t, s.Set[peerName].Map[topic1], newFlagValue(9, false))
		assert.EqualValues(t, s.Set[peerName].Map[topic2], newFlagValue(10, false))
		assert.EqualValues(t, s.Set[peerName].Map[topic3], newFlagValue(11, false))

		assert.Nil(t, delta)
	})

	t.Run("(Del,Del)", func(t *testing.T) {
		peerName := uint64(1)
		topic1 := "topic1"
		topic2 := "topic2"
		topic3 := "topic3"

		s := &State{
			Set: map[uint64]*TopicMap{
				peerName: {Map: map[string]int64{
					topic1: int64(newFlagValue(9, false)),
					topic2: int64(newFlagValue(10, false)),
					topic3: int64(newFlagValue(11, false)),
				}},
			},
		}
		o := &TopicMap{
			Map: map[string]int64{
				topic1: int64(newFlagValue(8, false)),
				topic2: int64(newFlagValue(10, false)),
				topic3: int64(newFlagValue(12, false)),
			},
		}
		delta := s.mergeMyselfDelta(o, peerName, time.Hour)
		assert.Len(t, s.Set, 1)
		assert.Len(t, s.Set[peerName].Map, 3)
		assert.EqualValues(t, s.Set[peerName].Map[topic1], newFlagValue(9, false))
		assert.EqualValues(t, s.Set[peerName].Map[topic2], newFlagValue(10, false))
		assert.EqualValues(t, s.Set[peerName].Map[topic3], newFlagValue(12, false))

		assert.Len(t, delta.Set, 1)
		assert.Len(t, delta.Set[peerName].Map, 1)
		assert.EqualValues(t, s.Set[peerName].Map[topic3], newFlagValue(12, false))

	})
}

func TestState_gc(t *testing.T) {
	t.Run("gc", func(t *testing.T) {
		peerName := uint64(1)
		topic1 := "topic1"
		topic2 := "topic2"
		topic3 := "topic3"
		topic4 := "topic4"

		now := (time.Now().Unix() + 4) * 1000
		expiredNow := (time.Now().Unix() - 4) * 1000

		s := &State{
			Set: map[uint64]*TopicMap{
				peerName: {Map: map[string]int64{
					topic1: int64(newFlagValue(expiredNow, false)),
					topic2: int64(newFlagValue(now, false)),
					topic3: int64(newFlagValue(expiredNow, true)),
					topic4: int64(newFlagValue(now, true)),
				}},
			},
		}

		s.gc(time.Second)

		assert.Len(t, s.Set, 1)
		assert.Len(t, s.Set[peerName].Map, 3)
		assert.EqualValues(t, s.Set[peerName].Map[topic2], newFlagValue(now, false))
		assert.EqualValues(t, s.Set[peerName].Map[topic3], newFlagValue(expiredNow, true))
		assert.EqualValues(t, s.Set[peerName].Map[topic4], newFlagValue(now, true))
	})

	t.Run("gc 回收空map", func(t *testing.T) {
		peerName1 := uint64(1)
		peerName2 := uint64(2)
		topic1 := "topic1"

		expiredNow := (time.Now().Unix() - 4) * 1000

		s := &State{
			Set: map[uint64]*TopicMap{
				peerName1: {Map: map[string]int64{
					topic1: int64(newFlagValue(expiredNow, false)),
				}},
				peerName2: {Map: map[string]int64{}},
			},
		}

		s.gc(time.Second)

		assert.Len(t, s.Set, 0)

	})
}
