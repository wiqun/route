package crdt

import (
	"fmt"
	"time"
)

//将other合并到当前状态,此方法只用于 CrdtState.Merge
func (s *State) merge(other *State) {
	for peerName, topicMap := range other.Set {
		peer := s.fetchPeerMap(peerName)
		for topic, ov := range topicMap.Map {
			sv, ok := peer.Map[topic]
			if !ok || FlagValue(ov).Time() > FlagValue(sv).Time() {
				peer.Map[topic] = ov
			}
		}
	}
}

//将other合并到当前状态,并裁剪other,使其只保存有变化的数据
//详细状态转换请看文档
func (s *State) mergeOtherDelta(other *State, expired time.Duration) int {
	count := 0
	for peerName, topicMap := range other.Set {
		peer := s.fetchPeerMap(peerName)
		for topic, ov := range topicMap.Map {
			sv, found := peer.Map[topic]
			oFlagValue := FlagValue(ov)

			if !found {
				if oFlagValue.IsAdd() {
					peer.Map[topic] = ov
					count++
				} else if getNow()-oFlagValue.Time() < expired.Milliseconds() {
					peer.Map[topic] = ov
					count++
				} else {
					delete(topicMap.Map, topic)
				}
				continue
			}

			sFlagValue := FlagValue(sv)
			if oFlagValue.Time() > sFlagValue.Time() {
				peer.Map[topic] = ov
				count++
			} else {
				delete(topicMap.Map, topic)
			}
		}
		if len(topicMap.Map) == 0 {
			delete(other.Set, peerName)
		}
	}
	if len(other.Set) == 0 {
		other.Set = nil
	}

	return count
}

//详细状态转换请看文档
func (s *State) mergeMyselfDelta(other *TopicMap, myselfPeerName uint64, expired time.Duration) *State {

	delta := &State{}
	deltaPeer := delta.fetchPeerMap(myselfPeerName)

	myselfPeer := s.fetchPeerMap(myselfPeerName)

	for topic, ov := range other.Map {
		sv, found := myselfPeer.Map[topic]
		oFlagValue := FlagValue(ov)

		if !found {
			flagTime := oFlagValue.Time()
			if oFlagValue.IsAdd() {
				now := getNow()
				if now <= flagTime {
					now = flagTime + 1
				}
				v := int64(newFlagValue(now, false)) //强制Del状态
				myselfPeer.Map[topic] = v
				deltaPeer.Map[topic] = v
			} else {
				if getNow()-flagTime < expired.Milliseconds() { //还没有过期
					myselfPeer.Map[topic] = ov
					deltaPeer.Map[topic] = ov
				}
			}
			continue
		}

		sFlagValue := FlagValue(sv)

		if sFlagValue.IsAdd() {
			if oFlagValue.IsRemove() {
				if sFlagValue.Time() > oFlagValue.Time() {
					continue
				}

				now := getNow()
				flagTime := oFlagValue.Time()
				if now <= flagTime {
					now = flagTime + 1
				}
				v := int64(newFlagValue(now, true)) //强制Add状态
				myselfPeer.Map[topic] = v
				deltaPeer.Map[topic] = v
			} else {
				if oFlagValue.Time() > sFlagValue.Time() {
					fmt.Printf("mergeMyselfDelta不可能的分支 oFlagValue: %d  sFlagValue: %d \n", oFlagValue, sFlagValue)
				}
			}
		} else {
			if oFlagValue.IsRemove() {
				if sFlagValue.Time() < oFlagValue.Time() {
					myselfPeer.Map[topic] = ov
					deltaPeer.Map[topic] = ov
				}
			} else {
				if oFlagValue.Time() > sFlagValue.Time() {
					fmt.Printf("mergeMyselfDelta不可能的分支 oFlagValue: %d  sFlagValue: %d \n", oFlagValue, sFlagValue)
				}
			}
		}

	}

	if len(deltaPeer.Map) == 0 {
		return nil
	}

	return delta
}

func (s *State) gc(expired time.Duration) {
	if len(s.Set) == 0 {
		return
	}

	now := getNow()
	for peerName, topicMap := range s.Set {
		for topic, v := range topicMap.Map {
			vFlagValue := FlagValue(v)
			if vFlagValue.IsAdd() {
				continue
			}
			if now-vFlagValue.Time() > expired.Milliseconds() { //已过期了
				delete(topicMap.Map, topic)
			}
		}
		if len(topicMap.Map) == 0 {
			delete(s.Set, peerName)
		}
	}
	//if len(s.Set) == 0 {
	//	s.Set = nil
	//}
}

//批量强制更新AddTime(不管是否在Add状态),并且将更新操作同步复制到addState
func (s *State) batchAdd(peerName uint64, topicList []string) (addState *State) {
	peer := s.fetchPeerMap(peerName)

	addState = &State{}
	addPeer := addState.fetchPeerMap(peerName)

	for _, topic := range topicList {
		sv, ok := peer.Map[topic]
		now := getNow()
		if ok {
			t := FlagValue(sv).Time()
			//判断是否可以取消,因为已经依靠了getNow,并且调用此方法修改的都是自己peerName
			if t >= now {
				now = t + 1
			}
		}

		v := int64(newFlagValue(now, true))
		peer.Map[topic] = v
		addPeer.Map[topic] = v
	}
	return
}

//批量强制更新DelTime(不管是否在Del状态),并且将更新操作同步复制到delState
func (s *State) batchDel(peerName uint64, topicList []string) (delState *State) {
	peer := s.fetchPeerMap(peerName)

	delState = &State{}
	delPeer := delState.fetchPeerMap(peerName)

	for _, topic := range topicList {
		sv, ok := peer.Map[topic]
		now := getNow()
		if ok {
			t := FlagValue(sv).Time()
			//判断是否可以取消,因为已经依靠了getNow,并且调用此方法修改的都是自己peerName
			if t >= now {
				now = t + 1
			}
		}

		v := int64(newFlagValue(now, false))
		peer.Map[topic] = v
		delPeer.Map[topic] = v
	}

	return delState
}

//更新peerName下所有topic的DelTime为当前时间
func (s *State) delPeerAllTopic(peerName uint64) (deleteList []string) {
	peer := s.fetchPeerMap(peerName)

	if peer.Map == nil || len(peer.Map) == 0 {
		return
	}

	deleteList = make([]string, 0, len(peer.Map))

	now := getNow()
	for topic, value := range peer.Map {

		v := FlagValue(value)

		if v.Time() >= now {
			now = v.Time() + 1
		}

		peer.Map[topic] = int64(newFlagValue(now, false))
		deleteList = append(deleteList, topic)
	}
	return
}

func (s *State) fetchPeerMap(peerName uint64) *TopicMap {
	if s.Set == nil {
		s.Set = make(map[uint64]*TopicMap)
	}
	topicMap, ok := s.Set[peerName]
	if !ok {
		s.Set[peerName] = &TopicMap{
			Map: make(map[string]int64),
		}
	} else {
		if topicMap.Map == nil {
			topicMap.Map = make(map[string]int64)
		}
	}
	return s.Set[peerName]
}

var old int64

//就算串行执行,在很短的间隔内 time.Now().Unix() 有可能会返回一样的值
//在串行执行中,此方法保证了后执行的getNow()一定大于前面执行的getNow()
//此方法不保证并发安全
//返回的时间单位为毫秒
func getNow() int64 {
	//todo不直接*1000
	now := time.Now().Unix() * 1000
	if now <= old {
		now = old + 1
	}
	old = now
	return now
}
