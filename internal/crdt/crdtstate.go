package crdt

import (
	"github.com/weaveworks/mesh"
	"sync"
	"time"
)

//此结构实现了crdt语义
type CrdtState struct {
	lock sync.RWMutex

	//公开此属性是为了方便测试
	//正常代码不要直接访问
	State *State
}

func NewCrdtState() *CrdtState {
	return &CrdtState{
		State: &State{},
	}
}

func NewCrdtStateWithOther(other *State) *CrdtState {
	crdt := &CrdtState{}
	crdt.State = other
	return crdt
}

func DecodeCrdtState(buf []byte) (*CrdtState, error) {
	out := NewCrdtState()
	err := out.State.Unmarshal(buf)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (s *CrdtState) Encode() [][]byte {
	s.lock.RLock()
	defer s.lock.RUnlock()

	data, _ := s.State.Marshal()
	return [][]byte{data}
}

func (s *CrdtState) ForEach(f func(peerName uint64, topicMap *TopicMap)) {
	s.lock.RLock()
	defer s.lock.RUnlock()

	for peerName, topicMap := range s.State.Set {
		f(peerName, topicMap)
	}
}

//将other合并到当前状态,并且返回完整的状态
func (s *CrdtState) Merge(other mesh.GossipData) mesh.GossipData {

	o := other.(*CrdtState)
	s.lock.Lock()
	o.lock.RLock() //不会对o进行修改因此使用Read锁
	defer s.lock.Unlock()
	defer o.lock.RUnlock()

	s.State.merge(o.State)

	return s
}

//将other合并到当前状态,并裁剪other,只保存有变化的数据,并且other[myselfPeerName]会特殊处理
func (s *CrdtState) MergeDelta(other mesh.GossipData, myselfPeerName uint64, expired time.Duration) (int, *State) {
	o := other.(*CrdtState)
	s.lock.Lock()
	o.lock.Lock() //会对o进行修改因此使用Write锁
	defer s.lock.Unlock()
	defer o.lock.Unlock()

	var myselfDelta *State
	if o.State.Set != nil {
		myself, ok := o.State.Set[myselfPeerName]
		if ok {
			delete(o.State.Set, myselfPeerName)
		}
		if myself != nil {
			myselfDelta = s.State.mergeMyselfDelta(myself, myselfPeerName, expired)
		}
	}

	return s.State.mergeOtherDelta(o.State, expired), myselfDelta
}

func (s *CrdtState) Gc(expired time.Duration) {
	s.lock.Lock()
	defer s.lock.Unlock()

	s.State.gc(expired)
}

//批量强制更新AddTime(不管是否在Add状态),并且将更新操作同步复制到addState
func (s *CrdtState) BatchAdd(peerName uint64, topicList []string) (addState *State) {
	s.lock.Lock()
	defer s.lock.Unlock()

	return s.State.batchAdd(peerName, topicList)
}

//批量强制更新DelTime(不管是否在Del状态),并且将更新操作同步复制到delState
func (s *CrdtState) BatchDel(peerName uint64, topicList []string) (delState *State) {
	s.lock.Lock()
	defer s.lock.Unlock()

	return s.State.batchDel(peerName, topicList)
}

//批量更新DelTime,如果实际有更新DelTime,则复制到delState
func (s *CrdtState) DelPeerAllTopic(peerName uint64) (deleteList []string) {
	s.lock.Lock()
	defer s.lock.Unlock()

	return s.State.delPeerAllTopic(peerName)
}

//打印快照
func (s *CrdtState) Snapshot() string {
	s.lock.RLock()
	defer s.lock.RUnlock()

	return s.State.String()
}
