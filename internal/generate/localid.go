package generate

import (
	"strconv"
	"sync/atomic"
	"time"
)

type LocalIdGenerator interface {
	Next() string
	NextUInt64() uint64
}

type localIdGenerator struct {
	current int64
}

//添加"_"前缀防止和 peerName重复
func (l *localIdGenerator) Next() string {
	r := atomic.AddInt64(&l.current, 1)
	return "_" + strconv.FormatInt(r, 10)
}

func NewLocalIdGenerator() LocalIdGenerator {
	return &localIdGenerator{
		current: time.Now().Unix() << 10,
	}
}

func (l *localIdGenerator) NextUInt64() uint64 {
	r := atomic.AddInt64(&l.current, 1)
	return uint64(r)
}
