package crdt

//FlagValue是标志位和时间的集合
//value的最低位为标志位,1表示是Add状态,0表示Del状态
//value>>1 :表示最后设置标志位时的时间戳(单位毫秒)
type FlagValue int64

//func (v FlagValue) setAdd() FlagValue {
//	return v | 0x01
//}
//
//func (v FlagValue) setDel() FlagValue {
//	return v &^ 0x01
//}

func (v FlagValue) IsAdd() bool {
	return (v & 0x01) == 1
}

func (v FlagValue) IsRemove() bool {
	return (v & 0x01) != 1
}

//返回时间单位毫秒
func (v FlagValue) Time() int64 {
	return int64(v >> 1)
}

func newFlagValue(time int64, isAdd bool) FlagValue {
	if isAdd {
		return FlagValue((time << 1) | 0x01)
	} else {
		return FlagValue(time << 1)
	}
}
