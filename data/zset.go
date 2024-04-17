package data

import (
	"errors"
)

type ZSet struct {
	dict *Dict
	skiplist *SkipList
}


func NewZset() *ZSet{
	return &ZSet{
		dict: DictCreate(DictType{HashFunc: GStrHash, EqualFunc: GStrEqual}),
		skiplist: NewSkipList(),
	}
}

type ZaddReply struct {
	Err error
	UpdateCount, NewCount int
}

func (zs *ZSet) Zadd(args []*Gobj) *ZaddReply {
	zaddReply := new(ZaddReply)
	// 判断一下参数的个数 如果参数不对 返回错误
	if (len(args) % 2 != 0) {
		zaddReply.Err = errors.New("-ERR:Syntax error")
		return zaddReply
	}
	// 逐个处理member score对
	for i := 0 ; i < len(args) ; i += 2 {
		score, err := args[i + 1].ParseFloat()
		if err != nil {
			zaddReply.Err = errors.New("-ERR:WRONGTYPE Operation against a key holding the wrong kind of value")
			return zaddReply
		}
		memberVal := zs.dict.Get(args[i])
		if memberVal != nil {
			oldScore, _ := memberVal.ParseFloat()
			// 如果分数和之前相等，则跳出
			if (oldScore == score) {
				continue
			}
			// 如果分数不想等，则更新
			zs.dict.Set(args[i], args[i + 1])
			zs.skiplist.UpdateScore(oldScore, args[i].StrVal(), score)
			zaddReply.UpdateCount ++
		} else {
			zs.dict.Set(args[i], args[i + 1])
			zs.skiplist.Insert(score, args[i].StrVal())
			zaddReply.NewCount ++
		}
		zs.skiplist.PrintSkipList()
	}
	return zaddReply
}

func (zs *ZSet) Zcard() uint64 {
	return zs.skiplist.length
}

func (zs *ZSet) Zscore(member *Gobj) string {
	obj := zs.dict.Get(member)
	if obj == nil {
		return ""
	}
	return obj.StrVal()
}