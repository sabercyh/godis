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


func (zs *ZSet) Zcount(start, end *Gobj) (error, int) {
	startScore, err := start.ParseFloat()
	endScore, err := end.ParseFloat()
	if err != nil {
		err = errors.New("-ERR:min or max is not a float")
		return err, 0
	}
	number := zs.skiplist.SearchByRangeScore(startScore, endScore)
	return nil, number
}

func (zs *ZSet) Zrange(start, end *Gobj) ([]string, error) {
	// 从object中提取 start end
	s, err := start.int64Val()
	if err != nil {
		return nil, errors.New("ERR value is not an integer or out of range")
	}
	e, err := end.int64Val()
	if err != nil {
		return nil, errors.New("ERR value is not an integer or out of range")
	}
	if s < 0 {
		s += int64(zs.skiplist.length)
	}
	if e < 0 {
		e += int64(zs.skiplist.length)
	}
	if s < 0 {
		s = 0
	}
	if s > e || s > int64(zs.skiplist.length - 1 ) {
		return nil, errors.New("-ERR: invalid range")
	}
	res := make([]string, e-s + 1)
	sln := zs.skiplist.getElememtByRank(uint64(e - s + 1))
	for i := 0 ; i < int((e - s)) ; i ++ {
		res[i] = sln.member
		i++
		sln = sln.level[0].forward
	}
	return res, nil
}

/*
TODO: zrank zrevrank的复用
处理withscore参数
*/

func (zs *ZSet) ZRANK(member *Gobj) (uint64, error){
	// 判断是否存在member
	valObj := zs.dict.Get(member)
	if valObj == nil {
		return 0, errors.New("nil")
	}
	return zs.skiplist.GetRank(member.StrVal(), valObj.FloatVal()), nil
}

/*
ZREM
TODO: 多个元素的删除
*/

func (zs *ZSet) ZREM(member, score *Gobj) (int, error) {
	if err := zs.skiplist.Delete(member.StrVal(), score.FloatVal()) ; err != nil {
		return 0, err
	} else {
		return 1, nil
	}
}