package data

import (
	"github.com/godis/errs"
)

type ZSet struct {
	Dict     *Dict
	skiplist *SkipList
}

func NewZset() *ZSet {
	return &ZSet{
		Dict:     DictCreate(DictType{HashFunc: GStrHash, EqualFunc: GStrEqual}),
		skiplist: NewSkipList(),
	}
}

func (zs *ZSet) Zadd(args []*Gobj) (int, error) {
	newCount := 0
	for i := 0; i < len(args); i += 2 {
		score, err := args[i].ParseFloat()
		if err != nil {
			return 0, errs.TypeCheckError
		}
		memberVal := zs.Dict.Get(args[i+1])
		if memberVal != nil {
			oldScore, _ := memberVal.ParseFloat()
			// 如果分数和之前相等，则跳出
			if oldScore == score {
				continue
			}
			// 如果分数不相等，则更新
			zs.Dict.Set(args[i+1], args[i])
			zs.skiplist.UpdateScore(oldScore, args[i].StrVal(), score)
		} else {
			zs.Dict.Set(args[i+1], args[i])
			zs.skiplist.Insert(score, args[i+1].StrVal())
			newCount++
		}
		// zs.skiplist.PrintSkipList()
	}
	return newCount, nil
}

func (zs *ZSet) Zcard() uint64 {
	return zs.skiplist.length
}

func (zs *ZSet) Zscore(member *Gobj) string {
	obj := zs.Dict.Get(member)
	if obj == nil {
		return ""
	}
	return obj.StrVal()
}

func (zs *ZSet) Zcount(start, end *Gobj) (int, error) {
	startScore, err := start.ParseFloat()
	if err != nil {
		err = errs.TypeCheckError
		return 0, err
	}
	endScore, err := end.ParseFloat()
	if err != nil {
		err = errs.TypeCheckError
		return 0, err
	}
	number := zs.skiplist.SearchByRangeScore(startScore, endScore)
	return number, nil
}

func (zs *ZSet) Zrange(start, end *Gobj) ([]string, error) {
	// 从object中提取 start end
	s, err := start.Int64Val()
	if err != nil {
		return nil, errs.TypeCheckError
	}
	e, err := end.Int64Val()
	if err != nil {
		return nil, errs.TypeCheckError
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
	if s > e || s > int64(zs.skiplist.length-1) {
		return nil, errs.OutOfRangeError
	}
	if e > int64(zs.skiplist.length-1) {
		e = int64(zs.skiplist.length - 1)
	}
	res := make([]string, e-s+1)
	sln := zs.skiplist.getElememtByRank(uint64(s) + 1)
	for i := 0; i < int(e-s+1); i++ {
		res[i] = sln.member
		sln = sln.level[0].forward
	}
	return res, nil
}

/*
TODO: zrank zrevrank的复用
处理withscore参数
*/

func (zs *ZSet) ZRANK(member *Gobj) (uint64, error) {
	// 判断是否存在member
	valObj := zs.Dict.Get(member)
	if valObj == nil {
		return 0, errs.CustomError
	}
	return zs.skiplist.GetRank(member.StrVal(), valObj.FloatVal()), nil
}

func (zs *ZSet) ZREM(member *Gobj) (int, error) {
	if err := zs.skiplist.Delete(member.StrVal()); err != nil {
		return 0, err
	} else {
		return 1, nil
	}
}
