package data

import (
	"hash/fnv"
	"strconv"

	"github.com/godis/conf"
	"github.com/godis/errs"
)

type Gobj struct {
	Type_    conf.Gtype
	Val_     conf.Gval
	RefCount int
}

func CreateObject(typ conf.Gtype, ptr interface{}) *Gobj {
	return &Gobj{
		Type_:    typ,
		Val_:     ptr,
		RefCount: 1,
	}
}

func CreateObjectFromInt(val int64) *Gobj {
	return &Gobj{
		Type_:    conf.GSTR,
		Val_:     strconv.FormatInt(val, 10),
		RefCount: 1,
	}
}

func (o *Gobj) IntVal() int64 {
	if o.Type_ != conf.GSTR {
		return 0
	}
	val, _ := strconv.ParseInt(o.Val_.(string), 10, 64)
	return val
}

func (o *Gobj) int64Val() (int64, error) {
	if o.Type_ != conf.GSTR {
		return 0, nil
	}
	val, err := strconv.ParseInt(o.Val_.(string), 10, 64)
	return val, err
}

func (o *Gobj) StrVal() string {
	if o.Type_ != conf.GSTR {
		return ""
	}
	return o.Val_.(string)
}

func (o *Gobj) FloatVal() float64 {
	if o.Type_ != conf.GSTR {
		return 0.0
	}
	val, _ := strconv.ParseFloat(o.Val_.(string), 64)
	return val
}

func (o *Gobj) IncrRefCount() {
	o.RefCount++
}

func (o *Gobj) DecrRefCount() {
	o.RefCount--
	if o.RefCount == 0 {
		// let GC do the work
		o.Val_ = nil
	}
}

func (o *Gobj) ParseFloat() (float64, error) {
	switch v := o.Val_.(type) {
	case float64:
		return v, nil
	case string:
		if parsedValue, err := strconv.ParseFloat(v, 64); err != nil {
			return 0.0, err
		} else {
			return parsedValue, nil
		}
	}
	return 0.0, errs.TypeConvertError
}

func (o *Gobj) CheckType(t conf.Gtype) error {
	if o.Type_ == t {
		return nil
	}
	return errs.TypeCheckError
}

// 计算两个Godis Object的类型是否相等
func GStrEqual(a, b *Gobj) bool {
	if a.Type_ != conf.GSTR || b.Type_ != conf.GSTR {
		return false
	}
	return a.StrVal() == b.StrVal()
}

// GStrHash 用于唯一标识一个Godis Object
func GStrHash(key *Gobj) int64 {
	if key.Type_ != conf.GSTR {
		return 0
	}
	hash := fnv.New64()
	hash.Write([]byte(key.StrVal()))
	return int64(hash.Sum64())
}
