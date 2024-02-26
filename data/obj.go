package data

import (
	"strconv"

	"github.com/godis/conf"
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

func (o *Gobj) StrVal() string {
	if o.Type_ != conf.GSTR {
		return ""
	}
	return o.Val_.(string)
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