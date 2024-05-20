package data

import (
	"math"
	"math/rand"

	"github.com/godis/errs"
)

const (
	INIT_SIZE    int64 = 8
	FORCE_RATIO  int64 = 2
	GROW_RATIO   int64 = 2
	DEFAULT_STEP int   = 1
)

type Entry struct {
	Key  *Gobj
	Val  *Gobj
	next *Entry
}

type htable struct {
	table []*Entry
	size  int64
	mask  int64
	used  int64
}

type DictType struct {
	HashFunc  func(key *Gobj) int64
	EqualFunc func(k1, k2 *Gobj) bool
}

type Dict struct {
	DictType
	hts       [2]*htable
	rehashidx int64
	// iterators
}

type dictIterator struct {
	dict    *Dict
	htIndex int
	index   int
	entry   *Entry
	Gobjs   [][2]*Gobj
}

func DictCreate(dictType DictType) *Dict {
	var dict Dict
	dict.DictType = dictType
	dict.rehashidx = -1
	return &dict
}

func dictIteratorCreate(dict *Dict) *dictIterator {
	return &dictIterator{
		dict:    dict,
		htIndex: 0,
		index:   0,
		entry:   nil,
		Gobjs:   make([][2]*Gobj, 0),
	}
}

func (iterator *dictIterator) Iterate() {
	if iterator.dict.isRehashing() {
		iterator.htIndex = 1
	}
	for i := 0; i <= iterator.htIndex; i++ {
		for j := 0; j < len(iterator.dict.hts[i].table); j++ {
			iterator.entry = iterator.dict.hts[i].table[j]
			for iterator.entry != nil {
				iterator.Gobjs = append(iterator.Gobjs, [2]*Gobj{iterator.entry.Key, iterator.entry.Val})
				iterator.entry = iterator.entry.next
			}
		}
	}
}

func (dict *Dict) isRehashing() bool {
	return dict.rehashidx != -1
}

func (dict *Dict) rehashStep() {
	// TODO: check iterators
	dict.rehash(DEFAULT_STEP)
}

func (dict *Dict) rehash(step int) {
	for step > 0 {
		if dict.hts[0].used == 0 {
			dict.hts[0] = dict.hts[1]
			dict.hts[1] = nil
			dict.rehashidx = -1
			return
		}
		// find an nonull slot
		for dict.hts[0].table[dict.rehashidx] == nil {
			dict.rehashidx++
		}
		// migrate all keys in this slot
		entry := dict.hts[0].table[dict.rehashidx]
		for entry != nil {
			ne := entry.next
			idx := dict.HashFunc(entry.Key) & dict.hts[1].mask
			entry.next = dict.hts[1].table[idx]
			dict.hts[1].table[idx] = entry
			dict.hts[0].used--
			dict.hts[1].used++
			entry = ne
		}
		dict.hts[0].table[dict.rehashidx] = nil
		dict.rehashidx++
		step--
	}
}

func nextPower(size int64) int64 {
	for i := INIT_SIZE; i < math.MaxInt64; i *= 2 {
		if i >= size {
			return i
		}
	}
	return -1
}

// 初始化 扩容
func (dict *Dict) expand(size int64) error {
	sz := nextPower(size)
	if dict.isRehashing() || (dict.hts[0] != nil && dict.hts[0].size >= sz) {
		return errs.ExpandError
	}
	var ht htable
	ht.size = sz
	ht.mask = sz - 1
	ht.table = make([]*Entry, sz)
	ht.used = 0
	// check for init
	if dict.hts[0] == nil {
		dict.hts[0] = &ht
		return nil
	}
	// start rehashing
	dict.hts[1] = &ht
	dict.rehashidx = 0
	return nil
}

// 是否要扩容
func (dict *Dict) expandIfNeeded() error {
	// 如果正在rehash，则不需要扩容
	if dict.isRehashing() {
		return nil
	}
	//
	if dict.hts[0] == nil {
		return dict.expand(INIT_SIZE)
	}
	// 当已经使用的空间是现存空间的三倍时，才会触发扩容
	if (dict.hts[0].used > dict.hts[0].size) && (dict.hts[0].used/dict.hts[0].size > FORCE_RATIO) {
		return dict.expand(dict.hts[0].size * GROW_RATIO)
	}
	return nil
}

// return the index of a free slot, return -1 if the key is exists or err.
func (dict *Dict) keyIndex(key *Gobj) int64 {
	err := dict.expandIfNeeded()
	if err != nil {
		return -1
	}
	h := dict.HashFunc(key)
	var idx int64
	for i := 0; i <= 1; i++ {
		idx = h & dict.hts[i].mask
		e := dict.hts[i].table[idx]
		for e != nil {
			if dict.EqualFunc(e.Key, key) {
				return -1
			}
			e = e.next
		}
		if !dict.isRehashing() {
			break
		}
	}
	return idx
}

func (dict *Dict) AddRaw(key *Gobj) *Entry {
	if dict.isRehashing() {
		dict.rehashStep()
	}
	idx := dict.keyIndex(key)
	if idx == -1 {
		return nil
	}
	// add key & return entry
	var ht *htable
	if dict.isRehashing() {
		ht = dict.hts[1]
	} else {
		ht = dict.hts[0]
	}
	var e Entry
	e.Key = key
	key.IncrRefCount()
	e.next = ht.table[idx]
	ht.table[idx] = &e
	ht.used++
	return &e
}

// add a new key-val pair, return err if key exists
func (dict *Dict) Add(key, val *Gobj) error {
	entry := dict.AddRaw(key)
	if entry == nil {
		return errs.KeyExistsError
	}
	entry.Val = val
	val.IncrRefCount()
	return nil
}

func (dict *Dict) Set(key, val *Gobj) {
	if err := dict.Add(key, val); err == nil {
		return
	}
	entry := dict.Find(key)
	entry.Val.DecrRefCount()
	entry.Val = val
	val.IncrRefCount()
}

func (dict *Dict) SetNx(key, val *Gobj) error {
	if err := dict.Add(key, val); err != nil {
		return err
	}
	return nil
}

func freeEntry(e *Entry) {
	e.Key.DecrRefCount()
	e.Val.DecrRefCount()
}

func (dict *Dict) Delete(key *Gobj) error {
	if dict.hts[0] == nil {
		return errs.KeyNotExistError
	}
	if dict.isRehashing() {
		dict.rehashStep()
	}
	// find key & delete & decr refcount
	h := dict.HashFunc(key)
	for i := 0; i <= 1; i++ {
		idx := h & dict.hts[i].mask
		e := dict.hts[i].table[idx]
		var prev *Entry
		for e != nil {
			if dict.EqualFunc(e.Key, key) {
				if prev == nil {
					dict.hts[i].table[idx] = e.next
				} else {
					prev.next = e.next
				}
				freeEntry(e)
				return nil
			}
			prev = e
			e = e.next
		}
		if !dict.isRehashing() {
			break
		}
	}
	// key doesnt exist
	return errs.KeyNotExistError
}

func (dict *Dict) Find(key *Gobj) *Entry {
	if dict.hts[0] == nil {
		return nil
	}
	if dict.isRehashing() {
		dict.rehashStep()
	}
	// find key in both ht
	h := dict.HashFunc(key)
	for i := 0; i <= 1; i++ {
		idx := h & dict.hts[i].mask
		e := dict.hts[i].table[idx]
		for e != nil {
			if dict.EqualFunc(e.Key, key) {
				return e
			}
			e = e.next
		}
		if !dict.isRehashing() {
			break
		}
	}
	return nil
}

func (dict *Dict) Get(key *Gobj) *Gobj {
	entry := dict.Find(key)
	if entry == nil {
		return nil
	}
	return entry.Val
}

func (dict *Dict) RandomGet() *Entry {
	if dict.hts[0] == nil {
		return nil
	}
	t := 0
	if dict.isRehashing() {
		dict.rehashStep()
		if dict.hts[1] != nil && dict.hts[1].used > dict.hts[0].used {
			// simplify the logic, random get in the bigger table
			t = 1
		}
	}
	// random slot
	idx := rand.Int63n(dict.hts[t].size)
	cnt := 0
	for dict.hts[t].table[idx] == nil && cnt < 1000 {
		idx = rand.Int63n(dict.hts[t].size)
		cnt++
	}
	if dict.hts[t].table[idx] == nil {
		return nil
	}
	// random entry
	var listLen int64
	p := dict.hts[t].table[idx]
	for p != nil {
		listLen++
		p = p.next
	}
	listIdx := rand.Int63n(listLen)
	p = dict.hts[t].table[idx]
	for i := int64(0); i < listIdx; i++ {
		p = p.next
	}
	return p
}

func (dict *Dict) IterateDict() [][2]*Gobj {
	iterator := dictIteratorCreate(dict)
	iterator.Iterate()
	return iterator.Gobjs
}
