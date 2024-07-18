package data

import (
	"bytes"
	"strconv"

	"github.com/godis/errs"
	"github.com/godis/util"
)

type Node struct {
	Val  *Gobj
	next *Node
	prev *Node
}

type ListType struct {
	EqualFunc func(a, b *Gobj) bool
}

type List struct {
	ListType
	Head   *Node
	Tail   *Node
	length int
}

func ListCreate(listType ListType) *List {
	var list List
	list.ListType = listType
	return &list
}

func (list *List) Length() int {
	return list.length
}

func (list *List) First() *Node {
	return list.Head
}

func (list *List) Last() *Node {
	return list.Tail
}

func (list *List) Rem(val *Gobj) error {
	node := list.Find(val)
	if node == nil {
		return errs.NodeNotFoundError
	}
	list.DelNode(node)
	return nil
}

func (list *List) Set(index int, val *Gobj) error {
	node := list.Index(index)
	if node == nil {
		return errs.OutOfRangeError
	}
	node.Val = val
	node.Val.IncrRefCount()
	return nil
}

func (list *List) Index(index int) *Node {
	if index < 0 {
		return list.ReverseIndex(index)
	}
	return list.ForwardIndex(index)
}

func (list *List) ForwardIndex(index int) *Node {
	if index >= list.length {
		return nil
	}
	node := list.Head
	for i := 0; i < index; i++ {
		node = node.next
	}
	return node
}

func (list *List) RangeVal(left, right int) []byte {
	reply := new(bytes.Buffer)
	if left < 0 {
		left = list.length + left
	}
	if right < 0 {
		right = list.length + right
	}
	if left > right {
		return reply.Bytes()
	}
	if left < 0 {
		left = 0
	}
	if right >= list.length {
		right = list.length - 1
	}
	node := list.Index(left)
	reply.WriteByte(byte('*'))
	reply.WriteString(strconv.Itoa(right - left + 1))
	reply.WriteString("\r\n")
	for i := 0; i <= right-left; i++ {
		val := node.Val.StrVal()
		reply.WriteByte(byte('$'))
		reply.WriteString(strconv.Itoa(len(val)))
		reply.WriteString("\r\n")
		reply.WriteString(val)
		reply.WriteString("\r\n")
		node = node.next
	}
	return reply.Bytes()
}

func (list *List) RangeSlowlog() []byte {
	reply := new(bytes.Buffer)
	reply.WriteByte(byte('*'))
	reply.WriteString(strconv.Itoa(list.length))
	reply.WriteString("\r\n")
	node := list.Head
	for i := 0; i < list.length; i++ {
		reply.WriteString("*4\r\n:")
		logEntry := node.Val.Val_.(*SlowLogEntry)
		reply.WriteString(strconv.Itoa(int(logEntry.ID)))
		reply.WriteString("\r\n:")
		reply.WriteString(strconv.Itoa(int(logEntry.Time)))
		reply.WriteString("\r\n:")
		reply.WriteString(strconv.Itoa(int(logEntry.Duration)))
		reply.WriteString("\r\n*")
		reply.WriteString(strconv.Itoa(logEntry.Argc))
		reply.WriteString("\r\n")

		for i := 0; i < logEntry.Argc; i++ {
			val := logEntry.Robj[i].StrVal()
			reply.WriteString("$")
			reply.WriteString(strconv.Itoa(len(val)))
			reply.WriteString("\r\n")
			reply.WriteString(val)
			reply.WriteString("\r\n")
		}
		node = node.next
	}
	return reply.Bytes()
}

func (list *List) ReverseIndex(index int) *Node {
	if util.Abs(index) > list.length {
		return nil
	}
	node := list.Tail
	for i := index + 1; i < 0; i++ {
		node = node.prev
	}
	return node
}

func (list *List) Find(val *Gobj) *Node {
	p := list.Head
	for p != nil {
		if list.EqualFunc(p.Val, val) {
			break
		}
		p = p.next
	}
	return p
}

func (list *List) Append(val *Gobj) {
	var n Node
	n.Val = val
	if list.Head == nil {
		list.Head = &n
		list.Tail = &n
	} else {
		n.prev = list.Tail
		list.Tail.next = &n
		list.Tail = list.Tail.next
	}
	list.length += 1
}

func (list *List) LPush(val *Gobj) {
	var n Node
	n.Val = val
	n.Val.IncrRefCount()
	if list.Head == nil {
		list.Head = &n
		list.Tail = &n
	} else {
		n.next = list.Head
		list.Head.prev = &n
		list.Head = &n
	}
	list.length += 1
}

func (list *List) LPop() *Gobj {
	if list.Head == nil {
		return nil
	}
	val := list.Head.Val
	list.DelNode(list.Head)
	return val
}

func (list *List) RPush(val *Gobj) {
	var n Node
	n.Val = val
	n.Val.IncrRefCount()
	if list.Head == nil {
		list.Head = &n
		list.Tail = &n
	} else {
		n.prev = list.Tail
		list.Tail.next = &n
		list.Tail = &n
	}
	list.length += 1
}

func (list *List) RPop() *Gobj {
	if list.Tail == nil {
		return nil
	}
	val := list.Tail.Val
	list.DelNode(list.Tail)
	return val
}

func (list *List) DelNode(n *Node) {
	if n == nil {
		return
	}
	if list.Head == n {
		if n.next != nil {
			n.next.prev = nil
		}
		list.Head = n.next
		n.next = nil
	} else if list.Tail == n {
		if n.prev != nil {
			n.prev.next = nil
		}
		list.Tail = n.prev
		n.prev = nil
	} else {
		if n.prev != nil {
			n.prev.next = n.next
		}
		if n.next != nil {
			n.next.prev = n.prev
		}
		n.prev = nil
		n.next = nil
	}
	list.length -= 1
}

func (list *List) Delete(val *Gobj) {
	list.DelNode(list.Find(val))
}

func (list *List) Clear() {
	for list.Head != nil {
		list.DelNode(list.Head)
	}
	list = nil
}
