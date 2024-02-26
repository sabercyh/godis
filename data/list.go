package data

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
