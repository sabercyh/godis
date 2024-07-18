package data

import (
	"fmt"
	"math/rand"

	"github.com/godis/errs"
)

const SkiplistMaxlevel int = 32 // 跳表的最大长度
const threshold = 25

type SkipList struct {
	head, tail *skipListNode
	length     uint64
	level      int
}

type skipListNode struct {
	member   string
	score    float64
	backward *skipListNode    // 前驱节点
	level    []*skipListLevel // 每一层次的后续节点
}

type skipListLevel struct {
	forward *skipListNode
	span    uint64 // 最低层次两个节点之间的间隔
}

func NewSkipList() *SkipList {
	return &SkipList{
		head:  NewSkipListNode(SkiplistMaxlevel, 0, ""),
		level: 1,
		tail:  nil,
	}
}

func NewSkipListNode(level int, score float64, member string) *skipListNode {
	return &skipListNode{
		member:   member,
		score:    score,
		level:    InitSkipListLevelSlice(level),
		backward: nil,
	}
}

func NewSkipListLevel(forward *skipListNode, span uint64) *skipListLevel {
	return &skipListLevel{
		forward: forward,
		span:    span,
	}
}

func InitSkipListLevelSlice(level int) []*skipListLevel {
	l := make([]*skipListLevel, level)
	for i := 0; i < level; i++ {
		l[i] = NewSkipListLevel(nil, 0)
	}
	return l
}

func (sl *SkipList) Insert(score float64, member string) {
	update := make([]*skipListNode, SkiplistMaxlevel)
	rank := make([]uint64, SkiplistMaxlevel)
	x := sl.head
	for i := sl.level - 1; i >= 0; i-- {
		if i == sl.level-1 {
			rank[i] = 0
		} else {
			rank[i] = rank[i+1]
		}
		for x.level[i].forward != nil && (x.level[i].forward.score < score || (x.level[i].forward.score == score && x.level[i].forward.member < member)) {
			rank[i] += x.level[i].span
			x = x.level[i].forward
		}
		update[i] = x
	}

	level := randomLevel()

	if level > sl.level {
		for i := sl.level; i < level; i++ {
			rank[i] = 0
			update[i] = sl.head
			update[i].level[i].span = sl.length
		}
		sl.level = level
	}

	node := NewSkipListNode(level, score, member)

	for i := 0; i < level; i++ {
		node.level[i] = &skipListLevel{
			forward: update[i].level[i].forward,
			span:    update[i].level[i].span - (rank[0] - rank[i]),
		}
		update[i].level[i].forward = node
		update[i].level[i].span = (rank[0] - rank[i]) + 1
	}

	for i := level; i < sl.level; i++ {
		update[i].level[i].span++
	}

	if update[0] != sl.head {
		node.backward = update[0]
	}

	if node.level[0].forward != nil {
		node.level[0].forward.backward = node
	} else {
		sl.tail = node
	}

	sl.length++
}

func randomLevel() int {
	level := 1
	for rand.Intn(100) < threshold {
		level++
	}
	return level
}

// PrintSkipList 对齐打印跳表结构
func (sl *SkipList) PrintSkipList() {
	fmt.Println("SkipList structure:")

	// 预先遍历跳表以确定每个元素的最大宽度
	elementWidths := make(map[*skipListNode]int)
	maxWidth := 0
	for i := 0; i < sl.level; i++ {
		current := sl.head.level[i].forward
		for current != nil {
			width := len(fmt.Sprintf("%v (score: %f)", current.member, current.score))
			if width > elementWidths[current] {
				elementWidths[current] = width
			}
			if width > maxWidth {
				maxWidth = width
			}
			current = current.level[i].forward
		}
	}

	// 根据最大宽度对齐打印每个元素
	for i := sl.level - 1; i >= 0; i-- {
		current := sl.head.level[i].forward
		fmt.Printf("Level %d: ", i)
		for current != nil {
			elementStr := fmt.Sprintf("%v (score: %f)", current.member, current.score)
			fmt.Printf("%-*s -> ", maxWidth, elementStr) // 使用最大宽度对齐
			current = current.level[i].forward
		}
		fmt.Println("nil")
	}
}
func (sl *SkipList) Delete(member string, score float64) error {

	update := make([]*skipListNode, SkiplistMaxlevel) // 存储要删除节点的前置节点

	x := sl.head

	for i := sl.level - 1; i >= 0; i-- {
		for x.level[i].forward != nil && (x.level[i].forward.score < score || (x.level[i].forward.score == score && x.level[i].forward.member < member)) {
			x = x.level[i].forward
		}
		update[i] = x
	}
	x = x.level[0].forward // 定位到要删除的节点
	// 判断当前查找到的节点是不是要删除的节点
	if x != nil && x.member == member {
		// 进行删除
		for i := 0; i < sl.level; i++ {
			if update[i].level[i].forward == x {
				update[i].level[i].forward = x.level[i].forward
				update[i].level[i].span += x.level[i].span - 1
			} else {
				update[i].level[i].span -= 1
			}
		}

		/* 如果删除的不是skiplist的尾部节点，进行backward指针的更新*/
		if x.level[0].forward != nil {
			x.level[0].forward.backward = x.backward
		} else {
			sl.tail = x.backward
		}

		/*当前节点有可能是唯一一个很高层次的节点，需要更新 sl.level*/
		for sl.level > 1 && sl.head.level[sl.level-1].forward == nil {
			sl.level--
		}

		/*删除成功，skiplist的长度减一*/
		sl.length--
		return nil
	}
	return errs.SkipListDeleteNodeError
}

// DeleteNode 删除skiplist中的节点
func (sl *SkipList) DeleteNode(x *skipListNode, update []*skipListNode) {
	// 进行链表的断开和重新连接并且更新span
	for i := 0; i < sl.level; i-- {
		if update[i].level[i].forward == x {
			update[i].level[i].forward = x.level[i].forward
			update[i].level[i].span += x.level[i].span - 1
		} else {
			update[i].level[i].span -= 1
		}
	}
	// 判断删除的节点是否是最后一个节点，如果不是，需要进行backend的更新
	if x.level[0].forward != nil {
		x.level[0].forward.backward = x.backward
	} else {
		sl.tail = x.backward
	}

	for sl.level > 1 && sl.head.level[sl.level-1].forward == nil {
		sl.level--
	}

	sl.length--
}

// UpdateScore 更新member-score的分数
func (sl *SkipList) UpdateScore(score float64, member string, newScore float64) {
	var update [SkiplistMaxlevel]*skipListNode // 存储目标节点在每个level的前驱节点
	// 查找目标节点在每个level的前驱节点
	x := sl.head
	for i := sl.level - 1; i >= 0; i-- {
		for x.level[i].forward != nil && (x.level[i].forward.score < score || (x.level[i].forward.score == score && x.level[i].forward.member < member)) {
			x = x.level[i].forward
		}
		update[i] = x
	}
	// 判断这个节点是不是我们需要的节点
	x = x.level[0].forward
	// 如果这个节点不是我们要修改的节点,则查找失败
	if !(x != nil && x.member == member && x.score == score) {
		return
	}
	/*
		如果新修改的节点的分数不会影响这个节点在skiplist中的位置则不需要进行位置的移动
		可能不需要更改位置的情况
		当前节点位于开头
			1. 只有1个
			2. 新的分数比第二节点的分数小
		当前节点位于结尾
			1. 只有一个
			2. 新的分数比前一节点的分数大
		当前节点位于中间
			1. 新的分数大于前一个节点、小于后一个节点
	*/
	if (x.backward == nil || x.backward.score < newScore) && (x.level[0].forward == nil || x.level[0].forward.score > newScore) {
		x.score = newScore
		return
	}
	/*
		如果新插入的节点会改变原来节点的相对位置，则需要
		1. 删除该节点
		2. 插入新的节点
	*/
	sl.Insert(score, member)
}

func (sl *SkipList) SearchByRangeScore(start, end float64) int {
	var n int

	head := sl.head.level[0].forward
	for head != nil && head.score < start {
		head = head.level[0].forward
	}

	if head == nil {
		return 0
	}

	for head != nil && head.score <= end {
		n++
		head = head.level[0].forward
	}

	return n
}

func (sl *SkipList) GetRank(member string, score float64) uint64 {
	node := sl.head
	var rank uint64 = 0
	for i := sl.level - 1; i >= 0; i-- {
		for node.level[i].forward != nil && (node.level[i].forward.score < score || (node.level[i].forward.score == score && node.level[i].forward.member < member)) {
			rank += node.level[i].span
			node = node.level[i].forward
		}

		if node.level[i].forward != nil && node.level[i].forward.member == member && node.level[i].forward.score == score {
			return rank + node.level[i].span
		}
	}
	return 1
}

func (sl *SkipList) getElememtByRank(rank uint64) *skipListNode {
	return sl.getElememtByRankFromNode(sl.head, rank, sl.level)
}

/*
与知道某个节点的rank区分开
没有加从head到第一个节点的span，rank从0开始
*/
func (sl *SkipList) getElememtByRankFromNode(start *skipListNode, rank uint64, level int) *skipListNode {
	head := start
	var traversal uint64 = 0
	for i := level - 1; i >= 0; i-- {
		// 省略判断
		for head.level[i].forward != nil && (traversal+head.level[i].span <= rank) {
			traversal += head.level[i].span
			head = head.level[i].forward
		}
		if traversal == rank {
			return head
		}
	}
	return nil
}

func (sl *SkipList) GetLen() uint64 {
	return sl.length
}
