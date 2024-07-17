package data

import (
	"fmt"
	"math"
	"math/rand"

	"github.com/godis/errs"
)

const SkiplistMaxlevel int = 32 // 跳表的最大长度
const ZskiplistP float64 = 0.25 // 每一个新的节点升高层次的可能性
const threshold = int(ZskiplistP * math.MaxInt)

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
	span    uint64 // 两个节点之间的间隔,以最低层次节点个数为例
}

// 创建新的跳表
func NewSkipList() *SkipList {
	return &SkipList{
		head:  NewSkipListNode(SkiplistMaxlevel, 0, ""),
		level: 1,
		tail:  nil,
	}
}

// NewSkipListNode creates a new skip list node with the specified level, score, and element.
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
	update := make([]*skipListNode, SkiplistMaxlevel) // 要插入的节点在skiplist中的backend
	rank := make([]uint64, SkiplistMaxlevel)          // 记录要插入的节点的backend距离head节点的距离（中间间隔节点的个数）

	x := sl.head
	for i := sl.level - 1; i >= 0; i-- {
		/*
			span记录的是每个level当前节点距离他的frontend的个数，
			所以为了计算新插入节点的span值，需要rank[i]
			A----------B-----------C
			B是新插入的节点
			AC之间的距离已经知道 即A节点对应的span值
			AB之间的距离可以用rank[i] - rank[0]来获得
			这样得到了BC之间的距离 即B节点的span值
			对于遍历一个节点的每一个level之前，rank[level]代表的是这个节点到头节点的距离，就是上一次循环结束后的结果
		*/
		if i == sl.level-1 {
			rank[i] = 0
		} else {
			rank[i] = rank[i+1]
		}
		// find backend
		for x.level[i].forward != nil && (x.level[i].forward.score < score || (x.level[i].forward.score == score && x.level[i].forward.member < member)) {
			rank[i] += x.level[i].span
			x = x.level[i].forward
		}
		// record backend
		update[i] = x
	}

	level := randomLevel()

	/*
		如果新的节点高于当前skiplist的高度，
		需要进行扩充高度
		rank[i] = 0
		设置它的backend为sl.head
	*/
	if level > sl.level {
		for i := sl.level; i < level; i++ {
			rank[i] = 0
			update[i] = sl.head
			update[i].level[i].span = sl.length
		}
		sl.level = level
	}

	node := NewSkipListNode(level, score, member) // 创建新的节点

	for i := 0; i < level; i++ {
		node.level[i] = &skipListLevel{
			forward: update[i].level[i].forward,
			span:    update[i].level[i].span - (rank[0] - rank[i]),
		}
		update[i].level[i].forward = node
		update[i].level[i].span = (rank[0] - rank[i]) + 1
	}

	// Adjust spans for levels above the new node's level.
	for i := level; i < sl.level; i++ {
		update[i].level[i].span++
	}

	// Set the backward pointer of the new node.
	if update[0] != sl.head {
		node.backward = update[0]
	}

	// Update the backward pointer of the node following the new node, if it exists.
	if node.level[0].forward != nil {
		node.level[0].forward.backward = x
	} else {
		// 代表当前skiplist中只有一个节点
		sl.tail = node
	}

	// Increment the length of the skip list.
	sl.length++
}

// randomLevel generates a random level for inserting a new node.
func randomLevel() int {
	level := 1
	for rand.Int() < threshold {
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
func (sl *SkipList) Delete(member string) error {
	update := make([]*skipListNode, SkiplistMaxlevel) // 存储要删除节点的前置节点

	x := sl.head
	for i := sl.level - 1; i >= 0; i-- {
		for x.level[i].forward != nil && (x.level[i].forward.member != member) {
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
		x.level[0].forward.backward = x.backward // backward指向一整个节点
	} else {
		sl.tail = x.backward // tail 节点的高度？？
	}

	// 如果删除的是一个最高节点(唯一)
	// 则需要进行下降
	for sl.level > 1 && sl.head.level[sl.level-1].forward == nil {
		sl.level--
	}
	// 删除成功，跳表的长度减1
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
	startNode := sl.SearchByScore(start)
	if startNode == nil {
		return 0
	}
	n := 0
	endNode := sl.SearchByScore(start)
	for startNode != endNode {
		n++
		startNode = startNode.level[0].forward
	}
	return n
}

func (sl *SkipList) SearchByScore(score float64) *skipListNode {
	head := sl.head
	for head.level[0].forward != nil && head.level[0].forward.score < score {
		head = head.level[0].forward
	}
	return head.level[0].forward
}

func (sl *SkipList) GetRank(member string, score float64) uint64 {
	head := sl.head
	var rank uint64 = 0
	for i := sl.level - 1; i >= 0; i-- {
		for head.level[i].forward != nil && (head.level[i].forward.score < score || (head.level[i].forward.score == score || head.level[i].forward.member < member)) {
			rank += head.level[0].span
			head = head.level[i].forward
		}

		if head != nil && head.level[i].forward.member == member && head.level[i].forward.score == score {
			return rank
		}
	}
	return 0
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
