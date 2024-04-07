package skiplist

import (
	"fmt"
	"math"
	"math/rand"
	"log"
)

// sds is a simple dynamic string type based on Go's string type.
type sds string

// SkiplistMaxlevel defines the maximum level a skip list node can have.
const SkiplistMaxlevel int = 32

// ZskiplistP is the probability factor used to determine the level of a new node.
const ZskiplistP float64 = 0.25

// threshold is used in randomLevel function to decide the new node's level.
const threshold = int(ZskiplistP * math.MaxInt)

// skipListNode represents a node in the skip list.
type skipListNode struct {
	ele      sds              // Key stored in the node.
	score    float64          // The score of the key-value pair, used for ordering.
	backward *skipListNode    // Pointer to the previous node at the bottom level.
	level    []*skipListLevel // Pointers to next nodes at different levels.
}

// skipListLevel holds the forward pointer and span for a level in a skipListNode.
type skipListLevel struct {
	forward *skipListNode // Pointer to the next node.
	span    uint64        // Number of nodes between this node and the next node at this level.
}

// SkipList represents the whole skip list structure.
type SkipList struct {
	head, tail *skipListNode // Head and tail pointers of the skip list.
	length     uint64        // Number of elements in the skip list.
	level      int           // Current level of the skip list.
}

// NewSkipList creates a new empty skip list.
func NewSkipList() *SkipList {
	head := NewSkipListNode(SkiplistMaxlevel, 0, "")
	skipList := &SkipList{
		head:  head,
		level: 1,
		tail:  nil,
	}
	for i := 0; i < SkiplistMaxlevel; i++ {
		skipList.head.level[i] = &skipListLevel{}
	}
	return skipList
}

// NewSkipListNode creates a new skip list node with the specified level, score, and element.
func NewSkipListNode(level int, score float64, ele sds) *skipListNode {
	slNode := &skipListNode{
		ele:      ele,
		score:    score,
		level:    make([]*skipListLevel, level),
		backward: nil,
	}
	return slNode
}

/*
Insert inserts a new element with a given score into the skip list.
*/

func (sl *SkipList) Insert(score float64, ele sds) {

	var update [SkiplistMaxlevel]*skipListNode // Array to hold pointers to the nodes before the insertion point at each level.
	var rank [SkiplistMaxlevel]uint64          // Array to hold the rank (distance from the head) at each level for the new node.

	x := sl.head

	for i := sl.level - 1; i >= 0; i-- {
		// Traverse forward pointers at level i to find the insertion point.
		if i == sl.level-1 {
			rank[i] = 0
		} else {
			rank[i] = rank[i+1]
		}
		// Traverse through the current level to find the correct position for the new node.
		for x.level[i].forward != nil && (x.level[i].forward.ele < ele || (x.level[i].forward.ele == ele && x.level[i].forward.score < score)) {
			rank[i] += x.level[i].span // Update the rank when moving forward.
			x = x.level[i].forward
		}

		update[i] = x // Store the pointer to the node before the insertion point at level i.
	}

	// Determine the level of the new node.
	level := randomLevel()

	// If the new node's level is higher than the current skip list level, initialize the new levels.
	if level > sl.level {
		for i := sl.level; i < level; i++ {
			rank[i] = 0
			update[i] = sl.head
			update[i].level[i].span = sl.length
		}
		sl.level = level
	}

	// Create the new node.
	node := NewSkipListNode(level, score, ele)

	// Insert the new node and adjust forward pointers and spans.
	for i := 0; i < level; i++ {
		node.level[i] = &skipListLevel{
			forward: update[i].level[i].forward,
			span:    update[i].level[i].span - (rank[0] - rank[i]),
		}
		update[i].level[i].forward = node
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
			width := len(fmt.Sprintf("%v (score: %f)", current.ele, current.score))
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
			elementStr := fmt.Sprintf("%v (score: %f)", current.ele, current.score)
			fmt.Printf("%-*s -> ", maxWidth, elementStr) // 使用最大宽度对齐
			current = current.level[i].forward
		}
		fmt.Println("nil")
	}
}
func (sl *SkipList) Delete(score float64, ele sds) {
	// 首先找到要删除的节点的前一个节点 使用update数组来存储这些节点
	var update [SkiplistMaxlevel]*skipListNode

	x := sl.head

	for i := sl.level - 1; i >= 0; i-- {
		for x.level[i].forward != nil && (x.ele < ele || (x.ele == ele && x.score < score)) {
			x = x.level[i].forward
		}
		update[i] = x
	}

	x = x.level[0].forward
	// 判断当前查找到的节点是不是要删除的节点
	if x != nil && x.ele == ele && x.score == score {
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

	} else {

	}
}

func (sl *SkipList) UpdateScore(score float64, ele sds, newScore float64) {
	var update [SkiplistMaxlevel]*skipListNode

	/*寻找要修改的节点的位置*/
	x := sl.head
	for i := sl.level - 1; i >= 0; i++ {
		for x.level[i].forward != nil && (x.ele < ele || (x.ele == ele && x.score < score)) {
			x = x.level[i].forward
		}
		update[i] = x
	}

	/*判断这个节点是不是我们需要的节点*/
	x = x.level[0].forward
	if x != nil && x.ele == ele && x.score == score {
		// 删除节点
		log.Println("删除节点")
		// 增加节点
	}

	/*
		如果更改分数的节点不需要修改节点在skiplist中的位置，则不需要进行位置的移动
	*/

	/*
		可能不需要更改位置的情况
		1. 当前skiplist中只有一个节点
		2. 前一个节点的分数比新增加的分数小
		3.
	*/

	if (x.backward == nil || x.backward.score < newScore) && (x.level[0].forward == nil || x.level[0].forward.score > newScore) {
		x.score = newScore
	}

}
