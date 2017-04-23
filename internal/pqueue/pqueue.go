package pqueue

import (
	"container/heap"
)

type Item struct {
	Value    interface{}	// 元素值
	Priority int64		// 优先级
	Index    int		// 保存自身的索引值
}

// this is a priority queue as implemented by a min heap
// ie. the 0th element is the *lowest* value
// 基于小堆实现的的优先队列

type PriorityQueue []*Item

// 创建容量为capacity的切片
func New(capacity int) PriorityQueue {
	return make(PriorityQueue, 0, capacity)
}

// 返回切片的长度
func (pq PriorityQueue) Len() int {
	return len(pq)
}

// 比较索引i，j元素优先级大小（）
func (pq PriorityQueue) Less(i, j int) bool {
	return pq[i].Priority < pq[j].Priority
}

// 交换索引i，j元素
func (pq PriorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].Index = i
	pq[j].Index = j
}

// 添加x
func (pq *PriorityQueue) Push(x interface{}) {
	n := len(*pq)
	c := cap(*pq)
	if n+1 > c {
		// 以两倍的方式扩容
		npq := make(PriorityQueue, n, c*2)
		copy(npq, *pq)
		*pq = npq
	}
	*pq = (*pq)[0 : n+1]
	item := x.(*Item)
	item.Index = n
	(*pq)[n] = item
}

// 弹出最小值
func (pq *PriorityQueue) Pop() interface{} {
	n := len(*pq)
	c := cap(*pq)
	if n < (c/2) && c > 25 {
		npq := make(PriorityQueue, n, c/2)
		copy(npq, *pq)
		*pq = npq
	}
	item := (*pq)[n-1]
	item.Index = -1
	*pq = (*pq)[0 : n-1]
	return item
}

// 最小元素的优先级大于max才返回Item
func (pq *PriorityQueue) PeekAndShift(max int64) (*Item, int64) {
	if pq.Len() == 0 {
		return nil, 0
	}

	item := (*pq)[0]
	if item.Priority > max {
		return nil, item.Priority - max
	}
	heap.Remove(pq, 0)  // Remove removes the element at index i from the heap.

	return item, 0
}
