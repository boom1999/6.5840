package mr

import (
	"errors"
	"sync"
)

type listNode struct {
	data interface{}
	next *listNode
	prev *listNode
}

// addBefore adds a new node before the current node
func (node *listNode) addBefore(data interface{}) {
	newNode := &listNode{data: data, next: node, prev: node.prev}
	node.prev.next = newNode
	node.prev = newNode
}

// addAfter adds a new node after the current node
func (node *listNode) addAfter(data interface{}) {
	newNode := &listNode{data: data, next: node.next, prev: node}
	node.next.prev = newNode
	node.next = newNode
}

// removeBefore removes the node before the current node
func (node *listNode) removeBefore() {
	node.prev = node.prev.prev
	node.prev.next = node
}

// removeAfter removes the node after the current node
func (node *listNode) removeAfter() {
	node.next = node.next.next
	node.next.prev = node
}

type linkedList struct {
	head  listNode
	count int
}

// NewLinkedList creates a new linked list
func NewLinkedList() *linkedList {
	list := linkedList{}
	list.count = 0
	list.head.next = &list.head
	list.head.prev = &list.head
	return &list
}

func (list *linkedList) size() int {
	return list.count
}

// pushFront adds a new element to the front of the list
func (list *linkedList) pushFront(data interface{}) {
	if list.count == 0 {
		list.head = listNode{data: data}
		list.head.next = &list.head
		list.head.prev = &list.head
		list.count++
		return
	}
	list.head.addAfter(data)
	list.count++
}

// pushBack adds a new element to the back of the list
func (list *linkedList) pushBack(data interface{}) {
	if list.count == 0 {
		list.head = listNode{data: data}
		list.head.next = &list.head
		list.head.prev = &list.head
		list.count++
		return
	}
	list.head.addBefore(data)
	list.count++
}

// peekFront return the element at the front of the list
func (list *linkedList) peekFront() (interface{}, error) {
	if list.count == 0 {
		return nil, errors.New("list is empty, cannot peek front")
	}
	return list.head.next.data, nil
}

// peekBack return the element at the back of the list
func (list *linkedList) peekBack() (interface{}, error) {
	if list.count == 0 {
		return nil, errors.New("list is empty, cannot peek back")
	}
	return list.head.prev.data, nil
}

// popFront removes the element at the front of the list
func (list *linkedList) popFront() (interface{}, error) {
	if list.count == 0 {
		return nil, errors.New("list is empty, cannot pop front")
	}
	data := list.head.next.data
	list.head.removeAfter()
	list.count--
	return data, nil
}

// popBack removes the element at the back of the list
func (list *linkedList) popBack() (interface{}, error) {
	if list.count == 0 {
		return nil, errors.New("list is empty, cannot pop back")
	}
	data := list.head.prev.data
	list.head.removeBefore()
	list.count--
	return data, nil
}

type BlockQueue struct {
	list *linkedList
	cond *sync.Cond
}

// NewBlockQueue creates a new block queue
func NewBlockQueue() *BlockQueue {
	queue := &BlockQueue{}
	queue.list = NewLinkedList()
	queue.cond = sync.NewCond(new(sync.Mutex))
	return queue
}

// Size returns the number of elements in the queue
func (queue *BlockQueue) Size() int {
	queue.cond.L.Lock()
	size := queue.list.size()
	queue.cond.L.Unlock()
	return size
}

// Empty returns true if the queue is empty
func (queue *BlockQueue) Empty() bool {
	queue.cond.L.Lock()
	empty := queue.list.size() == 0
	queue.cond.L.Unlock()
	return empty
}

// PutFront adds a new element to the front of the queue
func (queue *BlockQueue) PutFront(data interface{}) {
	queue.cond.L.Lock()
	queue.list.pushFront(data)
	queue.cond.L.Unlock()
	queue.cond.Broadcast()
}

// PutBack adds a new element to the back of the queue
func (queue *BlockQueue) PutBack(data interface{}) {
	queue.cond.L.Lock()
	queue.list.pushBack(data)
	queue.cond.L.Unlock()
	queue.cond.Broadcast()
}

// GetFront removes and returns the element at the front of the queue
func (queue *BlockQueue) GetFront() (interface{}, error) {
	queue.cond.L.Lock()
	for queue.list.size() == 0 {
		queue.cond.Wait()
	}
	data, err := queue.list.popFront()
	queue.cond.L.Unlock()
	return data, err
}

// GetBack removes and returns the element at the back of the queue
func (queue *BlockQueue) GetBack() (interface{}, error) {
	queue.cond.L.Lock()
	for queue.list.size() == 0 {
		queue.cond.Wait()
	}
	data, err := queue.list.popBack()
	queue.cond.L.Unlock()
	return data, err
}

// PopFront removes and returns the element at the front of the queue
func (queue *BlockQueue) PopFront() (interface{}, error) {
	queue.cond.L.Lock()
	data, err := queue.list.popFront()
	queue.cond.L.Unlock()
	return data, err
}

// PopBack removes and returns the element at the back of the queue
func (queue *BlockQueue) PopBack() (interface{}, error) {
	queue.cond.L.Lock()
	data, err := queue.list.popBack()
	queue.cond.L.Unlock()
	return data, err
}

// PeekFront returns the element at the front of the queue without removing it
func (queue *BlockQueue) PeekFront() (interface{}, error) {
	queue.cond.L.Lock()
	for queue.list.size() == 0 {
		queue.cond.Wait()
	}
	data, err := queue.list.peekFront()
	queue.cond.L.Unlock()
	return data, err
}

// PeekBack returns the element at the back of the queue without removing it
func (queue *BlockQueue) PeekBack() (interface{}, error) {
	queue.cond.L.Lock()
	for queue.list.size() == 0 {
		queue.cond.Wait()
	}
	data, err := queue.list.peekBack()
	queue.cond.L.Unlock()
	return data, err
}
