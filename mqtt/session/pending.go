// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package session

import "sync"

// pendingQueue holds messages that overflowed a subscriber's send quota until
// acknowledgements free it. Its storage grows as messages arrive and is
// dropped once it empties, so an idle session pays nothing for it.
//
// It is safe for concurrent use.
type pendingQueue struct {
	mu       sync.Mutex
	items    []pendingItem
	head     int
	capacity int
}

func newPendingQueue(capacity int) *pendingQueue {
	return &pendingQueue{capacity: capacity}
}

// push appends item and reports whether there was room for it.
func (q *pendingQueue) push(item pendingItem) bool {
	q.mu.Lock()
	defer q.mu.Unlock()

	if len(q.items)-q.head >= q.capacity {
		return false
	}
	q.items = append(q.items, item)
	return true
}

// pop removes and returns the oldest item.
func (q *pendingQueue) pop() (pendingItem, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.head == len(q.items) {
		return pendingItem{}, false
	}
	item := q.items[q.head]
	q.items[q.head] = pendingItem{}
	q.head++
	if q.head == len(q.items) {
		q.items, q.head = nil, 0
	}
	return item, true
}
