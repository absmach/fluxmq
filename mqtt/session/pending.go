// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package session

import (
	"slices"
	"sync"
)

// pendingQueue holds messages that overflowed a subscriber's send quota until
// acknowledgements free it. Its storage grows as messages arrive and is
// dropped once it empties, so an idle session pays nothing for it; while it
// is in use, consumed slots are reclaimed once they make up half of it, so it
// never holds more than about twice what is queued.
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

// pushFront puts item back at the head, ahead of everything queued. It is for
// an item just popped whose delivery could not proceed, so it ignores the
// capacity that item held a moment ago.
func (q *pendingQueue) pushFront(item pendingItem) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.head > 0 {
		q.head--
		q.items[q.head] = item
		return
	}
	q.items = slices.Insert(q.items, 0, item)
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
	switch {
	case q.head == len(q.items):
		q.items, q.head = nil, 0
	case q.head*2 >= len(q.items):
		// A queue that never quite empties would otherwise keep every
		// consumed slot. Moving the live half down costs no more than the
		// pops that consumed the other half.
		live := copy(q.items, q.items[q.head:])
		clear(q.items[live:])
		q.items, q.head = q.items[:live], 0
	}
	return item, true
}
