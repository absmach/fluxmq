// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package messages

import (
	"sync"

	"github.com/absmach/fluxmq/storage"
)

// Queue defines operations on offline message queue.
type Queue interface {
	Enqueue(msg *storage.Message) error
	Dequeue() *storage.Message
	Len() int
	IsEmpty() bool
	IsFull() bool
	Peek() *storage.Message
	Drain() []*storage.Message
}

// queue is a queue for offline messages (QoS > 0).
type queue struct {
	mu          sync.Mutex
	messages    []*storage.Message
	maxSize     int
	evictOnFull bool
}

// NewMessageQueue creates a new message queue.
// If evictOnFull is true, the oldest message is evicted when the queue is full.
// If false, Enqueue returns ErrQueueFull.
func NewMessageQueue(maxSize int, evictOnFull bool) *queue {
	if maxSize <= 0 {
		maxSize = 1000
	}
	return &queue{
		messages:    make([]*storage.Message, 0),
		maxSize:     maxSize,
		evictOnFull: evictOnFull,
	}
}

// Enqueue adds a message to the queue.
// If the queue is at capacity, the oldest message is evicted.
func (q *queue) Enqueue(msg *storage.Message) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	if len(q.messages) >= q.maxSize {
		if !q.evictOnFull {
			return ErrQueueFull
		}
		evicted := q.messages[0]
		evicted.ReleasePayload()
		storage.ReleaseMessage(evicted)
		q.messages = q.messages[1:]
	}

	cp := storage.CopyMessage(msg)
	q.messages = append(q.messages, cp)
	return nil
}

// Dequeue removes and returns the first message from the queue.
// Returns nil if the queue is empty.
func (q *queue) Dequeue() *storage.Message {
	q.mu.Lock()
	defer q.mu.Unlock()

	if len(q.messages) == 0 {
		return nil
	}

	msg := q.messages[0]
	q.messages = q.messages[1:]
	return msg
}

// Peek returns the first message without removing it.
func (q *queue) Peek() *storage.Message {
	q.mu.Lock()
	defer q.mu.Unlock()

	if len(q.messages) == 0 {
		return nil
	}
	return q.messages[0]
}

// Len returns the number of messages in the queue.
func (q *queue) Len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.messages)
}

// IsEmpty returns true if the queue is empty.
func (q *queue) IsEmpty() bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.messages) == 0
}

// IsFull returns true if the queue is at capacity.
func (q *queue) IsFull() bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.messages) >= q.maxSize
}

// Drain removes and returns all messages from the queue.
func (q *queue) Drain() []*storage.Message {
	q.mu.Lock()
	defer q.mu.Unlock()

	msgs := q.messages
	q.messages = make([]*storage.Message, 0)
	return msgs
}
