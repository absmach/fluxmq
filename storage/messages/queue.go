// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package messages

import (
	"sync"

	"github.com/absmach/fluxmq/message"
)

// Queue defines operations on offline message queue.
type Queue interface {
	// Enqueue borrows msg and stores an independent envelope clone.
	Enqueue(msg *message.Envelope) error
	// Dequeue transfers ownership of the returned envelope to the caller.
	Dequeue() *message.Envelope
	Len() int
	IsEmpty() bool
	IsFull() bool
	// Peek returns a borrowed envelope owned by the queue.
	Peek() *message.Envelope
	// Drain transfers ownership of every returned envelope to the caller.
	Drain() []*message.Envelope
}

// queue is a queue for offline messages (QoS > 0).
type queue struct {
	mu          sync.Mutex
	messages    []*message.Envelope
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
		messages:    make([]*message.Envelope, 0),
		maxSize:     maxSize,
		evictOnFull: evictOnFull,
	}
}

// Enqueue adds a message to the queue.
// If the queue is at capacity, the oldest message is evicted.
func (q *queue) Enqueue(msg *message.Envelope) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	if len(q.messages) >= q.maxSize {
		if !q.evictOnFull {
			return ErrQueueFull
		}
		evicted := q.messages[0]
		message.Release(evicted)
		q.messages = q.messages[1:]
	}

	cp := msg.Clone()
	q.messages = append(q.messages, cp)
	return nil
}

// Dequeue removes and returns the first message from the queue.
// Returns nil if the queue is empty.
func (q *queue) Dequeue() *message.Envelope {
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
func (q *queue) Peek() *message.Envelope {
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
func (q *queue) Drain() []*message.Envelope {
	q.mu.Lock()
	defer q.mu.Unlock()

	msgs := q.messages
	q.messages = make([]*message.Envelope, 0)
	return msgs
}
