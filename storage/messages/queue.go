// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package messages

import (
	"fmt"
	"sync"

	"github.com/absmach/mqtt/storage"
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
	mu       sync.Mutex
	messages []*storage.Message
	maxSize  int
}

// NewMessageQueue creates a new message queue.
func NewMessageQueue(maxSize int) *queue {
	if maxSize <= 0 {
		maxSize = 1000
	}
	return &queue{
		messages: make([]*storage.Message, 0),
		maxSize:  maxSize,
	}
}

// Enqueue adds a message to the queue.
// Returns ErrQueueFull if the queue is at capacity.
func (q *queue) Enqueue(msg *storage.Message) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	if len(q.messages) >= q.maxSize {
		return fmt.Errorf("enqueue message for topic %s (current: %d, max: %d): %w",
			msg.Topic, len(q.messages), q.maxSize, ErrQueueFull)
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
