package messages

import (
	"fmt"
	"sync"

	"github.com/dborovcanin/mqtt/store"
)

// Queue defines operations on offline message queue.
type Queue interface {
	Enqueue(msg *store.Message) error
	Dequeue() *store.Message
	Len() int
	IsEmpty() bool
	IsFull() bool
	Peek() *store.Message
	Drain() []*store.Message
}

// messageQueue is a queue for offline messages (QoS > 0).
type messageQueue struct {
	mu       sync.Mutex
	messages []*store.Message
	maxSize  int
}

// NewMessageQueue creates a new message queue.
func NewMessageQueue(maxSize int) *messageQueue {
	if maxSize <= 0 {
		maxSize = 1000
	}
	return &messageQueue{
		messages: make([]*store.Message, 0),
		maxSize:  maxSize,
	}
}

// Enqueue adds a message to the queue.
// Returns ErrQueueFull if the queue is at capacity.
func (q *messageQueue) Enqueue(msg *store.Message) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	if len(q.messages) >= q.maxSize {
		return fmt.Errorf("enqueue message for topic %s (current: %d, max: %d): %w",
			msg.Topic, len(q.messages), q.maxSize, ErrQueueFull)
	}

	cp := store.CopyMessage(msg)
	q.messages = append(q.messages, cp)
	return nil
}

// Dequeue removes and returns the first message from the queue.
// Returns nil if the queue is empty.
func (q *messageQueue) Dequeue() *store.Message {
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
func (q *messageQueue) Peek() *store.Message {
	q.mu.Lock()
	defer q.mu.Unlock()

	if len(q.messages) == 0 {
		return nil
	}
	return q.messages[0]
}

// Len returns the number of messages in the queue.
func (q *messageQueue) Len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.messages)
}

// IsEmpty returns true if the queue is empty.
func (q *messageQueue) IsEmpty() bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.messages) == 0
}

// IsFull returns true if the queue is at capacity.
func (q *messageQueue) IsFull() bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.messages) >= q.maxSize
}

// Drain removes and returns all messages from the queue.
func (q *messageQueue) Drain() []*store.Message {
	q.mu.Lock()
	defer q.mu.Unlock()

	msgs := q.messages
	q.messages = make([]*store.Message, 0)
	return msgs
}
