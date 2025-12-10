package session

import (
	"fmt"
	"sync"

	"github.com/dborovcanin/mqtt/store"
)

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

// enqueue adds a message to the queue.
// Returns ErrQueueFull if the queue is at capacity.
func (q *messageQueue) enqueue(msg *store.Message) error {
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

// dequeue removes and returns the first message from the queue.
// Returns nil if the queue is empty.
func (q *messageQueue) dequeue() *store.Message {
	q.mu.Lock()
	defer q.mu.Unlock()

	if len(q.messages) == 0 {
		return nil
	}

	msg := q.messages[0]
	q.messages = q.messages[1:]
	return msg
}

// peek returns the first message without removing it.
func (q *messageQueue) peek() *store.Message {
	q.mu.Lock()
	defer q.mu.Unlock()

	if len(q.messages) == 0 {
		return nil
	}
	return q.messages[0]
}

// len returns the number of messages in the queue.
func (q *messageQueue) len() int {
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

// drain removes and returns all messages from the queue.
func (q *messageQueue) drain() []*store.Message {
	q.mu.Lock()
	defer q.mu.Unlock()

	msgs := q.messages
	q.messages = make([]*store.Message, 0)
	return msgs
}

// Offline queue operations
func (s *Session) EnqueueOffline(msg *store.Message) error {
	return s.offlineQueue.enqueue(msg)
}

func (s *Session) DequeueOffline() *store.Message {
	return s.offlineQueue.dequeue()
}

func (s *Session) DrainOfflineQueue() []*store.Message {
	return s.offlineQueue.drain()
}

func (s *Session) OfflineQueueLen() int {
	return s.offlineQueue.len()
}

func (s *Session) OfflineQueuePeek() *store.Message {
	return s.offlineQueue.peek()
}
