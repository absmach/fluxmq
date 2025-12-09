package session

import (
	"fmt"
	"sync"

	"github.com/dborovcanin/mqtt/store"
)

// MessageQueue is a queue for offline messages (QoS > 0).
type MessageQueue struct {
	mu       sync.Mutex
	messages []*store.Message
	maxSize  int
}

// NewMessageQueue creates a new message queue.
func NewMessageQueue(maxSize int) *MessageQueue {
	if maxSize <= 0 {
		maxSize = 1000
	}
	return &MessageQueue{
		messages: make([]*store.Message, 0),
		maxSize:  maxSize,
	}
}

// Enqueue adds a message to the queue.
// Returns ErrQueueFull if the queue is at capacity.
func (q *MessageQueue) Enqueue(msg *store.Message) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	if len(q.messages) >= q.maxSize {
		return fmt.Errorf("enqueue message for topic %s (current: %d, max: %d): %w",
			msg.Topic, len(q.messages), q.maxSize, ErrQueueFull)
	}

	// Deep copy the message
	cp := copyMessage(msg)
	q.messages = append(q.messages, cp)
	return nil
}

// Dequeue removes and returns the first message from the queue.
// Returns nil if the queue is empty.
func (q *MessageQueue) Dequeue() *store.Message {
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
func (q *MessageQueue) Peek() *store.Message {
	q.mu.Lock()
	defer q.mu.Unlock()

	if len(q.messages) == 0 {
		return nil
	}
	return q.messages[0]
}

// Len returns the number of messages in the queue.
func (q *MessageQueue) Len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.messages)
}

// IsEmpty returns true if the queue is empty.
func (q *MessageQueue) IsEmpty() bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.messages) == 0
}

// IsFull returns true if the queue is at capacity.
func (q *MessageQueue) IsFull() bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.messages) >= q.maxSize
}

// Clear removes all messages from the queue.
func (q *MessageQueue) Clear() {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.messages = q.messages[:0]
}

// Drain removes and returns all messages from the queue.
func (q *MessageQueue) Drain() []*store.Message {
	q.mu.Lock()
	defer q.mu.Unlock()

	msgs := q.messages
	q.messages = make([]*store.Message, 0)
	return msgs
}

// MaxSize returns the maximum queue size.
func (q *MessageQueue) MaxSize() int {
	return q.maxSize
}

// SetMaxSize updates the maximum queue size.
func (q *MessageQueue) SetMaxSize(size int) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.maxSize = size
}

// copyMessage creates a deep copy of a message.
func copyMessage(msg *store.Message) *store.Message {
	if msg == nil {
		return nil
	}

	cp := &store.Message{
		Topic:    msg.Topic,
		QoS:      msg.QoS,
		Retain:   msg.Retain,
		PacketID: msg.PacketID,
		Expiry:   msg.Expiry,
	}

	if len(msg.Payload) > 0 {
		cp.Payload = make([]byte, len(msg.Payload))
		copy(cp.Payload, msg.Payload)
	}

	if len(msg.Properties) > 0 {
		cp.Properties = make(map[string]string, len(msg.Properties))
		for k, v := range msg.Properties {
			cp.Properties[k] = v
		}
	}

	return cp
}
