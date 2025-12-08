package memory

import (
	"strings"
	"sync"

	"github.com/dborovcanin/mqtt/store"
)

// MessageStore is an in-memory implementation of store.MessageStore.
type MessageStore struct {
	mu   sync.RWMutex
	data map[string]*store.Message
}

// NewMessageStore creates a new in-memory message store.
func NewMessageStore() *MessageStore {
	return &MessageStore{
		data: make(map[string]*store.Message),
	}
}

// Store stores a message.
func (s *MessageStore) Store(key string, msg *store.Message) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Deep copy the message to avoid mutation
	s.data[key] = copyMessage(msg)
	return nil
}

// Get retrieves a message by key.
func (s *MessageStore) Get(key string) (*store.Message, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	msg, ok := s.data[key]
	if !ok {
		return nil, store.ErrNotFound
	}
	return copyMessage(msg), nil
}

// Delete removes a message.
func (s *MessageStore) Delete(key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.data, key)
	return nil
}

// List returns all messages matching a key prefix.
func (s *MessageStore) List(prefix string) ([]*store.Message, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var result []*store.Message
	for key, msg := range s.data {
		if strings.HasPrefix(key, prefix) {
			result = append(result, copyMessage(msg))
		}
	}
	return result, nil
}

// DeleteByPrefix removes all messages matching a prefix.
func (s *MessageStore) DeleteByPrefix(prefix string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	for key := range s.data {
		if strings.HasPrefix(key, prefix) {
			delete(s.data, key)
		}
	}
	return nil
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

// Ensure MessageStore implements store.MessageStore.
var _ store.MessageStore = (*MessageStore)(nil)
