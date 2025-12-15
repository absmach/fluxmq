package memory

import (
	"strings"
	"sync"

	"github.com/absmach/mqtt/storage"
)

var _ storage.MessageStore = (*MessageStore)(nil)

// MessageStore is an in-memory implementation of store.MessageStore.
type MessageStore struct {
	mu   sync.RWMutex
	data map[string]*storage.Message
}

// NewMessageStore creates a new in-memory message store.
func NewMessageStore() *MessageStore {
	return &MessageStore{
		data: make(map[string]*storage.Message),
	}
}

// Store stores a message.
func (s *MessageStore) Store(key string, msg *storage.Message) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.data[key] = storage.CopyMessage(msg)
	return nil
}

// Get retrieves a message by key.
func (s *MessageStore) Get(key string) (*storage.Message, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	msg, ok := s.data[key]
	if !ok {
		return nil, storage.ErrNotFound
	}
	return storage.CopyMessage(msg), nil
}

// Delete removes a message.
func (s *MessageStore) Delete(key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.data, key)
	return nil
}

// List returns all messages matching a key prefix.
func (s *MessageStore) List(prefix string) ([]*storage.Message, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var result []*storage.Message
	for key, msg := range s.data {
		if strings.HasPrefix(key, prefix) {
			result = append(result, storage.CopyMessage(msg))
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
