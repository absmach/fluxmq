package memory

import (
	"strings"
	"sync"

	"github.com/dborovcanin/mqtt/store"
	"github.com/dborovcanin/mqtt/topics"
)

// RetainedStore is an in-memory implementation of store.RetainedStore.
type RetainedStore struct {
	mu   sync.RWMutex
	data map[string]*store.Message // topic -> message
}

// NewRetainedStore creates a new in-memory retained message store.
func NewRetainedStore() *RetainedStore {
	return &RetainedStore{
		data: make(map[string]*store.Message),
	}
}

// Set stores or updates a retained message.
// Empty payload deletes the retained message.
func (s *RetainedStore) Set(topic string, msg *store.Message) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Empty payload means delete
	if msg == nil || len(msg.Payload) == 0 {
		delete(s.data, topic)
		return nil
	}

	s.data[topic] = store.CopyMessage(msg)
	return nil
}

// Get retrieves a retained message by exact topic.
func (s *RetainedStore) Get(topic string) (*store.Message, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	msg, ok := s.data[topic]
	if !ok {
		return nil, store.ErrNotFound
	}
	return store.CopyMessage(msg), nil
}

// Delete removes a retained message.
func (s *RetainedStore) Delete(topic string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.data, topic)
	return nil
}

// Match returns all retained messages matching a filter (supports wildcards).
func (s *RetainedStore) Match(filter string) ([]*store.Message, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var result []*store.Message

	// Special case: "#" matches all non-system topics
	if filter == "#" {
		for topic, msg := range s.data {
			if !strings.HasPrefix(topic, "$") {
				result = append(result, store.CopyMessage(msg))
			}
		}
		return result, nil
	}

	for topic, msg := range s.data {
		if topics.TopicMatch(filter, topic) {
			result = append(result, store.CopyMessage(msg))
		}
	}

	return result, nil
}

// Ensure RetainedStore implements store.RetainedStore.
var _ store.RetainedStore = (*RetainedStore)(nil)
