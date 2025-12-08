package memory

import (
	"strings"
	"sync"

	"github.com/dborovcanin/mqtt/store"
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

	s.data[topic] = copyMessage(msg)
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
	return copyMessage(msg), nil
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

	// Handle special cases
	if filter == "#" {
		// Match all non-system topics
		for topic, msg := range s.data {
			if !strings.HasPrefix(topic, "$") {
				result = append(result, copyMessage(msg))
			}
		}
		return result, nil
	}

	filterLevels := strings.Split(filter, "/")

	for topic, msg := range s.data {
		if s.topicMatches(filterLevels, topic) {
			result = append(result, copyMessage(msg))
		}
	}

	return result, nil
}

// topicMatches checks if a topic matches a filter.
func (s *RetainedStore) topicMatches(filterLevels []string, topic string) bool {
	topicLevels := strings.Split(topic, "/")

	// Handle $-prefix topics: only match if filter also starts with $
	if strings.HasPrefix(topic, "$") {
		if len(filterLevels) == 0 || !strings.HasPrefix(filterLevels[0], "$") {
			// Filter with + or # as first level doesn't match $ topics
			if filterLevels[0] == "+" || filterLevels[0] == "#" {
				return false
			}
		}
	}

	fi := 0
	ti := 0

	for fi < len(filterLevels) && ti < len(topicLevels) {
		if filterLevels[fi] == "#" {
			// Multi-level wildcard matches rest
			return true
		}
		if filterLevels[fi] == "+" {
			// Single-level wildcard matches current level
			fi++
			ti++
			continue
		}
		if filterLevels[fi] != topicLevels[ti] {
			return false
		}
		fi++
		ti++
	}

	// Check if we consumed both entirely
	if fi == len(filterLevels) && ti == len(topicLevels) {
		return true
	}

	// Check for trailing # in filter
	if fi == len(filterLevels)-1 && filterLevels[fi] == "#" {
		return true
	}

	return false
}

// Ensure RetainedStore implements store.RetainedStore.
var _ store.RetainedStore = (*RetainedStore)(nil)
