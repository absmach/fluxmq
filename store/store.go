package store

import (
	"errors"
	"sync"
)

var ErrNotFound = errors.New("message not found")

// MessageStore defines the interface for persisting MQTT messages.
// This is used for Retained messages and QoS 1/2 offline buffering.
type MessageStore interface {
	// StorePersist persists a message for a given client (offline queue) or topic (retained).
	// key is either ClientID or Topic.
	Store(key string, payload []byte) error

	// Retrieve gets the message.
	Retrieve(key string) ([]byte, error)

	// Delete removes the message.
	Delete(key string) error
}

// MemoryStore is a simple in-memory implementation.
type MemoryStore struct {
	mu   sync.RWMutex
	data map[string][]byte
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		data: make(map[string][]byte),
	}
}

func (s *MemoryStore) Store(key string, payload []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	// Deep copy payload to avoid mutation races
	c := make([]byte, len(payload))
	copy(c, payload)
	s.data[key] = c
	return nil
}

func (s *MemoryStore) Retrieve(key string) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	val, ok := s.data[key]
	if !ok {
		return nil, ErrNotFound
	}
	// Return copy?
	c := make([]byte, len(val))
	copy(c, val)
	return c, nil
}

func (s *MemoryStore) Delete(key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.data, key)
	return nil
}
