// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package memory

import (
	"slices"
	"strings"
	"sync"

	"github.com/absmach/fluxmq/message"
	"github.com/absmach/fluxmq/storage"
)

var _ storage.MessageStore = (*MessageStore)(nil)

// MessageStore is an in-memory implementation of store.MessageStore.
type MessageStore struct {
	mu   sync.RWMutex
	data map[string]*message.Envelope
}

// NewMessageStore creates a new in-memory message store.
func NewMessageStore() *MessageStore {
	return &MessageStore{
		data: make(map[string]*message.Envelope),
	}
}

// Store stores a message.
func (s *MessageStore) Store(key string, msg *message.Envelope) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	replacement := msg.Clone()
	previous := s.data[key]
	s.data[key] = replacement
	message.Release(previous)
	return nil
}

// Get retrieves a message by key.
func (s *MessageStore) Get(key string) (*message.Envelope, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	msg, ok := s.data[key]
	if !ok {
		return nil, storage.ErrNotFound
	}
	return msg.Clone(), nil
}

// Delete removes a message.
func (s *MessageStore) Delete(key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	previous := s.data[key]
	delete(s.data, key)
	message.Release(previous)
	return nil
}

// List returns all messages matching a key prefix, in key order as the Badger
// store does: callers restoring a queue rely on it.
func (s *MessageStore) List(prefix string) ([]*message.Envelope, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var keys []string
	for key := range s.data {
		if strings.HasPrefix(key, prefix) {
			keys = append(keys, key)
		}
	}
	slices.Sort(keys)

	result := make([]*message.Envelope, 0, len(keys))
	for _, key := range keys {
		result = append(result, s.data[key].Clone())
	}
	return result, nil
}

// DeleteByPrefix removes all messages matching a prefix.
func (s *MessageStore) DeleteByPrefix(prefix string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	for key := range s.data {
		if strings.HasPrefix(key, prefix) {
			message.Release(s.data[key])
			delete(s.data, key)
		}
	}
	return nil
}
