// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package memory

import (
	"context"
	"strings"
	"sync"

	"github.com/absmach/mqtt/storage"
	"github.com/absmach/mqtt/topics"
)

var _ storage.RetainedStore = (*RetainedStore)(nil)

// RetainedStore is an in-memory implementation of store.RetainedStore.
type RetainedStore struct {
	mu   sync.RWMutex
	data map[string]*storage.Message // topic -> message
}

// NewRetainedStore creates a new in-memory retained message store.
func NewRetainedStore() *RetainedStore {
	return &RetainedStore{
		data: make(map[string]*storage.Message),
	}
}

// Set stores or updates a retained message.
// Empty payload deletes the retained message.
func (s *RetainedStore) Set(ctx context.Context, topic string, msg *storage.Message) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Empty payload means delete
	if msg == nil || len(msg.Payload) == 0 {
		delete(s.data, topic)
		return nil
	}

	s.data[topic] = storage.CopyMessage(msg)
	return nil
}

// Get retrieves a retained message by exact topic.
func (s *RetainedStore) Get(ctx context.Context, topic string) (*storage.Message, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	msg, ok := s.data[topic]
	if !ok {
		return nil, storage.ErrNotFound
	}
	return storage.CopyMessage(msg), nil
}

// Delete removes a retained message.
func (s *RetainedStore) Delete(_ context.Context, topic string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.data, topic)
	return nil
}

// Match returns all retained messages matching a filter (supports wildcards).
func (s *RetainedStore) Match(_ context.Context, filter string) ([]*storage.Message, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var result []*storage.Message

	// Special case: "#" matches all non-system topics
	if filter == "#" {
		for topic, msg := range s.data {
			if !strings.HasPrefix(topic, "$") {
				result = append(result, storage.CopyMessage(msg))
			}
		}
		return result, nil
	}

	for topic, msg := range s.data {
		if topics.TopicMatch(filter, topic) {
			result = append(result, storage.CopyMessage(msg))
		}
	}

	return result, nil
}
