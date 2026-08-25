// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package memory

import (
	"context"
	"strings"
	"sync"

	"github.com/absmach/fluxmq/message"
	"github.com/absmach/fluxmq/storage"
	"github.com/absmach/fluxmq/topics"
)

var _ storage.RetainedStore = (*RetainedStore)(nil)

// RetainedStore is an in-memory implementation of store.RetainedStore.
type RetainedStore struct {
	mu   sync.RWMutex
	data map[string]*message.Envelope // topic -> message
}

// NewRetainedStore creates a new in-memory retained message store.
func NewRetainedStore() *RetainedStore {
	return &RetainedStore{
		data: make(map[string]*message.Envelope),
	}
}

// Set stores or updates a retained message.
// Empty payload deletes the retained message.
func (s *RetainedStore) Set(ctx context.Context, topic string, msg *message.Envelope) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Empty payload means delete
	if msg == nil || len(msg.PayloadBytes()) == 0 {
		message.Release(s.data[topic])
		delete(s.data, topic)
		return nil
	}

	replacement := msg.Clone()
	previous := s.data[topic]
	s.data[topic] = replacement
	message.Release(previous)
	return nil
}

// Get retrieves a retained message by exact topic.
func (s *RetainedStore) Get(ctx context.Context, topic string) (*message.Envelope, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	msg, ok := s.data[topic]
	if !ok {
		return nil, storage.ErrNotFound
	}
	return msg.Clone(), nil
}

// Delete removes a retained message.
func (s *RetainedStore) Delete(ctx context.Context, topic string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	previous := s.data[topic]
	delete(s.data, topic)
	message.Release(previous)
	return nil
}

// Match returns all retained messages matching a filter (supports wildcards).
func (s *RetainedStore) Match(ctx context.Context, filter string) ([]*message.Envelope, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var result []*message.Envelope

	// Special case: "#" matches all non-system topics
	if filter == "#" {
		for topic, msg := range s.data {
			if !strings.HasPrefix(topic, "$") {
				result = append(result, msg.Clone())
			}
		}
		return result, nil
	}

	for topic, msg := range s.data {
		if topics.TopicMatch(filter, topic) {
			result = append(result, msg.Clone())
		}
	}

	return result, nil
}
