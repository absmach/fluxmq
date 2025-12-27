// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package memory

import (
	"context"
	"sync"
	"time"

	"github.com/absmach/mqtt/storage"
)

var _ storage.WillStore = (*WillStore)(nil)

// WillStore is an in-memory implementation of store.WillStore.
type WillStore struct {
	mu   sync.RWMutex
	data map[string]*willEntry // clientID -> will entry
}

type willEntry struct {
	will        *storage.WillMessage
	disconnedAt time.Time // When client disconnected (for delay calculation)
}

// NewWillStore creates a new in-memory will message store.
func NewWillStore() *WillStore {
	return &WillStore{
		data: make(map[string]*willEntry),
	}
}

// Set stores a will message for a client.
func (s *WillStore) Set(ctx context.Context, clientID string, will *storage.WillMessage) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.data[clientID] = &willEntry{
		will:        copyWill(will),
		disconnedAt: time.Now(), // Disconnected now
	}
	return nil
}

// Get retrieves the will message for a client.
func (s *WillStore) Get(ctx context.Context, clientID string) (*storage.WillMessage, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	entry, ok := s.data[clientID]
	if !ok {
		return nil, storage.ErrNotFound
	}
	return copyWill(entry.will), nil
}

// Delete removes the will message for a client.
func (s *WillStore) Delete(ctx context.Context, clientID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.data, clientID)
	return nil
}

// MarkDisconnected marks a client as disconnected for will delay calculation.
func (s *WillStore) MarkDisconnected(clientID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	entry, ok := s.data[clientID]
	if !ok {
		return storage.ErrNotFound
	}
	entry.disconnedAt = time.Now()
	return nil
}

// GetPending returns will messages that should be triggered.
// (will delay elapsed and client still disconnected).
func (s *WillStore) GetPending(ctx context.Context, before time.Time) ([]*storage.WillMessage, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var result []*storage.WillMessage
	for _, entry := range s.data {
		// Only check entries where client has disconnected
		if entry.disconnedAt.IsZero() {
			continue
		}

		// Calculate when will should trigger
		triggerTime := entry.disconnedAt.Add(time.Duration(entry.will.Delay) * time.Second)
		if triggerTime.Before(before) || triggerTime.Equal(before) {
			result = append(result, copyWill(entry.will))
		}
	}
	return result, nil
}

// copyWill creates a deep copy of a will message.
func copyWill(will *storage.WillMessage) *storage.WillMessage {
	if will == nil {
		return nil
	}

	cp := &storage.WillMessage{
		ClientID: will.ClientID,
		Topic:    will.Topic,
		QoS:      will.QoS,
		Retain:   will.Retain,
		Delay:    will.Delay,
		Expiry:   will.Expiry,
	}

	if len(will.Payload) > 0 {
		cp.Payload = make([]byte, len(will.Payload))
		copy(cp.Payload, will.Payload)
	}

	if len(will.Properties) > 0 {
		cp.Properties = make(map[string]string, len(will.Properties))
		for k, v := range will.Properties {
			cp.Properties[k] = v
		}
	}

	return cp
}
