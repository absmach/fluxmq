// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package memory

import (
	"sync"
)

// Store is an in-memory key/value implementation for MQTT message state.
type Store struct {
	mu       sync.RWMutex
	outbound map[uint16][]byte
	inbound  map[uint16][]byte
}

// New creates a new in-memory store.
func New() *Store {
	return &Store{
		outbound: make(map[uint16][]byte),
		inbound:  make(map[uint16][]byte),
	}
}

func (s *Store) PutOutbound(packetID uint16, payload []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.outbound[packetID] = cloneBytes(payload)
	return nil
}

func (s *Store) GetOutbound(packetID uint16) ([]byte, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	val, ok := s.outbound[packetID]
	if !ok {
		return nil, false, nil
	}

	return cloneBytes(val), true, nil
}

func (s *Store) DeleteOutbound(packetID uint16) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.outbound, packetID)
	return nil
}

func (s *Store) ListOutbound() ([][]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	values := make([][]byte, 0, len(s.outbound))
	for _, val := range s.outbound {
		values = append(values, cloneBytes(val))
	}

	return values, nil
}

func (s *Store) PutInbound(packetID uint16, payload []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.inbound[packetID] = cloneBytes(payload)
	return nil
}

func (s *Store) GetInbound(packetID uint16) ([]byte, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	val, ok := s.inbound[packetID]
	if !ok {
		return nil, false, nil
	}

	return cloneBytes(val), true, nil
}

func (s *Store) DeleteInbound(packetID uint16) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.inbound, packetID)
	return nil
}

func (s *Store) Reset() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	clear(s.outbound)
	clear(s.inbound)

	return nil
}

func (s *Store) Close() error {
	return s.Reset()
}

func cloneBytes(value []byte) []byte {
	if value == nil {
		return nil
	}

	out := make([]byte, len(value))
	copy(out, value)
	return out
}
