// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package client

import (
	"sync"
)

// MessageStore provides persistence for QoS 1/2 messages.
// It stores outbound messages waiting for acknowledgment and
// inbound QoS 2 messages waiting for PUBREL.
type MessageStore interface {
	// StoreOutbound stores an outbound message awaiting acknowledgment.
	StoreOutbound(packetID uint16, msg *Message) error

	// GetOutbound retrieves an outbound message by packet ID.
	GetOutbound(packetID uint16) (*Message, bool)

	// DeleteOutbound removes an outbound message after acknowledgment.
	DeleteOutbound(packetID uint16) error

	// GetAllOutbound returns all stored outbound messages (for reconnection).
	GetAllOutbound() []*Message

	// StoreInbound stores an inbound QoS 2 message awaiting PUBREL.
	StoreInbound(packetID uint16, msg *Message) error

	// GetInbound retrieves an inbound QoS 2 message by packet ID.
	GetInbound(packetID uint16) (*Message, bool)

	// DeleteInbound removes an inbound message after PUBREL/PUBCOMP.
	DeleteInbound(packetID uint16) error

	// Reset clears all stored messages.
	Reset() error

	// Close releases any resources.
	Close() error
}

// MemoryStore is an in-memory implementation of MessageStore.
type MemoryStore struct {
	mu       sync.RWMutex
	outbound map[uint16]*Message
	inbound  map[uint16]*Message
}

// NewMemoryStore creates a new in-memory message store.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		outbound: make(map[uint16]*Message),
		inbound:  make(map[uint16]*Message),
	}
}

func (s *MemoryStore) StoreOutbound(packetID uint16, msg *Message) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.outbound[packetID] = msg.Copy()
	return nil
}

func (s *MemoryStore) GetOutbound(packetID uint16) (*Message, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	msg, ok := s.outbound[packetID]
	if !ok {
		return nil, false
	}
	return msg.Copy(), true
}

func (s *MemoryStore) DeleteOutbound(packetID uint16) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.outbound, packetID)
	return nil
}

func (s *MemoryStore) GetAllOutbound() []*Message {
	s.mu.RLock()
	defer s.mu.RUnlock()
	msgs := make([]*Message, 0, len(s.outbound))
	for _, msg := range s.outbound {
		msgs = append(msgs, msg.Copy())
	}
	return msgs
}

func (s *MemoryStore) StoreInbound(packetID uint16, msg *Message) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.inbound[packetID] = msg.Copy()
	return nil
}

func (s *MemoryStore) GetInbound(packetID uint16) (*Message, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	msg, ok := s.inbound[packetID]
	if !ok {
		return nil, false
	}
	return msg.Copy(), true
}

func (s *MemoryStore) DeleteInbound(packetID uint16) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.inbound, packetID)
	return nil
}

func (s *MemoryStore) Reset() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.outbound = make(map[uint16]*Message)
	s.inbound = make(map[uint16]*Message)
	return nil
}

func (s *MemoryStore) Close() error {
	return s.Reset()
}

// Ensure MemoryStore implements MessageStore.
var _ MessageStore = (*MemoryStore)(nil)
