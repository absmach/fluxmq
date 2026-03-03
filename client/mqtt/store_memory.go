// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package mqtt

import "sync"

// memoryMessageStore is the default in-process message store.
// It stores messages directly (with deep copies) to avoid JSON encode/decode overhead.
type memoryMessageStore struct {
	mu       sync.RWMutex
	outbound map[uint16]*Message
	inbound  map[uint16]*Message
}

func newMemoryMessageStore() *memoryMessageStore {
	return &memoryMessageStore{
		outbound: make(map[uint16]*Message),
		inbound:  make(map[uint16]*Message),
	}
}

func (s *memoryMessageStore) StoreOutbound(packetID uint16, msg *Message) error {
	s.mu.Lock()
	s.outbound[packetID] = copyStoreMessage(msg)
	s.mu.Unlock()
	return nil
}

func (s *memoryMessageStore) GetOutbound(packetID uint16) (*Message, bool) {
	s.mu.RLock()
	msg, ok := s.outbound[packetID]
	s.mu.RUnlock()
	if !ok {
		return nil, false
	}
	return copyStoreMessage(msg), true
}

func (s *memoryMessageStore) DeleteOutbound(packetID uint16) error {
	s.mu.Lock()
	delete(s.outbound, packetID)
	s.mu.Unlock()
	return nil
}

func (s *memoryMessageStore) GetAllOutbound() []*Message {
	s.mu.RLock()
	out := make([]*Message, 0, len(s.outbound))
	for _, msg := range s.outbound {
		out = append(out, copyStoreMessage(msg))
	}
	s.mu.RUnlock()
	return out
}

func (s *memoryMessageStore) StoreInbound(packetID uint16, msg *Message) error {
	s.mu.Lock()
	s.inbound[packetID] = copyStoreMessage(msg)
	s.mu.Unlock()
	return nil
}

func (s *memoryMessageStore) GetInbound(packetID uint16) (*Message, bool) {
	s.mu.RLock()
	msg, ok := s.inbound[packetID]
	s.mu.RUnlock()
	if !ok {
		return nil, false
	}
	return copyStoreMessage(msg), true
}

func (s *memoryMessageStore) DeleteInbound(packetID uint16) error {
	s.mu.Lock()
	delete(s.inbound, packetID)
	s.mu.Unlock()
	return nil
}

func (s *memoryMessageStore) Reset() error {
	s.mu.Lock()
	clear(s.outbound)
	clear(s.inbound)
	s.mu.Unlock()
	return nil
}

func (s *memoryMessageStore) Close() error {
	return s.Reset()
}

func copyStoreMessage(msg *Message) *Message {
	if msg == nil {
		return nil
	}
	return msg.Copy()
}

var _ MessageStore = (*memoryMessageStore)(nil)
