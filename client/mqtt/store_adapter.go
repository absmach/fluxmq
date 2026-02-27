// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package mqtt

import (
	"encoding/json"

	storebadger "github.com/absmach/fluxmq/client/mqtt/store/badger"
	storememory "github.com/absmach/fluxmq/client/mqtt/store/memory"
)

type kvMessageStore struct {
	kv rawStore
}

// NewMemoryStore creates a MessageStore backed by in-memory key/value storage.
func NewMemoryStore() MessageStore {
	return newKVMessageStore(storememory.New())
}

// NewBadgerStore creates a MessageStore backed by BadgerDB key/value storage.
func NewBadgerStore(opts *storebadger.Options) (MessageStore, error) {
	kv, err := storebadger.New(opts)
	if err != nil {
		return nil, err
	}
	return newKVMessageStore(kv), nil
}

func newKVMessageStore(kv rawStore) MessageStore {
	return &kvMessageStore{kv: kv}
}

func (s *kvMessageStore) StoreOutbound(packetID uint16, msg *Message) error {
	encoded, err := marshalStoreMessage(msg)
	if err != nil {
		return err
	}
	return s.kv.PutOutbound(packetID, encoded)
}

func (s *kvMessageStore) GetOutbound(packetID uint16) (*Message, bool) {
	encoded, ok, err := s.kv.GetOutbound(packetID)
	if err != nil || !ok {
		return nil, false
	}

	msg, err := unmarshalStoreMessage(encoded)
	if err != nil {
		return nil, false
	}

	return msg, true
}

func (s *kvMessageStore) DeleteOutbound(packetID uint16) error {
	return s.kv.DeleteOutbound(packetID)
}

func (s *kvMessageStore) GetAllOutbound() []*Message {
	entries, err := s.kv.ListOutbound()
	if err != nil {
		return nil
	}

	outbound := make([]*Message, 0, len(entries))
	for _, encoded := range entries {
		msg, err := unmarshalStoreMessage(encoded)
		if err != nil {
			continue
		}
		outbound = append(outbound, msg)
	}

	return outbound
}

func (s *kvMessageStore) StoreInbound(packetID uint16, msg *Message) error {
	encoded, err := marshalStoreMessage(msg)
	if err != nil {
		return err
	}
	return s.kv.PutInbound(packetID, encoded)
}

func (s *kvMessageStore) GetInbound(packetID uint16) (*Message, bool) {
	encoded, ok, err := s.kv.GetInbound(packetID)
	if err != nil || !ok {
		return nil, false
	}

	msg, err := unmarshalStoreMessage(encoded)
	if err != nil {
		return nil, false
	}

	return msg, true
}

func (s *kvMessageStore) DeleteInbound(packetID uint16) error {
	return s.kv.DeleteInbound(packetID)
}

func (s *kvMessageStore) Reset() error {
	return s.kv.Reset()
}

func (s *kvMessageStore) Close() error {
	return s.kv.Close()
}

func marshalStoreMessage(msg *Message) ([]byte, error) {
	if msg == nil {
		return []byte{}, nil
	}

	return json.Marshal(msg)
}

func unmarshalStoreMessage(payload []byte) (*Message, error) {
	if len(payload) == 0 {
		return nil, nil
	}

	var msg Message
	if err := json.Unmarshal(payload, &msg); err != nil {
		return nil, err
	}

	return &msg, nil
}

type rawStore interface {
	PutOutbound(packetID uint16, payload []byte) error
	GetOutbound(packetID uint16) ([]byte, bool, error)
	DeleteOutbound(packetID uint16) error
	ListOutbound() ([][]byte, error)

	PutInbound(packetID uint16, payload []byte) error
	GetInbound(packetID uint16) ([]byte, bool, error)
	DeleteInbound(packetID uint16) error

	Reset() error
	Close() error
}

var _ MessageStore = (*kvMessageStore)(nil)
var _ rawStore = (*storememory.Store)(nil)
var _ rawStore = (*storebadger.Store)(nil)
