// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package badger

import (
	"encoding/json"
	"fmt"

	"github.com/absmach/fluxmq/message"
	"github.com/absmach/fluxmq/storage"
	"github.com/dgraph-io/badger/v4"
)

var _ storage.MessageStore = (*MessageStore)(nil)

// MessageStore implements storage.MessageStore using BadgerDB.
// Handles inflight messages and offline queue.
//
// Key format:
//   - Inflight: {clientID}/inflight/{packetID}
//   - Offline queue: {clientID}/queue/{seq}
type MessageStore struct {
	db *db
}

// NewMessageStore creates a new BadgerDB message store.
// The store guards handle against use after close; closing handle directly
// bypasses that guard and lets BadgerDB panic on a racing operation.
func NewMessageStore(handle *badger.DB) *MessageStore {
	return newMessageStore(newDB(handle))
}

func newMessageStore(db *db) *MessageStore {
	return &MessageStore{db: db}
}

// Store stores a message with the given key.
func (m *MessageStore) Store(key string, msg *message.Envelope) error {
	data, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("failed to marshal message: %w", err)
	}

	return m.db.Update(func(txn *badger.Txn) error {
		return txn.Set([]byte(key), data)
	})
}

// Get retrieves a message by key.
func (m *MessageStore) Get(key string) (*message.Envelope, error) {
	var msg *message.Envelope

	err := m.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(key))
		if err != nil {
			if err == badger.ErrKeyNotFound {
				return storage.ErrNotFound
			}
			return err
		}

		return item.Value(func(val []byte) error {
			msg = &message.Envelope{}
			return json.Unmarshal(val, msg)
		})
	})
	if err != nil {
		return nil, err
	}

	return msg, nil
}

// Delete removes a message.
func (m *MessageStore) Delete(key string) error {
	return m.db.Update(func(txn *badger.Txn) error {
		return txn.Delete([]byte(key))
	})
}

// List returns all messages matching a key prefix.
func (m *MessageStore) List(prefix string) ([]*message.Envelope, error) {
	var messages []*message.Envelope

	err := m.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = []byte(prefix)
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()

			err := item.Value(func(val []byte) error {
				var msg message.Envelope
				if err := json.Unmarshal(val, &msg); err != nil {
					return err
				}
				messages = append(messages, &msg)
				return nil
			})
			if err != nil {
				return fmt.Errorf("failed to unmarshal message: %w", err)
			}
		}

		return nil
	})
	if err != nil {
		for _, msg := range messages {
			message.Release(msg)
		}
		return nil, err
	}

	return messages, nil
}

// DeleteByPrefix removes all messages matching a prefix.
func (m *MessageStore) DeleteByPrefix(prefix string) error {
	return m.db.Update(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = []byte(prefix)
		opts.PrefetchValues = false // We only need keys
		it := txn.NewIterator(opts)
		defer it.Close()

		var keys [][]byte
		for it.Rewind(); it.Valid(); it.Next() {
			key := it.Item().KeyCopy(nil)
			keys = append(keys, key)
		}

		// Delete all collected keys
		for _, key := range keys {
			if err := txn.Delete(key); err != nil {
				return err
			}
		}

		return nil
	})
}
