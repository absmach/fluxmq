// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package badger

import (
	"encoding/json"
	"fmt"

	"github.com/absmach/mqtt/storage"
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
	db *badger.DB
}

// NewMessageStore creates a new BadgerDB message store.
func NewMessageStore(db *badger.DB) *MessageStore {
	return &MessageStore{db: db}
}

// Store stores a message with the given key.
func (m *MessageStore) Store(key string, msg *storage.Message) error {
	data, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("failed to marshal message: %w", err)
	}

	return m.db.Update(func(txn *badger.Txn) error {
		return txn.Set([]byte(key), data)
	})
}

// Get retrieves a message by key.
func (m *MessageStore) Get(key string) (*storage.Message, error) {
	var msg *storage.Message

	err := m.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(key))
		if err != nil {
			if err == badger.ErrKeyNotFound {
				return storage.ErrNotFound
			}
			return err
		}

		return item.Value(func(val []byte) error {
			msg = &storage.Message{}
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
func (m *MessageStore) List(prefix string) ([]*storage.Message, error) {
	var messages []*storage.Message

	err := m.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = []byte(prefix)
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()

			err := item.Value(func(val []byte) error {
				var msg storage.Message
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

	return messages, err
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
