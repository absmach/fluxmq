// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package badger

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/absmach/mqtt/storage"
	"github.com/dgraph-io/badger/v4"
)

var _ storage.WillStore = (*WillStore)(nil)

// WillStore implements storage.WillStore using BadgerDB.
//
// Key format: will:{clientID}.
type WillStore struct {
	db *badger.DB
}

// willEntry wraps a will message with disconnect timestamp for delay calculation.
type willEntry struct {
	Will           *storage.WillMessage `json:"will"`
	DisconnectedAt time.Time            `json:"disconnected_at"`
}

// NewWillStore creates a new BadgerDB will message store.
func NewWillStore(db *badger.DB) *WillStore {
	return &WillStore{db: db}
}

// Set stores a will message for a client.
func (w *WillStore) Set(ctx context.Context, clientID string, will *storage.WillMessage) error {
	key := []byte("will:" + clientID)

	entry := &willEntry{
		Will:           will,
		DisconnectedAt: time.Now(), // Mark as disconnected now
	}

	data, err := json.Marshal(entry)
	if err != nil {
		return fmt.Errorf("failed to marshal will message: %w", err)
	}

	return w.db.Update(func(txn *badger.Txn) error {
		return txn.Set(key, data)
	})
}

// Get retrieves the will message for a client.
func (w *WillStore) Get(ctx context.Context, clientID string) (*storage.WillMessage, error) {
	key := []byte("will:" + clientID)
	var entry *willEntry

	err := w.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err != nil {
			if err == badger.ErrKeyNotFound {
				return storage.ErrNotFound
			}
			return err
		}

		return item.Value(func(val []byte) error {
			entry = &willEntry{}
			return json.Unmarshal(val, entry)
		})
	})
	if err != nil {
		return nil, err
	}

	return entry.Will, nil
}

// Delete removes the will message for a client.
func (w *WillStore) Delete(ctx context.Context, clientID string) error {
	key := []byte("will:" + clientID)

	return w.db.Update(func(txn *badger.Txn) error {
		return txn.Delete(key)
	})
}

// GetPending returns will messages that should be triggered.
// (will delay elapsed and client still disconnected).
func (w *WillStore) GetPending(ctx context.Context, before time.Time) ([]*storage.WillMessage, error) {
	var pending []*storage.WillMessage

	err := w.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = []byte("will:")
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()

			err := item.Value(func(val []byte) error {
				var entry willEntry
				if err := json.Unmarshal(val, &entry); err != nil {
					return err
				}

				// Skip if disconnected time is not set
				if entry.DisconnectedAt.IsZero() {
					return nil
				}

				// Calculate when will should trigger
				triggerTime := entry.DisconnectedAt.Add(time.Duration(entry.Will.Delay) * time.Second)
				if triggerTime.Before(before) || triggerTime.Equal(before) {
					pending = append(pending, entry.Will)
				}

				return nil
			})
			if err != nil {
				return fmt.Errorf("failed to unmarshal will entry: %w", err)
			}
		}

		return nil
	})

	return pending, err
}
