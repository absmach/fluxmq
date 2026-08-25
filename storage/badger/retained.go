// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package badger

import (
	"context"
	"fmt"

	"github.com/absmach/fluxmq/message"
	"github.com/absmach/fluxmq/storage"
	"github.com/dgraph-io/badger/v4"
)

var _ storage.RetainedStore = (*RetainedStore)(nil)

// RetainedStore implements storage.RetainedStore using BadgerDB.
//
// Key format: retained:{topic}.
type RetainedStore struct {
	db *db
}

// NewRetainedStore creates a new BadgerDB retained message store.
// The store guards handle against use after close; closing handle directly
// bypasses that guard and lets BadgerDB panic on a racing operation.
func NewRetainedStore(handle *badger.DB) *RetainedStore {
	return newRetainedStore(newDB(handle))
}

func newRetainedStore(db *db) *RetainedStore {
	return &RetainedStore{db: db}
}

// Set stores or updates a retained message.
// Empty payload deletes the retained message.
func (r *RetainedStore) Set(ctx context.Context, topic string, msg *message.Envelope) error {
	// Empty payload means delete
	if len(msg.PayloadBytes()) == 0 {
		return r.Delete(ctx, topic)
	}

	key := []byte("retained:" + topic)
	data, err := message.MarshalBinary(msg)
	if err != nil {
		return fmt.Errorf("failed to marshal retained message: %w", err)
	}

	return r.db.Update(func(txn *badger.Txn) error {
		return txn.Set(key, data)
	})
}

// Get retrieves a retained message by exact topic.
func (r *RetainedStore) Get(ctx context.Context, topic string) (*message.Envelope, error) {
	key := []byte("retained:" + topic)
	var msg *message.Envelope

	err := r.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err != nil {
			if err == badger.ErrKeyNotFound {
				return storage.ErrNotFound
			}
			return err
		}

		return item.Value(func(val []byte) error {
			msg, err = message.UnmarshalBinary(val)
			return err
		})
	})
	if err != nil {
		return nil, err
	}

	return msg, nil
}

// Delete removes a retained message.
func (r *RetainedStore) Delete(ctx context.Context, topic string) error {
	key := []byte("retained:" + topic)

	return r.db.Update(func(txn *badger.Txn) error {
		return txn.Delete(key)
	})
}

// Match returns all retained messages matching a filter (supports wildcards).
func (r *RetainedStore) Match(ctx context.Context, filter string) ([]*message.Envelope, error) {
	var matched []*message.Envelope

	err := r.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = []byte("retained:")
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			key := string(item.Key())

			// Extract topic from key (remove "retained:" prefix)
			topic := key[len("retained:"):]

			// Check if topic matches the filter
			if topicMatchesFilter(topic, filter) {
				err := item.Value(func(val []byte) error {
					msg, err := message.UnmarshalBinary(val)
					if err != nil {
						return err
					}
					matched = append(matched, msg)
					return nil
				})
				if err != nil {
					return fmt.Errorf("failed to unmarshal retained message: %w", err)
				}
			}
		}

		return nil
	})
	if err != nil {
		for _, msg := range matched {
			message.Release(msg)
		}
		return nil, err
	}

	return matched, nil
}
