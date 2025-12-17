// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package badger

import (
	"encoding/json"
	"fmt"
	"strings"
	"sync/atomic"

	"github.com/absmach/mqtt/storage"
	"github.com/dgraph-io/badger/v4"
)

var _ storage.SubscriptionStore = (*SubscriptionStore)(nil)

// SubscriptionStore implements storage.SubscriptionStore using BadgerDB.
//
// Key format: sub:{clientID}:{filter}.
type SubscriptionStore struct {
	db    *badger.DB
	count atomic.Int64 // Cached subscription count
}

// NewSubscriptionStore creates a new BadgerDB subscription store.
func NewSubscriptionStore(db *badger.DB) *SubscriptionStore {
	s := &SubscriptionStore{db: db}
	s.refreshCount()
	return s
}

// Add adds or updates a subscription.
func (s *SubscriptionStore) Add(sub *storage.Subscription) error {
	key := fmt.Sprintf("sub:%s:%s", sub.ClientID, sub.Filter)
	data, err := json.Marshal(sub)
	if err != nil {
		return fmt.Errorf("failed to marshal subscription: %w", err)
	}

	err = s.db.Update(func(txn *badger.Txn) error {
		// Check if key exists (for count update)
		_, err := txn.Get([]byte(key))
		isNew := err == badger.ErrKeyNotFound

		if err := txn.Set([]byte(key), data); err != nil {
			return err
		}

		if isNew {
			s.count.Add(1)
		}

		return nil
	})

	return err
}

// Remove removes a subscription.
func (s *SubscriptionStore) Remove(clientID, filter string) error {
	key := fmt.Sprintf("sub:%s:%s", clientID, filter)

	return s.db.Update(func(txn *badger.Txn) error {
		// Check if key exists (for count update)
		_, err := txn.Get([]byte(key))
		if err == badger.ErrKeyNotFound {
			return nil // Already doesn't exist
		}

		if err := txn.Delete([]byte(key)); err != nil {
			return err
		}

		s.count.Add(-1)
		return nil
	})
}

// RemoveAll removes all subscriptions for a client.
func (s *SubscriptionStore) RemoveAll(clientID string) error {
	prefix := fmt.Sprintf("sub:%s:", clientID)

	return s.db.Update(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = []byte(prefix)
		opts.PrefetchValues = false
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
			s.count.Add(-1)
		}

		return nil
	})
}

// GetForClient returns all subscriptions for a client.
func (s *SubscriptionStore) GetForClient(clientID string) ([]*storage.Subscription, error) {
	prefix := fmt.Sprintf("sub:%s:", clientID)
	var subs []*storage.Subscription

	err := s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = []byte(prefix)
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()

			err := item.Value(func(val []byte) error {
				var sub storage.Subscription
				if err := json.Unmarshal(val, &sub); err != nil {
					return err
				}
				subs = append(subs, &sub)
				return nil
			})
			if err != nil {
				return fmt.Errorf("failed to unmarshal subscription: %w", err)
			}
		}

		return nil
	})

	return subs, err
}

// Match returns all subscriptions matching a topic.
// This scans all subscriptions and performs MQTT topic matching.
func (s *SubscriptionStore) Match(topic string) ([]*storage.Subscription, error) {
	var matched []*storage.Subscription

	err := s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = []byte("sub:")
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()

			err := item.Value(func(val []byte) error {
				var sub storage.Subscription
				if err := json.Unmarshal(val, &sub); err != nil {
					return err
				}

				// Check if topic matches the subscription filter
				if topicMatchesFilter(topic, sub.Filter) {
					matched = append(matched, &sub)
				}

				return nil
			})
			if err != nil {
				return fmt.Errorf("failed to unmarshal subscription: %w", err)
			}
		}

		return nil
	})

	return matched, err
}

// Count returns total subscription count.
func (s *SubscriptionStore) Count() int {
	return int(s.count.Load())
}

// refreshCount recalculates the subscription count by scanning the database.
// Called on initialization.
func (s *SubscriptionStore) refreshCount() {
	count := int64(0)

	s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = []byte("sub:")
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			count++
		}

		return nil
	})

	s.count.Store(count)
}

// topicMatchesFilter checks if a topic matches an MQTT subscription filter.
// Supports wildcards: + (single-level) and # (multi-level).
func topicMatchesFilter(topic, filter string) bool {
	topicLevels := strings.Split(topic, "/")
	filterLevels := strings.Split(filter, "/")

	return matchLevels(topicLevels, filterLevels, 0, 0)
}

func matchLevels(topic, filter []string, ti, fi int) bool {
	// Both exhausted - match
	if ti == len(topic) && fi == len(filter) {
		return true
	}

	// Filter exhausted but topic remains - no match
	if fi == len(filter) {
		return false
	}

	// Multi-level wildcard '#' matches everything remaining
	if filter[fi] == "#" {
		return true
	}

	// Topic exhausted but filter remains (and it's not #) - no match
	if ti == len(topic) {
		return false
	}

	// Single-level wildcard '+' or exact match
	if filter[fi] == "+" || filter[fi] == topic[ti] {
		return matchLevels(topic, filter, ti+1, fi+1)
	}

	return false
}
