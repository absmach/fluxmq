// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package raft

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/dgraph-io/badger/v4"
	"github.com/hashicorp/raft"
)

var (
	// ErrKeyNotFound is returned when a key is not found in the stable store.
	ErrKeyNotFound = errors.New("key not found")
)

// BadgerLogStore implements raft.LogStore using BadgerDB.
// It stores Raft log entries with efficient sequential writes.
type BadgerLogStore struct {
	db     *badger.DB
	prefix string // Prefix for all keys (e.g., "raft:log:{queueName}:{partitionID}:")
}

// NewBadgerLogStore creates a new Badger-backed log store.
func NewBadgerLogStore(db *badger.DB, queueName string, partitionID int) *BadgerLogStore {
	return &BadgerLogStore{
		db:     db,
		prefix: fmt.Sprintf("raft:log:%s:%d:", queueName, partitionID),
	}
}

// FirstIndex returns the index of the first log entry.
func (b *BadgerLogStore) FirstIndex() (uint64, error) {
	var first uint64

	err := b.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()

		// Seek to the first key with our prefix
		it.Seek([]byte(b.prefix))
		if !it.ValidForPrefix([]byte(b.prefix)) {
			return nil // No entries
		}

		// Extract index from key
		key := it.Item().Key()
		first = b.decodeKey(key)
		return nil
	})

	return first, err
}

// LastIndex returns the index of the last log entry.
func (b *BadgerLogStore) LastIndex() (uint64, error) {
	var last uint64

	err := b.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		opts.Reverse = true
		it := txn.NewIterator(opts)
		defer it.Close()

		// Create end key (prefix + max uint64)
		endKey := append([]byte(b.prefix), 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff)

		// Seek to the last key with our prefix
		it.Seek(endKey)
		if !it.ValidForPrefix([]byte(b.prefix)) {
			return nil // No entries
		}

		// Extract index from key
		key := it.Item().Key()
		last = b.decodeKey(key)
		return nil
	})

	return last, err
}

// GetLog retrieves a log entry at the given index.
func (b *BadgerLogStore) GetLog(index uint64, log *raft.Log) error {
	key := b.encodeKey(index)

	return b.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err == badger.ErrKeyNotFound {
			return raft.ErrLogNotFound
		}
		if err != nil {
			return err
		}

		return item.Value(func(val []byte) error {
			return json.Unmarshal(val, log)
		})
	})
}

// StoreLog stores a single log entry.
func (b *BadgerLogStore) StoreLog(log *raft.Log) error {
	return b.StoreLogs([]*raft.Log{log})
}

// StoreLogs stores multiple log entries in a batch.
func (b *BadgerLogStore) StoreLogs(logs []*raft.Log) error {
	return b.db.Update(func(txn *badger.Txn) error {
		for _, log := range logs {
			key := b.encodeKey(log.Index)
			val, err := json.Marshal(log)
			if err != nil {
				return err
			}

			if err := txn.Set(key, val); err != nil {
				return err
			}
		}
		return nil
	})
}

// DeleteRange deletes log entries from [min, max] inclusive.
func (b *BadgerLogStore) DeleteRange(min, max uint64) error {
	return b.db.Update(func(txn *badger.Txn) error {
		for idx := min; idx <= max; idx++ {
			key := b.encodeKey(idx)
			if err := txn.Delete(key); err != nil && err != badger.ErrKeyNotFound {
				return err
			}
		}
		return nil
	})
}

// encodeKey converts an index to a BadgerDB key.
func (b *BadgerLogStore) encodeKey(index uint64) []byte {
	key := make([]byte, len(b.prefix)+8)
	copy(key, b.prefix)
	binary.BigEndian.PutUint64(key[len(b.prefix):], index)
	return key
}

// decodeKey extracts the index from a BadgerDB key.
func (b *BadgerLogStore) decodeKey(key []byte) uint64 {
	return binary.BigEndian.Uint64(key[len(b.prefix):])
}

// BadgerStableStore implements raft.StableStore using BadgerDB.
// It stores Raft metadata like current term and voted-for.
type BadgerStableStore struct {
	db     *badger.DB
	prefix string // Prefix for all keys (e.g., "raft:stable:{queueName}:{partitionID}:")
}

// NewBadgerStableStore creates a new Badger-backed stable store.
func NewBadgerStableStore(db *badger.DB, queueName string, partitionID int) *BadgerStableStore {
	return &BadgerStableStore{
		db:     db,
		prefix: fmt.Sprintf("raft:stable:%s:%d:", queueName, partitionID),
	}
}

// Set stores a key-value pair.
func (b *BadgerStableStore) Set(key []byte, val []byte) error {
	fullKey := append([]byte(b.prefix), key...)
	return b.db.Update(func(txn *badger.Txn) error {
		return txn.Set(fullKey, val)
	})
}

// Get retrieves a value by key.
func (b *BadgerStableStore) Get(key []byte) ([]byte, error) {
	fullKey := append([]byte(b.prefix), key...)
	var val []byte

	err := b.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(fullKey)
		if err == badger.ErrKeyNotFound {
			return ErrKeyNotFound
		}
		if err != nil {
			return err
		}

		val, err = item.ValueCopy(nil)
		return err
	})

	return val, err
}

// SetUint64 stores a uint64 value.
func (b *BadgerStableStore) SetUint64(key []byte, val uint64) error {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, val)
	return b.Set(key, buf)
}

// GetUint64 retrieves a uint64 value.
func (b *BadgerStableStore) GetUint64(key []byte) (uint64, error) {
	val, err := b.Get(key)
	if err != nil {
		return 0, err
	}
	if len(val) != 8 {
		return 0, fmt.Errorf("invalid uint64 value length: %d", len(val))
	}
	return binary.BigEndian.Uint64(val), nil
}
