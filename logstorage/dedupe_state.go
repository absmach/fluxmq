// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package logstorage

import (
	"encoding/binary"
	"errors"
	"fmt"
	"path/filepath"

	bolt "go.etcd.io/bbolt"
)

const (
	dedupeStateFile = "dedupe.db"
	dedupeValueSize = 9
)

var dedupeQueuesBucket = []byte("queues")

type dedupeEntryState byte

const (
	dedupePending dedupeEntryState = iota + 1
	dedupeUnconfirmed
	dedupeConfirmed
)

// dedupeEntry is durable derived state for one transfer identity. Pending
// stores the tail observed before append, so recovery after a crash scans only
// the uncertain suffix instead of decoding the retained queue.
type dedupeEntry struct {
	offset uint64
	state  dedupeEntryState
}

type dedupeStateStore struct {
	db *bolt.DB
}

func openDedupeStateStore(baseDir string) (*dedupeStateStore, error) {
	db, err := bolt.Open(filepath.Join(baseDir, dedupeStateFile), 0o600, nil)
	if err != nil {
		return nil, fmt.Errorf("open deduplication state: %w", err)
	}
	if err := db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(dedupeQueuesBucket)
		return err
	}); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("initialize deduplication state: %w", err)
	}
	if err := SyncDir(baseDir); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("sync deduplication state directory: %w", err)
	}
	return &dedupeStateStore{db: db}, nil
}

func (s *dedupeStateStore) close() error {
	if s == nil || s.db == nil {
		return nil
	}
	return s.db.Close()
}

func (s *dedupeStateStore) lookup(queueName, key string) (dedupeEntry, bool, error) {
	var (
		entry dedupeEntry
		found bool
	)
	err := s.db.View(func(tx *bolt.Tx) error {
		queues := tx.Bucket(dedupeQueuesBucket)
		queue := queues.Bucket([]byte(queueName))
		if queue == nil {
			return nil
		}
		value := queue.Get([]byte(key))
		if value == nil {
			return nil
		}
		var err error
		entry, err = decodeDedupeEntry(value)
		found = err == nil
		return err
	})
	return entry, found, err
}

// reserve records the queue tail before the append. If the process stops
// before the final state is written, that offset is a precise lower bound for
// finding the record (if it landed at all).
func (s *dedupeStateStore) reserve(queueName, key string, tail uint64) (dedupeEntry, bool, error) {
	entry := dedupeEntry{offset: tail, state: dedupePending}
	found := false
	err := s.db.Update(func(tx *bolt.Tx) error {
		queues := tx.Bucket(dedupeQueuesBucket)
		queue, err := queues.CreateBucketIfNotExists([]byte(queueName))
		if err != nil {
			return err
		}
		if value := queue.Get([]byte(key)); value != nil {
			entry, err = decodeDedupeEntry(value)
			found = err == nil
			return err
		}
		return queue.Put([]byte(key), encodeDedupeEntry(entry))
	})
	return entry, found, err
}

func (s *dedupeStateStore) put(queueName, key string, entry dedupeEntry) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		queues := tx.Bucket(dedupeQueuesBucket)
		queue, err := queues.CreateBucketIfNotExists([]byte(queueName))
		if err != nil {
			return err
		}
		return queue.Put([]byte(key), encodeDedupeEntry(entry))
	})
}

func (s *dedupeStateStore) remove(queueName, key string) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		queue := tx.Bucket(dedupeQueuesBucket).Bucket([]byte(queueName))
		if queue == nil {
			return nil
		}
		return queue.Delete([]byte(key))
	})
}

func (s *dedupeStateStore) forget(queueName string) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		err := tx.Bucket(dedupeQueuesBucket).DeleteBucket([]byte(queueName))
		if errors.Is(err, bolt.ErrBucketNotFound) {
			return nil
		}
		return err
	})
}

func (s *dedupeStateStore) pruneBelow(queueName string, minOffset uint64) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		queue := tx.Bucket(dedupeQueuesBucket).Bucket([]byte(queueName))
		if queue == nil {
			return nil
		}
		cursor := queue.Cursor()
		for key, value := cursor.First(); key != nil; key, value = cursor.Next() {
			entry, err := decodeDedupeEntry(value)
			if err != nil {
				return err
			}
			if entry.state == dedupePending {
				if entry.offset < minOffset {
					entry.offset = minOffset
					if err := queue.Put(key, encodeDedupeEntry(entry)); err != nil {
						return err
					}
				}
				continue
			}
			if entry.offset < minOffset {
				if err := cursor.Delete(); err != nil {
					return err
				}
			}
		}
		return nil
	})
}

func encodeDedupeEntry(entry dedupeEntry) []byte {
	value := make([]byte, dedupeValueSize)
	value[0] = byte(entry.state)
	binary.BigEndian.PutUint64(value[1:], entry.offset)
	return value
}

func decodeDedupeEntry(value []byte) (dedupeEntry, error) {
	if len(value) != dedupeValueSize {
		return dedupeEntry{}, fmt.Errorf("invalid deduplication entry length %d", len(value))
	}
	entry := dedupeEntry{state: dedupeEntryState(value[0]), offset: binary.BigEndian.Uint64(value[1:])}
	switch entry.state {
	case dedupePending, dedupeUnconfirmed, dedupeConfirmed:
		return entry, nil
	default:
		return dedupeEntry{}, fmt.Errorf("invalid deduplication entry state %d", entry.state)
	}
}
