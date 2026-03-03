// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package badger

import (
	"encoding/binary"
	"errors"

	badgerdb "github.com/dgraph-io/badger/v4"
)

const (
	prefixOutbound byte = 'o'
	prefixInbound  byte = 'i'
)

// ErrMissingDir indicates persistent mode was requested without a directory.
var ErrMissingDir = errors.New("badger dir is required when in-memory mode is disabled")

// Options configures the Badger-backed store.
type Options struct {
	Dir      string
	ValueDir string
	InMemory bool
}

// NewOptions returns default options for a Badger-backed store.
func NewOptions() *Options {
	return &Options{
		InMemory: true,
	}
}

// Validate validates options.
func (o *Options) Validate() error {
	if o == nil {
		return nil
	}

	if !o.InMemory && o.Dir == "" {
		return ErrMissingDir
	}

	return nil
}

// Store is a Badger-backed key/value implementation for MQTT message state.
type Store struct {
	db *badgerdb.DB
}

// New creates a new Badger-backed store.
func New(opts *Options) (*Store, error) {
	if opts == nil {
		opts = NewOptions()
	}

	if err := opts.Validate(); err != nil {
		return nil, err
	}

	dir := opts.Dir
	if opts.InMemory {
		dir = ""
	}

	badgerOpts := badgerdb.DefaultOptions(dir).WithLogger(nil)
	if opts.InMemory {
		badgerOpts = badgerOpts.WithInMemory(true)
	} else {
		valueDir := opts.ValueDir
		if valueDir == "" {
			valueDir = dir
		}
		badgerOpts = badgerOpts.WithValueDir(valueDir)
	}

	db, err := badgerdb.Open(badgerOpts)
	if err != nil {
		return nil, err
	}

	return &Store{db: db}, nil
}

func (s *Store) PutOutbound(packetID uint16, payload []byte) error {
	dbKey := encodeKey(prefixOutbound, packetID)
	return s.db.Update(func(txn *badgerdb.Txn) error {
		return txn.Set(dbKey, cloneBytes(payload))
	})
}

func (s *Store) GetOutbound(packetID uint16) ([]byte, bool, error) {
	return s.get(prefixOutbound, packetID)
}

func (s *Store) DeleteOutbound(packetID uint16) error {
	dbKey := encodeKey(prefixOutbound, packetID)
	return s.db.Update(func(txn *badgerdb.Txn) error {
		return txn.Delete(dbKey)
	})
}

func (s *Store) ListOutbound() ([][]byte, error) {
	return s.list(prefixOutbound)
}

func (s *Store) PutInbound(packetID uint16, payload []byte) error {
	dbKey := encodeKey(prefixInbound, packetID)
	return s.db.Update(func(txn *badgerdb.Txn) error {
		return txn.Set(dbKey, cloneBytes(payload))
	})
}

func (s *Store) GetInbound(packetID uint16) ([]byte, bool, error) {
	return s.get(prefixInbound, packetID)
}

func (s *Store) DeleteInbound(packetID uint16) error {
	dbKey := encodeKey(prefixInbound, packetID)
	return s.db.Update(func(txn *badgerdb.Txn) error {
		return txn.Delete(dbKey)
	})
}

func (s *Store) Reset() error {
	return s.db.DropPrefix([]byte{prefixOutbound}, []byte{prefixInbound})
}

func (s *Store) Close() error {
	if s.db == nil {
		return nil
	}
	return s.db.Close()
}

func (s *Store) get(prefix byte, packetID uint16) ([]byte, bool, error) {
	dbKey := encodeKey(prefix, packetID)

	var value []byte
	err := s.db.View(func(txn *badgerdb.Txn) error {
		item, err := txn.Get(dbKey)
		if err != nil {
			return err
		}

		value, err = item.ValueCopy(nil)
		return err
	})
	if errors.Is(err, badgerdb.ErrKeyNotFound) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}

	return value, true, nil
}

func (s *Store) list(prefix byte) ([][]byte, error) {
	p := []byte{prefix}
	values := make([][]byte, 0)

	err := s.db.View(func(txn *badgerdb.Txn) error {
		it := txn.NewIterator(badgerdb.DefaultIteratorOptions)
		defer it.Close()

		for it.Seek(p); it.ValidForPrefix(p); it.Next() {
			item := it.Item()
			value, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			values = append(values, value)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	return values, nil
}

func encodeKey(prefix byte, key uint16) []byte {
	encoded := make([]byte, 3)
	encoded[0] = prefix
	binary.BigEndian.PutUint16(encoded[1:], key)
	return encoded
}

func cloneBytes(value []byte) []byte {
	if value == nil {
		return nil
	}

	out := make([]byte, len(value))
	copy(out, value)
	return out
}
