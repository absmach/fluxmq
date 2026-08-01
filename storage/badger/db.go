// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package badger

import (
	"sync"

	"github.com/absmach/fluxmq/storage"
	"github.com/dgraph-io/badger/v4"
)

// db owns a BadgerDB handle together with its close state.
//
// BadgerDB panics rather than returning an error when a closed handle is used
// — Txn.NewIterator is one such path — and the broker destroys sessions from
// goroutines that can outlive the store during shutdown. Every operation holds
// the read side of mu and Close holds the write side, so a handle is never used
// after it is closed: operations that lose the race return storage.ErrClosed.
//
// A db is safe for concurrent use.
type db struct {
	handle *badger.DB

	mu     sync.RWMutex
	closed bool
}

func newDB(handle *badger.DB) *db {
	return &db{handle: handle}
}

// View runs fn inside a read-only transaction.
func (d *db) View(fn func(txn *badger.Txn) error) error {
	d.mu.RLock()
	defer d.mu.RUnlock()

	if d.closed {
		return storage.ErrClosed
	}

	return d.handle.View(fn)
}

// Update runs fn inside a read-write transaction.
func (d *db) Update(fn func(txn *badger.Txn) error) error {
	d.mu.RLock()
	defer d.mu.RUnlock()

	if d.closed {
		return storage.ErrClosed
	}

	return d.handle.Update(fn)
}

// RunValueLogGC runs one round of value log garbage collection.
func (d *db) RunValueLogGC(discardRatio float64) error {
	d.mu.RLock()
	defer d.mu.RUnlock()

	if d.closed {
		return storage.ErrClosed
	}

	return d.handle.RunValueLogGC(discardRatio)
}

// Close closes the handle once every in-flight operation has returned. It is
// idempotent.
func (d *db) Close() error {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.closed {
		return nil
	}
	d.closed = true

	return d.handle.Close()
}
