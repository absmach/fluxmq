// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package badger

import (
	"sync"
	"time"

	"github.com/absmach/fluxmq/storage"
	"github.com/dgraph-io/badger/v4"
)

var _ storage.Store = (*Store)(nil)

// Store is the composite BadgerDB store implementing all storage interfaces.
type Store struct {
	db *badger.DB

	messages      *MessageStore
	sessions      *SessionStore
	subscriptions *SubscriptionStore
	retained      *RetainedStore
	wills         *WillStore

	gcStopCh chan struct{}
	gcDone   chan struct{}
	closed   bool
	mu       sync.Mutex
}

// Config holds BadgerDB configuration.
type Config struct {
	Dir string // Directory for BadgerDB data
}

// New creates a new BadgerDB-backed store.
func New(cfg Config) (*Store, error) {
	opts := badger.DefaultOptions(cfg.Dir)
	opts.Logger = nil // Disable BadgerDB's internal logging
	// Disable encryption to avoid "Invalid datakey id" errors on restart
	opts.EncryptionKey = nil
	opts.EncryptionKeyRotationDuration = 0
	// Async writes: MQTT messages are transient and can be re-delivered.
	// SyncWrites=true fsyncs on every write, which is 10-100x slower.
	opts.SyncWrites = false
	opts.NumVersionsToKeep = 1
	opts.NumCompactors = 2
	opts.NumLevelZeroTables = 5
	opts.NumLevelZeroTablesStall = 15

	db, err := badger.Open(opts)
	if err != nil {
		return nil, err
	}

	s := &Store{
		db:            db,
		messages:      NewMessageStore(db),
		sessions:      NewSessionStore(db),
		subscriptions: NewSubscriptionStore(db),
		retained:      NewRetainedStore(db),
		wills:         NewWillStore(db),
		gcStopCh:      make(chan struct{}),
		gcDone:        make(chan struct{}),
	}

	// Start background value log GC
	go s.runGC()

	return s, nil
}

// Messages returns the message store.
func (s *Store) Messages() storage.MessageStore {
	return s.messages
}

// Sessions returns the session store.
func (s *Store) Sessions() storage.SessionStore {
	return s.sessions
}

// Subscriptions returns the subscription store.
func (s *Store) Subscriptions() storage.SubscriptionStore {
	return s.subscriptions
}

// Retained returns the retained message store.
func (s *Store) Retained() storage.RetainedStore {
	return s.retained
}

// Wills returns the will message store.
func (s *Store) Wills() storage.WillStore {
	return s.wills
}

// Close gracefully closes the BadgerDB database.
func (s *Store) Close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	s.mu.Unlock()

	// Signal GC goroutine to stop
	close(s.gcStopCh)

	// Wait for GC to finish
	<-s.gcDone

	// Close the database
	return s.db.Close()
}

// runGC runs BadgerDB's value log garbage collection periodically.
func (s *Store) runGC() {
	defer close(s.gcDone)

	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			// Run GC with 0.5 discard ratio (reclaim if 50%+ of file is garbage)
			// This may return an error if no GC was needed, which is fine
			_ = s.db.RunValueLogGC(0.5)
		case <-s.gcStopCh:
			// Graceful shutdown: skip final GC to avoid vlog corruption
			// GC during close can cause "Invalid datakey id" errors on restart
			return
		}
	}
}
