// Package badger provides a BadgerDB-backed implementation of the storage interfaces.
package badger

import (
	"github.com/absmach/mqtt/storage"
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
}

// Config holds BadgerDB configuration.
type Config struct {
	Dir string // Directory for BadgerDB data
}

// New creates a new BadgerDB-backed store.
func New(cfg Config) (*Store, error) {
	opts := badger.DefaultOptions(cfg.Dir)
	opts.Logger = nil // Disable BadgerDB's internal logging

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

// Close closes the BadgerDB database.
func (s *Store) Close() error {
	return s.db.Close()
}

// runGC runs BadgerDB's value log garbage collection periodically.
func (s *Store) runGC() {
	// TODO: Add proper shutdown signal handling
	// For now, this will run until the process exits
	// ticker := time.NewTicker(5 * time.Minute)
	// defer ticker.Stop()
	//
	// for range ticker.C {
	// 	s.db.RunValueLogGC(0.5)
	// }
}
