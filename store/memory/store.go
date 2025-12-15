// Package memory provides an in-memory implementation of the store interfaces.
package memory

import (
	"github.com/absmach/mqtt/store"
)

var _ store.Store = (*Store)(nil)

// Store is the composite in-memory store.
type Store struct {
	messages      *MessageStore
	sessions      *SessionStore
	subscriptions *SubscriptionStore
	retained      *RetainedStore
	wills         *WillStore
}

// New creates a new in-memory store.
func New() *Store {
	return &Store{
		messages:      NewMessageStore(),
		sessions:      NewSessionStore(),
		subscriptions: NewSubscriptionStore(),
		retained:      NewRetainedStore(),
		wills:         NewWillStore(),
	}
}

// Messages returns the message store.
func (s *Store) Messages() store.MessageStore {
	return s.messages
}

// Sessions returns the session store.
func (s *Store) Sessions() store.SessionStore {
	return s.sessions
}

// Subscriptions returns the subscription store.
func (s *Store) Subscriptions() store.SubscriptionStore {
	return s.subscriptions
}

// Retained returns the retained message store.
func (s *Store) Retained() store.RetainedStore {
	return s.retained
}

// Wills returns the will message store.
func (s *Store) Wills() store.WillStore {
	return s.wills
}

// Close closes all stores (no-op for memory).
func (s *Store) Close() error {
	return nil
}
