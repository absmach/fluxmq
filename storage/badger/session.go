// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package badger

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/absmach/fluxmq/storage"
	"github.com/dgraph-io/badger/v4"
)

var _ storage.SessionStore = (*SessionStore)(nil)

// SessionStore implements storage.SessionStore using BadgerDB.
type SessionStore struct {
	db *db
}

// NewSessionStore creates a new BadgerDB session store.
// The store guards handle against use after close; closing handle directly
// bypasses that guard and lets BadgerDB panic on a racing operation.
func NewSessionStore(handle *badger.DB) *SessionStore {
	return newSessionStore(newDB(handle))
}

func newSessionStore(db *db) *SessionStore {
	return &SessionStore{db: db}
}

// Get retrieves a session by client ID.
func (s *SessionStore) Get(clientID string) (*storage.Session, error) {
	key := []byte("session:" + clientID)

	var session *storage.Session
	err := s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err != nil {
			if errors.Is(err, badger.ErrKeyNotFound) {
				return storage.ErrNotFound
			}
			return err
		}

		return item.Value(func(val []byte) error {
			session = &storage.Session{}
			return json.Unmarshal(val, session)
		})
	})
	if err != nil {
		return nil, err
	}

	return session, nil
}

// Save persists a session.
func (s *SessionStore) Save(session *storage.Session) error {
	key := []byte("session:" + session.ClientID)

	data, err := json.Marshal(session)
	if err != nil {
		return err
	}

	return s.db.Update(func(txn *badger.Txn) error {
		entry := badger.NewEntry(key, data)
		if ttl, ok := sessionTTL(session, time.Now()); ok {
			entry = entry.WithTTL(ttl)
		}
		return txn.SetEntry(entry)
	})
}

// sessionTTL returns how long a stored session record may outlive its client.
//
// The expiry interval runs from the moment the client disconnects
// [MQTT-3.1.2-23], so a connected session never expires: its record must
// survive a broker crash however long the connection lasted. A disconnected
// session keeps only what remains of its interval. One that has already run
// out keeps a second, long enough for an explicit cleanup to find it.
func sessionTTL(session *storage.Session, now time.Time) (time.Duration, bool) {
	if session.Connected || session.ExpiryInterval == 0 {
		return 0, false
	}

	interval := time.Duration(session.ExpiryInterval) * time.Second
	disconnectedAt := session.DisconnectedAt
	if disconnectedAt.IsZero() {
		disconnectedAt = now
	}
	return max(disconnectedAt.Add(interval).Sub(now), time.Second), true
}

// Delete removes a session.
func (s *SessionStore) Delete(clientID string) error {
	key := []byte("session:" + clientID)

	return s.db.Update(func(txn *badger.Txn) error {
		return txn.Delete(key)
	})
}

// GetExpired returns client IDs of sessions that have expired.
// Note: BadgerDB handles TTL automatically, so this queries for disconnected sessions
// that should have expired based on their DisconnectedAt + ExpiryInterval.
func (s *SessionStore) GetExpired(before time.Time) ([]string, error) {
	var expired []string

	err := s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = []byte("session:")
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()

			err := item.Value(func(val []byte) error {
				var session storage.Session
				if err := json.Unmarshal(val, &session); err != nil {
					return err
				}

				// Check if session is disconnected and expired
				if !session.Connected && session.ExpiryInterval > 0 {
					expiryTime := session.DisconnectedAt.Add(time.Duration(session.ExpiryInterval) * time.Second)
					if before.After(expiryTime) {
						expired = append(expired, session.ClientID)
					}
				}

				return nil
			})
			if err != nil {
				return err
			}
		}

		return nil
	})

	return expired, err
}

// List returns all sessions (for debugging/metrics).
func (s *SessionStore) List() ([]*storage.Session, error) {
	var sessions []*storage.Session

	err := s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = []byte("session:")
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()

			err := item.Value(func(val []byte) error {
				var session storage.Session
				if err := json.Unmarshal(val, &session); err != nil {
					return err
				}
				sessions = append(sessions, &session)
				return nil
			})
			if err != nil {
				return fmt.Errorf("failed to unmarshal session: %w", err)
			}
		}

		return nil
	})

	return sessions, err
}
