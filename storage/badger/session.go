package badger

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/absmach/mqtt/storage"
	"github.com/dgraph-io/badger/v4"
)

var _ storage.SessionStore = (*SessionStore)(nil)

// SessionStore implements storage.SessionStore using BadgerDB.
type SessionStore struct {
	db *badger.DB
}

// NewSessionStore creates a new BadgerDB session store.
func NewSessionStore(db *badger.DB) *SessionStore {
	return &SessionStore{db: db}
}

// Get retrieves a session by client ID.
func (s *SessionStore) Get(clientID string) (*storage.Session, error) {
	key := []byte("session:" + clientID)

	var session *storage.Session
	err := s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err != nil {
			if err == badger.ErrKeyNotFound {
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

		// Set TTL if session has expiry interval
		if session.ExpiryInterval > 0 {
			ttl := time.Duration(session.ExpiryInterval) * time.Second
			entry = entry.WithTTL(ttl)
		}

		return txn.SetEntry(entry)
	})
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
