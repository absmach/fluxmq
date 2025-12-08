package memory

import (
	"sync"
	"time"

	"github.com/dborovcanin/mqtt/store"
)

// SessionStore is an in-memory implementation of store.SessionStore.
type SessionStore struct {
	mu    sync.RWMutex
	data  map[string]*store.Session
	locks map[string]*sync.Mutex
}

// NewSessionStore creates a new in-memory session store.
func NewSessionStore() *SessionStore {
	return &SessionStore{
		data:  make(map[string]*store.Session),
		locks: make(map[string]*sync.Mutex),
	}
}

// Get retrieves a session by client ID.
func (s *SessionStore) Get(clientID string) (*store.Session, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	session, ok := s.data[clientID]
	if !ok {
		return nil, store.ErrNotFound
	}
	return copySession(session), nil
}

// Save persists a session.
func (s *SessionStore) Save(session *store.Session) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.data[session.ClientID] = copySession(session)
	return nil
}

// Delete removes a session.
func (s *SessionStore) Delete(clientID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.data, clientID)
	delete(s.locks, clientID)
	return nil
}

// Lock acquires an exclusive lock for session takeover.
func (s *SessionStore) Lock(clientID string) (unlock func(), err error) {
	s.mu.Lock()
	lock, ok := s.locks[clientID]
	if !ok {
		lock = &sync.Mutex{}
		s.locks[clientID] = lock
	}
	s.mu.Unlock()

	lock.Lock()
	return func() { lock.Unlock() }, nil
}

// GetExpired returns client IDs of sessions that have expired.
func (s *SessionStore) GetExpired(before time.Time) ([]string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var expired []string
	for clientID, session := range s.data {
		// Only check disconnected sessions with expiry
		if !session.Connected && session.ExpiryInterval > 0 {
			expiryTime := session.DisconnectedAt.Add(time.Duration(session.ExpiryInterval) * time.Second)
			if expiryTime.Before(before) {
				expired = append(expired, clientID)
			}
		}
	}
	return expired, nil
}

// List returns all sessions.
func (s *SessionStore) List() ([]*store.Session, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	result := make([]*store.Session, 0, len(s.data))
	for _, session := range s.data {
		result = append(result, copySession(session))
	}
	return result, nil
}

// copySession creates a deep copy of a session.
func copySession(session *store.Session) *store.Session {
	if session == nil {
		return nil
	}

	return &store.Session{
		ClientID:        session.ClientID,
		Version:         session.Version,
		CleanStart:      session.CleanStart,
		ExpiryInterval:  session.ExpiryInterval,
		ConnectedAt:     session.ConnectedAt,
		DisconnectedAt:  session.DisconnectedAt,
		Connected:       session.Connected,
		ReceiveMaximum:  session.ReceiveMaximum,
		MaxPacketSize:   session.MaxPacketSize,
		TopicAliasMax:   session.TopicAliasMax,
		RequestResponse: session.RequestResponse,
		RequestProblem:  session.RequestProblem,
	}
}

// Ensure SessionStore implements store.SessionStore.
var _ store.SessionStore = (*SessionStore)(nil)
