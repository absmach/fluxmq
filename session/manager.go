package session

import (
	"sync"
	"time"

	"github.com/dborovcanin/mqtt/store"
)

// Manager manages all sessions in the broker.
type Manager struct {
	mu       sync.RWMutex
	sessions map[string]*Session // clientID -> session

	// Storage backend
	store store.Store

	// Callbacks
	onSessionCreate  func(*Session)
	onSessionDestroy func(*Session)
	onWillTrigger    func(*store.WillMessage)

	// Background tasks
	stopCh chan struct{}
	wg     sync.WaitGroup
}

// NewManager creates a new session manager.
func NewManager(st store.Store) *Manager {
	m := &Manager{
		sessions: make(map[string]*Session),
		store:    st,
		stopCh:   make(chan struct{}),
	}

	// Start background tasks
	m.wg.Add(1)
	go m.expiryLoop()

	return m
}

// Get returns a session by client ID, or nil if not found.
func (m *Manager) Get(clientID string) *Session {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.sessions[clientID]
}

// GetOrCreate gets an existing session or creates a new one.
// If cleanStart is true and an existing session exists, it is destroyed first.
// Returns the session and whether it was newly created.
func (m *Manager) GetOrCreate(clientID string, version byte, opts Options) (*Session, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	existing := m.sessions[clientID]

	// Clean start: destroy existing session
	if opts.CleanStart && existing != nil {
		m.destroySessionLocked(existing)
		existing = nil
	}

	// Return existing session if not clean start
	if existing != nil {
		// Session takeover: disconnect existing connection
		if existing.IsConnected() {
			existing.Disconnect(false) // Not graceful (takeover)
		}

		// Update session options for new connection
		existing.Version = version
		existing.KeepAlive = opts.KeepAlive
		existing.Will = opts.Will

		return existing, false, nil
	}

	// Create new session
	session := New(clientID, version, opts)

	// Try to restore from storage
	if !opts.CleanStart && m.store != nil {
		if stored, err := m.store.Sessions().Get(clientID); err == nil {
			session.RestoreFrom(stored)

			// Restore subscriptions
			if subs, err := m.store.Subscriptions().GetForClient(clientID); err == nil {
				for _, sub := range subs {
					session.AddSubscription(sub.Filter, sub.Options)
				}
			}

			// Restore offline messages
			if msgs, err := m.store.Messages().List(clientID + "/queue/"); err == nil {
				for _, msg := range msgs {
					session.OfflineQueue.Enqueue(msg)
				}
				// Clear from storage after loading
				m.store.Messages().DeleteByPrefix(clientID + "/queue/")
			}
		}
	}

	// Set disconnect callback
	session.SetOnDisconnect(func(s *Session, graceful bool) {
		m.handleDisconnect(s, graceful)
	})

	m.sessions[clientID] = session

	// Persist session
	if m.store != nil {
		m.store.Sessions().Save(session.Info())
	}

	// Callback
	if m.onSessionCreate != nil {
		go m.onSessionCreate(session)
	}

	return session, true, nil
}

// Destroy removes a session completely.
func (m *Manager) Destroy(clientID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	session := m.sessions[clientID]
	if session == nil {
		return nil
	}

	return m.destroySessionLocked(session)
}

// destroySessionLocked destroys a session. Must be called with mu held.
func (m *Manager) destroySessionLocked(session *Session) error {
	// Disconnect if connected
	if session.IsConnected() {
		session.Disconnect(false)
	}

	// Clear from storage
	if m.store != nil {
		m.store.Sessions().Delete(session.ID)
		m.store.Subscriptions().RemoveAll(session.ID)
		m.store.Messages().DeleteByPrefix(session.ID + "/")
		m.store.Wills().Delete(session.ID)
	}

	delete(m.sessions, session.ID)

	// Callback
	if m.onSessionDestroy != nil {
		go m.onSessionDestroy(session)
	}

	return nil
}

// handleDisconnect handles session disconnect.
func (m *Manager) handleDisconnect(session *Session, graceful bool) {
	// Update storage
	if m.store != nil {
		m.store.Sessions().Save(session.Info())

		// Handle will message
		if !graceful && session.Will != nil {
			m.store.Wills().Set(session.ID, session.Will)
			// Note: Will triggering is handled by the will processor
		} else if graceful {
			// Clear will on graceful disconnect
			m.store.Wills().Delete(session.ID)
		}

		// Save offline queue to storage
		msgs := session.OfflineQueue.Drain()
		for i, msg := range msgs {
			key := session.ID + "/queue/" + string(rune(i))
			m.store.Messages().Store(key, msg)
		}

		// Save inflight messages
		for _, inf := range session.Inflight.GetAll() {
			key := session.ID + "/inflight/" + string(rune(inf.PacketID))
			m.store.Messages().Store(key, inf.Message)
		}
	}

	// Check if session should be destroyed (clean start with 0 expiry)
	if session.CleanStart && session.ExpiryInterval == 0 {
		m.mu.Lock()
		m.destroySessionLocked(session)
		m.mu.Unlock()
	}
}

// Count returns the number of sessions.
func (m *Manager) Count() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.sessions)
}

// ConnectedCount returns the number of connected sessions.
func (m *Manager) ConnectedCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()

	count := 0
	for _, s := range m.sessions {
		if s.IsConnected() {
			count++
		}
	}
	return count
}

// ForEach iterates over all sessions.
func (m *Manager) ForEach(fn func(*Session)) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	for _, s := range m.sessions {
		fn(s)
	}
}

// SetOnSessionCreate sets the session create callback.
func (m *Manager) SetOnSessionCreate(fn func(*Session)) {
	m.onSessionCreate = fn
}

// SetOnSessionDestroy sets the session destroy callback.
func (m *Manager) SetOnSessionDestroy(fn func(*Session)) {
	m.onSessionDestroy = fn
}

// SetOnWillTrigger sets the will trigger callback.
func (m *Manager) SetOnWillTrigger(fn func(*store.WillMessage)) {
	m.onWillTrigger = fn
}

// expiryLoop periodically checks for expired sessions.
func (m *Manager) expiryLoop() {
	defer m.wg.Done()

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			m.expireSessions()
			m.triggerWills()
		case <-m.stopCh:
			return
		}
	}
}

// expireSessions removes expired sessions.
func (m *Manager) expireSessions() {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := time.Now()
	var toDelete []string

	for clientID, session := range m.sessions {
		// Only check disconnected sessions
		if session.IsConnected() {
			continue
		}

		// Check expiry
		if session.ExpiryInterval > 0 {
			info := session.Info()
			expiryTime := info.DisconnectedAt.Add(time.Duration(session.ExpiryInterval) * time.Second)
			if now.After(expiryTime) {
				toDelete = append(toDelete, clientID)
			}
		}
	}

	for _, clientID := range toDelete {
		session := m.sessions[clientID]
		m.destroySessionLocked(session)
	}
}

// triggerWills processes pending will messages.
func (m *Manager) triggerWills() {
	if m.store == nil || m.onWillTrigger == nil {
		return
	}

	pending, err := m.store.Wills().GetPending(time.Now())
	if err != nil {
		return
	}

	for _, will := range pending {
		// Check if client has reconnected
		session := m.Get(will.ClientID)
		if session != nil && session.IsConnected() {
			// Client reconnected, don't trigger will
			m.store.Wills().Delete(will.ClientID)
			continue
		}

		// Trigger will
		m.onWillTrigger(will)
		m.store.Wills().Delete(will.ClientID)
	}
}

// Close stops the manager and cleans up.
func (m *Manager) Close() error {
	close(m.stopCh)
	m.wg.Wait()

	m.mu.Lock()
	defer m.mu.Unlock()

	// Disconnect all sessions
	for _, session := range m.sessions {
		if session.IsConnected() {
			session.Disconnect(false)
		}
	}

	return nil
}

// DrainOfflineQueue drains a session's offline queue and returns messages.
func (m *Manager) DrainOfflineQueue(clientID string) []*store.Message {
	session := m.Get(clientID)
	if session == nil {
		return nil
	}
	return session.OfflineQueue.Drain()
}

// QueueMessage adds a message to a session's offline queue.
func (m *Manager) QueueMessage(clientID string, msg *store.Message) error {
	session := m.Get(clientID)
	if session == nil {
		return nil // Session doesn't exist, drop message
	}

	if session.IsConnected() {
		return nil // Session is connected, don't queue
	}

	return session.OfflineQueue.Enqueue(msg)
}
