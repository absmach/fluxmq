package session

import (
	"fmt"
	"sync"
	"time"

	"github.com/dborovcanin/mqtt/store"
)

// Manager manages all sessions in the broker.
type Manager struct {
	mu    sync.RWMutex
	cache Cache

	messages      store.MessageStore
	sessions      store.SessionStore
	subscriptions store.SubscriptionStore
	retained      store.RetainedStore
	wills         store.WillStore

	onSessionCreate  func(*Session)
	onSessionDestroy func(*Session)
	onWillTrigger    func(*store.WillMessage)

	stopCh chan struct{}
	wg     sync.WaitGroup
}

// NewManager creates a new session manager.
func NewManager(st store.Store) *Manager {
	m := &Manager{
		cache:         NewMapCache(),
		messages:      st.Messages(),
		sessions:      st.Sessions(),
		subscriptions: st.Subscriptions(),
		retained:      st.Retained(),
		wills:         st.Wills(),

		stopCh: make(chan struct{}),
	}

	m.wg.Add(1)
	go m.expiryLoop()

	return m
}

// Get returns a session by client ID, or nil if not found.
func (m *Manager) Get(clientID string) *Session {
	return m.cache.Get(clientID)
}

// GetOrCreate gets an existing session or creates a new one.
// If cleanStart is true and an existing session exists, it is destroyed first.
// Returns the session and whether it was newly created.
func (m *Manager) GetOrCreate(clientID string, version byte, opts Options) (*Session, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	existing := m.cache.Get(clientID)
	if opts.CleanStart && existing != nil {
		m.destroySessionLocked(existing)
		existing = nil
	}

	if existing != nil {
		m.handleExistingSession(existing, version, opts)
		return existing, false, nil
	}

	session := New(clientID, version, opts)

	if err := m.restoreSessionFromStorage(session, clientID, opts); err != nil {
		return nil, false, err
	}

	session.SetOnDisconnect(func(s *Session, graceful bool) {
		m.handleDisconnect(s, graceful)
	})

	m.cache.Set(clientID, session)

	if m.sessions != nil {
		if err := m.sessions.Save(session.Info()); err != nil {
			return nil, false, fmt.Errorf("failed to save session to storage: %w", err)
		}
	}

	if m.onSessionCreate != nil {
		go m.onSessionCreate(session)
	}

	return session, true, nil
}

// handleExistingSession handles session takeover when a client reconnects.
func (m *Manager) handleExistingSession(session *Session, version byte, opts Options) {
	if session.IsConnected() {
		session.Disconnect(false) // Not graceful (takeover)
	}
	session.UpdateConnectionOptions(version, opts.KeepAlive, opts.Will)
}

// restoreSessionFromStorage restores session state from persistent storage.
func (m *Manager) restoreSessionFromStorage(session *Session, clientID string, opts Options) error {
	if opts.CleanStart || m.sessions == nil {
		return nil
	}

	stored, err := m.sessions.Get(clientID)
	if err != nil && err != store.ErrNotFound {
		return fmt.Errorf("failed to get session from storage: %w", err)
	}
	if stored != nil {
		session.RestoreFrom(stored)
	}

	subs, err := m.subscriptions.GetForClient(clientID)
	if err != nil {
		return fmt.Errorf("failed to get subscriptions from storage: %w", err)
	}
	for _, sub := range subs {
		session.AddSubscription(sub.Filter, sub.Options)
	}

	msgs, err := m.messages.List(clientID + "/queue/")
	if err != nil {
		return fmt.Errorf("failed to list offline messages from storage: %w", err)
	}
	for _, msg := range msgs {
		if err := session.OfflineQueue.Enqueue(msg); err != nil {
			return fmt.Errorf("failed to enqueue offline message: %w", err)
		}
	}

	if err := m.messages.DeleteByPrefix(clientID + "/queue/"); err != nil {
		return fmt.Errorf("failed to clear offline messages from storage: %w", err)
	}

	inflightMsgs, err := m.messages.List(clientID + "/inflight/")
	if err != nil {
		return fmt.Errorf("failed to list inflight messages from storage: %w", err)
	}
	for _, msg := range inflightMsgs {
		if msg.PacketID != 0 {
			if err := session.Inflight.Add(msg.PacketID, msg, Outbound); err != nil {
				continue
			}
		}
	}

	if err := m.messages.DeleteByPrefix(clientID + "/inflight/"); err != nil {
		return fmt.Errorf("failed to clear inflight messages from storage: %w", err)
	}

	return nil
}

// Destroy removes a session completely.
func (m *Manager) Destroy(clientID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	session := m.cache.Get(clientID)
	if session == nil {
		return nil
	}

	return m.destroySessionLocked(session)
}

// destroySessionLocked destroys a session. Must be called with mu held.
func (m *Manager) destroySessionLocked(session *Session) error {
	if session.IsConnected() {
		session.Disconnect(false)
	}

	if m.sessions != nil {
		if err := m.sessions.Delete(session.ID); err != nil {
			return fmt.Errorf("failed to delete session from storage: %w", err)
		}
	}
	if m.subscriptions != nil {
		if err := m.subscriptions.RemoveAll(session.ID); err != nil {
			return fmt.Errorf("failed to remove subscriptions from storage: %w", err)
		}
	}
	if m.messages != nil {
		if err := m.messages.DeleteByPrefix(session.ID + "/"); err != nil {
			return fmt.Errorf("failed to delete messages from storage: %w", err)
		}
	}
	if m.wills != nil {
		if err := m.wills.Delete(session.ID); err != nil {
			return fmt.Errorf("failed to delete will from storage: %w", err)
		}
	}

	m.cache.Delete(session.ID)

	if m.onSessionDestroy != nil {
		go m.onSessionDestroy(session)
	}

	return nil
}

// handleDisconnect handles session disconnect.
func (m *Manager) handleDisconnect(session *Session, graceful bool) {
	if m.sessions != nil {
		if err := m.sessions.Save(session.Info()); err != nil {
			_ = err
		}
	}
	if m.wills != nil {
		will := session.GetWill()
		if !graceful && will != nil {
			if err := m.wills.Set(session.ID, will); err != nil {
				_ = err
			}
		} else if graceful {
			if err := m.wills.Delete(session.ID); err != nil {
				_ = err
			}
		}
	}
	if m.messages != nil {
		msgs := session.OfflineQueue.Drain()
		for i, msg := range msgs {
			key := session.ID + "/queue/" + string(rune(i))
			if err := m.messages.Store(key, msg); err != nil {
				_ = err
			}
		}

		for _, inf := range session.Inflight.GetAll() {
			key := session.ID + "/inflight/" + string(rune(inf.PacketID))
			if err := m.messages.Store(key, inf.Message); err != nil {
				_ = err
			}
		}
	}

	if session.CleanStart && session.ExpiryInterval == 0 {
		m.mu.Lock()
		_ = m.destroySessionLocked(session)
		m.mu.Unlock()
	}
}

// Count returns the number of sessions.
func (m *Manager) Count() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.cache.Count()
}

// ConnectedCount returns the number of connected sessions.
func (m *Manager) ConnectedCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()

	count := 0
	m.cache.ForEach(func(s *Session) {
		if s.IsConnected() {
			count++
		}
	})
	return count
}

// ForEach iterates over all sessions.
func (m *Manager) ForEach(fn func(*Session)) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	m.cache.ForEach(func(s *Session) {
		fn(s)
	})
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
	m.mu.Lock()
	defer m.mu.Unlock()
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

	m.cache.ForEach(func(session *Session) {
		if session.IsConnected() {
			return
		}

		if session.ExpiryInterval > 0 {
			info := session.Info()
			expiryTime := info.DisconnectedAt.Add(time.Duration(session.ExpiryInterval) * time.Second)
			if now.After(expiryTime) {
				toDelete = append(toDelete, session.ID)
			}
		}
	})

	for _, clientID := range toDelete {
		session := m.cache.Get(clientID)
		m.destroySessionLocked(session)
	}
}

// triggerWills processes pending will messages.
func (m *Manager) triggerWills() {
	if m.wills == nil || m.onWillTrigger == nil {
		return
	}

	pending, err := m.wills.GetPending(time.Now())
	if err != nil {
		return
	}

	for _, will := range pending {
		session := m.Get(will.ClientID)
		if session != nil && session.IsConnected() {
			m.wills.Delete(will.ClientID)
			continue
		}

		m.onWillTrigger(will)
		m.wills.Delete(will.ClientID)
	}
}

// Close stops the manager and cleans up.
func (m *Manager) Close() error {
	close(m.stopCh)
	m.wg.Wait()

	m.mu.Lock()
	defer m.mu.Unlock()

	m.cache.ForEach(func(session *Session) {
		if session.IsConnected() {
			session.Disconnect(false)
		}
	})

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
		return nil
	}

	if session.IsConnected() {
		return nil
	}

	return session.OfflineQueue.Enqueue(msg)
}
