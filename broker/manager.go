package broker

import (
	"fmt"
	"time"

	"github.com/dborovcanin/mqtt/session"
	"github.com/dborovcanin/mqtt/store"
	"github.com/dborovcanin/mqtt/store/messages"
)

// Get returns a session by client ID, or nil if not found.
func (b *Broker) Get(clientID string) *session.Session {
	return b.sessionsMap.Get(clientID)
}

// GetOrCreate gets an existing session or creates a new one.
// If cleanStart is true and an existing session exists, it is destroyed first.
// Returns the session and whether it was newly created.
func (b *Broker) GetOrCreate(clientID string, version byte, opts session.Options) (*session.Session, bool, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	existing := b.sessionsMap.Get(clientID)
	if opts.CleanStart && existing != nil {
		b.destroySessionLocked(existing)
		existing = nil
	}

	if existing != nil {
		b.handleExistingSession(existing, version, opts)
		return existing, false, nil
	}

	receiveMax := opts.ReceiveMaximum
	if receiveMax == 0 {
		receiveMax = 65535
	}
	inflight := messages.NewInflightTracker(int(receiveMax))

	offlineQueue := messages.NewMessageQueue(1000)

	// Restore from storage if not clean start
	if !opts.CleanStart {
		if err := b.restoreInflightFromStorage(clientID, inflight); err != nil {
			return nil, false, err
		}
		if err := b.restoreQueueFromStorage(clientID, offlineQueue); err != nil {
			return nil, false, err
		}
	}

	// Create session with pre-configured dependencies
	sess := session.New(clientID, version, opts, inflight, offlineQueue)

	// Restore session metadata and subscriptions
	if err := b.restoreSessionFromStorage(sess, clientID, opts); err != nil {
		return nil, false, err
	}

	sess.SetOnDisconnect(func(s *session.Session, graceful bool) {
		b.handleDisconnect(s, graceful)
	})

	b.sessionsMap.Set(clientID, sess)

	if b.sessions != nil {
		if err := b.sessions.Save(sess.Info()); err != nil {
			return nil, false, fmt.Errorf("failed to save session to storage: %w", err)
		}
	}

	// onSessionCreate was purely internal/unused in original, skipping or unimplemented for now unless needed.
	// logic was: if m.onSessionCreate != nil { go m.onSessionCreate(session) }

	return sess, true, nil
}

// handleExistingSession handles session takeover when a client reconnects.
func (b *Broker) handleExistingSession(s *session.Session, version byte, opts session.Options) {
	if s.IsConnected() {
		s.Disconnect(false) // Not graceful (takeover)
	}
	s.UpdateConnectionOptions(version, opts.KeepAlive, opts.Will)
}

// restoreInflightFromStorage restores inflight messages from storage into the tracker.
func (b *Broker) restoreInflightFromStorage(clientID string, tracker messages.Inflight) error {
	if b.messages == nil {
		return nil
	}

	inflightMsgs, err := b.messages.List(clientID + "/inflight/")
	if err != nil {
		return fmt.Errorf("failed to list inflight messages from storage: %w", err)
	}

	for _, msg := range inflightMsgs {
		if msg.PacketID != 0 {
			if err := tracker.Add(msg.PacketID, msg, messages.Outbound); err != nil {
				continue
			}
		}
	}

	if err := b.messages.DeleteByPrefix(clientID + "/inflight/"); err != nil {
		return fmt.Errorf("failed to clear inflight messages from storage: %w", err)
	}

	return nil
}

// restoreQueueFromStorage restores offline messages from storage into the queue.
func (b *Broker) restoreQueueFromStorage(clientID string, queue messages.Queue) error {
	if b.messages == nil {
		return nil
	}

	msgs, err := b.messages.List(clientID + "/queue/")
	if err != nil {
		return fmt.Errorf("failed to list offline messages from storage: %w", err)
	}

	for _, msg := range msgs {
		if err := queue.Enqueue(msg); err != nil {
			return fmt.Errorf("failed to enqueue offline message: %w", err)
		}
	}

	if err := b.messages.DeleteByPrefix(clientID + "/queue/"); err != nil {
		return fmt.Errorf("failed to clear offline messages from storage: %w", err)
	}

	return nil
}

// restoreSessionFromStorage restores session metadata and subscriptions from persistent storage.
func (b *Broker) restoreSessionFromStorage(s *session.Session, clientID string, opts session.Options) error {
	if opts.CleanStart || b.sessions == nil {
		return nil
	}

	stored, err := b.sessions.Get(clientID)
	if err != nil && err != store.ErrNotFound {
		return fmt.Errorf("failed to get session from storage: %w", err)
	}
	if stored != nil {
		s.RestoreFrom(stored)
	}

	subs, err := b.subscriptions.GetForClient(clientID)
	if err != nil {
		return fmt.Errorf("failed to get subscriptions from storage: %w", err)
	}
	for _, sub := range subs {
		s.AddSubscription(sub.Filter, sub.Options)
	}

	return nil
}

// Destroy removes a session completely.
func (b *Broker) Destroy(clientID string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	s := b.sessionsMap.Get(clientID)
	if s == nil {
		return nil
	}

	return b.destroySessionLocked(s)
}

// destroySessionLocked destroys a session. Must be called with mu held.
func (b *Broker) destroySessionLocked(s *session.Session) error {
	if s.IsConnected() {
		s.Disconnect(false)
	}

	if b.sessions != nil {
		if err := b.sessions.Delete(s.ID); err != nil {
			return fmt.Errorf("failed to delete session from storage: %w", err)
		}
	}
	if b.subscriptions != nil {
		if err := b.subscriptions.RemoveAll(s.ID); err != nil {
			return fmt.Errorf("failed to remove subscriptions from storage: %w", err)
		}
	}
	if b.messages != nil {
		if err := b.messages.DeleteByPrefix(s.ID + "/"); err != nil {
			return fmt.Errorf("failed to delete messages from storage: %w", err)
		}
	}
	if b.wills != nil {
		if err := b.wills.Delete(s.ID); err != nil {
			return fmt.Errorf("failed to delete will from storage: %w", err)
		}
	}

	b.sessionsMap.Delete(s.ID)

	// Direct integration: Unsubscribe directly instead of using callback
	subs := s.GetSubscriptions()
	for filter := range subs {
		b.router.Unsubscribe(filter, s.ID)
	}

	return nil
}

// handleDisconnect handles session disconnect.
func (b *Broker) handleDisconnect(s *session.Session, graceful bool) {
	if b.sessions != nil {
		if err := b.sessions.Save(s.Info()); err != nil {
			// b.logger.Error("Failed to save session info", "error", err)
		}
	}
	if b.wills != nil {
		will := s.GetWill()
		if !graceful && will != nil {
			if err := b.wills.Set(s.ID, will); err != nil {
				// b.logger.Error("Failed to store will", "error", err)
			}
		} else if graceful {
			if err := b.wills.Delete(s.ID); err != nil {
				// b.logger.Error("Failed to delete will", "error", err)
			}
		}
	}
	if b.messages != nil {
		msgs := s.OfflineQueue().Drain()
		for i, msg := range msgs {
			key := s.ID + "/queue/" + string(rune(i))
			if err := b.messages.Store(key, msg); err != nil {
				// b.logger.Error("Failed to store offline msg", "error", err)
			}
		}

		for _, inf := range s.Inflight().GetAll() {
			key := s.ID + "/inflight/" + string(rune(inf.PacketID))
			if err := b.messages.Store(key, inf.Message); err != nil {
				// b.logger.Error("Failed to store inflight msg", "error", err)
			}
		}
	}

	if s.CleanStart && s.ExpiryInterval == 0 {
		b.mu.Lock()
		_ = b.destroySessionLocked(s)
		b.mu.Unlock()
	}
}

// Count returns the number of sessions.
func (b *Broker) Count() int {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.sessionsMap.Count()
}

// ConnectedCount returns the number of connected sessions.
func (b *Broker) ConnectedCount() int {
	b.mu.RLock()
	defer b.mu.RUnlock()

	count := 0
	b.sessionsMap.ForEach(func(s *session.Session) {
		if s.IsConnected() {
			count++
		}
	})
	return count
}

// ForEach iterates over all sessions.
func (b *Broker) ForEach(fn func(*session.Session)) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	b.sessionsMap.ForEach(func(s *session.Session) {
		fn(s)
	})
}

// expiryLoop periodically checks for expired sessions.
func (b *Broker) expiryLoop() {
	defer b.wg.Done()

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			b.expireSessions()
			b.triggerWills()
		case <-b.stopCh:
			return
		}
	}
}

// expireSessions removes expired sessions.
func (b *Broker) expireSessions() {
	b.mu.Lock()
	defer b.mu.Unlock()

	now := time.Now()
	var toDelete []string

	b.sessionsMap.ForEach(func(s *session.Session) {
		if s.IsConnected() {
			return
		}

		if s.ExpiryInterval > 0 {
			info := s.Info()
			expiryTime := info.DisconnectedAt.Add(time.Duration(s.ExpiryInterval) * time.Second)
			if now.After(expiryTime) {
				toDelete = append(toDelete, s.ID)
			}
		}
	})

	for _, clientID := range toDelete {
		s := b.sessionsMap.Get(clientID)
		b.destroySessionLocked(s)
	}
}

// triggerWills processes pending will messages.
func (b *Broker) triggerWills() {
	if b.wills == nil {
		return
	}

	pending, err := b.wills.GetPending(time.Now())
	if err != nil {
		return
	}

	for _, will := range pending {
		s := b.Get(will.ClientID)
		if s != nil && s.IsConnected() {
			b.wills.Delete(will.ClientID)
			continue
		}

		// Direct integration: Distribute directly instead of callback
		b.Distribute(will.Topic, will.Payload, will.QoS, will.Retain, will.Properties)
		b.wills.Delete(will.ClientID)
	}
}

// DrainOfflineQueue drains a session's offline queue and returns messages.
func (b *Broker) DrainOfflineQueue(clientID string) []*store.Message {
	s := b.Get(clientID)
	if s == nil {
		return nil
	}
	return s.OfflineQueue().Drain()
}

// QueueMessage adds a message to a session's offline queue.
func (b *Broker) QueueMessage(clientID string, msg *store.Message) error {
	s := b.Get(clientID)
	if s == nil {
		return nil
	}

	if s.IsConnected() {
		return nil
	}

	return s.OfflineQueue().Enqueue(msg)
}
