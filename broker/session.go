// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package broker

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/absmach/fluxmq/broker/events"
	clusterv1 "github.com/absmach/fluxmq/pkg/proto/cluster/v1"
	"github.com/absmach/fluxmq/session"
	"github.com/absmach/fluxmq/storage"
	"github.com/absmach/fluxmq/storage/messages"
	"github.com/absmach/fluxmq/topics"
)

// CreateSession creates a new session or returns an existing one.
// If opts.CleanStart is true and a session exists, it is destroyed first.
// Returns the session and whether it was newly created.
func (b *Broker) CreateSession(clientID string, version byte, opts session.Options) (*session.Session, bool, error) {
	b.sessionLocks.Lock(clientID)
	defer b.sessionLocks.Unlock(clientID)

	// Check if session is owned by another node in the cluster
	var takeoverState *clusterv1.SessionState
	if b.cluster != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		ownerNode, exists, err := b.cluster.GetSessionOwner(ctx, clientID)
		if err != nil {
			b.logError("get_session_owner", err, slog.String("client_id", clientID))
			return nil, false, fmt.Errorf("failed to check session ownership: %w", err)
		}

		if exists && ownerNode != b.cluster.NodeID() {
			// Session exists on different node - trigger takeover
			b.logger.Info("taking over session from remote node",
				slog.String("client_id", clientID),
				slog.String("from_node", ownerNode),
				slog.String("to_node", b.cluster.NodeID()))

			takeoverState, err = b.cluster.TakeoverSession(ctx, clientID, ownerNode, b.cluster.NodeID())
			if err != nil {
				b.logError("takeover_session", err, slog.String("client_id", clientID))
				return nil, false, fmt.Errorf("session takeover failed: %w", err)
			}

			b.logger.Info("session takeover completed", slog.String("client_id", clientID))

			// Webhook: session takeover
			if b.webhooks != nil {
				b.webhooks.Notify(ctx, events.SessionTakeover{
					ClientID: clientID,
					FromNode: ownerNode,
					ToNode:   b.cluster.NodeID(),
				})
			}
		}
	}

	existing := b.sessionsMap.Get(clientID)
	if opts.CleanStart && existing != nil {
		if err := b.destroySessionLocked(existing); err != nil {
			return nil, false, err
		}
		existing = nil
	}

	if existing != nil {
		return existing, false, nil
	}

	const serverReceiveMaximum = 256
	receiveMax := opts.ReceiveMaximum
	if receiveMax == 0 || receiveMax > serverReceiveMaximum {
		receiveMax = serverReceiveMaximum
	}
	inflight := messages.NewInflightTracker(int(receiveMax))
	offlineQueue := messages.NewMessageQueue(1000)

	// Restore from takeover state if present
	if takeoverState != nil {
		if err := b.restoreInflightFromTakeover(takeoverState, inflight); err != nil {
			return nil, false, fmt.Errorf("failed to restore inflight from takeover: %w", err)
		}
		if err := b.restoreQueueFromTakeover(takeoverState, offlineQueue); err != nil {
			return nil, false, fmt.Errorf("failed to restore queue from takeover: %w", err)
		}
	} else if !opts.CleanStart {
		if err := b.restoreInflightFromStorage(clientID, inflight); err != nil {
			return nil, false, err
		}
		if err := b.restoreQueueFromStorage(clientID, offlineQueue); err != nil {
			return nil, false, err
		}
	}

	// Handle will message from takeover or from opts
	if takeoverState != nil && takeoverState.Will != nil {
		// Restore will from takeover state
		opts.Will = &storage.WillMessage{
			ClientID:   clientID,
			Topic:      takeoverState.Will.Topic,
			Payload:    takeoverState.Will.Payload,
			QoS:        byte(takeoverState.Will.Qos),
			Retain:     takeoverState.Will.Retain,
			Delay:      takeoverState.Will.Delay,
			Properties: nil,
		}
	} else if opts.Will != nil {
		// Ensure ClientID is set
		opts.Will.ClientID = clientID
	}

	// Override session expiry from takeover state if available
	if takeoverState != nil && takeoverState.ExpiryInterval > 0 {
		opts.ExpiryInterval = takeoverState.ExpiryInterval
	}

	// Cap persistent session expiry to prevent indefinite memory growth.
	// Sessions with CleanStart=false and ExpiryInterval=0 would otherwise live forever.
	const maxSessionExpiry uint32 = 86400 // 24 hours
	if !opts.CleanStart && opts.ExpiryInterval == 0 {
		opts.ExpiryInterval = maxSessionExpiry
	}

	// Override receive maximum with normalized value
	opts.ReceiveMaximum = receiveMax

	s := session.New(clientID, version, opts, inflight, offlineQueue)

	// Restore subscriptions from takeover state or storage
	if takeoverState != nil {
		if err := b.restoreSubscriptionsFromTakeover(s, takeoverState); err != nil {
			return nil, false, fmt.Errorf("failed to restore subscriptions from takeover: %w", err)
		}
	} else if err := b.restoreSessionFromStorage(s, clientID, opts); err != nil {
		return nil, false, err
	}

	s.SetOnDisconnect(func(s *session.Session, graceful bool) {
		b.handleDisconnect(s, graceful)
	})

	b.sessionsMap.Set(clientID, s)

	if b.sessions != nil {
		if err := b.sessions.Save(s.Info()); err != nil {
			return nil, false, fmt.Errorf("failed to save session: %w", err)
		}
	}

	if b.cluster != nil {
		ctx := context.Background()
		nodeID := b.cluster.NodeID()
		if err := b.cluster.AcquireSession(ctx, clientID, nodeID); err != nil {
			b.logError("cluster_acquire_session", err, slog.String("client_id", clientID))
		}
	}

	return s, true, nil
}

// DestroySession removes a session completely.
func (b *Broker) DestroySession(clientID string) error {
	b.sessionLocks.Lock(clientID)
	defer b.sessionLocks.Unlock(clientID)

	s := b.sessionsMap.Get(clientID)
	if s == nil {
		return nil
	}

	return b.destroySessionLocked(s)
}

// destroySessionLocked destroys a session. Must be called with the session's key lock held.
func (b *Broker) destroySessionLocked(s *session.Session) error {
	if s.IsConnected() {
		s.Disconnect(false)
	}

	if b.sessions != nil {
		if err := b.sessions.Delete(s.ID); err != nil {
			return fmt.Errorf("failed to delete session: %w", err)
		}
	}
	if b.subscriptions != nil {
		if err := b.subscriptions.RemoveAll(s.ID); err != nil {
			return fmt.Errorf("failed to remove subscriptions: %w", err)
		}
	}
	if b.messages != nil {
		if err := b.messages.DeleteByPrefix(s.ID + "/"); err != nil {
			return fmt.Errorf("failed to delete messages: %w", err)
		}
	}
	if b.wills != nil {
		if err := b.wills.Delete(context.Background(), s.ID); err != nil {
			return fmt.Errorf("failed to delete will: %w", err)
		}
	}

	b.sessionsMap.Delete(s.ID)

	subs := s.GetSubscriptions()
	for filter := range subs {
		// Check if this is a shared subscription and clean up share groups
		if topics.IsShared(filter) {
			if b.sharedSubs.Unsubscribe(s.ID, filter) {
				shareName, topicFilter, _ := topics.ParseShared(filter)
				shareClientID := "$share/" + shareName + "/" + topicFilter
				b.router.Unsubscribe(shareClientID, topicFilter)
			}
		} else {
			b.router.Unsubscribe(s.ID, filter)
		}

		// Remove subscription from cluster
		if b.cluster != nil {
			ctx := context.Background()
			if err := b.cluster.RemoveSubscription(ctx, s.ID, filter); err != nil {
				b.logError("cluster_remove_subscription", err, slog.String("client_id", s.ID), slog.String("filter", filter))
			}
		}
	}

	// Release session ownership in cluster
	if b.cluster != nil {
		ctx := context.Background()
		if err := b.cluster.ReleaseSession(ctx, s.ID); err != nil {
			b.logError("cluster_release_session", err, slog.String("client_id", s.ID))
		}
	}

	return nil
}

// handleDisconnect handles session disconnect.
func (b *Broker) handleDisconnect(s *session.Session, graceful bool) {
	// Webhook: client disconnected
	if b.webhooks != nil {
		reason := "normal"
		if !graceful {
			reason = "error"
		}
		b.webhooks.Notify(context.Background(), events.ClientDisconnected{
			ClientID:   s.ID,
			Reason:     reason,
			RemoteAddr: "", // Not available at broker level
		})
	}

	if b.sessions != nil {
		b.sessions.Save(s.Info())
	}
	if b.wills != nil {
		ctx := context.Background()
		will := s.GetWill()
		if !graceful && will != nil {
			b.wills.Set(ctx, s.ID, will)
		} else if graceful {
			b.wills.Delete(ctx, s.ID)
		}
	}
	if b.messages != nil {
		msgs := s.OfflineQueue().Drain()
		for i, msg := range msgs {
			key := fmt.Sprintf("%s%s%d", s.ID, queuePrefix, i)
			b.messages.Store(key, msg)
		}

		for _, inf := range s.Inflight().GetAll() {
			key := fmt.Sprintf("%s%s%d", s.ID, inflightPrefix, inf.PacketID)
			b.messages.Store(key, inf.Message)
		}
	}

	if s.CleanStart && s.ExpiryInterval == 0 {
		b.sessionLocks.Lock(s.ID)
		b.destroySessionLocked(s)
		b.sessionLocks.Unlock(s.ID)

		// Release ownership for clean sessions
		if b.cluster != nil {
			ctx := context.Background()
			if err := b.cluster.ReleaseSession(ctx, s.ID); err != nil {
				b.logError("cluster_release_session", err, slog.String("client_id", s.ID))
			}
		}
	}
	// For persistent sessions, DON'T release ownership immediately
	// Keep ownership so messages can still be routed to this node
	// Ownership will expire naturally after TTL (30s)
}

// restoreInflightFromStorage restores inflight messages from storage.
func (b *Broker) restoreInflightFromStorage(clientID string, tracker messages.Inflight) error {
	if b.messages == nil {
		return nil
	}

	inflightMsgs, err := b.messages.List(clientID + inflightPrefix)
	if err != nil {
		return fmt.Errorf("failed to list inflight messages: %w", err)
	}

	for _, msg := range inflightMsgs {
		if msg.PacketID != 0 {
			tracker.Add(msg.PacketID, msg, messages.Outbound)
		}
	}

	if err := b.messages.DeleteByPrefix(clientID + inflightPrefix); err != nil {
		return fmt.Errorf("failed to clear inflight messages: %w", err)
	}

	return nil
}

// restoreQueueFromStorage restores offline messages from storage.
func (b *Broker) restoreQueueFromStorage(clientID string, queue messages.Queue) error {
	if b.messages == nil {
		return nil
	}

	msgs, err := b.messages.List(clientID + queuePrefix)
	if err != nil {
		return fmt.Errorf("failed to list offline messages: %w", err)
	}

	for _, msg := range msgs {
		queue.Enqueue(msg)
	}

	if err := b.messages.DeleteByPrefix(clientID + queuePrefix); err != nil {
		return fmt.Errorf("failed to clear offline messages: %w", err)
	}

	return nil
}

// restoreSessionFromStorage restores session metadata and subscriptions.
func (b *Broker) restoreSessionFromStorage(s *session.Session, clientID string, opts session.Options) error {
	if opts.CleanStart || b.sessions == nil {
		return nil
	}

	stored, err := b.sessions.Get(clientID)
	if err != nil && err != storage.ErrNotFound {
		return fmt.Errorf("failed to get session: %w", err)
	}
	if stored != nil {
		s.RestoreFrom(stored)
	}

	// Restore subscriptions from cluster if available, otherwise from local storage
	var subs []*storage.Subscription
	if b.cluster != nil {
		ctx := context.Background()
		subs, err = b.cluster.GetSubscriptionsForClient(ctx, clientID)
		if err != nil {
			return fmt.Errorf("failed to get subscriptions from cluster: %w", err)
		}
	} else {
		subs, err = b.subscriptions.GetForClient(clientID)
		if err != nil {
			return fmt.Errorf("failed to get subscriptions: %w", err)
		}
	}

	for _, sub := range subs {
		// Add to local router (critical for message routing!)
		b.router.Subscribe(s.ID, sub.Filter, sub.QoS, sub.Options)

		// Add to session
		s.AddSubscription(sub.Filter, sub.Options)

		// Add to local subscription storage
		if err := b.subscriptions.Add(sub); err != nil {
			b.logError("restore_subscription", err, slog.String("filter", sub.Filter))
			continue
		}
	}

	return nil
}

// restoreInflightFromTakeover restores inflight messages from takeover state.
func (b *Broker) restoreInflightFromTakeover(state *clusterv1.SessionState, tracker messages.Inflight) error {
	if state == nil || state.InflightMessages == nil {
		return nil
	}

	for _, msg := range state.InflightMessages {
		storeMsg := &storage.Message{
			Topic:    msg.Topic,
			Payload:  msg.Payload,
			QoS:      byte(msg.Qos),
			Retain:   msg.Retain,
			PacketID: uint16(msg.PacketId),
		}
		if err := tracker.Add(uint16(msg.PacketId), storeMsg, messages.Outbound); err != nil {
			b.logError("restore_inflight", err, slog.Uint64("packet_id", uint64(msg.PacketId)))
			continue
		}
	}

	return nil
}

// restoreQueueFromTakeover restores offline queue from takeover state.
func (b *Broker) restoreQueueFromTakeover(state *clusterv1.SessionState, queue messages.Queue) error {
	if state == nil || state.QueuedMessages == nil {
		return nil
	}

	for _, msg := range state.QueuedMessages {
		storeMsg := &storage.Message{
			Topic:   msg.Topic,
			Payload: msg.GetPayload(),
			QoS:     byte(msg.Qos),
			Retain:  msg.Retain,
		}
		if err := queue.Enqueue(storeMsg); err != nil {
			b.logError("restore_queue", err, slog.String("topic", msg.Topic))
			continue
		}
	}

	return nil
}

// restoreSubscriptionsFromTakeover restores subscriptions from takeover state.
func (b *Broker) restoreSubscriptionsFromTakeover(s *session.Session, state *clusterv1.SessionState) error {
	if state == nil || state.Subscriptions == nil {
		return nil
	}

	for _, sub := range state.Subscriptions {
		opts := storage.SubscribeOptions{
			NoLocal:           false,
			RetainAsPublished: false,
			RetainHandling:    0,
		}

		// Add to local router
		b.router.Subscribe(s.ID, sub.Filter, byte(sub.Qos), opts)

		// Add to session
		s.AddSubscription(sub.Filter, opts)

		// Add to local subscription storage
		if err := b.subscriptions.Add(&storage.Subscription{
			ClientID: s.ID,
			Filter:   sub.Filter,
			QoS:      byte(sub.Qos),
			Options:  opts,
		}); err != nil {
			b.logError("restore_subscription", err, slog.String("filter", sub.Filter))
			continue
		}

		// Note: No need to add to cluster since it was already there
		// The subscription exists in etcd from the previous node
	}

	return nil
}

// GetSessionStateAndClose disconnects a session, retrieves its state, and returns it.
// This is used during session takeover.
func (b *Broker) GetSessionStateAndClose(ctx context.Context, clientID string) (*clusterv1.SessionState, error) {
	b.sessionLocks.Lock(clientID)
	defer b.sessionLocks.Unlock(clientID)

	s := b.sessionsMap.Get(clientID)
	if s == nil {
		return nil, nil // Session not found
	}

	// Capture state before destroying
	state := &clusterv1.SessionState{
		ExpiryInterval: uint32(s.ExpiryInterval),
		CleanStart:     s.CleanStart,
	}

	// Capture subscriptions from storage (includes QoS)
	if b.subscriptions != nil {
		subs, err := b.subscriptions.GetForClient(s.ID)
		if err == nil {
			for _, sub := range subs {
				state.Subscriptions = append(state.Subscriptions, &clusterv1.Subscription{
					Filter: sub.Filter,
					Qos:    uint32(sub.QoS),
				})
			}
		}
	}

	// Capture inflight messages
	for _, msg := range s.Inflight().GetAll() {
		state.InflightMessages = append(state.InflightMessages, &clusterv1.InflightMessage{
			PacketId:  uint32(msg.PacketID),
			Topic:     msg.Message.Topic,
			Payload:   msg.Message.Payload,
			Qos:       uint32(msg.Message.QoS),
			Retain:    msg.Message.Retain,
			Timestamp: time.Now().Unix(),
		})
	}

	// Capture queued messages
	for _, msg := range s.OfflineQueue().Drain() {
		state.QueuedMessages = append(state.QueuedMessages, &clusterv1.QueuedMessage{
			Topic:     msg.Topic,
			Payload:   msg.GetPayload(),
			Qos:       uint32(msg.QoS),
			Retain:    msg.Retain,
			Timestamp: time.Now().Unix(),
		})
	}

	// Capture will message
	if will := s.GetWill(); will != nil {
		state.Will = &clusterv1.WillMessage{
			Topic:   will.Topic,
			Payload: will.Payload,
			Qos:     uint32(will.QoS),
			Retain:  will.Retain,
			Delay:   will.Delay,
		}
	}

	// Forcefully disconnect and remove
	if err := b.destroySessionLocked(s); err != nil {
		return nil, fmt.Errorf("failed to destroy session: %w", err)
	}

	return state, nil
}

// persistOfflineQueue saves a session's offline queue to storage.
func (b *Broker) persistOfflineQueue(s *session.Session) {
	if b.messages == nil {
		return
	}

	msgs := s.OfflineQueue().Drain()
	for i, msg := range msgs {
		key := fmt.Sprintf("%s%s%d", s.ID, queuePrefix, i)
		b.messages.Store(key, msg)
	}
}
