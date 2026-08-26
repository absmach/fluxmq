// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package broker

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/absmach/fluxmq/broker/events"
	"github.com/absmach/fluxmq/config"
	"github.com/absmach/fluxmq/message"
	v5 "github.com/absmach/fluxmq/mqtt/packets/v5"
	"github.com/absmach/fluxmq/mqtt/session"
	clusterv1 "github.com/absmach/fluxmq/pkg/proto/cluster/v1"
	"github.com/absmach/fluxmq/storage"
	"github.com/absmach/fluxmq/storage/messages"
	"github.com/absmach/fluxmq/topics"
)

// CreateSession creates a new session or returns an existing one.
// If opts.CleanStart is true and a session exists, it is destroyed first.
// Returns the session and whether it was newly created.
func (b *Broker) CreateSession(clientID string, version byte, opts session.Options) (sess *session.Session, created bool, err error) {
	sessionLock := b.sessionLocks.Key(clientID)
	sessionLock.Lock()
	defer sessionLock.Unlock()

	ctx := context.Background()

	// ownershipAcquired records that this call took cluster ownership of the
	// session. Failing after that point has to hand it back, or the session is
	// stranded on a node that never finished creating it. Reading the outcome
	// from the returned error rather than from a second flag means every early
	// return is covered without having to remember to mark one.
	ownershipAcquired := false
	defer func() {
		if err == nil || !ownershipAcquired || b.cluster == nil {
			return
		}
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
		defer cancel()
		if err := b.cluster.ReleaseSession(cleanupCtx, clientID); err != nil {
			b.logError("cluster_release_session_after_create_failure", err, slog.String("client_id", clientID))
		}
	}()

	// Check if session is owned by another node in the cluster
	var takeoverState *clusterv1.SessionState
	if b.cluster != nil {
		clusterCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		ctx = clusterCtx

		ownerNode, exists, err := b.cluster.GetSessionOwner(ctx, clientID)
		if err != nil {
			b.logError("get_session_owner", err, slog.String("client_id", clientID))
			return nil, false, fmt.Errorf("failed to check session ownership: %w", err)
		}

		if exists && ownerNode != b.cluster.NodeID() {
			// Session exists on different node - trigger takeover
			b.telemetry.logger.Info("taking over session from remote node",
				slog.String("client_id", clientID),
				slog.String("from_node", ownerNode),
				slog.String("to_node", b.cluster.NodeID()))

			takeoverState, err = b.cluster.TakeoverSession(ctx, clientID, ownerNode, b.cluster.NodeID())
			if err != nil {
				b.logError("takeover_session", err, slog.String("client_id", clientID))
				return nil, false, fmt.Errorf("session takeover failed: %w", err)
			}

			b.telemetry.logger.Info("session takeover completed", slog.String("client_id", clientID))
			ownershipAcquired = true

			// Webhook: session takeover
			if b.telemetry.webhooks != nil {
				b.telemetry.webhooks.Notify(ctx, events.SessionTakeover{ //nolint:errcheck // fire-and-forget webhook notification
					ClientID: clientID,
					FromNode: ownerNode,
					ToNode:   b.cluster.NodeID(),
				})
			}
		}
	}

	existing := b.sessionsMap.Get(clientID)
	if opts.CleanStart && existing != nil {
		if err := b.destroySessionLocked(ctx, existing); err != nil {
			return nil, false, err
		}
		existing = nil
	}

	if existing != nil {
		if b.cluster != nil {
			if err := b.cluster.AcquireSession(ctx, clientID, b.cluster.NodeID()); err != nil {
				return nil, false, fmt.Errorf("failed to acquire session ownership: %w", err)
			}
		}
		ownershipAcquired = true
		return existing, false, nil
	}

	sessionCfg := b.cfg.sessionCfg.Load()
	if sessionCfg == nil {
		sessionCfg = &config.SessionConfig{}
	}

	if sessionCfg.MaxSessions > 0 && b.sessionsMap.Count() >= sessionCfg.MaxSessions {
		return nil, false, ErrMaxSessionsExceeded
	}

	serverReceiveMax := sessionCfg.MaxInflightMessages
	if serverReceiveMax <= 0 {
		serverReceiveMax = config.DefaultMaxInflightMessages
	}
	if serverReceiveMax > 65535 {
		serverReceiveMax = 65535
	}
	receiveMax := opts.ReceiveMaximum
	if receiveMax == 0 || int(receiveMax) > serverReceiveMax {
		receiveMax = uint16(serverReceiveMax)
	}
	// The persistent inflight store is the bidirectional server-side limit; it
	// must not be sized by the client's outbound Receive Maximum, or an inbound
	// QoS 2 transaction from a client advertising a small Receive Maximum could
	// starve outbound delivery. The outbound quota is the session's send window.
	inflight := messages.NewInflightTracker(serverReceiveMax)
	offlineQueue := messages.NewMessageQueue(sessionCfg.MaxOfflineQueueSize, sessionCfg.OfflineQueuePolicy == config.OfflineQueuePolicyEvict)

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

	// Apply default expiry for persistent sessions that don't specify one,
	// preventing indefinite memory growth.
	if !opts.CleanStart && opts.ExpiryInterval == 0 && sessionCfg.DefaultExpiryInterval > 0 {
		opts.ExpiryInterval = sessionCfg.DefaultExpiryInterval
	}

	// Override receive maximum with normalized value
	opts.ReceiveMaximum = receiveMax

	s := session.New(clientID, version, opts, inflight, offlineQueue, *sessionCfg)

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

	if b.cluster != nil {
		if err := b.cluster.AcquireSession(ctx, clientID, b.cluster.NodeID()); err != nil {
			return nil, false, fmt.Errorf("failed to acquire session ownership: %w", err)
		}
		ownershipAcquired = true
	}

	if b.stores.sessions != nil {
		if err := b.stores.sessions.Save(s.Info()); err != nil {
			return nil, false, fmt.Errorf("failed to save session: %w", err)
		}
	}

	b.sessionsMap.Set(clientID, s)

	return s, true, nil
}

// DestroySession removes a session completely.
func (b *Broker) DestroySession(clientID string) error {
	sessionLock := b.sessionLocks.Key(clientID)
	sessionLock.Lock()
	defer sessionLock.Unlock()

	s := b.sessionsMap.Get(clientID)
	if s == nil {
		return nil
	}

	return b.destroySessionLocked(context.Background(), s)
}

// destroySessionLocked destroys a session. Must be called with the session's key lock held.
func (b *Broker) destroySessionLocked(ctx context.Context, s *session.Session) error {
	return b.destroySessionLockedWithOwnership(ctx, s, true)
}

// destroySessionForTakeoverLocked removes local state while deliberately
// leaving the distributed owner key in place. The new node replaces that key
// with a CAS after it has received the captured state.
func (b *Broker) destroySessionForTakeoverLocked(ctx context.Context, s *session.Session) error {
	return b.destroySessionLockedWithOwnership(ctx, s, false)
}

func (b *Broker) destroySessionLockedWithOwnership(ctx context.Context, s *session.Session, releaseOwnership bool) error {
	if s.IsConnected() {
		s.Disconnect(false, v5.DisconnectAdministrativeAction) //nolint:errcheck // disconnect during session destroy; connection is being removed
	}

	if b.stores.sessions != nil {
		if err := b.stores.sessions.Delete(s.ID); err != nil {
			return fmt.Errorf("failed to delete session: %w", err)
		}
	}
	if b.stores.subscriptions != nil {
		if err := b.stores.subscriptions.RemoveAll(s.ID); err != nil {
			return fmt.Errorf("failed to remove subscriptions: %w", err)
		}
	}
	if b.stores.messages != nil {
		if err := b.stores.messages.DeleteByPrefix(s.ID + "/"); err != nil {
			return fmt.Errorf("failed to delete messages: %w", err)
		}
	}
	if b.stores.wills != nil {
		if err := b.stores.wills.Delete(ctx, s.ID); err != nil {
			return fmt.Errorf("failed to delete will: %w", err)
		}
	}

	b.sessionsMap.Delete(s.ID)

	subs := s.GetSubscriptions()
	for filter := range subs {
		if topics.IsShared(filter) {
			if b.sharedSubs.Unsubscribe(s.ID, filter) {
				shareName, topicFilter, _ := topics.ParseShared(filter)
				shareClientID := "$share/" + shareName + "/" + topicFilter
				b.router.Unsubscribe(shareClientID, topicFilter) //nolint:errcheck // best-effort cleanup during session destroy
			}
		} else {
			b.router.Unsubscribe(s.ID, filter) //nolint:errcheck // best-effort cleanup during session destroy
		}
	}

	if b.cluster != nil {
		if err := b.cluster.RemoveAllSubscriptions(ctx, s.ID); err != nil {
			b.logError("cluster_remove_all_subscriptions", err, slog.String("client_id", s.ID))
		}
	}

	// Release session ownership in cluster
	if releaseOwnership && b.cluster != nil {
		if err := b.cluster.ReleaseSession(ctx, s.ID); err != nil {
			b.logError("cluster_release_session", err, slog.String("client_id", s.ID))
		}
	}
	s.ClearMessages()

	return nil
}

// HandleSessionLeaseLost fences connections that this node can no longer
// prove it owns. Persistent session state is retained locally for a later
// explicit takeover or reconnect; only the live connection is stopped.
func (b *Broker) HandleSessionLeaseLost(_ context.Context, clientIDs []string) {
	for _, clientID := range clientIDs {
		// Lease recovery can be triggered by AcquireSession while CreateSession
		// already holds the client key lock. Session.Disconnect has its own
		// generation lock, so fencing here must not try to re-enter that key lock.
		s := b.sessionsMap.Get(clientID)
		if s != nil && s.IsConnected() {
			_ = s.Disconnect(false, v5.DisconnectServerUnavailable)
		}
	}
}

// handleDisconnect handles session disconnect.
func (b *Broker) handleDisconnect(s *session.Session, graceful bool) {
	if b.auth != nil {
		b.auth.Forget(s.ID)
	}

	// Webhook: client disconnected
	disconnectReason := "normal"
	if !graceful {
		disconnectReason = "error"
	}
	if b.telemetry.webhooks != nil {
		b.telemetry.webhooks.Notify(context.Background(), events.ClientDisconnected{ //nolint:errcheck // fire-and-forget webhook notification
			ClientID:   s.ID,
			Reason:     disconnectReason,
			RemoteAddr: "", // Not available at broker level
		})
	}

	// Event hook: client disconnected
	if b.eventHook != nil {
		if err := b.eventHook.OnDisconnect(context.Background(), s.ID, disconnectReason); err != nil {
			b.logError("event_hook_disconnect", err, slog.String("client_id", s.ID))
		}
	}

	b.persistSessionInfo(s)
	if b.stores.wills != nil {
		ctx := context.Background()
		will := s.GetWill()
		if !graceful && will != nil {
			b.stores.wills.Set(ctx, s.ID, will) //nolint:errcheck // best-effort will persistence on disconnect
		} else if graceful {
			b.stores.wills.Delete(ctx, s.ID) //nolint:errcheck // best-effort will cleanup on graceful disconnect
		}
	}
	if b.stores.messages != nil {
		msgs := s.OfflineQueue().Drain()
		for i, msg := range msgs {
			key := fmt.Sprintf("%s%s%d", s.ID, queuePrefix, i)
			b.stores.messages.Store(key, msg) //nolint:errcheck // best-effort offline message persistence
			message.Release(msg)
		}

		for _, inf := range s.Inflight().GetAll() {
			// Key by direction so an inbound and outbound entry sharing a packet
			// ID do not overwrite each other, and carry the direction and state
			// on the message so they survive a restore.
			key := fmt.Sprintf("%s%s%d/%d", s.ID, inflightPrefix, inf.Direction, inf.PacketID)
			inf.Message.Broker.Delivery.InflightDirection = byte(inf.Direction)
			inf.Message.Broker.Delivery.InflightState = byte(inf.State)
			b.stores.messages.Store(key, inf.Message) //nolint:errcheck // best-effort inflight message persistence
		}
	}

	if s.CleanStart && s.ExpiryInterval == 0 {
		sessionLock := b.sessionLocks.Key(s.ID)
		sessionLock.Lock()
		b.destroySessionLocked(context.Background(), s) //nolint:errcheck // best-effort session cleanup for clean-start sessions
		sessionLock.Unlock()

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

func (b *Broker) persistSessionInfo(s *session.Session) {
	if b.stores.sessions == nil || s == nil {
		return
	}
	if err := b.stores.sessions.Save(s.Info()); err != nil {
		b.logError("save_session_info", err, slog.String("client_id", s.ID))
	}
}

// restoreInflightEntry transfers msg to the tracker or releases it when the
// entry cannot be restored. An invalid direction (corrupt persisted or
// transferred value) is skipped rather than risking an out-of-range panic. The
// (direction, packetID) entry the Add restores is itself the inbound QoS 2
// duplicate-detection state, so a retransmitted PUBLISH after restore is
// recognised.
func restoreInflightEntry(tracker messages.Inflight, packetID uint16, msg *message.Envelope, rawDirection, rawState uint32) {
	var direction messages.Direction
	switch messages.Direction(rawDirection) {
	case messages.Outbound:
		direction = messages.Outbound
	case messages.Inbound:
		direction = messages.Inbound
	default:
		message.Release(msg)
		return // unknown/corrupt direction: skip
	}

	if err := tracker.Add(packetID, msg, direction); err != nil {
		message.Release(msg)
		return
	}

	if direction == messages.Inbound {
		return // inbound entries carry no extra delivery state
	}

	state := messages.StatePublishSent
	if messages.InflightState(rawState) == messages.StatePubRecReceived {
		state = messages.StatePubRecReceived
	}
	tracker.UpdateState(packetID, state) //nolint:errcheck // restore the QoS 2 delivery phase
}

// restoreInflightFromStorage restores inflight messages from storage.
func (b *Broker) restoreInflightFromStorage(clientID string, tracker messages.Inflight) error {
	if b.stores.messages == nil {
		return nil
	}

	inflightMsgs, err := b.stores.messages.List(clientID + inflightPrefix)
	if err != nil {
		return fmt.Errorf("failed to list inflight messages: %w", err)
	}

	for _, msg := range inflightMsgs {
		if msg.Broker.Delivery.PacketID == 0 {
			message.Release(msg)
			continue
		}
		restoreInflightEntry(tracker, msg.Broker.Delivery.PacketID, msg, uint32(msg.Broker.Delivery.InflightDirection), uint32(msg.Broker.Delivery.InflightState))
	}

	if err := b.stores.messages.DeleteByPrefix(clientID + inflightPrefix); err != nil {
		return fmt.Errorf("failed to clear inflight messages: %w", err)
	}

	return nil
}

// restoreQueueFromStorage restores offline messages from storage.
func (b *Broker) restoreQueueFromStorage(clientID string, queue messages.Queue) error {
	if b.stores.messages == nil {
		return nil
	}

	msgs, err := b.stores.messages.List(clientID + queuePrefix)
	if err != nil {
		return fmt.Errorf("failed to list offline messages: %w", err)
	}

	for _, msg := range msgs {
		_ = queue.Enqueue(msg) // best-effort offline queue restore; overflow is handled by queue capacity
		message.Release(msg)
	}

	if err := b.stores.messages.DeleteByPrefix(clientID + queuePrefix); err != nil {
		return fmt.Errorf("failed to clear offline messages: %w", err)
	}

	return nil
}

// restoreSessionFromStorage restores session metadata and subscriptions.
func (b *Broker) restoreSessionFromStorage(s *session.Session, clientID string, opts session.Options) error {
	if opts.CleanStart || b.stores.sessions == nil {
		return nil
	}

	stored, err := b.stores.sessions.Get(clientID)
	if err != nil && !errors.Is(err, storage.ErrNotFound) {
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
		subs, err = b.stores.subscriptions.GetForClient(clientID)
		if err != nil {
			return fmt.Errorf("failed to get subscriptions: %w", err)
		}
	}

	for _, sub := range subs {
		// Add to local router (critical for message routing!)
		b.router.Subscribe(s.ID, sub.Filter, sub.QoS, sub.Options) //nolint:errcheck // subscribe errors are non-fatal; message routing degrades gracefully

		// Add to session
		s.AddSubscription(sub.Filter, sub.Options)

		// Add to local subscription storage
		if err := b.stores.subscriptions.Add(sub); err != nil {
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
		storeMsg := message.New(msg.Topic, msg.GetPayload())
		storeMsg.Broker.Delivery.QoS = byte(msg.Qos)
		storeMsg.Broker.Delivery.Retain = msg.Retain
		storeMsg.Broker.Delivery.PacketID = uint16(msg.PacketId)
		message.ApplyTrustedProperties(storeMsg, msg.Properties)
		restoreInflightEntry(tracker, uint16(msg.PacketId), storeMsg, msg.Direction, msg.State)
	}

	return nil
}

// restoreQueueFromTakeover restores offline queue from takeover state.
func (b *Broker) restoreQueueFromTakeover(state *clusterv1.SessionState, queue messages.Queue) error {
	if state == nil || state.QueuedMessages == nil {
		return nil
	}

	for _, msg := range state.QueuedMessages {
		storeMsg := message.New(msg.Topic, msg.GetPayload())
		storeMsg.Broker.Delivery.QoS = byte(msg.Qos)
		storeMsg.Broker.Delivery.Retain = msg.Retain
		if err := queue.Enqueue(storeMsg); err != nil {
			b.logError("restore_queue", err, slog.String("topic", msg.Topic))
			message.Release(storeMsg)
			continue
		}
		message.Release(storeMsg)
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
		b.router.Subscribe(s.ID, sub.Filter, byte(sub.Qos), opts) //nolint:errcheck // subscribe errors are non-fatal; message routing degrades gracefully

		// Add to session
		s.AddSubscription(sub.Filter, opts)

		// Add to local subscription storage
		if err := b.stores.subscriptions.Add(&storage.Subscription{
			ClientID: s.ID,
			Filter:   sub.Filter,
			QoS:      byte(sub.Qos),
			Options:  opts,
		}); err != nil {
			b.logError("restore_subscription", err, slog.String("filter", sub.Filter))
			continue
		}

		// The old owner removes the cluster subscription while closing the
		// transferred session. Re-register it so publishers on other nodes can
		// discover the subscriber after takeover.
		if b.cluster != nil {
			ctx := context.Background()
			if err := b.cluster.AddSubscription(ctx, s.ID, sub.Filter, byte(sub.Qos), opts); err != nil {
				return fmt.Errorf("failed to restore subscription in cluster: %w", err)
			}
		}
	}

	return nil
}

// GetSessionStateAndClose disconnects a session, retrieves its state, and returns it.
// This is used during session takeover.
func (b *Broker) GetSessionStateAndClose(ctx context.Context, clientID string) (*clusterv1.SessionState, error) {
	sessionLock := b.sessionLocks.Key(clientID)
	sessionLock.Lock()
	defer sessionLock.Unlock()

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
	if b.stores.subscriptions != nil {
		subs, err := b.stores.subscriptions.GetForClient(s.ID)
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
			PacketId:   uint32(msg.PacketID),
			Topic:      msg.Message.Topic,
			Payload:    msg.Message.StablePayload(),
			Qos:        uint32(msg.Message.Broker.Delivery.QoS),
			Retain:     msg.Message.Broker.Delivery.Retain,
			Timestamp:  time.Now().Unix(),
			Direction:  uint32(msg.Direction),
			State:      uint32(msg.State),
			Properties: message.ProjectProperties(msg.Message, message.TrustedServiceProjection),
		})
	}

	// Capture queued messages
	for _, msg := range s.OfflineQueue().Drain() {
		state.QueuedMessages = append(state.QueuedMessages, &clusterv1.QueuedMessage{
			Topic:     msg.Topic,
			Payload:   msg.StablePayload(),
			Qos:       uint32(msg.Broker.Delivery.QoS),
			Retain:    msg.Broker.Delivery.Retain,
			Timestamp: time.Now().Unix(),
		})
		message.Release(msg)
	}

	// Capture will message
	if will := s.GetWill(); will != nil {
		state.Will = &clusterv1.WillMessage{
			Topic:   will.Topic,
			Payload: bytes.Clone(will.Payload),
			Qos:     uint32(will.QoS),
			Retain:  will.Retain,
			Delay:   will.Delay,
		}
	}

	// The takeover caller owns the distributed handoff. Suppress the ordinary
	// disconnect callback and retain the old owner key until its final CAS.
	s.SetOnDisconnect(nil)
	if err := b.destroySessionForTakeoverLocked(ctx, s); err != nil {
		return nil, fmt.Errorf("failed to destroy session: %w", err)
	}

	return state, nil
}

// persistOfflineQueue saves a session's offline queue to storage.
func (b *Broker) persistOfflineQueue(s *session.Session) {
	if b.stores.messages == nil {
		return
	}

	msgs := s.OfflineQueue().Drain()
	for i, msg := range msgs {
		key := fmt.Sprintf("%s%s%d", s.ID, queuePrefix, i)
		b.stores.messages.Store(key, msg) //nolint:errcheck // best-effort offline message persistence during close
		message.Release(msg)
	}
}
