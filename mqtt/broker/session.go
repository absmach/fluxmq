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
	"github.com/absmach/fluxmq/cluster"
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
	return b.createSession(clientID, version, opts, nil)
}

// CreateSessionForIdentity creates or resumes an MQTT session while ensuring
// that any existing local or clustered session belongs to the authenticated
// external principal. The current owner evaluates the guard before a remote
// takeover performs any destructive work.
func (b *Broker) CreateSessionForIdentity(clientID string, version byte, opts session.Options, requireBound bool) (sess *session.Session, created bool, err error) {
	identity := &cluster.SessionIdentityGuard{
		ExternalID:   opts.ExternalID,
		RequireBound: requireBound,
	}
	return b.createSession(clientID, version, opts, identity)
}

func (b *Broker) createSession(clientID string, version byte, opts session.Options, identity *cluster.SessionIdentityGuard) (sess *session.Session, created bool, err error) {
	sessionLock := b.sessionLocks.Key(clientID)
	sessionLock.Lock()
	var superseded []*session.Superseded
	defer func() {
		sessionLock.Unlock()
		for _, retired := range superseded {
			go b.drainSuperseded(context.Background(), retired)
		}
	}()

	ctx := context.Background()

	// ownershipAcquired records that this call took cluster ownership of the
	// session. Failing after that point has to hand it back, or the session is
	// stranded on a node that never finished creating it. Reading the outcome
	// from the returned error rather than from a second flag means every early
	// return is covered without having to remember to mark one.
	// keepOwnership marks the one failure that must not hand ownership back:
	// a migrated session this CONNECT was not authorized to use still lives on
	// this node, and its real owner has to be able to find it here.
	ownershipAcquired := false
	keepOwnership := false
	defer func() {
		if err == nil || !ownershipAcquired || keepOwnership || b.cluster == nil {
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

			// Clean Start inherits nothing, so there is no state for the
			// owner to protect and no identity for it to check. Guarding it
			// would refuse a principal that the same-node path lets start a
			// fresh session, which is the documented way to take a client ID
			// over deliberately.
			takeoverGuard := identity
			if opts.CleanStart {
				takeoverGuard = nil
			}

			takeoverState, err = b.cluster.TakeoverSession(ctx, clientID, ownerNode, b.cluster.NodeID(), takeoverGuard)
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
		retired := existing.DetachForTakeover(true)
		// If the network callback already scheduled the Will, it consumed the
		// in-memory copy. Ending the old session makes any stored delayed Will
		// due now, while an already-published Will is absent from the store.
		if retired.Conn == nil && retired.Will == nil && b.stores.wills != nil {
			storedWill, err := b.stores.wills.Get(ctx, clientID)
			if err != nil && !errors.Is(err, storage.ErrNotFound) {
				return nil, false, fmt.Errorf("failed to load replaced session will: %w", err)
			}
			retired.Will = storedWill
		}
		superseded = append(superseded, retired)
		if err := b.destroySessionLocked(ctx, existing); err != nil {
			return nil, false, err
		}
		existing = nil
	}

	if existing != nil && identity != nil && identity.RequireBound && existing.ExternalIdentity() == "" {
		// Same rule as the persisted and migrated paths: a session bound to no
		// principal predates certificate binding, so it is discarded rather
		// than inherited by a bound client or used to lock that client out of
		// its own client ID.
		b.telemetry.logger.Warn("mqtt_unbound_session_discarded",
			slog.String("client_id", clientID),
			slog.String("source", "memory"),
			slog.String("external_id", identity.ExternalID))
		if err := b.destroySessionLocked(ctx, existing); err != nil {
			return nil, false, err
		}
		existing = nil
	}

	if existing != nil {
		// Checking and binding in one step, before ownership is claimed, keeps
		// a rejected principal from moving the session and keeps two
		// concurrent CONNECTs from both adopting an unbound one and then
		// interleaving their writes.
		if identity != nil && !existing.BindExternalIdentity(identity.ExternalID, identity.RequireBound) {
			b.logIdentityRejected("mqtt_session_identity_mismatch", clientID, existing.ExternalIdentity(), identity.ExternalID)
			return nil, false, cluster.ErrSessionIdentityMismatch
		}
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

	// Which state this CONNECT may inherit is decided before any of it is
	// loaded, so an unauthorized client never reaches another principal's
	// inflight, queued, or subscribed messages.
	restore, err := b.resolveSessionRestore(ctx, clientID, &opts, takeoverState, identity)
	if err != nil {
		return nil, false, err
	}
	takeoverWill := willFromTakeover(clientID, takeoverState)
	takeoverState = restore.takeover
	if takeoverWill != nil {
		switch {
		case restore.preserveForOwner:
			// The attempted reconnect was rejected after the previous owner had
			// already transferred and closed the session. Keep a delayed Will
			// pending for the real owner; a zero-delay Will is due now.
			if takeoverWill.Delay == 0 {
				superseded = append(superseded, &session.Superseded{ClientID: clientID, Will: takeoverWill})
			} else if b.stores.wills != nil {
				if err := b.stores.wills.Set(ctx, clientID, takeoverWill); err != nil {
					return nil, false, fmt.Errorf("failed to preserve takeover will: %w", err)
				}
			}
		default:
			// The old connection's Will belongs to that connection, not to the
			// new CONNECT. A continued session cancels only a delayed Will; when
			// the old session was discarded, every Will is due immediately.
			superseded = append(superseded, &session.Superseded{
				ClientID:    clientID,
				Will:        takeoverWill,
				SessionEnds: takeoverState == nil,
			})
		}
	}
	if restore.preserveForOwner {
		// This CONNECT is rejected below, so its proposed Will must never become
		// part of the migrated session retained for another principal.
		opts.Will = nil
	}

	if takeoverState != nil {
		if err := b.restoreInflightFromTakeover(takeoverState, inflight); err != nil {
			return nil, false, fmt.Errorf("failed to restore inflight from takeover: %w", err)
		}
		if err := b.restoreQueueFromTakeover(takeoverState, offlineQueue); err != nil {
			return nil, false, fmt.Errorf("failed to restore queue from takeover: %w", err)
		}
	} else if restore.local {
		if err := b.restoreInflightFromStorage(clientID, inflight); err != nil {
			return nil, false, err
		}
		if err := b.restoreQueueFromStorage(clientID, offlineQueue); err != nil {
			return nil, false, err
		}
	}

	if opts.Will != nil {
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
	} else if restore.local {
		if err := b.restoreSessionFromStorage(s, clientID, opts, restore.stored, identity); err != nil {
			return nil, false, err
		}
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

	// The migrated session belongs to another principal. It is registered and
	// owned here because the previous node has already closed its copy, and
	// dropping it now would lose that principal's state; the expiry sweep
	// reclaims it from this point if its owner never comes back.
	if restore.preserveForOwner {
		s.MarkOffline()
		keepOwnership = true

		return nil, false, cluster.ErrSessionIdentityMismatch
	}

	return s, true, nil
}

// sessionRestore is the decision about which persisted or migrated state a
// CONNECT may inherit, taken before any of that state is loaded.
type sessionRestore struct {
	// takeover is the migrated state to restore, nil when there is none or
	// when this CONNECT must not inherit it.
	takeover *clusterv1.SessionState
	// stored is the persisted session record backing a local restore.
	stored *storage.Session
	// local reports whether local storage may be restored.
	local bool
	// preserveForOwner marks a migrated session that belongs to a different
	// principal: it is kept for that principal and this CONNECT is rejected.
	preserveForOwner bool
}

// resolveSessionRestore decides what a CONNECT may inherit and binds the
// identity the new session will carry.
func (b *Broker) resolveSessionRestore(ctx context.Context, clientID string, opts *session.Options, takeoverState *clusterv1.SessionState, identity *cluster.SessionIdentityGuard) (sessionRestore, error) {
	// Clean Start asked for a fresh session. Discarding the migrated state
	// keeps the cross-node path identical to the same-node one, which destroys
	// the existing session outright instead of restoring it.
	if takeoverState != nil && opts.CleanStart {
		takeoverState = nil
	}

	if takeoverState != nil && identity != nil {
		if identity.RequireBound && takeoverState.ExternalId == "" {
			// A session bound to no principal predates certificate binding.
			// Adopting it would hand its subscriptions to a bound client, so
			// it is discarded rather than inherited or used to lock the
			// client out of its own client ID.
			b.telemetry.logger.Warn("mqtt_unbound_session_discarded",
				slog.String("client_id", clientID),
				slog.String("source", "takeover"),
				slog.String("external_id", opts.ExternalID))
			if err := b.purgeSessionState(ctx, clientID); err != nil {
				return sessionRestore{}, err
			}

			return sessionRestore{}, nil
		}
		if !session.IdentityAllows(takeoverState.ExternalId, identity.ExternalID, identity.RequireBound) {
			b.logIdentityRejected("mqtt_takeover_identity_mismatch", clientID, takeoverState.ExternalId, opts.ExternalID)
			// A migrated session with no expiry of its own dies with the
			// connection the previous node has already closed, so there is
			// nothing left to keep for its owner. Keeping it would also mean
			// holding it for the expiry this CONNECT asked for, which is not
			// its owner's to set.
			if takeoverState.ExpiryInterval == 0 {
				if err := b.purgeSessionState(ctx, clientID); err != nil {
					return sessionRestore{}, err
				}

				return sessionRestore{}, nil
			}
			opts.ExternalID = takeoverState.ExternalId

			return sessionRestore{takeover: takeoverState, preserveForOwner: true}, nil
		}
	}

	if takeoverState != nil {
		if takeoverState.ExternalId != "" {
			opts.ExternalID = takeoverState.ExternalId
		}

		return sessionRestore{takeover: takeoverState}, nil
	}

	if opts.CleanStart || b.stores.sessions == nil {
		return sessionRestore{local: !opts.CleanStart}, nil
	}

	stored, err := b.stores.sessions.Get(clientID)
	if err != nil && !errors.Is(err, storage.ErrNotFound) {
		return sessionRestore{}, fmt.Errorf("failed to get session: %w", err)
	}
	if stored == nil {
		return sessionRestore{local: true}, nil
	}

	if identity == nil {
		return sessionRestore{stored: stored, local: true}, nil
	}

	if identity.RequireBound && stored.ExternalID == "" {
		b.telemetry.logger.Warn("mqtt_unbound_session_discarded",
			slog.String("client_id", clientID),
			slog.String("source", "storage"),
			slog.String("external_id", opts.ExternalID))
		if err := b.purgeSessionState(ctx, clientID); err != nil {
			return sessionRestore{}, err
		}

		return sessionRestore{}, nil
	}
	if !session.IdentityAllows(stored.ExternalID, identity.ExternalID, identity.RequireBound) {
		b.logIdentityRejected("mqtt_persistent_session_identity_mismatch", clientID, stored.ExternalID, identity.ExternalID)
		return sessionRestore{}, cluster.ErrSessionIdentityMismatch
	}

	return sessionRestore{stored: stored, local: true}, nil
}

// purgeSessionState removes every trace of a persisted session that will not
// be restored, so a session started fresh cannot resurrect it later.
func (b *Broker) purgeSessionState(ctx context.Context, clientID string) error {
	if b.stores.sessions != nil {
		if err := b.stores.sessions.Delete(clientID); err != nil {
			return fmt.Errorf("failed to delete session: %w", err)
		}
	}
	if b.stores.subscriptions != nil {
		if err := b.stores.subscriptions.RemoveAll(clientID); err != nil {
			return fmt.Errorf("failed to remove subscriptions: %w", err)
		}
	}
	if b.stores.messages != nil {
		if err := b.stores.messages.DeleteByPrefix(clientID + "/"); err != nil {
			return fmt.Errorf("failed to delete messages: %w", err)
		}
	}
	if b.stores.wills != nil {
		if err := b.stores.wills.Delete(ctx, clientID); err != nil {
			return fmt.Errorf("failed to delete will: %w", err)
		}
	}
	if b.cluster != nil {
		if err := b.cluster.RemoveAllSubscriptions(ctx, clientID); err != nil {
			b.logError("cluster_remove_all_subscriptions", err, slog.String("client_id", clientID))
		}
	}

	return nil
}

// logIdentityRejected records a CONNECT refused because the session it named
// belongs to another principal. The bound identity is logged so an operator
// can tell a misconfigured client from a client ID collision.
func (b *Broker) logIdentityRejected(event, clientID, bound, incoming string) {
	b.telemetry.logger.Warn(event,
		slog.String("client_id", clientID),
		slog.String("bound_external_id", bound),
		slog.String("connect_external_id", incoming))
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
	sessionLock := b.sessionLocks.Key(s.ID)
	sessionLock.Lock()
	var publishWill *storage.WillMessage
	notifyDisconnect := false
	disconnectReason := "normal"
	if !graceful {
		disconnectReason = "error"
	}
	defer func() {
		sessionLock.Unlock()
		if publishWill != nil {
			if err := b.publishWillMessage(context.Background(), publishWill); err != nil {
				b.logError("publish_disconnected_will", err, slog.String("client_id", publishWill.ClientID))
			}
		}
		if notifyDisconnect {
			b.emitClientDisconnected(context.Background(), s.ID, disconnectReason)
		}
	}()

	// The connection really did drop, so the notification is owed regardless of
	// which session currently holds the client ID. Everything past the identity
	// check writes state keyed by that ID instead, and must not run on behalf of
	// a session that has already been replaced.
	notifyDisconnect = true

	// Disconnect callbacks run asynchronously. A clean reconnect can replace
	// the session before the old callback starts; that stale callback must not
	// delete or persist state belonging to the replacement session.
	if b.sessionsMap.Get(s.ID) != s {
		return
	}

	if b.auth != nil {
		b.auth.Forget(s.ID)
	}

	b.persistSessionInfo(s)
	sessionEnds := s.CleanStart && s.ExpiryInterval == 0
	will := s.TakeWill()
	if b.stores.wills != nil {
		ctx := context.Background()
		if !graceful && will != nil {
			if sessionEnds {
				publishWill = will
			} else {
				b.stores.wills.Set(ctx, s.ID, will) //nolint:errcheck // best-effort will persistence on disconnect
			}
		} else if graceful {
			b.stores.wills.Delete(ctx, s.ID) //nolint:errcheck // best-effort will cleanup on graceful disconnect
		}
	} else if !graceful && sessionEnds {
		publishWill = will
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
			inf.Message.BrokerMeta.Delivery.InflightDirection = byte(inf.Direction)
			inf.Message.BrokerMeta.Delivery.InflightState = byte(inf.State)
			b.stores.messages.Store(key, inf.Message) //nolint:errcheck // best-effort inflight message persistence
		}
	}

	if sessionEnds {
		b.destroySessionLocked(context.Background(), s) //nolint:errcheck // best-effort session cleanup for clean-start sessions
	}
	// For persistent sessions, DON'T release ownership immediately
	// Keep ownership so messages can still be routed to this node
	// Ownership will expire naturally after TTL (30s)
}

// emitClientDisconnected runs external notifications without holding the
// client-ID shard lock. Event hooks are user-supplied and webhooks may perform
// network I/O, so neither may delay CONNECT or cleanup for colliding keys.
func (b *Broker) emitClientDisconnected(ctx context.Context, clientID, reason string) {
	if b.telemetry.webhooks != nil {
		b.telemetry.webhooks.Notify(ctx, events.ClientDisconnected{ //nolint:errcheck // fire-and-forget webhook notification
			ClientID:   clientID,
			Reason:     reason,
			RemoteAddr: "", // Not available at broker level
		})
	}

	if b.eventHook != nil {
		if err := b.eventHook.OnDisconnect(ctx, clientID, reason); err != nil {
			b.logError("event_hook_disconnect", err, slog.String("client_id", clientID))
		}
	}
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
		if msg.BrokerMeta.Delivery.PacketID == 0 {
			message.Release(msg)
			continue
		}
		restoreInflightEntry(tracker, msg.BrokerMeta.Delivery.PacketID, msg, uint32(msg.BrokerMeta.Delivery.InflightDirection), uint32(msg.BrokerMeta.Delivery.InflightState))
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
// stored is the persisted record resolved by resolveSessionRestore, already
// checked against the CONNECT identity; nil means there is nothing to restore
// beyond the subscriptions.
func (b *Broker) restoreSessionFromStorage(s *session.Session, clientID string, opts session.Options, stored *storage.Session, identity *cluster.SessionIdentityGuard) error {
	if opts.CleanStart {
		return nil
	}

	var err error
	if stored != nil {
		// RestoreFrom replays the persisted identity, which is empty for a
		// session written before identities were resolved. Re-binding puts the
		// identity this CONNECT authenticated as back on the session.
		s.RestoreFrom(stored)
		if identity != nil && !s.BindExternalIdentity(identity.ExternalID, identity.RequireBound) {
			b.logIdentityRejected("mqtt_persistent_session_identity_mismatch", clientID, stored.ExternalID, identity.ExternalID)
			return cluster.ErrSessionIdentityMismatch
		}
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

	// An entry that will not decode is dropped, not fatal: the caller aborts the
	// whole session takeover on an error, and losing one inflight message beats
	// losing the session.
	for _, msg := range state.InflightMessages {
		storeMsg, err := message.UnmarshalBinary(msg.Envelope)
		if err != nil {
			b.logError("restore_inflight", err, slog.Uint64("packet_id", uint64(msg.PacketId)))
			continue
		}
		storeMsg.BrokerMeta.Delivery.PacketID = uint16(msg.PacketId)
		restoreInflightEntry(tracker, uint16(msg.PacketId), storeMsg, msg.Direction, msg.State)
	}

	return nil
}

func willFromTakeover(clientID string, state *clusterv1.SessionState) *storage.WillMessage {
	if state == nil || state.Will == nil {
		return nil
	}

	return &storage.WillMessage{
		ClientID: clientID,
		Topic:    state.Will.Topic,
		Payload:  bytes.Clone(state.Will.Payload),
		QoS:      byte(state.Will.Qos),
		Retain:   state.Will.Retain,
		Delay:    state.Will.Delay,
	}
}

// restoreQueueFromTakeover restores offline queue from takeover state.
func (b *Broker) restoreQueueFromTakeover(state *clusterv1.SessionState, queue messages.Queue) error {
	if state == nil || state.QueuedMessages == nil {
		return nil
	}

	for _, msg := range state.QueuedMessages {
		storeMsg, err := message.UnmarshalBinary(msg.Envelope)
		if err != nil {
			b.logError("restore_queue", err)
			continue
		}
		if err := queue.Enqueue(storeMsg); err != nil {
			b.logError("restore_queue", err, slog.String("topic", storeMsg.Topic))
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
func (b *Broker) GetSessionStateAndClose(ctx context.Context, clientID string, identity *cluster.SessionIdentityGuard) (*clusterv1.SessionState, error) {
	sessionLock := b.sessionLocks.Key(clientID)
	sessionLock.Lock()
	var superseded *session.Superseded
	defer func() {
		sessionLock.Unlock()
		if superseded != nil {
			go b.drainSuperseded(context.WithoutCancel(ctx), superseded)
		}
	}()

	s := b.sessionsMap.Get(clientID)
	if s == nil {
		return nil, nil // Session not found
	}
	// RequireBound is deliberately not applied here. It is the requesting
	// node's policy for what it will adopt, and that node discards an unbound
	// session and starts fresh. Enforcing it on this side would instead refuse
	// the transfer and lock the client out of its own client ID.
	if identity != nil && !s.CanUseExternalIdentity(identity.ExternalID, false) {
		return nil, cluster.ErrSessionIdentityMismatch
	}

	// Capture state before destroying
	state := &clusterv1.SessionState{
		ExpiryInterval: uint32(s.ExpiryInterval),
		CleanStart:     s.CleanStart,
		ExternalId:     s.ExternalIdentity(),
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
		encoded, err := message.MarshalBinary(msg.Message)
		if err != nil {
			b.logError("capture_inflight", err, slog.String("topic", msg.Message.Topic))
			continue
		}
		state.InflightMessages = append(state.InflightMessages, &clusterv1.InflightMessage{
			PacketId:  uint32(msg.PacketID),
			Timestamp: time.Now().Unix(),
			Direction: uint32(msg.Direction),
			State:     uint32(msg.State),
			Envelope:  encoded,
		})
	}

	// Capture queued messages
	for _, msg := range s.OfflineQueue().Drain() {
		encoded, err := message.MarshalBinary(msg)
		if err != nil {
			b.logError("capture_offline_queue", err, slog.String("topic", msg.Topic))
			message.Release(msg)
			continue
		}
		state.QueuedMessages = append(state.QueuedMessages, &clusterv1.QueuedMessage{
			Timestamp: time.Now().Unix(),
			Envelope:  encoded,
		})
		message.Release(msg)
	}

	// Capture the active Will, or a delayed Will already scheduled by a
	// completed disconnect callback. Destruction below removes the old store.
	will := s.GetWill()
	if will == nil && b.stores.wills != nil {
		storedWill, err := b.stores.wills.Get(ctx, s.ID)
		if err != nil && !errors.Is(err, storage.ErrNotFound) {
			return nil, fmt.Errorf("failed to capture session will: %w", err)
		}
		will = storedWill
	}
	if will != nil {
		state.Will = &clusterv1.WillMessage{
			Topic:   will.Topic,
			Payload: bytes.Clone(will.Payload),
			Qos:     uint32(will.QoS),
			Retain:  will.Retain,
			Delay:   will.Delay,
		}
	}

	// Detach while the client-ID lock still protects the old owner, then notify
	// and close after unlocking. The Will travels in state so the new owner can
	// decide whether Clean Start ends the session or a reconnect cancels its
	// delay; the old owner must not publish it independently.
	superseded = s.DetachForTakeover(false)
	superseded.Will = nil
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
