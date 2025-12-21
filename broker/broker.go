// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package broker

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"time"

	"github.com/absmach/mqtt/cluster"
	"github.com/absmach/mqtt/cluster/grpc"
	"github.com/absmach/mqtt/core"
	"github.com/absmach/mqtt/core/packets"
	v3 "github.com/absmach/mqtt/core/packets/v3"
	v5 "github.com/absmach/mqtt/core/packets/v5"
	"github.com/absmach/mqtt/session"
	"github.com/absmach/mqtt/storage"
	"github.com/absmach/mqtt/storage/memory"
	"github.com/absmach/mqtt/storage/messages"
)

const (
	inflightPrefix = "/inflight/"
	queuePrefix    = "/queue/"
)

// Broker is the core MQTT broker with clean domain methods.
type Broker struct {
	mu            sync.RWMutex
	wg            sync.WaitGroup
	sessionsMap   session.Cache
	router        Router
	messages      storage.MessageStore
	sessions      storage.SessionStore
	subscriptions storage.SubscriptionStore
	retained      storage.RetainedStore
	wills         storage.WillStore
	cluster       cluster.Cluster // nil for single-node mode
	auth          *AuthEngine
	logger        *slog.Logger
	stats         *Stats
	stopCh        chan struct{}
}

// NewBroker creates a new broker instance.
// Parameters:
//   - store: Storage backend for messages, sessions, subscriptions, retained, and wills (nil uses memory)
//   - cl: Cluster coordination interface (nil for single-node mode)
//   - logger: Logger instance (nil uses default)
//   - stats: Stats collector (nil creates new one)
func NewBroker(store storage.Store, cl cluster.Cluster, logger *slog.Logger, stats *Stats) *Broker {
	if store == nil {
		// Fallback to memory storage if none provided
		store = memory.New()
	}

	router := NewRouter()

	if logger == nil {
		logger = slog.Default()
	}
	if stats == nil {
		stats = NewStats()
	}

	b := &Broker{
		sessionsMap:   session.NewMapCache(),
		router:        router,
		messages:      store.Messages(),
		sessions:      store.Sessions(),
		subscriptions: store.Subscriptions(),
		retained:      store.Retained(),
		wills:         store.Wills(),
		cluster:       cl,
		logger:        logger,
		stats:         stats,
		stopCh:        make(chan struct{}),
	}

	b.wg.Add(2)
	go b.expiryLoop()
	go b.statsLoop()

	return b
}

func (b *Broker) logOp(op string, attrs ...any) {
	b.logger.Debug(op, attrs...)
}

func (b *Broker) logError(op string, err error, attrs ...any) {
	if err != nil {
		allAttrs := append([]any{slog.String("error", err.Error())}, attrs...)
		b.logger.Error(op, allAttrs...)
	}
}

// CreateSession creates a new session or returns an existing one.
// If opts.CleanStart is true and a session exists, it is destroyed first.
// Returns the session and whether it was newly created.
func (b *Broker) CreateSession(clientID string, version byte, opts session.Options) (*session.Session, bool, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	// Check if session is owned by another node in the cluster
	var takeoverState *grpc.SessionState
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

	receiveMax := opts.ReceiveMaximum
	if receiveMax == 0 {
		receiveMax = 65535
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
	b.mu.Lock()
	defer b.mu.Unlock()

	s := b.sessionsMap.Get(clientID)
	if s == nil {
		return nil
	}

	return b.destroySessionLocked(s)
}

// Publish publishes a message, handling retained storage and distribution to subscribers.
func (b *Broker) Publish(msg *storage.Message) error {
	b.logOp("publish", slog.String("topic", msg.Topic), slog.Int("qos", int(msg.QoS)), slog.Bool("retain", msg.Retain))
	b.stats.IncrementPublishReceived()
	b.stats.AddBytesReceived(uint64(len(msg.Payload)))

	if msg.Retain {
		if len(msg.Payload) == 0 {
			if err := b.retained.Delete(msg.Topic); err != nil {
				return err
			}
		} else {
			msg.Retain = true
			if err := b.retained.Set(msg.Topic, msg); err != nil {
				return err
			}
		}
	}

	return b.distribute(msg.Topic, msg.Payload, msg.QoS, false, msg.Properties)
}

// subscribe adds a subscription for a session (internal domain method).
func (b *Broker) subscribe(s *session.Session, filter string, qos byte, opts storage.SubscribeOptions) error {
	b.logOp("subscribe", slog.String("client_id", s.ID), slog.String("filter", filter), slog.Int("qos", int(qos)))
	b.stats.IncrementSubscriptions()

	b.router.Subscribe(s.ID, filter, qos, opts)

	sub := &storage.Subscription{
		ClientID: s.ID,
		Filter:   filter,
		QoS:      qos,
		Options:  opts,
	}
	if err := b.subscriptions.Add(sub); err != nil {
		return fmt.Errorf("failed to persist subscription: %w", err)
	}

	// Add subscription to cluster
	if b.cluster != nil {
		ctx := context.Background()
		if err := b.cluster.AddSubscription(ctx, s.ID, filter, qos, opts); err != nil {
			b.logError("cluster_add_subscription", err, slog.String("client_id", s.ID), slog.String("filter", filter))
		}
	}

	s.AddSubscription(filter, opts)

	return nil
}

// unsubscribeInternal removes a subscription for a session (internal domain method).
func (b *Broker) unsubscribeInternal(s *session.Session, filter string) error {
	b.logOp("unsubscribe", slog.String("client_id", s.ID), slog.String("filter", filter))
	b.stats.DecrementSubscriptions()

	b.router.Unsubscribe(s.ID, filter)

	if err := b.subscriptions.Remove(s.ID, filter); err != nil {
		return fmt.Errorf("failed to remove subscription: %w", err)
	}

	// Remove subscription from cluster
	if b.cluster != nil {
		ctx := context.Background()
		if err := b.cluster.RemoveSubscription(ctx, s.ID, filter); err != nil {
			b.logError("cluster_remove_subscription", err, slog.String("client_id", s.ID), slog.String("filter", filter))
		}
	}

	s.RemoveSubscription(filter)

	return nil
}

// Subscribe adds a subscription (implements Service interface).
func (b *Broker) Subscribe(clientID string, filter string, qos byte, opts storage.SubscribeOptions) error {
	s := b.Get(clientID)
	if s == nil {
		return ErrSessionNotFound
	}

	return b.subscribe(s, filter, qos, opts)
}

// Unsubscribe removes a subscription (implements Service interface).
func (b *Broker) Unsubscribe(clientID string, filter string) error {
	s := b.Get(clientID)
	if s == nil {
		return ErrSessionNotFound
	}

	return b.unsubscribeInternal(s, filter)
}

// DeliverToSession queues a message for delivery to a session.
// Returns packet ID (>0) if session is connected and QoS>0, otherwise 0.
func (b *Broker) DeliverToSession(s *session.Session, msg *storage.Message) (uint16, error) {
	if !s.IsConnected() {
		if msg.QoS > 0 {
			return 0, s.OfflineQueue().Enqueue(msg)
		}
		return 0, nil
	}

	if msg.QoS == 0 {
		return 0, b.DeliverMessage(s, msg)
	}

	packetID := s.NextPacketID()
	msg.PacketID = packetID
	if err := s.Inflight().Add(packetID, msg, messages.Outbound); err != nil {
		return 0, err
	}

	if err := b.DeliverMessage(s, msg); err != nil {
		return packetID, err
	}

	return packetID, nil
}

// AckMessage acknowledges a message by packet ID.
func (b *Broker) AckMessage(s *session.Session, packetID uint16) error {
	s.Inflight().Ack(packetID)
	return nil
}

// PublishWill publishes a session's will message if it exists.
func (b *Broker) PublishWill(clientID string) error {
	if b.wills == nil {
		return nil
	}

	will, err := b.wills.Get(clientID)
	if err != nil {
		if err == storage.ErrNotFound {
			return nil
		}
		return err
	}

	if err := b.distribute(will.Topic, will.Payload, will.QoS, will.Retain, will.Properties); err != nil {
		return err
	}

	return b.wills.Delete(clientID)
}

// Distribute distributes a message to all matching subscribers (implements Service interface).
func (b *Broker) Distribute(topic string, payload []byte, qos byte, retain bool, props map[string]string) error {
	return b.distribute(topic, payload, qos, retain, props)
}

// Match returns all subscriptions matching a topic (implements Service interface).
func (b *Broker) Match(topic string) ([]*storage.Subscription, error) {
	return b.router.Match(topic)
}

// distribute distributes a message to all matching subscribers (local and remote).
func (b *Broker) distribute(topic string, payload []byte, qos byte, retain bool, props map[string]string) error {
	// Deliver to local subscribers
	matched, err := b.router.Match(topic)
	if err != nil {
		return err
	}

	for _, sub := range matched {
		s := b.sessionsMap.Get(sub.ClientID)
		if s == nil {
			continue
		}

		deliverQoS := qos
		if sub.QoS < deliverQoS {
			deliverQoS = sub.QoS
		}

		msg := &storage.Message{
			Topic:      topic,
			Payload:    payload,
			QoS:        deliverQoS,
			Retain:     retain,
			Properties: props,
		}

		if _, err := b.DeliverToSession(s, msg); err != nil {
			continue
		}
	}

	// Route to remote subscribers in cluster
	if b.cluster != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()

		if err := b.cluster.RoutePublish(ctx, topic, payload, qos, retain, props); err != nil {
			b.logError("cluster_route_publish", err, slog.String("topic", topic))
		}
	}

	return nil
}

// Get returns a session by client ID.
func (b *Broker) Get(clientID string) *session.Session {
	return b.sessionsMap.Get(clientID)
}

// destroySessionLocked destroys a session. Must be called with mu held.
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
		if err := b.wills.Delete(s.ID); err != nil {
			return fmt.Errorf("failed to delete will: %w", err)
		}
	}

	b.sessionsMap.Delete(s.ID)

	subs := s.GetSubscriptions()
	for filter := range subs {
		b.router.Unsubscribe(s.ID, filter)

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

// handleDisconnect handles session disconnect.
func (b *Broker) handleDisconnect(s *session.Session, graceful bool) {
	if b.sessions != nil {
		b.sessions.Save(s.Info())
	}
	if b.wills != nil {
		will := s.GetWill()
		if !graceful && will != nil {
			b.wills.Set(s.ID, will)
		} else if graceful {
			b.wills.Delete(s.ID)
		}
	}
	if b.messages != nil {
		msgs := s.OfflineQueue().Drain()
		for i, msg := range msgs {
			key := fmt.Sprintf("%s%s%d", queuePrefix, s.ID, i)
			b.messages.Store(key, msg)
		}

		for _, inf := range s.Inflight().GetAll() {
			key := fmt.Sprintf("%s%s%d", inflightPrefix, s.ID, inf.PacketID)
			b.messages.Store(key, inf.Message)
		}
	}

	if s.CleanStart && s.ExpiryInterval == 0 {
		b.mu.Lock()
		b.destroySessionLocked(s)
		b.mu.Unlock()

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

		b.distribute(will.Topic, will.Payload, will.QoS, will.Retain, will.Properties)
		b.wills.Delete(will.ClientID)
	}
}

// statsLoop periodically publishes broker statistics.
func (b *Broker) statsLoop() {
	defer b.wg.Done()

	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			b.publishStats()
		case <-b.stopCh:
			return
		}
	}
}

// publishStats publishes current broker statistics to $SYS topics.
func (b *Broker) publishStats() {
	if b.stats == nil {
		return
	}

	stats := []struct {
		topic string
		value string
	}{
		{"$SYS/broker/version", "mqtt-broker-0.1.0"},
		{"$SYS/broker/uptime", fmt.Sprintf("%d", int64(b.stats.GetUptime().Seconds()))},
		{"$SYS/broker/clients/connected", fmt.Sprintf("%d", b.stats.GetCurrentConnections())},
		{"$SYS/broker/clients/total", fmt.Sprintf("%d", b.stats.GetTotalConnections())},
		{"$SYS/broker/clients/disconnected", fmt.Sprintf("%d", b.stats.GetDisconnections())},
		{"$SYS/broker/messages/received", fmt.Sprintf("%d", b.stats.GetMessagesReceived())},
		{"$SYS/broker/messages/sent", fmt.Sprintf("%d", b.stats.GetMessagesSent())},
		{"$SYS/broker/messages/publish/received", fmt.Sprintf("%d", b.stats.GetPublishReceived())},
		{"$SYS/broker/messages/publish/sent", fmt.Sprintf("%d", b.stats.GetPublishSent())},
		{"$SYS/broker/bytes/received", fmt.Sprintf("%d", b.stats.GetBytesReceived())},
		{"$SYS/broker/bytes/sent", fmt.Sprintf("%d", b.stats.GetBytesSent())},
		{"$SYS/broker/subscriptions/count", fmt.Sprintf("%d", b.stats.GetSubscriptions())},
		{"$SYS/broker/retained/count", fmt.Sprintf("%d", b.stats.GetRetainedMessages())},
		{"$SYS/broker/errors/protocol", fmt.Sprintf("%d", b.stats.GetProtocolErrors())},
		{"$SYS/broker/errors/auth", fmt.Sprintf("%d", b.stats.GetAuthErrors())},
		{"$SYS/broker/errors/authz", fmt.Sprintf("%d", b.stats.GetAuthzErrors())},
		{"$SYS/broker/errors/packet", fmt.Sprintf("%d", b.stats.GetPacketErrors())},
	}

	for _, s := range stats {
		b.distribute(s.topic, []byte(s.value), 0, true, nil)
	}
}

func (b *Broker) DeliverMessage(s *session.Session, msg *storage.Message) error {
	b.stats.IncrementPublishSent()
	b.stats.AddBytesSent(uint64(len(msg.Payload)))

	var pub packets.ControlPacket

	switch s.Version {
	case 5:
		pub = &v5.Publish{
			FixedHeader: packets.FixedHeader{
				PacketType: packets.PublishType,
				QoS:        msg.QoS,
				Retain:     msg.Retain,
			},
			TopicName:  msg.Topic,
			Payload:    msg.Payload,
			ID:         msg.PacketID,
			Properties: &v5.PublishProperties{},
		}
	default:
		pub = &v3.Publish{
			FixedHeader: packets.FixedHeader{
				PacketType: packets.PublishType,
				QoS:        msg.QoS,
				Retain:     msg.Retain,
			},
			TopicName: msg.Topic,
			Payload:   msg.Payload,
			ID:        msg.PacketID,
		}
	}

	return s.WritePacket(pub)
}

// runSession runs the main packet loop for a session using a Handler.
func (b *Broker) runSession(handler Handler, s *session.Session) error {
	conn := s.Conn()
	if conn == nil {
		return nil
	}

	if s.KeepAlive > 0 {
		deadline := time.Now().Add(s.KeepAlive + s.KeepAlive/2)
		conn.SetReadDeadline(deadline)
	}

	for {
		pkt, err := s.ReadPacket()
		if err != nil {
			if err != io.EOF && err != session.ErrNotConnected {
				b.stats.IncrementPacketErrors()
			}
			b.stats.DecrementConnections()
			s.Disconnect(false)
			return err
		}

		if s.KeepAlive > 0 {
			deadline := time.Now().Add(s.KeepAlive + s.KeepAlive/2)
			conn.SetReadDeadline(deadline)
		}

		b.stats.IncrementMessagesReceived()
		s.Touch()

		if err := dispatchPacket(handler, s, pkt); err != nil {
			if err == io.EOF {
				b.stats.DecrementConnections()
				return nil
			}
			b.stats.IncrementProtocolErrors()
			b.stats.DecrementConnections()
			s.Disconnect(false)
			return err
		}
	}
}

func dispatchPacket(handler Handler, s *session.Session, pkt packets.ControlPacket) error {
	switch pkt.Type() {
	case packets.PublishType:
		return handler.HandlePublish(s, pkt)
	case packets.PubAckType:
		return handler.HandlePubAck(s, pkt)
	case packets.PubRecType:
		return handler.HandlePubRec(s, pkt)
	case packets.PubRelType:
		return handler.HandlePubRel(s, pkt)
	case packets.PubCompType:
		return handler.HandlePubComp(s, pkt)
	case packets.SubscribeType:
		return handler.HandleSubscribe(s, pkt)
	case packets.UnsubscribeType:
		return handler.HandleUnsubscribe(s, pkt)
	case packets.PingReqType:
		return handler.HandlePingReq(s)
	case packets.DisconnectType:
		return handler.HandleDisconnect(s, pkt)
	case packets.AuthType:
		return handler.HandleAuth(s, pkt)
	default:
		return ErrInvalidPacketType
	}
}

// Close shuts down the broker.
func (b *Broker) Close() error {
	close(b.stopCh)
	b.wg.Wait()

	b.mu.Lock()
	defer b.mu.Unlock()

	b.sessionsMap.ForEach(func(session *session.Session) {
		if session.IsConnected() {
			session.Disconnect(false)
		}
	})
	return nil
}

// GetSessionStateAndClose disconnects a session, retrieves its state, and returns it.
// This is used during session takeover.
func (b *Broker) GetSessionStateAndClose(ctx context.Context, clientID string) (*grpc.SessionState, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	s := b.sessionsMap.Get(clientID)
	if s == nil {
		return nil, nil // Session not found
	}

	// Capture state before destroying
	state := &grpc.SessionState{
		ExpiryInterval: uint32(s.ExpiryInterval),
		CleanStart:     s.CleanStart,
	}

	// Capture subscriptions from storage (includes QoS)
	if b.subscriptions != nil {
		subs, err := b.subscriptions.GetForClient(s.ID)
		if err == nil {
			for _, sub := range subs {
				state.Subscriptions = append(state.Subscriptions, &grpc.Subscription{
					Filter: sub.Filter,
					Qos:    uint32(sub.QoS),
				})
			}
		}
	}

	// Capture inflight messages
	for _, msg := range s.Inflight().GetAll() {
		state.InflightMessages = append(state.InflightMessages, &grpc.InflightMessage{
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
		state.QueuedMessages = append(state.QueuedMessages, &grpc.QueuedMessage{
			Topic:     msg.Topic,
			Payload:   msg.Payload,
			Qos:       uint32(msg.QoS),
			Retain:    msg.Retain,
			Timestamp: time.Now().Unix(),
		})
	}

	// Capture will message
	if will := s.GetWill(); will != nil {
		state.Will = &grpc.WillMessage{
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

// DeliverToClient implements cluster.MessageHandler.DeliverToClient.
func (b *Broker) DeliverToClient(ctx context.Context, clientID string, msg *core.Message) error {
	s := b.Get(clientID)
	if s == nil {
		return fmt.Errorf("session not found: %s", clientID)
	}

	storeMsg := &storage.Message{
		Topic:      msg.Topic,
		Payload:    msg.Payload,
		QoS:        msg.QoS,
		Retain:     msg.Retain,
		Properties: msg.Properties,
	}

	_, err := b.DeliverToSession(s, storeMsg)
	return err
}

// restoreInflightFromTakeover restores inflight messages from takeover state.
func (b *Broker) restoreInflightFromTakeover(state *grpc.SessionState, tracker messages.Inflight) error {
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
func (b *Broker) restoreQueueFromTakeover(state *grpc.SessionState, queue messages.Queue) error {
	if state == nil || state.QueuedMessages == nil {
		return nil
	}

	for _, msg := range state.QueuedMessages {
		storeMsg := &storage.Message{
			Topic:   msg.Topic,
			Payload: msg.Payload,
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
func (b *Broker) restoreSubscriptionsFromTakeover(s *session.Session, state *grpc.SessionState) error {
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
