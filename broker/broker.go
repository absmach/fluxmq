package broker

import (
	"fmt"
	"sync"
	"time"

	"github.com/dborovcanin/mqtt/core"
	"github.com/dborovcanin/mqtt/core/packets"
	v3 "github.com/dborovcanin/mqtt/core/packets/v3"
	v5 "github.com/dborovcanin/mqtt/core/packets/v5"
	"github.com/dborovcanin/mqtt/session"
	"github.com/dborovcanin/mqtt/store"
	"github.com/dborovcanin/mqtt/store/memory"
	"github.com/dborovcanin/mqtt/store/messages"
)

// MessageDeliverer handles protocol-specific message delivery.
type MessageDeliverer interface {
	DeliverMessage(s *session.Session, msg Message) error
}

// Broker is the core MQTT broker with clean domain methods.
type Broker struct {
	mu          sync.RWMutex
	sessionsMap session.Cache
	router      Router

	messages      store.MessageStore
	sessions      store.SessionStore
	subscriptions store.SubscriptionStore
	retained      store.RetainedStore
	wills         store.WillStore

	deliverer MessageDeliverer
	auth      *AuthEngine
	stats     *Stats

	stopCh chan struct{}
	wg     sync.WaitGroup
}

// NewBroker creates a new broker instance.
func NewBroker() *Broker {
	st := memory.New()
	router := NewRouter()

	b := &Broker{
		sessionsMap:   session.NewMapCache(),
		router:        router,
		messages:      st.Messages(),
		sessions:      st.Sessions(),
		subscriptions: st.Subscriptions(),
		retained:      st.Retained(),
		wills:         st.Wills(),
		stats:         NewStats(),
		stopCh:        make(chan struct{}),
	}

	b.SetDeliverer(&multiVersionDeliverer{
		broker: b,
	})

	b.wg.Add(2)
	go b.expiryLoop()
	go b.statsLoop()

	return b
}

// SetDeliverer sets the message deliverer for protocol-specific packet creation.
func (b *Broker) SetDeliverer(d MessageDeliverer) {
	b.deliverer = d
}

// SetAuth sets the authentication and authorization engine.
func (b *Broker) SetAuth(auth *AuthEngine) {
	b.auth = auth
}

// Stats returns the broker statistics.
func (b *Broker) Stats() *Stats {
	return b.stats
}

// --- Public Service interface adapters ---

// These methods adapt between the Service interface (for middleware) and the domain methods.

// SubscribeService adapts the Service.Subscribe signature to the domain Subscribe method.
func (b *Broker) SubscribeService(clientID string, filter string, qos byte, opts store.SubscribeOptions) error {
	sess := b.Get(clientID)
	if sess == nil {
		return ErrSessionNotFound
	}

	subOpts := SubscriptionOptions{
		QoS:               qos,
		NoLocal:           opts.NoLocal,
		RetainAsPublished: opts.RetainAsPublished,
		RetainHandling:    opts.RetainHandling,
	}

	return b.subscribeInternal(sess, filter, subOpts)
}

// UnsubscribeService adapts the Service.Unsubscribe signature to the domain Unsubscribe method.
func (b *Broker) UnsubscribeService(clientID string, filter string) error {
	sess := b.Get(clientID)
	if sess == nil {
		return ErrSessionNotFound
	}

	return b.unsubscribeInternal(sess, filter)
}

// --- Core Domain Methods ---

// CreateSession creates a new session or returns an existing one.
// If opts.CleanStart is true and a session exists, it is destroyed first.
// Returns the session and whether it was newly created.
func (b *Broker) CreateSession(clientID string, opts SessionOptions) (*session.Session, bool, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

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

	if !opts.CleanStart {
		if err := b.restoreInflightFromStorage(clientID, inflight); err != nil {
			return nil, false, err
		}
		if err := b.restoreQueueFromStorage(clientID, offlineQueue); err != nil {
			return nil, false, err
		}
	}

	var will *store.WillMessage
	if opts.WillMessage != nil {
		will = &store.WillMessage{
			ClientID:   clientID,
			Topic:      opts.WillMessage.Topic,
			Payload:    opts.WillMessage.Payload,
			QoS:        opts.WillMessage.QoS,
			Retain:     opts.WillMessage.Retain,
			Properties: opts.WillMessage.Properties,
		}
	}

	sessionOpts := session.Options{
		CleanStart:     opts.CleanStart,
		ReceiveMaximum: receiveMax,
		KeepAlive:      time.Duration(opts.KeepAlive) * time.Second,
		ExpiryInterval: opts.SessionExpiry,
		Will:           will,
	}

	sess := session.New(clientID, 0, sessionOpts, inflight, offlineQueue)

	if err := b.restoreSessionFromStorage(sess, clientID, sessionOpts); err != nil {
		return nil, false, err
	}

	sess.SetOnDisconnect(func(s *session.Session, graceful bool) {
		b.handleDisconnect(s, graceful)
	})

	b.sessionsMap.Set(clientID, sess)

	if b.sessions != nil {
		if err := b.sessions.Save(sess.Info()); err != nil {
			return nil, false, fmt.Errorf("failed to save session: %w", err)
		}
	}

	return sess, true, nil
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
func (b *Broker) Publish(sess *session.Session, msg Message) error {
	if msg.Retain {
		if len(msg.Payload) == 0 {
			if err := b.retained.Delete(msg.Topic); err != nil {
				return err
			}
		} else {
			storeMsg := &store.Message{
				Topic:      msg.Topic,
				Payload:    msg.Payload,
				QoS:        msg.QoS,
				Retain:     true,
				Properties: msg.Properties,
			}
			if err := b.retained.Set(msg.Topic, storeMsg); err != nil {
				return err
			}
		}
	}

	return b.distribute(msg.Topic, msg.Payload, msg.QoS, false, msg.Properties)
}

// subscribeInternal adds a subscription for a session (internal domain method).
func (b *Broker) subscribeInternal(sess *session.Session, filter string, opts SubscriptionOptions) error {
	storeOpts := store.SubscribeOptions{
		NoLocal:           opts.NoLocal,
		RetainAsPublished: opts.RetainAsPublished,
		RetainHandling:    opts.RetainHandling,
	}

	b.router.Subscribe(sess.ID, filter, opts.QoS, storeOpts)

	sub := &store.Subscription{
		ClientID: sess.ID,
		Filter:   filter,
		QoS:      opts.QoS,
		Options:  storeOpts,
	}
	if err := b.subscriptions.Add(sub); err != nil {
		return fmt.Errorf("failed to persist subscription: %w", err)
	}

	sess.AddSubscription(filter, storeOpts)

	return nil
}

// unsubscribeInternal removes a subscription for a session (internal domain method).
func (b *Broker) unsubscribeInternal(sess *session.Session, filter string) error {
	b.router.Unsubscribe(sess.ID, filter)

	if err := b.subscriptions.Remove(sess.ID, filter); err != nil {
		return fmt.Errorf("failed to remove subscription: %w", err)
	}

	sess.RemoveSubscription(filter)

	return nil
}

// Subscribe adds a subscription (implements Service interface).
func (b *Broker) Subscribe(clientID string, filter string, qos byte, opts store.SubscribeOptions) error {
	return b.SubscribeService(clientID, filter, qos, opts)
}

// Unsubscribe removes a subscription (implements Service interface).
func (b *Broker) Unsubscribe(clientID string, filter string) error {
	return b.UnsubscribeService(clientID, filter)
}

// DeliverToSession queues a message for delivery to a session.
// Returns packet ID (>0) if session is connected and QoS>0, otherwise 0.
func (b *Broker) DeliverToSession(sess *session.Session, msg Message) (uint16, error) {
	if !sess.IsConnected() {
		if msg.QoS > 0 {
			storeMsg := &store.Message{
				Topic:      msg.Topic,
				Payload:    msg.Payload,
				QoS:        msg.QoS,
				Properties: msg.Properties,
			}
			return 0, sess.OfflineQueue().Enqueue(storeMsg)
		}
		return 0, nil
	}

	if msg.QoS == 0 {
		if b.deliverer != nil {
			return 0, b.deliverer.DeliverMessage(sess, msg)
		}
		return 0, nil
	}

	packetID := sess.NextPacketID()
	storeMsg := &store.Message{
		Topic:      msg.Topic,
		Payload:    msg.Payload,
		QoS:        msg.QoS,
		PacketID:   packetID,
		Properties: msg.Properties,
	}
	if err := sess.Inflight().Add(packetID, storeMsg, messages.Outbound); err != nil {
		return 0, err
	}

	if b.deliverer != nil {
		deliverMsg := msg
		deliverMsg.PacketID = packetID
		if err := b.deliverer.DeliverMessage(sess, deliverMsg); err != nil {
			return packetID, err
		}
	}

	return packetID, nil
}

// AckMessage acknowledges a message by packet ID.
func (b *Broker) AckMessage(sess *session.Session, packetID uint16) error {
	sess.Inflight().Ack(packetID)
	return nil
}

// PublishWill publishes a session's will message if it exists.
func (b *Broker) PublishWill(clientID string) error {
	if b.wills == nil {
		return nil
	}

	will, err := b.wills.Get(clientID)
	if err != nil {
		if err == store.ErrNotFound {
			return nil
		}
		return err
	}

	if err := b.distribute(will.Topic, will.Payload, will.QoS, will.Retain, will.Properties); err != nil {
		return err
	}

	return b.wills.Delete(clientID)
}

// --- Public Service interface methods ---

// Distribute distributes a message to all matching subscribers (implements Service interface).
func (b *Broker) Distribute(topic string, payload []byte, qos byte, retain bool, props map[string]string) error {
	return b.distribute(topic, payload, qos, retain, props)
}

// Match returns all subscriptions matching a topic (implements Service interface).
func (b *Broker) Match(topic string) ([]*store.Subscription, error) {
	return b.router.Match(topic)
}

// --- Internal helper methods ---

// distribute distributes a message to all matching subscribers.
func (b *Broker) distribute(topic string, payload []byte, qos byte, retain bool, props map[string]string) error {
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

		msg := Message{
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
	}

	return nil
}

// restoreInflightFromStorage restores inflight messages from storage.
func (b *Broker) restoreInflightFromStorage(clientID string, tracker messages.Inflight) error {
	if b.messages == nil {
		return nil
	}

	inflightMsgs, err := b.messages.List(clientID + "/inflight/")
	if err != nil {
		return fmt.Errorf("failed to list inflight messages: %w", err)
	}

	for _, msg := range inflightMsgs {
		if msg.PacketID != 0 {
			tracker.Add(msg.PacketID, msg, messages.Outbound)
		}
	}

	if err := b.messages.DeleteByPrefix(clientID + "/inflight/"); err != nil {
		return fmt.Errorf("failed to clear inflight messages: %w", err)
	}

	return nil
}

// restoreQueueFromStorage restores offline messages from storage.
func (b *Broker) restoreQueueFromStorage(clientID string, queue messages.Queue) error {
	if b.messages == nil {
		return nil
	}

	msgs, err := b.messages.List(clientID + "/queue/")
	if err != nil {
		return fmt.Errorf("failed to list offline messages: %w", err)
	}

	for _, msg := range msgs {
		queue.Enqueue(msg)
	}

	if err := b.messages.DeleteByPrefix(clientID + "/queue/"); err != nil {
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
	if err != nil && err != store.ErrNotFound {
		return fmt.Errorf("failed to get session: %w", err)
	}
	if stored != nil {
		s.RestoreFrom(stored)
	}

	subs, err := b.subscriptions.GetForClient(clientID)
	if err != nil {
		return fmt.Errorf("failed to get subscriptions: %w", err)
	}
	for _, sub := range subs {
		s.AddSubscription(sub.Filter, sub.Options)
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
			key := fmt.Sprintf("%s/queue/%d", s.ID, i)
			b.messages.Store(key, msg)
		}

		for _, inf := range s.Inflight().GetAll() {
			key := fmt.Sprintf("%s/inflight/%d", s.ID, inf.PacketID)
			b.messages.Store(key, inf.Message)
		}
	}

	if s.CleanStart && s.ExpiryInterval == 0 {
		b.mu.Lock()
		b.destroySessionLocked(s)
		b.mu.Unlock()
	}
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

// multiVersionDeliverer routes messages to the correct protocol handler based on session version.
type multiVersionDeliverer struct {
	broker *Broker
}

func (d *multiVersionDeliverer) DeliverMessage(sess *session.Session, msg Message) error {
	switch sess.Version {
	case 5:
		return d.broker.DeliverV5Message(sess, msg)
	default:
		return d.broker.DeliverV3Message(sess, msg)
	}
}

// --- Handler Interface Implementation ---

// HandleConnect implements Handler.HandleConnect by dispatching based on packet type.
func (b *Broker) HandleConnect(conn core.Connection, pkt packets.ControlPacket) error {
	if p3, ok := pkt.(*v3.Connect); ok {
		return b.HandleV3Connect(conn, p3)
	}
	if p5, ok := pkt.(*v5.Connect); ok {
		return b.HandleV5Connect(conn, p5)
	}
	return ErrInvalidPacketType
}

// HandlePublish implements Handler.HandlePublish by dispatching based on session version.
func (b *Broker) HandlePublish(s *session.Session, pkt packets.ControlPacket) error {
	switch p := pkt.(type) {
	case *v5.Publish:
		return b.handleV5Publish(s, p)
	case *v3.Publish:
		return b.handleV3Publish(s, p)
	default:
		return ErrInvalidPacketType
	}
}

// HandlePubAck implements Handler.HandlePubAck by dispatching based on session version.
func (b *Broker) HandlePubAck(s *session.Session, pkt packets.ControlPacket) error {
	switch p := pkt.(type) {
	case *v5.PubAck:
		return b.handleV5PubAck(s, p)
	case *v3.PubAck:
		return b.handleV3PubAck(s, p)
	default:
		return ErrInvalidPacketType
	}
}

// HandlePubRec implements Handler.HandlePubRec by dispatching based on session version.
func (b *Broker) HandlePubRec(s *session.Session, pkt packets.ControlPacket) error {
	switch p := pkt.(type) {
	case *v5.PubRec:
		return b.handleV5PubRec(s, p)
	case *v3.PubRec:
		return b.handleV3PubRec(s, p)
	default:
		return ErrInvalidPacketType
	}
}

// HandlePubRel implements Handler.HandlePubRel by dispatching based on session version.
func (b *Broker) HandlePubRel(s *session.Session, pkt packets.ControlPacket) error {
	switch p := pkt.(type) {
	case *v5.PubRel:
		return b.handleV5PubRel(s, p)
	case *v3.PubRel:
		return b.handleV3PubRel(s, p)
	default:
		return ErrInvalidPacketType
	}
}

// HandlePubComp implements Handler.HandlePubComp by dispatching based on session version.
func (b *Broker) HandlePubComp(s *session.Session, pkt packets.ControlPacket) error {
	switch p := pkt.(type) {
	case *v5.PubComp:
		return b.handleV5PubComp(s, p)
	case *v3.PubComp:
		return b.handleV3PubComp(s, p)
	default:
		return ErrInvalidPacketType
	}
}

// HandleSubscribe implements Handler.HandleSubscribe by dispatching based on session version.
func (b *Broker) HandleSubscribe(s *session.Session, pkt packets.ControlPacket) error {
	switch p := pkt.(type) {
	case *v5.Subscribe:
		return b.handleV5Subscribe(s, p)
	case *v3.Subscribe:
		return b.handleV3Subscribe(s, p)
	default:
		return ErrInvalidPacketType
	}
}

// HandleUnsubscribe implements Handler.HandleUnsubscribe by dispatching based on session version.
func (b *Broker) HandleUnsubscribe(s *session.Session, pkt packets.ControlPacket) error {
	switch p := pkt.(type) {
	case *v5.Unsubscribe:
		return b.handleV5Unsubscribe(s, p)
	case *v3.Unsubscribe:
		return b.handleV3Unsubscribe(s, p)
	default:
		return ErrInvalidPacketType
	}
}

// HandlePingReq implements Handler.HandlePingReq by dispatching based on session version.
func (b *Broker) HandlePingReq(s *session.Session) error {
	switch s.Version {
	case 5:
		return b.handleV5PingReq(s)
	case 3:
		return b.handleV3PingReq(s)
	default:

		return ErrInvalidPacketType
	}
}

// HandleDisconnect implements Handler.HandleDisconnect by dispatching based on session version.
func (b *Broker) HandleDisconnect(s *session.Session, pkt packets.ControlPacket) error {
	switch p := pkt.(type) {
	case *v5.Disconnect:
		return b.handleV5Disconnect(s, p)
	case *v3.Disconnect:
		return b.handleV3Disconnect(s, p)
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
