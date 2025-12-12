package broker

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"time"

	"github.com/dborovcanin/mqtt/core"
	"github.com/dborovcanin/mqtt/core/packets"
	v3 "github.com/dborovcanin/mqtt/core/packets/v3"
	"github.com/dborovcanin/mqtt/session"
	"github.com/dborovcanin/mqtt/store"
	"github.com/dborovcanin/mqtt/store/memory"
	"github.com/dborovcanin/mqtt/store/messages"
)

var (
	_ Router    = (*Broker)(nil)
	_ Publisher = (*Broker)(nil)
)

// Broker is the core MQTT broker.
type Broker struct {
	mu          sync.RWMutex
	sessionsMap session.Cache
	router      Router

	messages      store.MessageStore
	sessions      store.SessionStore
	subscriptions store.SubscriptionStore
	retained      store.RetainedStore
	wills         store.WillStore

	handler    Handler
	dispatcher *Dispatcher
	auth       *AuthEngine
	stats      *Stats
	logger     *slog.Logger

	stopCh chan struct{}
	wg     sync.WaitGroup
}

// NewBroker creates a new broker instance.
func NewBroker(logger *slog.Logger) *Broker {
	if logger == nil {
		logger = slog.Default()
	}

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
		logger:        logger,
		stopCh:        make(chan struct{}),
	}

	b.handler = NewV3Handler(V3HandlerConfig{
		Broker: b,
	})
	b.dispatcher = NewDispatcher(b.handler)
	b.wg.Add(2)
	go b.expiryLoop()
	go b.statsLoop()

	return b
}

// HandleConnection handles a new incoming connection from the TCP server.
// This implements the broker.Handler interface.
func (b *Broker) HandleConnection(conn core.Connection) {
	// 1. Read CONNECT packet
	pkt, err := conn.ReadPacket()
	if err != nil {
		b.logger.Error("Failed to read CONNECT packet",
			slog.String("remote_addr", conn.RemoteAddr().String()),
			slog.String("error", err.Error()))
		b.stats.IncrementPacketErrors()
		conn.Close()
		return
	}

	// 2. Validate it is CONNECT
	if pkt.Type() != packets.ConnectType {
		b.logger.Warn("Expected CONNECT packet, received different packet type",
			slog.String("remote_addr", conn.RemoteAddr().String()),
			slog.String("packet_type", pkt.String()))
		b.stats.IncrementProtocolErrors()
		conn.Close()
		return
	}

	p, ok := pkt.(*v3.Connect)
	if !ok {
		b.logger.Error("Failed to parse CONNECT packet as v3",
			slog.String("remote_addr", conn.RemoteAddr().String()))
		b.stats.IncrementProtocolErrors()
		conn.Close()
		return
	}

	// 3. Validate protocol version
	// MQTT 3.1.1 = version 4, MQTT 3.1 = version 3, MQTT 5.0 = version 5
	if p.ProtocolVersion != 3 && p.ProtocolVersion != 4 {
		b.logger.Warn("Unsupported protocol version",
			slog.String("remote_addr", conn.RemoteAddr().String()),
			slog.Int("version", int(p.ProtocolVersion)))
		b.stats.IncrementProtocolErrors()
		b.sendConnAck(conn, false, 0x01) // Unacceptable protocol version
		conn.Close()
		return
	}

	// Version 5 is detected but not fully implemented yet - placeholder for future
	if p.ProtocolVersion == 5 {
		b.logger.Warn("MQTT v5 not fully supported yet",
			slog.String("remote_addr", conn.RemoteAddr().String()))
		b.stats.IncrementProtocolErrors()
		b.sendConnAck(conn, false, 0x01) // Unacceptable protocol version
		conn.Close()
		return
	}

	clientID := p.ClientID
	cleanStart := p.CleanSession

	// 4. Client ID auto-generation (MQTT v3.1.1 spec requirement)
	if clientID == "" {
		if cleanStart {
			generated, err := GenerateClientID()
			if err != nil {
				b.logger.Error("Failed to generate client ID",
					slog.String("remote_addr", conn.RemoteAddr().String()),
					slog.String("error", err.Error()))
				b.stats.IncrementProtocolErrors()
				b.sendConnAck(conn, false, 0x02) // Identifier rejected
				conn.Close()
				return
			}
			clientID = generated
			b.logger.Debug("Generated client ID",
				slog.String("client_id", clientID),
				slog.String("remote_addr", conn.RemoteAddr().String()))
		} else {
			b.logger.Warn("Empty client ID with clean session false",
				slog.String("remote_addr", conn.RemoteAddr().String()))
			b.stats.IncrementProtocolErrors()
			b.sendConnAck(conn, false, 0x02) // Identifier rejected
			conn.Close()
			return
		}
	}

	// 5. Authenticate if auth engine is configured
	if b.auth != nil {
		username := p.Username
		password := string(p.Password)

		authenticated, err := b.auth.Authenticate(clientID, username, password)
		if err != nil {
			b.logger.Error("Authentication error",
				slog.String("client_id", clientID),
				slog.String("remote_addr", conn.RemoteAddr().String()),
				slog.String("error", err.Error()))
			b.stats.IncrementAuthErrors()
			b.sendConnAck(conn, false, 0x04) // Bad username or password
			conn.Close()
			return
		}

		if !authenticated {
			b.logger.Warn("Authentication failed",
				slog.String("client_id", clientID),
				slog.String("username", username),
				slog.String("remote_addr", conn.RemoteAddr().String()))
			b.stats.IncrementAuthErrors()
			b.sendConnAck(conn, false, 0x04) // Bad username or password
			conn.Close()
			return
		}
	}

	keepAlive := time.Second * time.Duration(p.KeepAlive)

	var will *store.WillMessage
	if p.WillFlag {
		will = &store.WillMessage{
			ClientID: clientID,
			Topic:    p.WillTopic,
			Payload:  p.WillMessage,
			QoS:      p.WillQoS,
			Retain:   p.WillRetain,
		}
	}

	opts := session.Options{
		CleanStart:     cleanStart,
		ReceiveMaximum: 65535,
		KeepAlive:      keepAlive,
		Will:           will,
	}

	// 6. Get or create session
	s, isNew, err := b.getOrCreate(clientID, p.ProtocolVersion, opts)
	if err != nil {
		b.logger.Error("Failed to create session",
			slog.String("client_id", clientID),
			slog.String("error", err.Error()))
		b.stats.IncrementProtocolErrors()
		b.sendConnAck(conn, false, 0x03) // Server unavailable
		conn.Close()
		return
	}

	// 7. Attach connection to session
	if err := s.Connect(conn); err != nil {
		b.logger.Error("Failed to attach connection to session",
			slog.String("client_id", clientID),
			slog.String("error", err.Error()))
		b.stats.IncrementProtocolErrors()
		conn.Close()
		return
	}

	// 8. Send CONNACK with SessionPresent flag
	// SessionPresent = true if session existed and was not cleaned
	sessionPresent := !isNew && !cleanStart
	if err := b.sendConnAck(conn, sessionPresent, 0x00); err != nil {
		b.logger.Error("Failed to send CONNACK",
			slog.String("client_id", clientID),
			slog.String("error", err.Error()))
		s.Disconnect(false)
		return
	}

	b.stats.IncrementConnections()

	b.logger.Info("Client connected",
		slog.String("client_id", clientID),
		slog.String("remote_addr", conn.RemoteAddr().String()),
		slog.Bool("clean_start", cleanStart),
		slog.Bool("session_present", sessionPresent),
		slog.Uint64("keep_alive", uint64(keepAlive)))

	b.deliverOfflineMessages(s)

	b.runSession(s)
}

// runSession runs the main packet loop for a session.
func (b *Broker) runSession(s *session.Session) {
	conn := s.Conn()
	if conn == nil {
		return
	}

	// Set initial keep-alive deadline if keep-alive is configured
	if s.KeepAlive > 0 {
		deadline := time.Now().Add(s.KeepAlive + s.KeepAlive/2) // 1.5x keep-alive
		if err := conn.SetReadDeadline(deadline); err != nil {
			b.logger.Error("Failed to set read deadline",
				slog.String("client_id", s.ID),
				slog.String("error", err.Error()))
		}
	}

	for {
		pkt, err := s.ReadPacket()
		if err != nil {
			if err != io.EOF && err != session.ErrNotConnected {
				b.logger.Error("Failed to read packet from client",
					slog.String("client_id", s.ID),
					slog.String("error", err.Error()))
				b.stats.IncrementPacketErrors()
			}
			b.stats.DecrementConnections()
			s.Disconnect(false)
			return
		}

		// MaxPacketSize validation is primarily an MQTT v5 feature
		// For v3.1.1, validation would need to be done at the codec level before full decode
		// Placeholder for future enhancement

		// Update keep-alive deadline after each packet
		if s.KeepAlive > 0 {
			deadline := time.Now().Add(s.KeepAlive + s.KeepAlive/2) // 1.5x keep-alive
			if err := conn.SetReadDeadline(deadline); err != nil {
				b.logger.Error("Failed to update read deadline",
					slog.String("client_id", s.ID),
					slog.String("error", err.Error()))
			}
		}

		b.stats.IncrementMessagesReceived()

		if err := b.dispatcher.Dispatch(s, pkt); err != nil {
			if err == io.EOF {
				b.stats.DecrementConnections()
				return // Clean disconnect
			}
			b.logger.Error("Packet handler error",
				slog.String("client_id", s.ID),
				slog.String("error", err.Error()))
			b.stats.IncrementProtocolErrors()
			b.stats.DecrementConnections()
			s.Disconnect(false)
			return
		}
	}
}

// deliverOfflineMessages sends queued messages to reconnected client.
func (b *Broker) deliverOfflineMessages(s *session.Session) {
	msgs := b.DrainOfflineQueue(s.ID)
	for _, msg := range msgs {
		b.deliverToSession(s, msg.Topic, msg.Payload, msg.QoS, msg.Retain)
	}
}

func (b *Broker) sendConnAck(conn core.Connection, sessionPresent bool, returnCode byte) error {
	ack := &v3.ConnAck{
		FixedHeader:    packets.FixedHeader{PacketType: packets.ConnAckType},
		SessionPresent: sessionPresent,
		ReturnCode:     returnCode,
	}
	return conn.WritePacket(ack)
}

func (b *Broker) Subscribe(clientID string, filter string, qos byte, opts store.SubscribeOptions) error {
	b.router.Subscribe(clientID, filter, qos, opts)

	sub := &store.Subscription{
		ClientID: clientID,
		Filter:   filter,
		QoS:      qos,
		Options:  opts,
	}
	if err := b.subscriptions.Add(sub); err != nil {
		return fmt.Errorf("failed to persist subscription: %w", err)
	}

	b.stats.IncrementSubscriptions()

	s := b.Get(clientID)
	if s != nil {
		s.AddSubscription(filter, opts)

		if s.IsConnected() {
			retained, err := b.retained.Match(filter)
			if err != nil {
				return fmt.Errorf("failed to match retained messages: %w", err)
			}

			for _, msg := range retained {
				deliverQoS := qos
				if msg.QoS < deliverQoS {
					deliverQoS = msg.QoS
				}
				b.deliverToSession(s, msg.Topic, msg.Payload, deliverQoS, true)
			}
		}
	}

	return nil
}

// Unsubscribe removes a subscription.
func (b *Broker) Unsubscribe(clientID string, filter string) error {
	b.router.Unsubscribe(clientID, filter)

	if err := b.subscriptions.Remove(clientID, filter); err != nil {
		return fmt.Errorf("failed to remove subscription from storage: %w", err)
	}

	b.stats.DecrementSubscriptions()

	s := b.Get(clientID)
	if s != nil {
		s.RemoveSubscription(filter)
	}

	return nil
}

// Match returns all subscriptions matching a topic.
func (b *Broker) Match(topic string) ([]*store.Subscription, error) {
	return b.router.Match(topic)
}

// --- handlers.Publisher implementation ---

// Distribute distributes a message to all matching subscribers.
func (b *Broker) Distribute(topic string, payload []byte, qos byte, retain bool, props map[string]string) error {
	if retain {
		if len(payload) == 0 {
			if err := b.retained.Delete(topic); err != nil {
				return fmt.Errorf("failed to delete retained message for topic %s: %w", topic, err)
			}
			b.stats.DecrementRetainedMessages()
		} else {
			if err := b.retained.Set(topic, &store.Message{
				Topic:      topic,
				Payload:    payload,
				QoS:        qos,
				Retain:     true,
				Properties: props,
			}); err != nil {
				return fmt.Errorf("failed to set retained message for topic %s: %w", topic, err)
			}
			b.stats.IncrementRetainedMessages()
		}
	}

	matched, err := b.router.Match(topic)
	if err != nil {
		return err
	}

	for _, sub := range matched {
		s := b.Get(sub.ClientID)
		if s == nil {
			continue
		}

		// Downgrade QoS if needed
		deliverQoS := qos
		if sub.QoS < deliverQoS {
			deliverQoS = sub.QoS
		}

		if s.IsConnected() {
			b.deliverToSession(s, topic, payload, deliverQoS, false)
		} else if deliverQoS > 0 {
			msg := &store.Message{
				Topic:   topic,
				Payload: payload,
				QoS:     deliverQoS,
				Retain:  false,
			}
			s.OfflineQueue().Enqueue(msg)
		}
	}

	return nil
}

// --- OperationHandler implementation ---

// Publish implements OperationHandler.Publish for stateless injection.
func (b *Broker) Publish(ctx context.Context, clientID string, topic string, payload []byte, qos byte, retain bool) error {
	return b.Distribute(topic, payload, qos, retain, nil)
}

// deliverToSession sends a message to a connected session.
func (b *Broker) deliverToSession(s *session.Session, topic string, payload []byte, qos byte, retain bool) error {
	pub := &v3.Publish{
		FixedHeader: packets.FixedHeader{
			PacketType: packets.PublishType,
			QoS:        qos,
			Retain:     retain,
		},
		TopicName: topic,
		Payload:   payload,
	}
	if qos > 0 {
		pub.ID = s.NextPacketID()
		msg := &store.Message{
			Topic:    topic,
			Payload:  payload,
			QoS:      qos,
			PacketID: pub.ID,
		}
		s.Inflight().Add(pub.ID, msg, messages.Outbound)
	}

	if err := s.WritePacket(pub); err != nil {
		return err
	}

	b.stats.IncrementPublishSent()
	b.stats.AddBytesSent(uint64(len(payload)))

	return nil
}

// SetAuth sets the authentication and authorization engine.
func (b *Broker) SetAuth(auth *AuthEngine) {
	b.auth = auth
}

// Stats returns the broker statistics.
func (b *Broker) Stats() *Stats {
	return b.stats
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
