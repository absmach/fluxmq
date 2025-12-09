package broker

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"time"

	"github.com/dborovcanin/mqtt/handlers"
	"github.com/dborovcanin/mqtt/packets"
	v3 "github.com/dborovcanin/mqtt/packets/v3"
	"github.com/dborovcanin/mqtt/session"
	"github.com/dborovcanin/mqtt/store"
	"github.com/dborovcanin/mqtt/store/memory"
)

// Broker is the core MQTT broker.
type Broker struct {
	// Session management
	sessionMgr *session.Manager

	// Routing
	router *Router

	// Storage
	store store.Store

	// Handlers
	handler    handlers.Handler
	dispatcher *handlers.Dispatcher

	// Logger
	logger *slog.Logger
}

// NewBroker creates a new broker instance.
func NewBroker(logger *slog.Logger) *Broker {
	if logger == nil {
		logger = slog.Default()
	}

	st := memory.New()
	sessionMgr := session.NewManager(st)
	router := NewRouter()

	b := &Broker{
		sessionMgr: sessionMgr,
		router:     router,
		store:      st,
		logger:     logger,
	}

	// Create handler with dependencies
	b.handler = handlers.NewBrokerHandler(handlers.BrokerHandlerConfig{
		SessionManager: sessionMgr,
		Router:         b, // Broker implements handlers.Router
		Publisher:      b, // Broker implements handlers.Publisher
		Retained:       st.Retained(),
	})
	b.dispatcher = handlers.NewDispatcher(b.handler)

	sessionMgr.SetOnWillTrigger(func(will *store.WillMessage) {
		b.Distribute(will.Topic, will.Payload, will.QoS, will.Retain, will.Properties)
	})

	return b
}

// HandleConnection handles a new incoming connection from the TCP server.
// This implements the broker.Handler interface.
func (b *Broker) HandleConnection(netConn net.Conn) {
	// Wrap net.Conn with MQTT codec to get broker.Connection
	conn := NewConnection(netConn)

	// 1. Read CONNECT packet
	pkt, err := conn.ReadPacket()
	if err != nil {
		b.logger.Error("Failed to read CONNECT packet",
			slog.String("remote_addr", netConn.RemoteAddr().String()),
			slog.String("error", err.Error()))
		conn.Close()
		return
	}

	// 2. Validate it is CONNECT
	if pkt.Type() != packets.ConnectType {
		b.logger.Warn("Expected CONNECT packet, received different packet type",
			slog.String("remote_addr", netConn.RemoteAddr().String()),
			slog.String("packet_type", pkt.String()))
		conn.Close()
		return
	}

	// Extract connection parameters from CONNECT packet
	p, ok := pkt.(*v3.Connect)
	if !ok {
		b.logger.Error("Failed to parse CONNECT packet as v3",
			slog.String("remote_addr", netConn.RemoteAddr().String()))
		conn.Close()
		return
	}

	clientID := p.ClientID
	cleanStart := p.CleanSession
	keepAlive := p.KeepAlive

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

	// 3. Get or create session
	opts := session.Options{
		CleanStart:     cleanStart,
		ReceiveMaximum: 65535,
		KeepAlive:      keepAlive,
		Will:           will,
	}

	sess, _, err := b.sessionMgr.GetOrCreate(clientID, 4, opts) // v3.1.1 = version 4
	if err != nil {
		b.logger.Error("Failed to create session",
			slog.String("client_id", clientID),
			slog.String("error", err.Error()))
		conn.Close()
		return
	}

	// 4. Attach connection to session
	sessConn := &sessionConnection{conn: conn}
	if err := sess.Connect(sessConn); err != nil {
		b.logger.Error("Failed to attach connection to session",
			slog.String("client_id", clientID),
			slog.String("error", err.Error()))
		conn.Close()
		return
	}

	// 5. Send CONNACK
	if err := b.sendConnAck(conn); err != nil {
		b.logger.Error("Failed to send CONNACK",
			slog.String("client_id", clientID),
			slog.String("error", err.Error()))
		sess.Disconnect(false)
		return
	}

	b.logger.Info("Client connected",
		slog.String("client_id", clientID),
		slog.String("remote_addr", netConn.RemoteAddr().String()),
		slog.Bool("clean_start", cleanStart),
		slog.Uint64("keep_alive", uint64(keepAlive)))

	// 6. Deliver any queued offline messages
	b.deliverOfflineMessages(sess)

	// 7. Run packet loop
	b.runSession(sess)
}

// runSession runs the main packet loop for a session.
func (b *Broker) runSession(sess *session.Session) {
	for {
		pkt, err := sess.ReadPacket()
		if err != nil {
			if err != io.EOF && err != session.ErrNotConnected {
				b.logger.Error("Failed to read packet from client",
					slog.String("client_id", sess.ID),
					slog.String("error", err.Error()))
			}
			sess.Disconnect(false)
			return
		}

		if err := b.dispatcher.Dispatch(sess, pkt); err != nil {
			if err == io.EOF {
				return // Clean disconnect
			}
			b.logger.Error("Packet handler error",
				slog.String("client_id", sess.ID),
				slog.String("error", err.Error()))
			sess.Disconnect(false)
			return
		}
	}
}

// deliverOfflineMessages sends queued messages to reconnected client.
func (b *Broker) deliverOfflineMessages(sess *session.Session) {
	msgs := b.sessionMgr.DrainOfflineQueue(sess.ID)
	for _, msg := range msgs {
		b.deliverToSession(sess, msg.Topic, msg.Payload, msg.QoS, msg.Retain)
	}
}

func (b *Broker) sendConnAck(conn Connection) error {
	ack := &v3.ConnAck{
		FixedHeader: packets.FixedHeader{PacketType: packets.ConnAckType},
		ReturnCode:  0,
	}
	return conn.WritePacket(ack)
}

// --- handlers.Router implementation ---

// Subscribe adds a subscription.
func (b *Broker) Subscribe(clientID string, filter string, qos byte, opts store.SubscribeOptions) error {
	b.router.Subscribe(filter, Subscription{SessionID: clientID, QoS: qos})

	// Check for retained messages
	retained, err := b.store.Retained().Match(filter)
	if err == nil {
		sess := b.sessionMgr.Get(clientID)
		if sess != nil && sess.IsConnected() {
			for _, msg := range retained {
				deliverQoS := qos
				if msg.QoS < deliverQoS {
					deliverQoS = msg.QoS
				}
				b.deliverToSession(sess, msg.Topic, msg.Payload, deliverQoS, true)
			}
		}
	}

	return nil
}

// Unsubscribe removes a subscription.
func (b *Broker) Unsubscribe(clientID string, filter string) error {
	// Router doesn't track by client, so this is a no-op for now
	// In a full implementation, we'd track subscriptions per client
	return nil
}

// Match returns all subscriptions matching a topic.
func (b *Broker) Match(topic string) ([]*store.Subscription, error) {
	matched := b.router.Match(topic)
	result := make([]*store.Subscription, len(matched))
	for i, sub := range matched {
		result[i] = &store.Subscription{
			ClientID: sub.SessionID,
			Filter:   topic,
			QoS:      sub.QoS,
		}
	}
	return result, nil
}

// --- handlers.Publisher implementation ---

// Distribute distributes a message to all matching subscribers.
func (b *Broker) Distribute(topic string, payload []byte, qos byte, retain bool, props map[string]string) error {
	// Handle retained message
	if retain {
		if len(payload) == 0 {
			// Empty payload means remove retained message
			b.store.Retained().Delete(topic)
		} else {
			// Update retained message
			b.store.Retained().Set(topic, &store.Message{
				Topic:      topic,
				Payload:    payload,
				QoS:        qos,
				Retain:     true,
				Properties: props,
			})
		}
	}

	matched := b.router.Match(topic)

	for _, sub := range matched {
		s := b.sessionMgr.Get(sub.SessionID)
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
			// Queue for offline delivery
			msg := &store.Message{
				Topic:   topic,
				Payload: payload,
				QoS:     deliverQoS,
				Retain:  false,
			}
			s.OfflineQueue.Enqueue(msg)
		}
	}

	return nil
}

// --- OperationHandler implementation ---

// Publish implements OperationHandler.Publish for stateless injection.
func (b *Broker) Publish(ctx context.Context, clientID string, topic string, payload []byte, qos byte, retain bool) error {
	return b.Distribute(topic, payload, qos, retain, nil)
}

// SubscribeToTopic implements OperationHandler.SubscribeToTopic.
// Note: This requires a mechanism to pump messages to a channel.
// We can use a special internal session type or a channel-based adapter.
func (b *Broker) SubscribeToTopic(ctx context.Context, clientID string, topicFilter string) (<-chan *store.Message, error) {
	return nil, fmt.Errorf("stateless subscribe not implemented yet")
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
		s.Inflight.Add(pub.ID, msg, session.Outbound)
	}
	return s.WritePacket(pub)
}

// Close shuts down the broker.
func (b *Broker) Close() error {
	b.sessionMgr.Close()
	b.store.Close()
	return nil
}

// sessionConnection wraps a Connection to implement session.Connection.
type sessionConnection struct {
	conn Connection
}

func (c *sessionConnection) ReadPacket() (packets.ControlPacket, error) {
	return c.conn.ReadPacket()
}

func (c *sessionConnection) WritePacket(p packets.ControlPacket) error {
	return c.conn.WritePacket(p)
}

func (c *sessionConnection) Close() error {
	return c.conn.Close()
}

func (c *sessionConnection) RemoteAddr() net.Addr {
	return c.conn.RemoteAddr()
}

func (c *sessionConnection) SetReadDeadline(t time.Time) error {
	return nil
}

func (c *sessionConnection) SetWriteDeadline(t time.Time) error {
	return nil
}

// Ensure sessionConnection implements session.Connection.
var _ session.Connection = (*sessionConnection)(nil)
