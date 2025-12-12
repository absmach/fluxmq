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
		logger:        logger,
		stopCh:        make(chan struct{}),
	}

	b.handler = NewV3Handler(V3HandlerConfig{
		Broker: b,
	})
	b.dispatcher = NewDispatcher(b.handler)
	b.wg.Add(1)
	go b.expiryLoop()

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
		conn.Close()
		return
	}

	// 2. Validate it is CONNECT
	if pkt.Type() != packets.ConnectType {
		b.logger.Warn("Expected CONNECT packet, received different packet type",
			slog.String("remote_addr", conn.RemoteAddr().String()),
			slog.String("packet_type", pkt.String()))
		conn.Close()
		return
	}

	p, ok := pkt.(*v3.Connect)
	if !ok {
		b.logger.Error("Failed to parse CONNECT packet as v3",
			slog.String("remote_addr", conn.RemoteAddr().String()))
		conn.Close()
		return
	}

	clientID := p.ClientID
	cleanStart := p.CleanSession
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

	s, _, err := b.GetOrCreate(clientID, 4, opts) // v3.1.1 = version 4
	if err != nil {
		b.logger.Error("Failed to create session",
			slog.String("client_id", clientID),
			slog.String("error", err.Error()))
		conn.Close()
		return
	}

	// 3. Attach connection to session
	if err := s.Connect(conn); err != nil {
		b.logger.Error("Failed to attach connection to session",
			slog.String("client_id", clientID),
			slog.String("error", err.Error()))
		conn.Close()
		return
	}

	// 4. Send CONNACK
	if err := b.sendConnAck(conn); err != nil {
		b.logger.Error("Failed to send CONNACK",
			slog.String("client_id", clientID),
			slog.String("error", err.Error()))
		s.Disconnect(false)
		return
	}

	b.logger.Info("Client connected",
		slog.String("client_id", clientID),
		slog.String("remote_addr", conn.RemoteAddr().String()),
		slog.Bool("clean_start", cleanStart),
		slog.Uint64("keep_alive", uint64(keepAlive)))

	b.deliverOfflineMessages(s)

	b.runSession(s)
}

// runSession runs the main packet loop for a session.
func (b *Broker) runSession(s *session.Session) {
	for {
		pkt, err := s.ReadPacket()
		if err != nil {
			if err != io.EOF && err != session.ErrNotConnected {
				b.logger.Error("Failed to read packet from client",
					slog.String("client_id", s.ID),
					slog.String("error", err.Error()))
			}
			s.Disconnect(false)
			return
		}

		if err := b.dispatcher.Dispatch(s, pkt); err != nil {
			if err == io.EOF {
				return // Clean disconnect
			}
			b.logger.Error("Packet handler error",
				slog.String("client_id", s.ID),
				slog.String("error", err.Error()))
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

func (b *Broker) sendConnAck(conn core.Connection) error {
	ack := &v3.ConnAck{
		FixedHeader: packets.FixedHeader{PacketType: packets.ConnAckType},
		ReturnCode:  0,
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
	return s.WritePacket(pub)
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
