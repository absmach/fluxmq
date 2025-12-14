package broker

import (
	"context"
	"fmt"
	"io"
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

	dispatcher *Dispatcher
	auth       *AuthEngine
	stats      *Stats

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

	handlerV3 := NewV3Handler(V3HandlerConfig{
		Broker: b,
	})
	handlerV5 := NewV5Handler(V5HandlerConfig{
		Broker: b,
	})
	b.dispatcher = NewVersionAwareDispatcher(handlerV3, handlerV5)
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
		b.stats.IncrementPacketErrors()
		conn.Close()
		return
	}

	// 2. Validate it is CONNECT
	if pkt.Type() != packets.ConnectType {
		b.stats.IncrementProtocolErrors()
		conn.Close()
		return
	}

	p3, ok := pkt.(*v3.Connect)
	if ok {
		if p3.ProtocolVersion != 3 && p3.ProtocolVersion != 4 {
			b.stats.IncrementProtocolErrors()
			b.sendConnAck(conn, false, 0x01)
			conn.Close()
			return
		}
		b.handleV3Connect(conn, p3)
		return
	}

	p5, ok := pkt.(*v5.Connect)
	if ok {
		if p5.ProtocolVersion != 5 {
			b.stats.IncrementProtocolErrors()
			b.sendV5ConnAck(conn, false, 0x84)
			conn.Close()
			return
		}
		b.handleV5Connect(conn, p5)
		return
	}

	b.stats.IncrementProtocolErrors()
	conn.Close()
}

// handleV3Connect handles v3.1.1 CONNECT packets.
func (b *Broker) handleV3Connect(conn core.Connection, p *v3.Connect) {
	clientID := p.ClientID
	cleanStart := p.CleanSession

	if clientID == "" {
		if cleanStart {
			generated, err := GenerateClientID()
			if err != nil {
				b.stats.IncrementProtocolErrors()
				b.sendConnAck(conn, false, 0x02)
				conn.Close()
				return
			}
			clientID = generated
		} else {
			b.stats.IncrementProtocolErrors()
			b.sendConnAck(conn, false, 0x02)
			conn.Close()
			return
		}
	}

	if b.auth != nil {
		username := p.Username
		password := string(p.Password)

		authenticated, err := b.auth.Authenticate(clientID, username, password)
		if err != nil {
			b.stats.IncrementAuthErrors()
			b.sendConnAck(conn, false, 0x04)
			conn.Close()
			return
		}

		if !authenticated {
			b.stats.IncrementAuthErrors()
			b.sendConnAck(conn, false, 0x04)
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

	s, isNew, err := b.getOrCreate(clientID, p.ProtocolVersion, opts)
	if err != nil {
		b.stats.IncrementProtocolErrors()
		b.sendConnAck(conn, false, 0x03)
		conn.Close()
		return
	}

	if err := s.Connect(conn); err != nil {
		b.stats.IncrementProtocolErrors()
		conn.Close()
		return
	}

	sessionPresent := !isNew && !cleanStart
	if err := b.sendConnAck(conn, sessionPresent, 0x00); err != nil {
		s.Disconnect(false)
		return
	}

	b.stats.IncrementConnections()

	b.deliverOfflineMessages(s)

	b.runSession(s)
}

// handleV5Connect handles v5.0 CONNECT packets.
func (b *Broker) handleV5Connect(conn core.Connection, p *v5.Connect) {
	clientID := p.ClientID
	cleanStart := p.CleanStart

	if clientID == "" {
		if cleanStart {
			generated, err := GenerateClientID()
			if err != nil {
				b.stats.IncrementProtocolErrors()
				b.sendV5ConnAck(conn, false, 0x85)
				conn.Close()
				return
			}
			clientID = generated
		} else {
			b.stats.IncrementProtocolErrors()
			b.sendV5ConnAck(conn, false, 0x85)
			conn.Close()
			return
		}
	}

	if b.auth != nil {
		username := p.Username
		password := string(p.Password)

		authenticated, err := b.auth.Authenticate(clientID, username, password)
		if err != nil {
			b.stats.IncrementAuthErrors()
			b.sendV5ConnAck(conn, false, 0x86)
			conn.Close()
			return
		}

		if !authenticated {
			b.stats.IncrementAuthErrors()
			b.sendV5ConnAck(conn, false, 0x86)
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
			Payload:  p.WillPayload,
			QoS:      p.WillQoS,
			Retain:   p.WillRetain,
		}
	}

	opts := session.Options{
		CleanStart:     cleanStart,
		ReceiveMaximum: 65535,
		TopicAliasMax:  10,
		KeepAlive:      keepAlive,
		Will:           will,
	}

	if p.Properties != nil {
		if p.Properties.SessionExpiryInterval != nil {
			opts.ExpiryInterval = *p.Properties.SessionExpiryInterval
		}
		if p.Properties.ReceiveMaximum != nil {
			opts.ReceiveMaximum = *p.Properties.ReceiveMaximum
		}
		if p.Properties.MaximumPacketSize != nil {
			opts.MaxPacketSize = *p.Properties.MaximumPacketSize
		}
		if p.Properties.TopicAliasMaximum != nil {
			opts.TopicAliasMax = *p.Properties.TopicAliasMaximum
		}
	}

	s, isNew, err := b.getOrCreate(clientID, p.ProtocolVersion, opts)
	if err != nil {
		b.stats.IncrementProtocolErrors()
		b.sendV5ConnAck(conn, false, 0x80)
		conn.Close()
		return
	}

	if p.Properties != nil {
		if p.Properties.RequestResponseInfo != nil && *p.Properties.RequestResponseInfo == 1 {
			s.SetRequestResponseInfo(true)
		}
		if p.Properties.RequestProblemInfo != nil && *p.Properties.RequestProblemInfo == 0 {
			s.SetRequestProblemInfo(false)
		}
	}

	if err := s.Connect(conn); err != nil {
		b.stats.IncrementProtocolErrors()
		conn.Close()
		return
	}

	sessionPresent := !isNew && !cleanStart
	if err := b.sendV5ConnAckWithProperties(conn, s, sessionPresent, 0x00); err != nil {
		s.Disconnect(false)
		return
	}

	b.stats.IncrementConnections()

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
		}
	}

	for {
		pkt, err := s.ReadPacket()
		if err != nil {
			if err != io.EOF && err != session.ErrNotConnected {
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
			}
		}

		b.stats.IncrementMessagesReceived()

		if err := b.dispatcher.Dispatch(s, pkt); err != nil {
			if err == io.EOF {
				b.stats.DecrementConnections()
				return // Clean disconnect
			}
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

func (b *Broker) sendV5ConnAck(conn core.Connection, sessionPresent bool, reasonCode byte) error {
	ack := &v5.ConnAck{
		FixedHeader:    packets.FixedHeader{PacketType: packets.ConnAckType},
		SessionPresent: sessionPresent,
		ReasonCode:     reasonCode,
		Properties:     &v5.ConnAckProperties{},
	}
	return conn.WritePacket(ack)
}

func (b *Broker) sendV5ConnAckWithProperties(conn core.Connection, s *session.Session, sessionPresent bool, reasonCode byte) error {
	receiveMax := uint16(65535)
	topicAliasMax := s.TopicAliasMax
	retainAvailable := byte(1)
	wildcardSubAvailable := byte(1)
	subIDAvailable := byte(1)

	ack := &v5.ConnAck{
		FixedHeader:    packets.FixedHeader{PacketType: packets.ConnAckType},
		SessionPresent: sessionPresent,
		ReasonCode:     reasonCode,
		Properties: &v5.ConnAckProperties{
			ReceiveMax:           &receiveMax,
			TopicAliasMax:        &topicAliasMax,
			RetainAvailable:      &retainAvailable,
			WildcardSubAvailable: &wildcardSubAvailable,
			SubIDAvailable:       &subIDAvailable,
		},
	}

	if s.ExpiryInterval > 0 {
		ack.Properties.SessionExpiryInterval = &s.ExpiryInterval
	}

	if s.MaxPacketSize > 0 {
		ack.Properties.MaximumPacketSize = &s.MaxPacketSize
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
