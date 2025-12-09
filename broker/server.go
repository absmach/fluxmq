package broker

import (
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/dborovcanin/mqtt/handlers"
	"github.com/dborovcanin/mqtt/packets"
	v3 "github.com/dborovcanin/mqtt/packets/v3"
	v5 "github.com/dborovcanin/mqtt/packets/v5"
	"github.com/dborovcanin/mqtt/session"
	"github.com/dborovcanin/mqtt/store"
	"github.com/dborovcanin/mqtt/store/memory"
)

// Server is the core MQTT broker.
type Server struct {
	// listeners tracks active Frontends
	listeners []Frontend
	mu        sync.Mutex

	// Session management
	sessionMgr *session.Manager

	// Routing
	router *Router

	// Storage
	store store.Store

	// Handlers
	handler    handlers.Handler
	dispatcher *handlers.Dispatcher
}

// NewServer creates a new broker instance.
func NewServer() *Server {
	st := memory.New()
	sessionMgr := session.NewManager(st)
	router := NewRouter()

	s := &Server{
		sessionMgr: sessionMgr,
		router:     router,
		store:      st,
	}

	// Create handler with dependencies
	s.handler = handlers.NewBrokerHandler(handlers.BrokerHandlerConfig{
		SessionManager: sessionMgr,
		Router:         s,     // Server implements handlers.Router
		Publisher:      s,     // Server implements handlers.Publisher
		Retained:       st.Retained(),
	})
	s.dispatcher = handlers.NewDispatcher(s.handler)

	return s
}

// AddFrontend registers and starts a new frontend.
func (s *Server) AddFrontend(f Frontend) error {
	s.mu.Lock()
	s.listeners = append(s.listeners, f)
	s.mu.Unlock()

	go func() {
		if err := f.Serve(s); err != nil {
			fmt.Printf("Frontend error: %v\n", err)
		}
	}()
	return nil
}

// HandleConnection handles a new incoming connection from a Frontend.
func (s *Server) HandleConnection(conn Connection) {
	// 1. Read CONNECT packet
	pkt, err := conn.ReadPacket()
	if err != nil {
		fmt.Printf("Server ReadPacket error: %v\n", err)
		conn.Close()
		return
	}

	// 2. Validate it is CONNECT
	if pkt.Type() != packets.ConnectType {
		fmt.Printf("Expected CONNECT, got %s\n", pkt.String())
		conn.Close()
		return
	}

	var clientID string
	var version byte
	var cleanStart bool
	var keepAlive uint16

	// Determine version and ClientID
	if p, ok := pkt.(*v5.Connect); ok {
		clientID = p.ClientID
		version = 5
		cleanStart = p.CleanStart
		keepAlive = p.KeepAlive
	} else if p, ok := pkt.(*v3.Connect); ok {
		clientID = p.ClientID
		version = 4
		cleanStart = p.CleanSession
		keepAlive = p.KeepAlive
	} else {
		fmt.Printf("Unknown CONNECT packet type\n")
		conn.Close()
		return
	}

	// 3. Get or create session
	opts := session.Options{
		CleanStart:     cleanStart,
		ReceiveMaximum: 65535,
		KeepAlive:      keepAlive,
	}

	sess, _, err := s.sessionMgr.GetOrCreate(clientID, version, opts)
	if err != nil {
		fmt.Printf("Session creation error: %v\n", err)
		conn.Close()
		return
	}

	// 4. Attach connection to session
	sessConn := &sessionConnection{conn: conn}
	if err := sess.Connect(sessConn); err != nil {
		fmt.Printf("Session connect error: %v\n", err)
		conn.Close()
		return
	}

	// 5. Send CONNACK
	if err := s.sendConnAck(conn, int(version)); err != nil {
		fmt.Printf("CONNACK error: %v\n", err)
		sess.Disconnect(false)
		return
	}

	fmt.Printf("Starting session for %s\n", clientID)

	// 6. Deliver any queued offline messages
	s.deliverOfflineMessages(sess)

	// 7. Run packet loop
	s.runSession(sess)
}

// runSession runs the main packet loop for a session.
func (s *Server) runSession(sess *session.Session) {
	for {
		pkt, err := sess.ReadPacket()
		if err != nil {
			if err != io.EOF && err != session.ErrNotConnected {
				fmt.Printf("Read error for %s: %v\n", sess.ID, err)
			}
			sess.Disconnect(false)
			return
		}

		if err := s.dispatcher.Dispatch(sess, pkt); err != nil {
			if err == io.EOF {
				return // Clean disconnect
			}
			fmt.Printf("Handler error for %s: %v\n", sess.ID, err)
			sess.Disconnect(false)
			return
		}
	}
}

// deliverOfflineMessages sends queued messages to reconnected client.
func (s *Server) deliverOfflineMessages(sess *session.Session) {
	msgs := s.sessionMgr.DrainOfflineQueue(sess.ID)
	for _, msg := range msgs {
		s.deliverToSession(sess, msg.Topic, msg.Payload, msg.QoS, msg.Retain)
	}
}

func (s *Server) sendConnAck(conn Connection, version int) error {
	if version == 5 {
		ack := &v5.ConnAck{
			FixedHeader: packets.FixedHeader{PacketType: packets.ConnAckType},
			ReasonCode:  0,
		}
		return conn.WritePacket(ack)
	}
	ack := &v3.ConnAck{
		FixedHeader: packets.FixedHeader{PacketType: packets.ConnAckType},
		ReturnCode:  0,
	}
	return conn.WritePacket(ack)
}

// --- handlers.Router implementation ---

// Subscribe adds a subscription.
func (s *Server) Subscribe(clientID string, filter string, qos byte, opts store.SubscribeOptions) error {
	s.router.Subscribe(filter, Subscription{SessionID: clientID, QoS: qos})
	return nil
}

// Unsubscribe removes a subscription.
func (s *Server) Unsubscribe(clientID string, filter string) error {
	// Router doesn't track by client, so this is a no-op for now
	// In a full implementation, we'd track subscriptions per client
	return nil
}

// Match returns all subscriptions matching a topic.
func (s *Server) Match(topic string) ([]*store.Subscription, error) {
	matched := s.router.Match(topic)
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

// Publish distributes a message to all matching subscribers.
func (s *Server) Publish(topic string, payload []byte, qos byte, retain bool, props map[string]string) error {
	matched := s.router.Match(topic)

	for _, sub := range matched {
		sess := s.sessionMgr.Get(sub.SessionID)
		if sess == nil {
			continue
		}

		// Downgrade QoS if needed
		deliverQoS := qos
		if sub.QoS < deliverQoS {
			deliverQoS = sub.QoS
		}

		if sess.IsConnected() {
			s.deliverToSession(sess, topic, payload, deliverQoS, false)
		} else if deliverQoS > 0 {
			// Queue for offline delivery
			msg := &store.Message{
				Topic:   topic,
				Payload: payload,
				QoS:     deliverQoS,
				Retain:  false,
			}
			sess.OfflineQueue.Enqueue(msg)
		}
	}

	return nil
}

// deliverToSession sends a message to a connected session.
func (s *Server) deliverToSession(sess *session.Session, topic string, payload []byte, qos byte, retain bool) error {
	if sess.Version == 5 {
		pub := &v5.Publish{
			FixedHeader: packets.FixedHeader{
				PacketType: packets.PublishType,
				QoS:        qos,
				Retain:     retain,
			},
			TopicName: topic,
			Payload:   payload,
		}
		if qos > 0 {
			pub.ID = sess.NextPacketID()
			msg := &store.Message{
				Topic:    topic,
				Payload:  payload,
				QoS:      qos,
				PacketID: pub.ID,
			}
			sess.Inflight.Add(pub.ID, msg, session.Outbound)
		}
		return sess.WritePacket(pub)
	}

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
		pub.ID = sess.NextPacketID()
		msg := &store.Message{
			Topic:    topic,
			Payload:  payload,
			QoS:      qos,
			PacketID: pub.ID,
		}
		sess.Inflight.Add(pub.ID, msg, session.Outbound)
	}
	return sess.WritePacket(pub)
}

// Close shuts down the server.
func (s *Server) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, l := range s.listeners {
		l.Close()
	}

	s.sessionMgr.Close()
	s.store.Close()

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
