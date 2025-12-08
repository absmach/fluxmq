package broker

import (
	"fmt"
	"sync"

	packets "github.com/dborovcanin/mqtt/packets"
	v3 "github.com/dborovcanin/mqtt/packets/v3"
	v5 "github.com/dborovcanin/mqtt/packets/v5"
	"github.com/dborovcanin/mqtt/session"
)

// Server is the core MQTT broker.
type Server struct {
	// listeners tracks active Frontends
	listeners []Frontend
	mu        sync.Mutex

	// sessions maps ClientID to Session
	sessions   map[string]*session.Session // Changed from *clientSession
	sessionsMu sync.RWMutex

	router *Router
}

// NewServer creates a new broker instance.
func NewServer() *Server {
	return &Server{
		sessions: make(map[string]*session.Session),
		router:   NewRouter(),
	}
}

// AddFrontend registers and starts a new frontend.
func (s *Server) AddFrontend(f Frontend) error {
	s.mu.Lock()
	s.listeners = append(s.listeners, f)
	s.mu.Unlock()

	// Start serving in a goroutine
	// Note: In a real app we might want to manage lifecycle better (WaitGroups etc)
	go func() {
		if err := f.Serve(s); err != nil {
			// Log error?
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
		fmt.Printf("Server ReadPacket error: %v\n", err) // DEBUG
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
	var version int

	// Determine version and ClientID
	if p, ok := pkt.(*v5.Connect); ok {
		clientID = p.ClientID
		version = 5
	} else if p, ok := pkt.(*v3.Connect); ok {
		clientID = p.ClientID
		version = 4
	} else {
		fmt.Printf("Unknown CONNECT packet type\n")
		conn.Close()
		return
	}

	// 3. Create Session
	session := session.New(s, conn, clientID, version)

	s.sessionsMu.Lock()
	s.sessions[clientID] = session
	s.sessionsMu.Unlock()

	// 4. Send CONNACK
	if err := s.sendConnAck(conn, version); err != nil {
		fmt.Printf("CONNACK error: %v\n", err)
		session.Close()
		return
	}

	// 5. Start Loop
	fmt.Printf("Starting session for %s\n", clientID)
	session.Start()

	// Cleanup on exit
	s.sessionsMu.Lock()
	delete(s.sessions, clientID)
	s.sessionsMu.Unlock()
}

func (s *Server) sendConnAck(conn Connection, version int) error {
	if version == 5 {
		ack := &v5.ConnAck{
			FixedHeader: packets.FixedHeader{PacketType: packets.ConnAckType},
			ReasonCode:  0, // Success
		}
		return conn.WritePacket(ack)
	} else {
		ack := &v3.ConnAck{
			FixedHeader: packets.FixedHeader{PacketType: packets.ConnAckType},
			ReturnCode:  0, // Accepted
		}
		return conn.WritePacket(ack)
	}
}

// Subscribe subscribes a session to a topic.
// This matches the session.BrokerInterface.
func (s *Server) Subscribe(topic string, sessionID string, qos byte) {
	s.router.Subscribe(topic, Subscription{SessionID: sessionID, QoS: qos})
}

// Distribute sends a message to all subscribers.
func (s *Server) Distribute(topic string, payload []byte) {
	matched := s.router.Match(topic)

	// Simple fan-out
	s.sessionsMu.RLock()
	defer s.sessionsMu.RUnlock()

	for _, sub := range matched {
		if session, ok := s.sessions[sub.SessionID]; ok {
			session.Deliver(topic, payload)
		}
	}
}
