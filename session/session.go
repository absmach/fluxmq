package session

import (
	"sync"
	"time"

	"github.com/dborovcanin/mqtt/packets"
	"github.com/dborovcanin/mqtt/store"
)

// State represents the session state.
type State int

const (
	StateNew State = iota
	StateConnecting
	StateConnected
	StateDisconnecting
	StateDisconnected
)

func (s State) String() string {
	switch s {
	case StateNew:
		return "new"
	case StateConnecting:
		return "connecting"
	case StateConnected:
		return "connected"
	case StateDisconnecting:
		return "disconnecting"
	case StateDisconnected:
		return "disconnected"
	default:
		return "unknown"
	}
}

// Session represents an MQTT client session with full state management.
type Session struct {
	mu sync.RWMutex

	ID      string
	Version byte // MQTT version (3=3.1, 4=3.1.1, 5=5.0)

	conn       Connection
	msgHandler *MessageHandler

	state          State
	connectedAt    time.Time
	disconnectedAt time.Time

	CleanStart     bool
	ExpiryInterval uint32 // Session expiry in seconds (v5)
	ReceiveMaximum uint16 // Max inflight (v5), default 65535
	MaxPacketSize  uint32 // Max packet size (v5), default unlimited
	TopicAliasMax  uint16 // Max topic aliases (v5)
	KeepAlive      uint16 // Keep-alive in seconds

	Will *store.WillMessage // set on CONNECT, cleared on clean disconnect

	subscriptions map[string]store.SubscribeOptions // cached from store for fast lookup

	onDisconnect func(s *Session, graceful bool)
}

// Options holds options for creating a new session.
type Options struct {
	CleanStart     bool
	ExpiryInterval uint32
	ReceiveMaximum uint16
	MaxPacketSize  uint32
	TopicAliasMax  uint16
	KeepAlive      uint16
	Will           *store.WillMessage
}

// DefaultOptions returns default session options.
func DefaultOptions() Options {
	return Options{
		CleanStart:     true,
		ExpiryInterval: 0,
		ReceiveMaximum: 65535,
		MaxPacketSize:  0, // Unlimited
		TopicAliasMax:  0,
		KeepAlive:      60,
	}
}

// New creates a new session with injected dependencies.
// The inflight tracker and offline queue should be created and restored by the Manager.
func New(clientID string, version byte, opts Options, inflight *inflightTracker, offlineQueue *messageQueue) *Session {
	receiveMax := opts.ReceiveMaximum
	if receiveMax == 0 {
		receiveMax = 65535
	}

	msgHandler := NewMessageHandler(inflight, offlineQueue)

	s := &Session{
		ID:             clientID,
		Version:        version,
		state:          StateNew,
		msgHandler:     msgHandler,
		CleanStart:     opts.CleanStart,
		ExpiryInterval: opts.ExpiryInterval,
		ReceiveMaximum: receiveMax,
		MaxPacketSize:  opts.MaxPacketSize,
		TopicAliasMax:  opts.TopicAliasMax,
		KeepAlive:      opts.KeepAlive,
		Will:           opts.Will,
		subscriptions:  make(map[string]store.SubscribeOptions),
	}

	return s
}

// Connect attaches a connection to the session.
func (s *Session) Connect(conn Connection) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.conn = conn
	s.state = StateConnected
	s.connectedAt = time.Now()

	conn.SetKeepAlive(s.KeepAlive)

	// Set callback to handle connection loss/keepalive expiry
	conn.SetOnDisconnect(func(graceful bool) {
		s.Disconnect(graceful)
	})

	// Start retry loop using the connection as the packet writer
	s.msgHandler.StartRetryLoop(conn)

	return nil
}

// Disconnect disconnects the session.
func (s *Session) Disconnect(graceful bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.state != StateConnected {
		return nil
	}

	s.state = StateDisconnecting

	if s.conn != nil {
		// Cache info before closing
		s.disconnectedAt = time.Now()
		// If connection tracks its own timestamps, we might want to sync,
		// but since we are closing it here, "now" is correct.

		s.conn.Close()
		s.conn = nil
	}
	s.state = StateDisconnected

	// MessageHandler stops background tasks
	s.msgHandler.Stop()

	if graceful {
		s.Will = nil
	}

	s.msgHandler.ClearAliases()

	callback := s.onDisconnect
	if callback != nil {
		go callback(s, graceful)
	}

	return nil
}

// State returns the current session state.
func (s *Session) State() State {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.state
}

// IsConnected returns true if the session has an active connection.
func (s *Session) IsConnected() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.state == StateConnected && s.conn != nil
}

// Conn returns the current connection (may be nil).
func (s *Session) Conn() Connection {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.conn
}

// Inflight returns the inflight message tracker for operations.
func (s *Session) Inflight() InflightOps {
	return s.msgHandler.Inflight()
}

// OfflineQueue returns the offline message queue for operations.
func (s *Session) OfflineQueue() QueueOps {
	return s.msgHandler.OfflineQueue()
}

// InflightSnapshot returns the inflight tracker for persistence/inspection.
func (s *Session) InflightSnapshot() InflightSnapshot {
	return s.msgHandler.Inflight()
}

// QueueSnapshot returns the queue for persistence operations.
func (s *Session) QueueSnapshot() QueueSnapshot {
	return s.msgHandler.OfflineQueue()
}

// NextPacketID generates the next packet ID.
func (s *Session) NextPacketID() uint16 {
	return s.msgHandler.NextPacketID()
}

// WritePacket writes a packet to the connection.
func (s *Session) WritePacket(pkt packets.ControlPacket) error {
	s.mu.RLock()
	conn := s.conn
	s.mu.RUnlock()

	if conn == nil {
		return ErrNotConnected
	}
	return conn.WritePacket(pkt)
}

// ReadPacket reads a packet from the connection.
func (s *Session) ReadPacket() (packets.ControlPacket, error) {
	s.mu.RLock()
	conn := s.conn
	s.mu.RUnlock()

	if conn == nil {
		return nil, ErrNotConnected
	}
	return conn.ReadPacket()
}

// TouchActivity updates the last activity timestamp.
func (s *Session) TouchActivity() {
	s.mu.RLock()
	conn := s.conn
	s.mu.RUnlock()

	if conn != nil {
		conn.TouchActivity()
	}
}

// SetOnDisconnect sets the disconnect callback.
func (s *Session) SetOnDisconnect(fn func(*Session, bool)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.onDisconnect = fn
}

// AddSubscription adds a subscription to the cache.
func (s *Session) AddSubscription(filter string, opts store.SubscribeOptions) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.subscriptions[filter] = opts
}

// RemoveSubscription removes a subscription from the cache.
func (s *Session) RemoveSubscription(filter string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.subscriptions, filter)
}

// GetSubscriptions returns all subscriptions.
func (s *Session) GetSubscriptions() map[string]store.SubscribeOptions {
	s.mu.RLock()
	defer s.mu.RUnlock()

	result := make(map[string]store.SubscribeOptions, len(s.subscriptions))
	for k, v := range s.subscriptions {
		result[k] = v
	}
	return result
}

// SetTopicAlias sets a topic alias for outbound use.
func (s *Session) SetTopicAlias(topic string, alias uint16) {
	s.msgHandler.SetTopicAlias(topic, alias)
}

// GetTopicAlias returns the alias for a topic (outbound).
func (s *Session) GetTopicAlias(topic string) (uint16, bool) {
	return s.msgHandler.GetTopicAlias(topic)
}

// SetInboundAlias sets an inbound topic alias.
func (s *Session) SetInboundAlias(alias uint16, topic string) {
	s.msgHandler.SetInboundAlias(alias, topic)
}

// ResolveInboundAlias resolves an inbound alias to a topic.
func (s *Session) ResolveInboundAlias(alias uint16) (string, bool) {
	return s.msgHandler.ResolveInboundAlias(alias)
}

// UpdateConnectionOptions updates session options during reconnection.
// Must be called before Connect.
func (s *Session) UpdateConnectionOptions(version byte, keepAlive uint16, will *store.WillMessage) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.Version = version
	s.KeepAlive = keepAlive
	s.Will = will

	if s.conn != nil {
		s.conn.SetKeepAlive(keepAlive)
	}
}

// GetWill returns a copy of the will message.
func (s *Session) GetWill() *store.WillMessage {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.Will
}

// Info returns session info for persistence.
func (s *Session) Info() *store.Session {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return &store.Session{
		ClientID:        s.ID,
		Version:         s.Version,
		CleanStart:      s.CleanStart,
		ExpiryInterval:  s.ExpiryInterval,
		ConnectedAt:     s.connectedAt,
		DisconnectedAt:  s.disconnectedAt,
		Connected:       s.state == StateConnected,
		ReceiveMaximum:  s.ReceiveMaximum,
		MaxPacketSize:   s.MaxPacketSize,
		TopicAliasMax:   s.TopicAliasMax,
		RequestResponse: false,
		RequestProblem:  true,
	}
}

// RestoreFrom restores session state from persistence.
func (s *Session) RestoreFrom(stored *store.Session) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.ExpiryInterval = stored.ExpiryInterval
	s.ReceiveMaximum = stored.ReceiveMaximum
	s.MaxPacketSize = stored.MaxPacketSize
	s.TopicAliasMax = stored.TopicAliasMax
}
