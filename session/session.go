package session

import (
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dborovcanin/mqtt/packets"
	v3 "github.com/dborovcanin/mqtt/packets/v3"
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

// Connection represents a network connection that can read/write MQTT packets.
type Connection interface {
	ReadPacket() (packets.ControlPacket, error)
	WritePacket(p packets.ControlPacket) error
	Close() error
	RemoteAddr() net.Addr
	SetReadDeadline(t time.Time) error
	SetWriteDeadline(t time.Time) error
}

// Session represents an MQTT client session with full state management.
type Session struct {
	mu sync.RWMutex

	// Identity
	ID      string // Client ID
	Version byte   // MQTT version (3=3.1, 4=3.1.1, 5=5.0)

	// Connection (nil when disconnected)
	conn Connection

	// State
	state          State
	connectedAt    time.Time
	disconnectedAt time.Time

	// MQTT options from CONNECT
	CleanStart     bool
	ExpiryInterval uint32 // Session expiry in seconds (v5)
	ReceiveMaximum uint16 // Max inflight (v5), default 65535
	MaxPacketSize  uint32 // Max packet size (v5), default unlimited
	TopicAliasMax  uint16 // Max topic aliases (v5)
	KeepAlive      uint16 // Keep-alive in seconds

	// Will message (set on CONNECT, cleared on clean disconnect)
	Will *store.WillMessage

	// QoS tracking
	Inflight     *InflightTracker // Outgoing QoS 1/2 messages
	OfflineQueue *MessageQueue    // Messages for disconnected client

	// Packet ID generator
	nextPacketID uint32

	// Subscriptions (cached from store for fast lookup)
	subscriptions map[string]store.SubscribeOptions

	// Keep-alive timer
	keepAliveTimer  *time.Timer
	lastActivity    time.Time
	keepAliveExpiry time.Duration

	// Topic aliases (v5) - bidirectional mapping
	outboundAliases map[string]uint16 // topic -> alias (for sending)
	inboundAliases  map[uint16]string // alias -> topic (for receiving)

	// Callbacks
	onDisconnect func(s *Session, graceful bool)

	// Background tasks
	stopCh chan struct{}
	wg     sync.WaitGroup
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

// New creates a new session.
func New(clientID string, version byte, opts Options) *Session {
	receiveMax := opts.ReceiveMaximum
	if receiveMax == 0 {
		receiveMax = 65535
	}

	s := &Session{
		ID:              clientID,
		Version:         version,
		state:           StateNew,
		CleanStart:      opts.CleanStart,
		ExpiryInterval:  opts.ExpiryInterval,
		ReceiveMaximum:  receiveMax,
		MaxPacketSize:   opts.MaxPacketSize,
		TopicAliasMax:   opts.TopicAliasMax,
		KeepAlive:       opts.KeepAlive,
		Will:            opts.Will,
		Inflight:        NewInflightTracker(int(receiveMax)),
		OfflineQueue:    NewMessageQueue(1000), // Default max queue size
		subscriptions:   make(map[string]store.SubscribeOptions),
		outboundAliases: make(map[string]uint16),
		inboundAliases:  make(map[uint16]string),
		lastActivity:    time.Now(),
		stopCh:          make(chan struct{}),
	}

	if opts.KeepAlive > 0 {
		s.keepAliveExpiry = time.Duration(opts.KeepAlive) * time.Second * 3 / 2 // 1.5x per spec
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
	s.lastActivity = time.Now()

	// Start keep-alive timer if enabled
	if s.keepAliveExpiry > 0 {
		s.startKeepAliveTimer()
	}

	// Start retry loop
	s.wg.Add(1)
	go s.retryLoop()

	return nil
}

// Disconnect disconnects the session.
func (s *Session) Disconnect(graceful bool) error {
	s.mu.Lock()

	if s.state != StateConnected {
		s.mu.Unlock()
		return nil
	}

	s.state = StateDisconnecting
	s.cleanupConnectionResources(graceful)
	callback := s.onDisconnect

	s.mu.Unlock()

	// Wait for background tasks without holding lock to avoid deadlock
	s.wg.Wait()

	// Reset stopCh for potential reconnection
	s.mu.Lock()
	s.stopCh = make(chan struct{})
	s.mu.Unlock()

	if callback != nil {
		go callback(s, graceful)
	}

	return nil
}

// cleanupConnectionResources cleans up resources when disconnecting.
// Must be called with s.mu held.
func (s *Session) cleanupConnectionResources(graceful bool) {
	if s.keepAliveTimer != nil {
		s.keepAliveTimer.Stop()
		s.keepAliveTimer = nil
	}

	if s.conn != nil {
		s.conn.Close()
		s.conn = nil
	}

	s.state = StateDisconnected
	s.disconnectedAt = time.Now()

	if graceful {
		s.Will = nil
	}

	s.outboundAliases = make(map[string]uint16)
	s.inboundAliases = make(map[uint16]string)

	close(s.stopCh)
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

// NextPacketID generates the next packet ID.
func (s *Session) NextPacketID() uint16 {
	for {
		id := atomic.AddUint32(&s.nextPacketID, 1)
		id16 := uint16(id & 0xFFFF)
		if id16 == 0 {
			continue // Packet ID 0 is reserved
		}
		// Check if ID is already in use
		if !s.Inflight.Has(id16) {
			return id16
		}
	}
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
	s.mu.Lock()
	s.lastActivity = time.Now()
	s.mu.Unlock()
}

// startKeepAliveTimer starts the keep-alive timer.
// Must be called with mu held.
func (s *Session) startKeepAliveTimer() {
	if s.keepAliveTimer != nil {
		s.keepAliveTimer.Stop()
	}

	s.keepAliveTimer = time.AfterFunc(s.keepAliveExpiry, func() {
		s.checkKeepAlive()
	})
}

// checkKeepAlive checks if keep-alive has expired.
func (s *Session) checkKeepAlive() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.state != StateConnected {
		return
	}

	elapsed := time.Since(s.lastActivity)
	if elapsed >= s.keepAliveExpiry {
		// Keep-alive expired, disconnect
		s.state = StateDisconnecting
		if s.conn != nil {
			s.conn.Close()
			s.conn = nil
		}
		s.state = StateDisconnected
		s.disconnectedAt = time.Now()

		if s.onDisconnect != nil {
			go s.onDisconnect(s, false)
		}
		return
	}

	// Reschedule timer
	remaining := s.keepAliveExpiry - elapsed
	s.keepAliveTimer = time.AfterFunc(remaining, func() {
		s.checkKeepAlive()
	})
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
	s.mu.Lock()
	defer s.mu.Unlock()
	s.outboundAliases[topic] = alias
}

// GetTopicAlias returns the alias for a topic (outbound).
func (s *Session) GetTopicAlias(topic string) (uint16, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	alias, ok := s.outboundAliases[topic]
	return alias, ok
}

// SetInboundAlias sets an inbound topic alias.
func (s *Session) SetInboundAlias(alias uint16, topic string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.inboundAliases[alias] = topic
}

// ResolveInboundAlias resolves an inbound alias to a topic.
func (s *Session) ResolveInboundAlias(alias uint16) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	topic, ok := s.inboundAliases[alias]
	return topic, ok
}

// UpdateConnectionOptions updates session options during reconnection.
// Must be called before Connect.
func (s *Session) UpdateConnectionOptions(version byte, keepAlive uint16, will *store.WillMessage) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.Version = version
	s.KeepAlive = keepAlive
	s.Will = will

	if keepAlive > 0 {
		s.keepAliveExpiry = time.Duration(keepAlive) * time.Second * 3 / 2
	} else {
		s.keepAliveExpiry = 0
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
		RequestResponse: false, // TODO: from CONNECT
		RequestProblem:  true,  // Default
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

// retryLoop periodically checks for expired inflight messages and resends them.
func (s *Session) retryLoop() {
	defer s.wg.Done()

	ticker := time.NewTicker(1 * time.Second) // Check every second
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			// Get expired messages (20s timeout)
			expired := s.Inflight.GetExpired(20 * time.Second)
			for _, inflight := range expired {
				if err := s.resendMessage(inflight); err != nil {
					// If write fails, we'll try again next tick
					continue
				}
				s.Inflight.MarkRetry(inflight.PacketID)
			}
		case <-s.stopCh:
			return
		}
	}
}

func (s *Session) resendMessage(inflight *InflightMessage) error {
	msg := inflight.Message

	pub := &v3.Publish{
		FixedHeader: packets.FixedHeader{
			PacketType: packets.PublishType,
			QoS:        msg.QoS,
			Retain:     msg.Retain,
			Dup:        true,
		},
		TopicName: msg.Topic,
		Payload:   msg.Payload,
		ID:        inflight.PacketID,
	}

	return s.WritePacket(pub)
}
