package session

import (
	"sync"
	"time"

	"github.com/absmach/mqtt/core"
	"github.com/absmach/mqtt/core/packets"
	"github.com/absmach/mqtt/storage"
	"github.com/absmach/mqtt/storage/messages"
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
	mu             sync.RWMutex
	connectedAt    time.Time
	disconnectedAt time.Time

	ID                   string
	authMethod           string
	authState            any
	conn                 core.Connection
	msgHandler           *messages.MessageHandler
	Will                 *storage.WillMessage
	subscriptions        map[string]storage.SubscribeOptions
	subscriptionIDs      map[string][]uint32
	onDisconnect         func(s *Session, graceful bool)
	KeepAlive            time.Duration
	state                State
	ExpiryInterval       uint32
	MaxPacketSize        uint32
	ReceiveMaximum       uint16
	TopicAliasMax        uint16
	Version              byte
	maxQoS               byte
	CleanStart           bool
	requestResponseInfo  bool
	requestProblemInfo   bool
	retainAvailable      bool
	wildcardSubAvailable bool
	sharedSubAvailable   bool
}

// Options holds options for creating a new session.
type Options struct {
	KeepAlive      time.Duration
	Will           *storage.WillMessage
	ExpiryInterval uint32
	MaxPacketSize  uint32
	ReceiveMaximum uint16
	TopicAliasMax  uint16
	CleanStart     bool
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
func New(clientID string, version byte, opts Options, inflight messages.Inflight, offlineQueue messages.Queue) *Session {
	receiveMax := opts.ReceiveMaximum
	if receiveMax == 0 {
		receiveMax = 65535
	}

	msgHandler := messages.NewMessageHandler(inflight, offlineQueue)

	s := &Session{
		ID:                   clientID,
		Version:              version,
		state:                StateNew,
		msgHandler:           msgHandler,
		CleanStart:           opts.CleanStart,
		ExpiryInterval:       opts.ExpiryInterval,
		ReceiveMaximum:       receiveMax,
		MaxPacketSize:        opts.MaxPacketSize,
		TopicAliasMax:        opts.TopicAliasMax,
		KeepAlive:            opts.KeepAlive,
		Will:                 opts.Will,
		subscriptions:        make(map[string]storage.SubscribeOptions),
		subscriptionIDs:      make(map[string][]uint32),
		retainAvailable:      true,
		wildcardSubAvailable: true,
		sharedSubAvailable:   true,
		maxQoS:               2,
		requestProblemInfo:   true,
	}

	return s
}

// Connect attaches a connection to the session.
func (s *Session) Connect(c core.Connection) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.conn = c
	s.state = StateConnected
	s.connectedAt = time.Now()

	c.SetKeepAlive(s.KeepAlive)

	// Set callback to handle connection loss/keepalive expiry
	c.SetOnDisconnect(func(graceful bool) {
		s.Disconnect(graceful)
	})

	// Start retry loop using the connection as the packet writer
	s.msgHandler.StartRetryLoop(c)

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
func (s *Session) Conn() core.Connection {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.conn
}

// Inflight returns the inflight message tracker for operations.
func (s *Session) Inflight() messages.Inflight {
	return s.msgHandler.Inflight()
}

// OfflineQueue returns the offline message queue for operations.
func (s *Session) OfflineQueue() messages.Queue {
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

// Touch updates the last activity timestamp.
func (s *Session) Touch() {
	s.mu.RLock()
	conn := s.conn
	s.mu.RUnlock()

	if conn != nil {
		conn.Touch()
	}
}

// SetOnDisconnect sets the disconnect callback.
func (s *Session) SetOnDisconnect(fn func(*Session, bool)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.onDisconnect = fn
}

// AddSubscription adds a subscription to the cache.
func (s *Session) AddSubscription(filter string, opts storage.SubscribeOptions) {
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
func (s *Session) GetSubscriptions() map[string]storage.SubscribeOptions {
	s.mu.RLock()
	defer s.mu.RUnlock()

	result := make(map[string]storage.SubscribeOptions, len(s.subscriptions))
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
func (s *Session) UpdateConnectionOptions(version byte, keepAlive time.Duration, will *storage.WillMessage) {
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
func (s *Session) GetWill() *storage.WillMessage {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.Will
}

// Info returns session info for persistence.
func (s *Session) Info() *storage.Session {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return &storage.Session{
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
		RequestResponse: s.requestResponseInfo,
		RequestProblem:  s.requestProblemInfo,
	}
}

// RestoreFrom restores session state from persistence.
func (s *Session) RestoreFrom(stored *storage.Session) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.ExpiryInterval = stored.ExpiryInterval
	s.ReceiveMaximum = stored.ReceiveMaximum
	s.MaxPacketSize = stored.MaxPacketSize
	s.TopicAliasMax = stored.TopicAliasMax
	s.requestResponseInfo = stored.RequestResponse
	s.requestProblemInfo = stored.RequestProblem
}

// AddSubscriptionID adds a subscription ID for a filter.
func (s *Session) AddSubscriptionID(filter string, id uint32) {
	s.mu.Lock()
	defer s.mu.Unlock()

	ids := s.subscriptionIDs[filter]
	for _, existing := range ids {
		if existing == id {
			return
		}
	}
	s.subscriptionIDs[filter] = append(ids, id)
}

// GetSubscriptionIDs returns subscription IDs for a filter.
func (s *Session) GetSubscriptionIDs(filter string) []uint32 {
	s.mu.RLock()
	defer s.mu.RUnlock()

	ids := s.subscriptionIDs[filter]
	if len(ids) == 0 {
		return nil
	}

	result := make([]uint32, len(ids))
	copy(result, ids)
	return result
}

// RemoveSubscriptionID removes subscription IDs for a filter.
func (s *Session) RemoveSubscriptionID(filter string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.subscriptionIDs, filter)
}

// UpdateSessionExpiry updates the session expiry interval.
func (s *Session) UpdateSessionExpiry(interval uint32) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ExpiryInterval = interval
}

// SetAuthState sets enhanced auth state.
func (s *Session) SetAuthState(method string, state any) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.authMethod = method
	s.authState = state
}

// GetAuthState returns the enhanced auth state.
func (s *Session) GetAuthState() (string, any) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.authMethod, s.authState
}

// SetRequestResponseInfo sets whether client requested response info.
func (s *Session) SetRequestResponseInfo(requested bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.requestResponseInfo = requested
}

// RequestsResponseInfo returns whether client requested response info.
func (s *Session) RequestsResponseInfo() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.requestResponseInfo
}

// SetRequestProblemInfo sets whether client wants detailed errors.
func (s *Session) SetRequestProblemInfo(requested bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.requestProblemInfo = requested
}

// RequestsProblemInfo returns whether client wants detailed errors.
func (s *Session) RequestsProblemInfo() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.requestProblemInfo
}

// ServerCapabilities returns server capability flags.
func (s *Session) ServerCapabilities() (maxQoS byte, retainAvailable, wildcardSubAvailable, sharedSubAvailable bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.maxQoS, s.retainAvailable, s.wildcardSubAvailable, s.sharedSubAvailable
}
