// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package session

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/absmach/fluxmq/config"
	core "github.com/absmach/fluxmq/mqtt"
	"github.com/absmach/fluxmq/mqtt/packets"
	v5 "github.com/absmach/fluxmq/mqtt/packets/v5"
	"github.com/absmach/fluxmq/storage"
	"github.com/absmach/fluxmq/storage/messages"
)

// pendingItem holds a message that overflowed the inflight window and is
// waiting for an inflight slot to open before being sent on the wire.
type pendingItem struct {
	msg    *storage.Message
	onSent func()
}

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

	ID              string
	ExternalID      string
	authMethod      string
	authState       any
	conn            core.Connection
	msgHandler      *msgHandler
	Will            *storage.WillMessage
	subscriptions   map[string]storage.SubscribeOptions
	subscriptionIDs map[string][]uint32
	onDisconnect    func(s *Session, graceful bool)
	KeepAlive       time.Duration
	state           State
	// epoch is bumped on every Connect. It identifies the current connection
	// generation so a stale runSession goroutine (from a superseded connection)
	// can detect that the session has moved on and avoid tearing down the new
	// connection. Guarded by mu.
	epoch                uint64
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

	// sendWindow is the outbound QoS 1/2 send quota (Receive Maximum), bounded
	// by serverMaxInflight. Independent of the bidirectional inflight store.
	sendWindow *sendWindow

	// serverMaxInflight is the configured upper bound on the send quota; the
	// negotiated Receive Maximum is clamped to it on every (re)connect.
	serverMaxInflight int

	// pendingCh buffers messages that exceeded the send quota.
	// Non-nil only when InflightOverflow == InflightOverflowQueue.
	// Drained into inflight as ACKs arrive; spilled to offline on disconnect.
	pendingCh chan pendingItem

	// deliverStop is closed when the session disconnects to unblock goroutines
	// waiting on the send window.
	deliverStop chan struct{}

	// lastHeartbeatUpdate tracks the last queue heartbeat update emitted
	// from PINGREQ handling. Accessed atomically to avoid lock contention.
	lastHeartbeatUpdate int64
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

// clampServerMaxInflight normalises a configured MaxInflightMessages value:
// values <= 0 fall back to the shared default, and the result is capped at the
// 16-bit packet-ID space.
func clampServerMaxInflight(maxInflight int) int {
	if maxInflight <= 0 {
		maxInflight = config.DefaultMaxInflightMessages
	}
	if maxInflight > 65535 {
		maxInflight = 65535
	}
	return maxInflight
}

// New creates a new session with injected dependencies.
// The inflight tracker and offline queue should be created and restored by the Manager.
func New(clientID string, version byte, opts Options, inflight messages.Inflight, offlineQueue messages.Queue, sessionCfg config.SessionConfig) *Session {
	receiveMax := opts.ReceiveMaximum
	if receiveMax == 0 {
		receiveMax = 65535
	}

	msgHandler := newMessageHandler(inflight, offlineQueue, version)

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
		deliverStop:          make(chan struct{}),
	}

	// serverMaxInflight is the configured cap the send quota is clamped to. The
	// negotiated receiveMax is already clamped to it by the broker, but the
	// session re-clamps on reconnect. Uses the same default as the broker so the
	// clamp is consistent across the first CONNECT.
	s.serverMaxInflight = clampServerMaxInflight(sessionCfg.MaxInflightMessages)

	backpressure := sessionCfg.InflightOverflow == config.InflightOverflowBackpressure
	// gen 0: no connection yet. attach sets the generation to the connection
	// epoch on (re)connect.
	s.sendWindow = newSendWindow(int(receiveMax), 0, backpressure)

	switch sessionCfg.InflightOverflow {
	case config.InflightOverflowBackpressure:
		// Send quota provides the backpressure; no pending queue.
	case config.InflightOverflowQueue:
		pendingCap := sessionCfg.PendingQueueSize
		if pendingCap <= 0 {
			pendingCap = 1000
		}
		s.pendingCh = make(chan pendingItem, pendingCap)
	}

	return s
}

// ConnectOptions carries the per-connection settings negotiated by a CONNECT.
// They are applied to the session on (re)connect so a persistent session does
// not retain the previous connection's protocol version, keep-alive, Will,
// Receive Maximum, or topic-alias maximum. [MQTT-3.1.0-2]
//
// Session expiry is applied separately (see SetExpiryInterval): zero is a valid
// value that must replace a previous positive one, which a struct field with an
// "apply when non-zero" rule cannot express.
type ConnectOptions struct {
	Version        byte
	KeepAlive      time.Duration
	Will           *storage.WillMessage
	ReceiveMaximum uint16
	TopicAliasMax  uint16
}

// Superseded describes a connection displaced by a takeover, so the broker can
// drain it outside the session lock: notify the displaced MQTT 5 client with a
// DISCONNECT (0x8E), close the socket, and publish its Will if required.
type Superseded struct {
	Conn    core.Connection
	Version byte
	Will    *storage.WillMessage
}

// Connect attaches a connection using the session's existing options. The
// superseded connection, if any, is closed here. Intended for callers that are
// not (re)negotiating connection parameters (e.g. tests); the broker uses
// ConnectWithOptions.
func (s *Session) Connect(c core.Connection) (uint64, error) {
	epoch, superseded := s.attach(c, ConnectOptions{}, false)
	if superseded != nil && superseded.Conn != nil {
		superseded.Conn.Close() //nolint:errcheck // best-effort close of superseded connection
	}
	return epoch, nil
}

// ConnectWithOptions attaches a connection and applies the negotiated options,
// performing a local same-node takeover of any existing connection. It returns
// the connection epoch and the superseded connection (if any). The caller MUST
// drain the superseded connection (notify, close, Will). [MQTT-3.1.4-2].
func (s *Session) ConnectWithOptions(c core.Connection, opts ConnectOptions) (uint64, *Superseded) {
	return s.attach(c, opts, true)
}

// attach swaps in connection c under the lock: it bumps the epoch, applies
// options when applyOpts is set, and clears per-connection topic aliases (they
// never carry across connections, MQTT-3.3.2-7). It returns the new epoch and
// any superseded connection. The superseded connection is NOT closed here —
// closing while holding the lock can deadlock transports (e.g. WebSocket) whose
// Close synchronously invokes onDisconnect.
func (s *Session) attach(c core.Connection, opts ConnectOptions, applyOpts bool) (uint64, *Superseded) {
	s.mu.Lock()

	var superseded *Superseded
	if s.conn != nil {
		superseded = &Superseded{Conn: s.conn, Version: s.Version, Will: s.Will}
	}

	if applyOpts {
		s.Version = opts.Version
		s.KeepAlive = opts.KeepAlive
		s.Will = opts.Will
		s.TopicAliasMax = opts.TopicAliasMax
	}

	// Topic alias mappings are scoped to a single network connection and must
	// not survive a takeover. [MQTT-3.3.2-7]
	s.msgHandler.ClearAliases()

	select {
	case <-s.deliverStop:
		s.deliverStop = make(chan struct{})
	default:
	}

	s.epoch++
	epoch := s.epoch

	// Bind the send quota to this connection generation. Receive Maximum is
	// connection-scoped, so on a reconnect (applyOpts) the capacity is the newly
	// negotiated value (clamped by the server limit); otherwise the existing
	// capacity is kept. Advancing the generation invalidates any lease held by a
	// superseded delivery or retry, so it can neither consume nor free this
	// generation's quota. [MQTT-4.9]
	s.resetSendWindowLocked(opts.ReceiveMaximum, applyOpts)

	keepAlive := s.KeepAlive

	s.conn = c
	s.state = StateConnected
	s.connectedAt = time.Now()

	s.mu.Unlock()

	c.SetKeepAlive(keepAlive) //nolint:errcheck // keepalive timer setup; connection already validated at this point

	// Set callback to handle connection loss/keepalive expiry. Scoped to this
	// epoch so a stale callback cannot disconnect a newer connection.
	c.SetOnDisconnect(func(graceful bool) {
		s.DisconnectIf(graceful, epoch, 0x00) //nolint:errcheck // disconnect callback; session cleanup is best-effort
	})

	return epoch, superseded
}

// resetSendWindowLocked rebinds the outbound send quota to the current
// connection generation (s.epoch). When applyCapacity is set it also resizes the
// quota to the negotiated Receive Maximum, clamped by the configured server
// limit; otherwise the existing capacity is kept. Caller must hold s.mu. The
// persistent bidirectional inflight store is left untouched.
func (s *Session) resetSendWindowLocked(clientReceiveMax uint16, applyCapacity bool) {
	capacity := int(s.ReceiveMaximum)
	if applyCapacity {
		capacity = int(clientReceiveMax)
		if capacity <= 0 || capacity > s.serverMaxInflight {
			capacity = s.serverMaxInflight
		}
	}
	if capacity < 1 {
		capacity = 1
	}
	s.ReceiveMaximum = uint16(capacity)
	s.sendWindow.reset(capacity, s.epoch)
}

// SetExpiryInterval sets the session expiry interval. Unlike the option struct,
// this applies the value verbatim, so a reconnect carrying expiry 0 (expire on
// disconnect) replaces a previous positive value. [MQTT-3.1.2.11].
func (s *Session) SetExpiryInterval(interval uint32) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ExpiryInterval = interval
}

// Epoch returns the current connection generation. A runSession goroutine
// captures this at connect time and uses it to bind writes and teardown to its
// own connection, so a superseded goroutine cannot affect the replacement.
func (s *Session) Epoch() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.epoch
}

// SendBackpressure reports whether outbound delivery blocks on the send quota
// (vs. falling back to the pending queue) when the quota is exhausted.
func (s *Session) SendBackpressure() bool {
	return s.sendWindow.blocking
}

// DeliveryLease atomically captures the current connection, its protocol
// version, and its generation. A delivery uses these together: it acquires send
// quota for the generation, encodes the PUBLISH for the captured version, and
// writes it to the captured connection. Capturing the version with the
// connection prevents a cross-version takeover (v3<->v5) from encoding a packet
// for one protocol and writing it to a connection of the other.
func (s *Session) DeliveryLease() (conn core.Connection, version byte, gen uint64) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.conn, s.Version, s.epoch
}

// ServerMaxInflight returns the configured upper bound on concurrent inflight
// messages. It is the server's inbound Receive Maximum advertised in CONNACK and
// the capacity of the bidirectional inflight store.
func (s *Session) ServerMaxInflight() int {
	return s.serverMaxInflight
}

// AcquireSendQuota consumes a send-quota token for packetID in generation gen,
// blocking until one is available, the generation is superseded by a takeover,
// or the session disconnects. A retransmission that already holds a token in the
// generation succeeds immediately. Returns false if gen is no longer current or
// the session disconnected while waiting. Used in backpressure mode.
func (s *Session) AcquireSendQuota(packetID uint16, gen uint64) bool {
	s.mu.RLock()
	stop := s.deliverStop
	s.mu.RUnlock()
	return s.sendWindow.acquire(packetID, gen, stop)
}

// TryAcquireSendQuota consumes a send-quota token for packetID in generation gen
// without blocking. Returns false if gen is superseded or no token is available.
func (s *Session) TryAcquireSendQuota(packetID uint16, gen uint64) bool {
	return s.sendWindow.tryAcquire(packetID, gen)
}

// ReleaseSendQuota returns the token held for packetID in generation gen (on
// PUBACK/PUBCOMP, or to roll back a failed delivery). A release carrying a
// superseded generation is a no-op.
func (s *Session) ReleaseSendQuota(packetID uint16, gen uint64) {
	s.sendWindow.release(packetID, gen)
}

// AddInbound atomically admits an inbound QoS 2 transaction. accepted reports
// whether the tracker took ownership of msg. Trackers that do not implement the
// optional directional extension are rejected explicitly because their base
// Add implementation may not isolate inbound and outbound packet-ID spaces.
func (s *Session) AddInbound(packetID uint16, msg *storage.Message) (bool, error) {
	if ia, ok := s.msgHandler.Inflight().(messages.InboundAdder); ok {
		return ia.AddInbound(packetID, msg)
	}
	return false, messages.ErrInboundUnsupported
}

// AckInbound acknowledges and removes an inbound message (PUBREL). Trackers
// that do not implement the optional directional extension are rejected
// explicitly so an inbound acknowledgement cannot remove an outbound entry.
func (s *Session) AckInbound(packetID uint16) (*storage.Message, error) {
	if ib, ok := s.msgHandler.Inflight().(messages.InboundAcker); ok {
		return ib.AckInbound(packetID)
	}
	return nil, messages.ErrInboundUnsupported
}

// MarkSentIfEpoch marks the outbound packet as sent only while gen is still the
// current connection generation, atomically under the session lock. This closes
// the window where a takeover between an epoch check and MarkSent would let a
// stale onSent (a packet flushed on a displaced connection) postpone
// retransmission to the replacement connection.
func (s *Session) MarkSentIfEpoch(packetID uint16, gen uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.epoch == gen {
		s.msgHandler.Inflight().MarkSent(packetID)
	}
}

// TryEnqueuePending attempts to push an overflow item into the pending queue.
// Returns true on success, false if the pending queue is also full (caller drops).
// Only meaningful when pendingCh is non-nil (pending queue mode).
func (s *Session) TryEnqueuePending(msg *storage.Message, onSent func()) bool {
	if s.pendingCh == nil {
		return false
	}
	select {
	case s.pendingCh <- pendingItem{msg: msg, onSent: onSent}:
		return true
	default:
		return false
	}
}

// DrainOnePending moves one pending item into the inflight tracker and sends it.
// Called from ACK handlers to refill the pipeline after a slot opens.
// Returns without action if there is nothing pending or the session is disconnected.
func (s *Session) DrainOnePending(deliver func(msg *storage.Message, onSent func()) error) {
	if s.pendingCh == nil {
		return
	}
	select {
	case item := <-s.pendingCh:
		if err := deliver(item.msg, item.onSent); err != nil {
			item.msg.ReleasePayload()
			storage.ReleaseMessage(item.msg)
		}
	default:
	}
}

// drainPendingToOffline moves all remaining pending messages to the offline queue.
// Called on disconnect so QoS > 0 pending messages are preserved.
func (s *Session) drainPendingToOffline() {
	if s.pendingCh == nil {
		return
	}
	for {
		select {
		case item := <-s.pendingCh:
			if item.msg.QoS > 0 {
				if err := s.OfflineQueue().Enqueue(item.msg); err != nil {
					item.msg.ReleasePayload()
					storage.ReleaseMessage(item.msg)
					continue
				}
				item.msg.ReleasePayload()
				storage.ReleaseMessage(item.msg)
			} else {
				item.msg.ReleasePayload()
				storage.ReleaseMessage(item.msg)
			}
		default:
			return
		}
	}
}

// Disconnect disconnects the session unconditionally.
func (s *Session) Disconnect(graceful bool, reasonCode byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.disconnectLocked(graceful, reasonCode)
}

// DisconnectIf disconnects the session only if epoch still matches the current
// connection generation. A stale runSession goroutine (whose connection has
// been superseded by a local takeover) passes its own epoch here and becomes a
// no-op, so it cannot tear down the connection that replaced it.
func (s *Session) DisconnectIf(graceful bool, epoch uint64, reasonCode byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.epoch != epoch {
		return nil
	}
	return s.disconnectLocked(graceful, reasonCode)
}

func (s *Session) sendDisconnect(reasonCode byte) {
	if s.conn == nil || s.Version != 5 {
		return
	}
	disc := &v5.Disconnect{
		FixedHeader: packets.FixedHeader{PacketType: packets.DisconnectType},
		ReasonCode:  reasonCode,
	}
	_ = s.conn.WritePacket(disc)
}

// disconnectLocked performs the disconnect. Caller must hold s.mu.
func (s *Session) disconnectLocked(graceful bool, reasonCode byte) error {
	if s.state != StateConnected {
		return nil
	}

	s.state = StateDisconnecting
	select {
	case <-s.deliverStop:
	default:
		close(s.deliverStop)
	}

	if s.conn != nil {
		// Cache info before closing
		s.disconnectedAt = time.Now()
		// If connection tracks its own timestamps, we might want to sync,
		// but since we are closing it here, "now" is correct.

		s.sendDisconnect(reasonCode)
		s.conn.Close()
		s.conn = nil
	}
	s.state = StateDisconnected

	if graceful {
		s.Will = nil
	}

	// Move any pending (overflow) messages to the offline queue so they
	// survive the disconnect and are delivered on next reconnect.
	s.drainPendingToOffline()

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

// GetDisconnectedAt returns when the session was disconnected.
// Cheaper than Info() when only the disconnect timestamp is needed.
func (s *Session) GetDisconnectedAt() time.Time {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.disconnectedAt
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

// ProcessRetriesTo resends due inflight messages on the provided writer,
// encoded for generation gen's protocol version and gated by that generation's
// send quota. runSession uses this via connCtx so a superseded goroutine can
// neither redeliver onto the replacement connection nor consume or update the
// replacement generation's retry state.
func (s *Session) ProcessRetriesTo(w core.PacketWriter, gen uint64) {
	if w == nil {
		return
	}
	s.mu.RLock()
	if s.epoch != gen {
		s.mu.RUnlock()
		return
	}
	version := s.Version
	s.mu.RUnlock()

	s.msgHandler.ProcessRetries(w, version, s.sendQuotaAcquirer(gen), s.markRetryIfEpoch(gen))
}

// sendQuotaAcquirer returns a retry-gate bound to generation gen.
func (s *Session) sendQuotaAcquirer(gen uint64) func(packetID uint16) bool {
	return func(packetID uint16) bool {
		return s.sendWindow.tryAcquire(packetID, gen)
	}
}

// markRetryIfEpoch marks a retransmission as sent only while gen is still the
// current connection generation. A retry flushed by a superseded connection must
// not postpone redelivery on the replacement connection.
func (s *Session) markRetryIfEpoch(gen uint64) func(packetID uint16) {
	return func(packetID uint16) {
		s.mu.Lock()
		defer s.mu.Unlock()
		if s.epoch == gen {
			_ = s.msgHandler.Inflight().MarkRetry(packetID)
		}
	}
}

// WritePacket writes a packet to the connection.
func (s *Session) WritePacket(pkt packets.ControlPacket) error {
	return s.WriteControlPacket(pkt, nil)
}

// WriteControlPacket writes a control packet to the connection.
func (s *Session) WriteControlPacket(pkt packets.ControlPacket, onSent func()) error {
	s.mu.RLock()
	conn := s.conn
	s.mu.RUnlock()

	if conn == nil {
		return ErrNotConnected
	}
	return conn.WriteControlPacket(pkt, onSent)
}

// WriteDataPacket writes a data packet (PUBLISH path) to the connection.
func (s *Session) WriteDataPacket(pkt packets.ControlPacket, onSent func()) error {
	s.mu.RLock()
	conn := s.conn
	s.mu.RUnlock()

	if conn == nil {
		return ErrNotConnected
	}
	return conn.WriteDataPacket(pkt, onSent)
}

// TryWriteDataPacket is a non-blocking variant that returns ErrSendQueueFull
// immediately if the send queue is full, without blocking or disconnecting.
func (s *Session) TryWriteDataPacket(pkt packets.ControlPacket, onSent func()) error {
	s.mu.RLock()
	conn := s.conn
	s.mu.RUnlock()

	if conn == nil {
		return ErrNotConnected
	}
	return conn.TryWriteDataPacket(pkt, onSent)
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

// HasSubscription reports whether a filter is already tracked for the session.
func (s *Session) HasSubscription(filter string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	_, ok := s.subscriptions[filter]
	return ok
}

// ShouldUpdateHeartbeat applies a minimum interval between queue heartbeat updates.
// It returns true when the caller should emit a heartbeat update.
func (s *Session) ShouldUpdateHeartbeat(now time.Time, minInterval time.Duration) bool {
	if minInterval <= 0 {
		return true
	}

	nowNano := now.UnixNano()
	minDelta := minInterval.Nanoseconds()

	for {
		last := atomic.LoadInt64(&s.lastHeartbeatUpdate)
		if last != 0 && nowNano-last < minDelta {
			return false
		}
		if atomic.CompareAndSwapInt64(&s.lastHeartbeatUpdate, last, nowNano) {
			return true
		}
	}
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
		s.conn.SetKeepAlive(keepAlive) //nolint:errcheck // keepalive timer update; connection already validated
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
		ExternalID:      s.ExternalID,
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

	s.ExternalID = stored.ExternalID
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
