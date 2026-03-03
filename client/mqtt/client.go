// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package mqtt

import (
	"bytes"
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"math/rand/v2"
	"net"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/absmach/fluxmq/mqtt/packets"
	v3 "github.com/absmach/fluxmq/mqtt/packets/v3"
	v5 "github.com/absmach/fluxmq/mqtt/packets/v5"
)

type writeRequest struct {
	data     []byte
	deadline time.Time
	errCh    chan error // nil for fire-and-forget
	// Indicates errCh came from the write error channel pool.
	pooledErrCh bool
	ctx         context.Context
	// Set for outbound publish writes subject to outbound pressure limits.
	trackOutbound bool
	dropTopic     string
	dropQoS       byte
	dropPayloadSz int
}

const defaultControlWriteChanSize = 64
const defaultCallbackQueueSize = 256

var (
	callbackQueueOnce sync.Once
	callbackQueueCh   chan func()

	writeErrChPool = sync.Pool{
		New: func() any {
			return make(chan error, 1)
		},
	}
)

type writeRuntime struct {
	conn           net.Conn
	stopCh         chan struct{}
	doneCh         chan struct{}
	writeCh        chan writeRequest
	controlWriteCh chan writeRequest
	qos0Wake       chan struct{}
	writeDone      chan struct{}
}

type dispatchRuntime struct {
	msgCh   chan *Message
	msgStop chan struct{}
}

type reconnectBuffered struct {
	wire       []byte
	topic      string
	qos        byte
	payloadSz  int
	bufferedSz int
}

// Client is a thread-safe MQTT client.
type Client struct {
	opts *Options

	// State management
	state *stateManager

	// Guards connection and write-loop channel pointers.
	writeStateMu sync.RWMutex
	writeRT      atomic.Pointer[writeRuntime]

	// Connection — only touched by writeLoop (writes) and readLoop (reads) after Connect().
	conn net.Conn

	// Write serialization
	writeCh        chan writeRequest
	controlWriteCh chan writeRequest
	qos0Wake       chan struct{}
	writeDone      chan struct{}
	qos0BufMu      sync.Mutex
	qos0Buf        []writeRequest

	// Server capabilities (MQTT 5.0)
	serverCaps   *ServerCapabilities
	serverCapsMu sync.RWMutex

	// Topic aliases (MQTT 5.0)
	topicAliases *topicAliasManager

	// Queue subscriptions
	queueSubs     *queueSubscriptions
	queueAckCache *queueAckCache
	subscriptions *subscriptionRegistry

	// Pending operations
	pending *pendingStore

	// Message store for QoS 1/2
	store MessageStore

	// QoS 2 incoming messages waiting for PUBREL
	qos2Incoming   map[uint16]*Message
	qos2IncomingMu sync.Mutex

	// Lifecycle
	lifecycleMu sync.Mutex
	stopCh      chan struct{}
	doneCh      chan struct{}
	reconnMu    sync.Mutex

	// Keep-alive
	lastActivity atomic.Int64 // UnixNano timestamp, no mutex needed
	pingMu       sync.Mutex
	pingTimer    *time.Timer
	waitingPing  bool
	lastPingSent time.Time

	// Message dispatching
	dispatchMu sync.RWMutex
	dispatchRT atomic.Pointer[dispatchRuntime]
	msgCh      chan *Message
	msgStop    chan struct{}
	dispatchWg sync.WaitGroup

	// Callback queue pressure accounting.
	pendingMu       sync.Mutex
	pendingCond     *sync.Cond // signaled when pendingMessages/pendingBytes decrease
	pendingMessages int
	pendingBytes    int64
	droppedMessages atomic.Uint64

	// Avoid flooding async error callback with repeated slow-consumer notifications.
	slowConsumerNotified atomic.Bool

	// Outbound publish pressure accounting.
	outboundMu              sync.Mutex
	outboundCond            *sync.Cond // signaled when outbound pending decreases
	outboundPendingMessages int
	outboundPendingBytes    int64

	// Buffered publishes while disconnected.
	reconnectBufMu    sync.Mutex
	reconnectBuf      []*reconnectBuffered
	reconnectBufBytes int

	// Server index for round-robin
	serverIdx int

	// Cleanup guard to keep teardown idempotent under concurrent lifecycle calls.
	cleanupInProgress uint32

	// Drain mode rejects new publishes until disconnect completes.
	draining atomic.Bool
}

// New creates a new MQTT client with the given options.
func New(opts *Options) (*Client, error) {
	if opts == nil {
		return nil, ErrNilOptions
	}

	if err := opts.Validate(); err != nil {
		return nil, err
	}

	store := opts.Store
	if store == nil {
		store = NewMemoryStore()
	}

	c := &Client{
		opts:          opts,
		state:         newStateManager(),
		pending:       newPendingStore(opts.MaxInflight),
		store:         store,
		qos2Incoming:  make(map[uint16]*Message),
		queueSubs:     newQueueSubscriptions(),
		queueAckCache: newQueueAckCache(5 * time.Minute),
		subscriptions: newSubscriptionRegistry(),
	}
	c.pendingCond = sync.NewCond(&c.pendingMu)
	c.outboundCond = sync.NewCond(&c.outboundMu)
	return c, nil
}

func (c *Client) loadWriteRuntime() *writeRuntime {
	if rt := c.writeRT.Load(); rt != nil {
		return rt
	}

	c.writeStateMu.RLock()
	defer c.writeStateMu.RUnlock()
	if c.conn == nil && c.stopCh == nil && c.doneCh == nil && c.writeCh == nil && c.controlWriteCh == nil && c.qos0Wake == nil && c.writeDone == nil {
		return nil
	}

	rt := &writeRuntime{
		conn:           c.conn,
		stopCh:         c.stopCh,
		doneCh:         c.doneCh,
		writeCh:        c.writeCh,
		controlWriteCh: c.controlWriteCh,
		qos0Wake:       c.qos0Wake,
		writeDone:      c.writeDone,
	}
	// Cache the snapshot so subsequent calls on the same connection cycle
	// don't allocate. CAS avoids overwriting a value stored by Connect().
	c.writeRT.CompareAndSwap(nil, rt)
	return rt
}

func (c *Client) isMQTTv5() bool {
	return c.opts.ProtocolVersion == 5
}

func (c *Client) readPacketFromConn(conn net.Conn) (packets.ControlPacket, error) {
	if c.isMQTTv5() {
		pkt, _, _, err := v5.ReadPacket(conn)
		return pkt, err
	}
	return v3.ReadPacket(conn)
}

func packetIDFromControlPacket(pkt packets.ControlPacket) uint16 {
	switch p := pkt.(type) {
	case *v3.PubAck:
		return p.ID
	case *v5.PubAck:
		return p.ID
	case *v3.PubRec:
		return p.ID
	case *v5.PubRec:
		return p.ID
	case *v3.PubRel:
		return p.ID
	case *v5.PubRel:
		return p.ID
	case *v3.PubComp:
		return p.ID
	case *v5.PubComp:
		return p.ID
	case *v3.SubAck:
		return p.ID
	case *v5.SubAck:
		return p.ID
	case *v3.UnSubAck:
		return p.ID
	case *v5.UnsubAck:
		return p.ID
	default:
		return 0
	}
}

func subAckDetails(pkt packets.ControlPacket) (uint16, []byte, bool) {
	switch p := pkt.(type) {
	case *v5.SubAck:
		var reasonCodes []byte
		if p.ReasonCodes != nil {
			reasonCodes = *p.ReasonCodes
		}
		return p.ID, reasonCodes, true
	case *v3.SubAck:
		return p.ID, p.ReturnCodes, false
	default:
		return 0, nil, false
	}
}

func (c *Client) encodePingReqPacket() []byte {
	if c.isMQTTv5() {
		pkt := &v5.PingReq{
			FixedHeader: packets.FixedHeader{PacketType: packets.PingReqType},
		}
		return pkt.Encode()
	}

	pkt := &v3.PingReq{
		FixedHeader: packets.FixedHeader{PacketType: packets.PingReqType},
	}
	return pkt.Encode()
}

func (c *Client) encodeQoSControlPacket(packetType byte, packetID uint16, qos byte) []byte {
	if c.isMQTTv5() {
		switch packetType {
		case packets.PubAckType:
			return (&v5.PubAck{
				FixedHeader: packets.FixedHeader{PacketType: packetType, QoS: qos},
				ID:          packetID,
			}).Encode()
		case packets.PubRecType:
			return (&v5.PubRec{
				FixedHeader: packets.FixedHeader{PacketType: packetType, QoS: qos},
				ID:          packetID,
			}).Encode()
		case packets.PubRelType:
			return (&v5.PubRel{
				FixedHeader: packets.FixedHeader{PacketType: packetType, QoS: qos},
				ID:          packetID,
			}).Encode()
		case packets.PubCompType:
			return (&v5.PubComp{
				FixedHeader: packets.FixedHeader{PacketType: packetType, QoS: qos},
				ID:          packetID,
			}).Encode()
		default:
			panic(fmt.Sprintf("encodeQoSControlPacket: unexpected v5 packet type 0x%02X", packetType))
		}
	}

	switch packetType {
	case packets.PubAckType:
		return (&v3.PubAck{
			FixedHeader: packets.FixedHeader{PacketType: packetType, QoS: qos},
			ID:          packetID,
		}).Encode()
	case packets.PubRecType:
		return (&v3.PubRec{
			FixedHeader: packets.FixedHeader{PacketType: packetType, QoS: qos},
			ID:          packetID,
		}).Encode()
	case packets.PubRelType:
		return (&v3.PubRel{
			FixedHeader: packets.FixedHeader{PacketType: packetType, QoS: qos},
			ID:          packetID,
		}).Encode()
	case packets.PubCompType:
		return (&v3.PubComp{
			FixedHeader: packets.FixedHeader{PacketType: packetType, QoS: qos},
			ID:          packetID,
		}).Encode()
	default:
		panic(fmt.Sprintf("encodeQoSControlPacket: unexpected v3 packet type 0x%02X", packetType))
	}
}

func (c *Client) Connect() error {
	c.lifecycleMu.Lock()
	defer c.lifecycleMu.Unlock()

	if c.state.isClosed() {
		return ErrClientClosed
	}

	if !c.state.transitionFrom(StateConnecting, StateDisconnected, StateReconnecting) {
		return ErrAlreadyConnected
	}

	err := c.doConnect()
	if err != nil {
		if !c.state.isClosed() {
			c.state.set(StateDisconnected)
		}
		return err
	}

	// If another goroutine closed the client while connecting, do not transition to connected.
	if !c.state.transition(StateConnecting, StateConnected) {
		c.writeStateMu.Lock()
		if c.conn != nil {
			c.conn.Close()
			c.conn = nil
		}
		c.writeRT.Store(nil)
		c.writeStateMu.Unlock()
		if c.state.isClosed() {
			return ErrClientClosed
		}
		return ErrConnectionLost
	}

	// Start background goroutines
	stopCh := make(chan struct{})
	doneCh := make(chan struct{})

	size := c.opts.MessageChanSize
	if size <= 0 {
		size = DefaultMessageChanSize
	}
	writeCh := make(chan writeRequest, size)
	controlSize := size / 4
	if controlSize < defaultControlWriteChanSize {
		controlSize = defaultControlWriteChanSize
	}
	controlWriteCh := make(chan writeRequest, controlSize)
	qos0Wake := make(chan struct{}, 1)
	writeDone := make(chan struct{})
	c.writeStateMu.Lock()
	c.stopCh = stopCh
	c.doneCh = doneCh
	c.writeCh = writeCh
	c.controlWriteCh = controlWriteCh
	c.qos0Wake = qos0Wake
	c.writeDone = writeDone
	c.writeRT.Store(&writeRuntime{
		conn:           c.conn,
		stopCh:         stopCh,
		doneCh:         doneCh,
		writeCh:        writeCh,
		controlWriteCh: controlWriteCh,
		qos0Wake:       qos0Wake,
		writeDone:      writeDone,
	})
	c.writeStateMu.Unlock()

	go c.writeLoop()

	c.startDispatcher()
	go c.readLoop()

	if c.opts.KeepAlive > 0 {
		c.startKeepAlive()
	}

	// Restore client-side state on reconnect (subscriptions and QoS in-flight).
	c.restoreState()
	c.flushReconnectBuffer()

	// Callback
	if c.opts.OnConnect != nil {
		go c.opts.OnConnect()
	}

	return nil
}

func (c *Client) doConnect() error {
	// Try each server in order
	var lastErr error
	for i := 0; i < len(c.opts.Servers); i++ {
		idx := (c.serverIdx + i) % len(c.opts.Servers)
		addr := c.opts.Servers[idx]

		err := c.connectToServer(addr)
		if err == nil {
			c.serverIdx = idx
			return nil
		}
		lastErr = err
	}

	if lastErr != nil {
		return fmt.Errorf("%w: %v", ErrConnectFailed, lastErr)
	}
	return ErrConnectFailed
}

func (c *Client) connectToServer(addr string) error {
	// Establish TCP connection
	var conn net.Conn
	var err error

	dialer := &net.Dialer{Timeout: c.opts.ConnectTimeout}
	if c.opts.TLSConfig != nil {
		conn, err = tls.DialWithDialer(dialer, "tcp", addr, c.opts.TLSConfig)
	} else {
		conn, err = dialer.Dial("tcp", addr)
	}
	if err != nil {
		return err
	}

	// Send CONNECT packet
	if err := c.sendConnect(conn); err != nil {
		conn.Close()
		return err
	}

	// Read CONNACK
	code, err := c.readConnAck(conn)
	if err != nil {
		conn.Close()
		return err
	}
	if code != ConnAccepted {
		conn.Close()
		return code
	}

	c.writeStateMu.Lock()
	c.conn = conn
	c.writeStateMu.Unlock()
	c.updateActivity()
	return nil
}

func (c *Client) sendConnect(conn net.Conn) error {
	conn.SetWriteDeadline(time.Now().Add(c.opts.WriteTimeout))
	defer conn.SetWriteDeadline(time.Time{})

	keepAlive := uint16(c.opts.KeepAlive.Seconds())

	if c.isMQTTv5() {
		pkt := &v5.Connect{
			FixedHeader:     packets.FixedHeader{PacketType: packets.ConnectType},
			ClientID:        c.opts.ClientID,
			KeepAlive:       keepAlive,
			ProtocolName:    "MQTT",
			ProtocolVersion: 5,
			CleanStart:      c.opts.CleanSession,
		}

		if c.opts.Username != "" {
			pkt.UsernameFlag = true
			pkt.Username = c.opts.Username
		}
		if c.opts.Password != "" {
			pkt.PasswordFlag = true
			pkt.Password = []byte(c.opts.Password)
		}

		if c.opts.Will != nil {
			pkt.WillFlag = true
			pkt.WillQoS = c.opts.Will.QoS
			pkt.WillRetain = c.opts.Will.Retain
			pkt.WillTopic = c.opts.Will.Topic
			pkt.WillPayload = c.opts.Will.Payload

			// Set v5 Will properties
			if c.opts.Will.WillDelayInterval > 0 || c.opts.Will.PayloadFormat != nil ||
				c.opts.Will.MessageExpiry > 0 || c.opts.Will.ContentType != "" ||
				c.opts.Will.ResponseTopic != "" || len(c.opts.Will.CorrelationData) > 0 ||
				len(c.opts.Will.UserProperties) > 0 {
				pkt.WillProperties = &v5.WillProperties{}

				if c.opts.Will.WillDelayInterval > 0 {
					pkt.WillProperties.WillDelayInterval = &c.opts.Will.WillDelayInterval
				}

				if c.opts.Will.PayloadFormat != nil {
					pkt.WillProperties.PayloadFormat = c.opts.Will.PayloadFormat
				}

				if c.opts.Will.MessageExpiry > 0 {
					pkt.WillProperties.MessageExpiry = &c.opts.Will.MessageExpiry
				}

				if c.opts.Will.ContentType != "" {
					pkt.WillProperties.ContentType = c.opts.Will.ContentType
				}

				if c.opts.Will.ResponseTopic != "" {
					pkt.WillProperties.ResponseTopic = c.opts.Will.ResponseTopic
				}

				if len(c.opts.Will.CorrelationData) > 0 {
					pkt.WillProperties.CorrelationData = c.opts.Will.CorrelationData
				}

				if len(c.opts.Will.UserProperties) > 0 {
					pkt.WillProperties.User = make([]v5.User, 0, len(c.opts.Will.UserProperties))
					for k, v := range c.opts.Will.UserProperties {
						pkt.WillProperties.User = append(pkt.WillProperties.User, v5.User{Key: k, Value: v})
					}
				}
			}
		}

		// Set v5 Connect properties
		if c.opts.SessionExpiry > 0 || c.opts.ReceiveMaximum > 0 ||
			c.opts.MaximumPacketSize > 0 || c.opts.TopicAliasMaximum > 0 ||
			c.opts.RequestResponseInfo || !c.opts.RequestProblemInfo ||
			c.opts.AuthMethod != "" {
			if pkt.Properties == nil {
				pkt.Properties = &v5.ConnectProperties{}
			}

			if c.opts.SessionExpiry > 0 {
				pkt.Properties.SessionExpiryInterval = &c.opts.SessionExpiry
			}

			if c.opts.ReceiveMaximum > 0 {
				pkt.Properties.ReceiveMaximum = &c.opts.ReceiveMaximum
			}

			if c.opts.MaximumPacketSize > 0 {
				pkt.Properties.MaximumPacketSize = &c.opts.MaximumPacketSize
			}

			if c.opts.TopicAliasMaximum > 0 {
				pkt.Properties.TopicAliasMaximum = &c.opts.TopicAliasMaximum
			}

			if c.opts.RequestResponseInfo {
				one := byte(1)
				pkt.Properties.RequestResponseInfo = &one
			}

			// RequestProblemInfo defaults to true, only set if explicitly false
			if !c.opts.RequestProblemInfo {
				zero := byte(0)
				pkt.Properties.RequestProblemInfo = &zero
			}

			if c.opts.AuthMethod != "" {
				pkt.Properties.AuthMethod = c.opts.AuthMethod
				pkt.Properties.AuthData = c.opts.AuthData
			}
		}

		c.updateActivity()
		return pkt.Pack(conn)
	}

	// MQTT 3.1.1
	pkt := &v3.Connect{
		FixedHeader:     packets.FixedHeader{PacketType: packets.ConnectType},
		ClientID:        c.opts.ClientID,
		KeepAlive:       keepAlive,
		ProtocolName:    "MQTT",
		ProtocolVersion: 4,
		CleanSession:    c.opts.CleanSession,
	}

	if c.opts.Username != "" {
		pkt.UsernameFlag = true
		pkt.Username = c.opts.Username
	}
	if c.opts.Password != "" {
		pkt.PasswordFlag = true
		pkt.Password = []byte(c.opts.Password)
	}

	if c.opts.Will != nil {
		pkt.WillFlag = true
		pkt.WillQoS = c.opts.Will.QoS
		pkt.WillRetain = c.opts.Will.Retain
		pkt.WillTopic = c.opts.Will.Topic
		pkt.WillMessage = c.opts.Will.Payload
	}

	return pkt.Pack(conn)
}

func (c *Client) readConnAck(conn net.Conn) (ConnAckCode, error) {
	conn.SetReadDeadline(time.Now().Add(c.opts.ConnectTimeout))
	defer conn.SetReadDeadline(time.Time{})

	if c.isMQTTv5() {
		// Enhanced auth may require multiple AUTH packet exchanges
		// before the final CONNACK arrives.
		for {
			pkt, _, _, err := v5.ReadPacket(conn)
			if err != nil {
				return 0, err
			}

			if auth, ok := pkt.(*v5.Auth); ok {
				if err := c.handleConnectAuth(conn, auth); err != nil {
					return 0, err
				}
				continue
			}

			ack, ok := pkt.(*v5.ConnAck)
			if !ok {
				return 0, ErrUnexpectedPacket
			}

			// Parse and store server capabilities
			caps := parseConnAckProperties(ack.Properties)
			c.serverCapsMu.Lock()
			c.serverCaps = caps
			c.serverCapsMu.Unlock()

			// Initialize topic alias manager with server's limits
			c.topicAliases = newTopicAliasManager(
				c.opts.TopicAliasMaximum, // client accepts from server
				caps.TopicAliasMaximum,   // server accepts from client
			)

			// Invoke callback if set
			if c.opts.OnServerCapabilities != nil {
				c.opts.OnServerCapabilities(caps)
			}

			return ConnAckCode(ack.ReasonCode), nil
		}
	}

	pkt, err := v3.ReadPacket(conn)
	if err != nil {
		return 0, err
	}
	ack, ok := pkt.(*v3.ConnAck)
	if !ok {
		return 0, ErrUnexpectedPacket
	}
	return ConnAckCode(ack.ReturnCode), nil
}

// Disconnect gracefully disconnects from the broker.
func (c *Client) Disconnect() error {
	return c.DisconnectWithReason(0, 0, "")
}

// DisconnectWithReason disconnects with an optional reason code (MQTT 5.0).
// For MQTT 3.1.1, reasonCode and sessionExpiry are ignored.
// Parameters:
//   - reasonCode: MQTT 5.0 disconnect reason (0 = normal disconnect)
//   - sessionExpiry: Update session expiry interval (0 = use current value, ignored if 0)
//   - reasonString: Human-readable reason (empty = no reason string)
func (c *Client) DisconnectWithReason(reasonCode byte, sessionExpiry uint32, reasonString string) error {
	c.lifecycleMu.Lock()
	if !c.state.transition(StateConnected, StateDisconnecting) {
		c.lifecycleMu.Unlock()
		return nil
	}

	c.stopKeepAlive()
	c.sendDisconnectWithReason(reasonCode, sessionExpiry, reasonString)
	c.lifecycleMu.Unlock()

	c.cleanup(nil)
	c.lifecycleMu.Lock()
	c.state.transition(StateDisconnecting, StateDisconnected)
	c.lifecycleMu.Unlock()

	return nil
}

// Drain stops accepting new publishes, waits for in-flight publishes to finish,
// and then disconnects gracefully.
func (c *Client) Drain(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if c.state.get() != StateConnected {
		return ErrNotConnected
	}
	if !c.draining.CompareAndSwap(false, true) {
		return ErrDraining
	}
	defer c.draining.Store(false)

	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for {
		if !c.state.isConnected() {
			return ErrConnectionLost
		}

		if c.pending.countByType(pendingPublish) == 0 &&
			c.writeQueueDepth() == 0 &&
			c.outboundPendingDepth() == 0 &&
			c.reconnectBufferDepth() == 0 {
			break
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}

	return c.DisconnectWithReason(0, 0, "")
}

// sendDisconnectWithReason writes the DISCONNECT directly to conn (bypassing writeLoop)
// because it runs right before cleanup tears down the write infrastructure.
func (c *Client) sendDisconnectWithReason(reasonCode byte, sessionExpiry uint32, reasonString string) {
	rt := c.loadWriteRuntime()
	if rt == nil {
		return
	}
	conn := rt.conn
	if conn == nil {
		return
	}
	conn.SetWriteDeadline(time.Now().Add(c.opts.WriteTimeout))

	if c.isMQTTv5() {
		pkt := &v5.Disconnect{
			FixedHeader: packets.FixedHeader{PacketType: packets.DisconnectType},
			ReasonCode:  reasonCode,
		}

		if sessionExpiry > 0 || reasonString != "" {
			pkt.Properties = &v5.DisconnectProperties{}

			if sessionExpiry > 0 {
				pkt.Properties.SessionExpiryInterval = &sessionExpiry
			}

			if reasonString != "" {
				pkt.Properties.ReasonString = reasonString
			}
		}

		pkt.Pack(conn)
	} else {
		pkt := &v3.Disconnect{
			FixedHeader: packets.FixedHeader{PacketType: packets.DisconnectType},
		}
		pkt.Pack(conn)
	}
}

// Close permanently closes the client.
func (c *Client) Close() error {
	c.lifecycleMu.Lock()
	c.state.set(StateClosed)
	c.lifecycleMu.Unlock()

	c.stopKeepAlive()
	c.cleanup(ErrClientClosed)
	if c.store != nil {
		c.store.Close()
	}
	return nil
}

func (c *Client) cleanup(err error) {
	if !atomic.CompareAndSwapUint32(&c.cleanupInProgress, 0, 1) {
		return
	}
	defer atomic.StoreUint32(&c.cleanupInProgress, 0)

	rt := c.loadWriteRuntime()
	var stopCh chan struct{}
	var doneCh chan struct{}
	var writeDone chan struct{}
	var conn net.Conn
	if rt != nil {
		stopCh = rt.stopCh
		doneCh = rt.doneCh
		writeDone = rt.writeDone
		conn = rt.conn
	}

	// Signal read/write loops.
	if stopCh != nil {
		close(stopCh)
	}

	// Close connection to unblock any blocked read/write syscalls.
	if conn != nil {
		conn.Close()
	}

	// Wait for readLoop to exit.
	if doneCh != nil {
		<-doneCh
	}

	// Wait for writeLoop to exit.
	if writeDone != nil {
		<-writeDone
	}

	c.writeStateMu.Lock()
	c.conn = nil
	c.stopCh = nil
	c.doneCh = nil
	c.writeCh = nil
	c.controlWriteCh = nil
	c.qos0Wake = nil
	c.writeDone = nil
	c.writeRT.Store(nil)
	c.writeStateMu.Unlock()

	c.qos0BufMu.Lock()
	c.qos0Buf = nil
	c.qos0BufMu.Unlock()

	c.pendingMu.Lock()
	c.pendingMessages = 0
	c.pendingBytes = 0
	c.pendingCond.Broadcast()
	c.pendingMu.Unlock()
	c.slowConsumerNotified.Store(false)
	c.draining.Store(false)

	c.outboundMu.Lock()
	c.outboundPendingMessages = 0
	c.outboundPendingBytes = 0
	c.outboundCond.Broadcast()
	c.outboundMu.Unlock()

	c.stopDispatcher()

	// Clear pending operations
	c.pending.clear(err)

	// Clear QoS 2 state
	c.qos2IncomingMu.Lock()
	c.qos2Incoming = make(map[uint16]*Message)
	c.qos2IncomingMu.Unlock()

	// Reset topic aliases
	if c.topicAliases != nil {
		c.topicAliases.reset()
	}

	c.pingMu.Lock()
	c.waitingPing = false
	c.lastPingSent = time.Time{}
	c.pingTimer = nil
	c.pingMu.Unlock()
}

// IsConnected returns true if the client is connected.
func (c *Client) IsConnected() bool {
	return c.state.isConnected()
}

// State returns the current client state.
func (c *Client) State() State {
	return c.state.get()
}

// Publish sends a message to the broker.

func (c *Client) Publish(ctx context.Context, topic string, payload []byte, qos byte, retain bool) error {
	msg := NewMessage(topic, payload, qos, retain)
	_, _, err := c.publishInternal(ctx, msg, true)
	return err
}

// PublishMessage sends a message with optional MQTT 5.0 publish properties.
// For MQTT 3.1.1, publish properties are ignored.
func (c *Client) PublishMessage(ctx context.Context, msg *Message) error {
	_, _, err := c.publishInternal(ctx, msg, true)
	return err
}

func (c *Client) validatePublishMessage(msg *Message) error {
	if msg == nil {
		return ErrInvalidMessage
	}
	if msg.QoS > 2 {
		return ErrInvalidQoS
	}
	if msg.Topic == "" {
		return ErrInvalidTopic
	}
	return nil
}

func (c *Client) publishInternal(ctx context.Context, msg *Message, waitAck bool) (*pendingOp, uint16, error) {
	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return nil, 0, err
		}
	}
	if err := c.validatePublishMessage(msg); err != nil {
		return nil, 0, err
	}

	if c.draining.Load() {
		return nil, 0, ErrDraining
	}

	if !c.state.isConnected() {
		return nil, 0, c.handleDisconnectedPublish(msg)
	}

	if msg.QoS == 0 {
		return nil, 0, c.sendPublish(ctx, msg, 0)
	}

	packetID := c.pending.nextPacketID()
	if packetID == 0 {
		return nil, 0, ErrMaxInflight
	}

	publishMsg := msg.Copy()
	if publishMsg == nil {
		return nil, 0, ErrInvalidMessage
	}
	publishMsg.PacketID = packetID

	if err := c.store.StoreOutbound(packetID, publishMsg); err != nil {
		return nil, 0, err
	}

	op, err := c.pending.add(packetID, pendingPublish, publishMsg)
	if err != nil {
		c.store.DeleteOutbound(packetID)
		return nil, 0, err
	}

	if err := c.sendPublish(ctx, publishMsg, packetID); err != nil {
		c.pending.remove(packetID)
		c.store.DeleteOutbound(packetID)
		return nil, 0, err
	}

	if !waitAck {
		return op, packetID, nil
	}

	if err := op.waitWithContext(ctx, c.opts.AckTimeout); err != nil {
		c.pending.remove(packetID)
		c.store.DeleteOutbound(packetID)
		return nil, 0, err
	}
	return nil, packetID, nil
}

// PublishAsync publishes without blocking for acknowledgments and returns a completion token.
func (c *Client) PublishAsync(ctx context.Context, topic string, payload []byte, qos byte, retain bool) *PublishToken {
	tok := &PublishToken{token: newToken()}
	msg := NewMessage(topic, payload, qos, retain)
	op, packetID, err := c.publishInternal(ctx, msg, false)
	if err != nil {
		tok.complete(err)
		return tok
	}
	tok.MessageID = packetID
	if op != nil {
		tok.bindPending(op, c.opts.AckTimeout)
		return tok
	}
	tok.complete(nil)
	return tok
}

// PublishMessageAsync publishes a message with properties without blocking for acknowledgments.
func (c *Client) PublishMessageAsync(ctx context.Context, msg *Message) *PublishToken {
	tok := &PublishToken{token: newToken()}
	op, packetID, err := c.publishInternal(ctx, msg, false)
	if err != nil {
		tok.complete(err)
		return tok
	}
	tok.MessageID = packetID
	if op != nil {
		tok.bindPending(op, c.opts.AckTimeout)
		return tok
	}
	tok.complete(nil)
	return tok
}

func (c *Client) sendPublish(ctx context.Context, msg *Message, packetID uint16) error {
	return c.sendPublishWithDeadline(ctx, msg, packetID, c.writeDeadline(ctx))
}

func (c *Client) sendPublishWithDeadline(ctx context.Context, msg *Message, packetID uint16, deadline time.Time) error {
	if msg == nil {
		return ErrInvalidMessage
	}

	data := c.encodePublishPacket(msg, packetID, true, nil)
	c.updateActivity()
	return c.queuePublishWrite(ctx, data, deadline, msg, msg.QoS > 0)
}

func (c *Client) encodePublishPacket(msg *Message, packetID uint16, useTopicAlias bool, dst []byte) []byte {
	if msg == nil {
		return dst
	}

	if c.isMQTTv5() {
		pkt := &v5.Publish{
			FixedHeader: packets.FixedHeader{
				PacketType: packets.PublishType,
				QoS:        msg.QoS,
				Retain:     msg.Retain,
				Dup:        msg.Dup,
			},
			TopicName: msg.Topic,
			Payload:   msg.Payload,
			ID:        packetID,
		}

		if msg.PayloadFormat != nil || msg.MessageExpiry != nil || msg.ContentType != "" || msg.ResponseTopic != "" || len(msg.CorrelationData) > 0 || len(msg.UserProperties) > 0 {
			pkt.Properties = &v5.PublishProperties{}
		}

		if pkt.Properties != nil {
			pkt.Properties.PayloadFormat = msg.PayloadFormat
			pkt.Properties.MessageExpiry = msg.MessageExpiry
			pkt.Properties.ContentType = msg.ContentType
			pkt.Properties.ResponseTopic = msg.ResponseTopic
			pkt.Properties.CorrelationData = msg.CorrelationData
		}

		if len(msg.UserProperties) > 0 {
			if pkt.Properties == nil {
				pkt.Properties = &v5.PublishProperties{}
			}
			pkt.Properties.User = make([]v5.User, 0, len(msg.UserProperties))
			for k, v := range msg.UserProperties {
				pkt.Properties.User = append(pkt.Properties.User, v5.User{Key: k, Value: v})
			}
		}

		if useTopicAlias && c.topicAliases != nil {
			if alias, isNew, ok := c.topicAliases.getOrAssignOutbound(msg.Topic); ok {
				if pkt.Properties == nil {
					pkt.Properties = &v5.PublishProperties{}
				}
				pkt.Properties.TopicAlias = &alias
				if !isNew {
					pkt.TopicName = ""
				}
			}
		}

		return pkt.EncodeTo(dst)
	}

	pkt := &v3.Publish{
		FixedHeader: packets.FixedHeader{
			PacketType: packets.PublishType,
			QoS:        msg.QoS,
			Retain:     msg.Retain,
			Dup:        msg.Dup,
		},
		TopicName: msg.Topic,
		Payload:   msg.Payload,
		ID:        packetID,
	}

	return pkt.EncodeTo(dst)
}

func (c *Client) writeDeadline(ctx context.Context) time.Time {
	if ctx != nil {
		if deadline, ok := ctx.Deadline(); ok {
			if c.opts.WriteTimeout > 0 {
				fallback := time.Now().Add(c.opts.WriteTimeout)
				if fallback.Before(deadline) {
					return fallback
				}
			}
			return deadline
		}
	}
	if c.opts.WriteTimeout > 0 {
		return time.Now().Add(c.opts.WriteTimeout)
	}
	return time.Time{}
}

// Subscribe subscribes to one or more topics.
func (c *Client) Subscribe(ctx context.Context, topics map[string]byte) error {
	if err := c.subscribe(ctx, topics); err != nil {
		return err
	}
	for topic, qos := range topics {
		c.subscriptions.setBasic(topic, qos)
	}
	return nil
}

// subscribe performs a protocol subscribe exchange without mutating stored subscription state.
func (c *Client) subscribe(ctx context.Context, topics map[string]byte) error {
	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return err
		}
	}
	if !c.state.isConnected() {
		return ErrNotConnected
	}
	if len(topics) == 0 {
		return ErrInvalidTopic
	}
	for topic, qos := range topics {
		if topic == "" {
			return ErrInvalidTopic
		}
		if qos > 2 {
			return ErrInvalidQoS
		}
	}

	packetID := c.pending.nextPacketID()
	if packetID == 0 {
		return ErrMaxInflight
	}

	op, err := c.pending.add(packetID, pendingSubscribe, nil)
	if err != nil {
		return err
	}

	if err := c.sendSubscribe(ctx, packetID, topics); err != nil {
		c.pending.remove(packetID)
		return err
	}

	if err := op.waitWithContext(ctx, c.opts.AckTimeout); err != nil {
		c.pending.remove(packetID)
		return err
	}

	return nil
}

// SubscribeSingle is a convenience method for subscribing to a single topic.
func (c *Client) SubscribeSingle(ctx context.Context, topic string, qos byte) error {
	return c.Subscribe(ctx, map[string]byte{topic: qos})
}

// SubscribeWithOptions subscribes with advanced MQTT 5.0 options.
// For MQTT 3.1.1 connections, advanced options are ignored.
func (c *Client) SubscribeWithOptions(ctx context.Context, opts ...*SubscribeOption) error {
	if err := c.subscribeWithOptions(ctx, opts); err != nil {
		return err
	}
	for _, opt := range opts {
		c.subscriptions.setOption(opt)
	}
	return nil
}

// subscribeWithOptions performs a protocol subscribe exchange without mutating stored subscription state.
func (c *Client) subscribeWithOptions(ctx context.Context, opts []*SubscribeOption) error {
	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return err
		}
	}
	if !c.state.isConnected() {
		return ErrNotConnected
	}
	if len(opts) == 0 {
		return ErrInvalidTopic
	}
	for _, opt := range opts {
		if opt == nil {
			return ErrInvalidSubscribeOpt
		}
		if opt.Topic == "" {
			return ErrInvalidTopic
		}
		if opt.QoS > 2 {
			return ErrInvalidQoS
		}
		if opt.RetainHandling > 2 {
			return ErrInvalidSubscribeOpt
		}
	}

	packetID := c.pending.nextPacketID()
	if packetID == 0 {
		return ErrMaxInflight
	}

	op, err := c.pending.add(packetID, pendingSubscribe, nil)
	if err != nil {
		return err
	}

	if err := c.sendSubscribeWithOptions(ctx, packetID, opts); err != nil {
		c.pending.remove(packetID)
		return err
	}

	if err := op.waitWithContext(ctx, c.opts.AckTimeout); err != nil {
		c.pending.remove(packetID)
		return err
	}

	return nil
}

func (c *Client) sendSubscribe(ctx context.Context, packetID uint16, topics map[string]byte) error {
	deadline := c.writeDeadline(ctx)

	if c.isMQTTv5() {
		opts := make([]v5.SubOption, 0, len(topics))
		for topic, qos := range topics {
			opts = append(opts, v5.SubOption{Topic: topic, MaxQoS: qos})
		}
		pkt := &v5.Subscribe{
			FixedHeader: packets.FixedHeader{PacketType: packets.SubscribeType, QoS: 1},
			ID:          packetID,
			Opts:        opts,
		}
		c.updateActivity()
		return c.queueWrite(pkt.Encode(), deadline)
	}

	ts := make([]v3.Topic, 0, len(topics))
	for topic, qos := range topics {
		ts = append(ts, v3.Topic{Name: topic, QoS: qos})
	}
	pkt := &v3.Subscribe{
		FixedHeader: packets.FixedHeader{PacketType: packets.SubscribeType, QoS: 1},
		ID:          packetID,
		Topics:      ts,
	}
	c.updateActivity()
	return c.queueWrite(pkt.Encode(), deadline)
}

func (c *Client) sendSubscribeWithOptions(ctx context.Context, packetID uint16, opts []*SubscribeOption) error {
	deadline := c.writeDeadline(ctx)

	if c.isMQTTv5() {
		v5Opts := make([]v5.SubOption, len(opts))
		for i, opt := range opts {
			v5Opts[i] = v5.SubOption{
				Topic:  opt.Topic,
				MaxQoS: opt.QoS,
			}

			if opt.NoLocal {
				noLocal := true
				v5Opts[i].NoLocal = &noLocal
			}

			if opt.RetainAsPublished {
				rap := true
				v5Opts[i].RetainAsPublished = &rap
			}

			if opt.RetainHandling > 0 {
				v5Opts[i].RetainHandling = &opt.RetainHandling
			}
		}

		pkt := &v5.Subscribe{
			FixedHeader: packets.FixedHeader{PacketType: packets.SubscribeType, QoS: 1},
			ID:          packetID,
			Opts:        v5Opts,
		}

		if len(opts) > 0 && opts[0].SubscriptionID > 0 {
			subID := int(opts[0].SubscriptionID)
			pkt.Properties = &v5.SubscribeProperties{
				SubscriptionIdentifier: &subID,
			}
		}

		c.updateActivity()
		return c.queueWrite(pkt.Encode(), deadline)
	}

	ts := make([]v3.Topic, len(opts))
	for i, opt := range opts {
		ts[i] = v3.Topic{Name: opt.Topic, QoS: opt.QoS}
	}
	pkt := &v3.Subscribe{
		FixedHeader: packets.FixedHeader{PacketType: packets.SubscribeType, QoS: 1},
		ID:          packetID,
		Topics:      ts,
	}
	c.updateActivity()
	return c.queueWrite(pkt.Encode(), deadline)
}

// Unsubscribe unsubscribes from one or more topics.
func (c *Client) Unsubscribe(ctx context.Context, topics ...string) error {
	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return err
		}
	}
	if !c.state.isConnected() {
		return ErrNotConnected
	}
	if len(topics) == 0 {
		return ErrInvalidTopic
	}

	packetID := c.pending.nextPacketID()
	if packetID == 0 {
		return ErrMaxInflight
	}

	op, err := c.pending.add(packetID, pendingUnsubscribe, nil)
	if err != nil {
		return err
	}

	if err := c.sendUnsubscribe(ctx, packetID, topics); err != nil {
		c.pending.remove(packetID)
		return err
	}

	if err := op.waitWithContext(ctx, c.opts.AckTimeout); err != nil {
		c.pending.remove(packetID)
		return err
	}

	c.subscriptions.remove(topics...)

	return nil
}

func (c *Client) sendUnsubscribe(ctx context.Context, packetID uint16, topics []string) error {
	deadline := c.writeDeadline(ctx)

	if c.isMQTTv5() {
		pkt := &v5.Unsubscribe{
			FixedHeader: packets.FixedHeader{PacketType: packets.UnsubscribeType, QoS: 1},
			ID:          packetID,
			Topics:      topics,
		}
		c.updateActivity()
		return c.queueWrite(pkt.Encode(), deadline)
	}

	pkt := &v3.Unsubscribe{
		FixedHeader: packets.FixedHeader{PacketType: packets.UnsubscribeType, QoS: 1},
		ID:          packetID,
		Topics:      topics,
	}
	c.updateActivity()
	return c.queueWrite(pkt.Encode(), deadline)
}

// SubscribeAsync subscribes in a background goroutine and returns a completion token.
func (c *Client) SubscribeAsync(ctx context.Context, topics map[string]byte) *SubscribeToken {
	tok := &SubscribeToken{token: newToken()}
	go func() {
		tok.complete(c.Subscribe(ctx, topics))
	}()
	return tok
}

// SubscribeWithOptionsAsync subscribes with MQTT 5 options in a background goroutine.
func (c *Client) SubscribeWithOptionsAsync(ctx context.Context, opts ...*SubscribeOption) *SubscribeToken {
	tok := &SubscribeToken{token: newToken()}
	go func() {
		tok.complete(c.SubscribeWithOptions(ctx, opts...))
	}()
	return tok
}

// UnsubscribeAsync unsubscribes in a background goroutine and returns a completion token.
func (c *Client) UnsubscribeAsync(ctx context.Context, topics ...string) *UnsubscribeToken {
	tok := &UnsubscribeToken{token: newToken()}
	go func() {
		tok.complete(c.Unsubscribe(ctx, topics...))
	}()
	return tok
}

// readLoop reads packets from the connection.

func (c *Client) readLoop() {
	rt := c.loadWriteRuntime()
	if rt == nil || rt.doneCh == nil {
		return
	}
	defer close(rt.doneCh)

	conn := rt.conn

	for {
		if conn == nil {
			return
		}

		pkt, err := c.readPacketFromConn(conn)

		if err != nil {
			if err == io.EOF || c.state.get() != StateConnected {
				return
			}
			c.handleConnectionLost(err)
			return
		}

		c.updateActivity()
		c.handlePacket(pkt)
	}
}

func (c *Client) handlePacket(pkt packets.ControlPacket) {
	switch pkt.Type() {
	case packets.PublishType:
		c.handlePublish(pkt)
	case packets.PubAckType:
		c.handlePubAck(pkt)
	case packets.PubRecType:
		c.handlePubRec(pkt)
	case packets.PubRelType:
		c.handlePubRel(pkt)
	case packets.PubCompType:
		c.handlePubComp(pkt)
	case packets.SubAckType:
		c.handleSubAck(pkt)
	case packets.UnsubAckType:
		c.handleUnsubAck(pkt)
	case packets.PingRespType:
		c.pingMu.Lock()
		c.waitingPing = false
		c.lastPingSent = time.Time{}
		c.pingMu.Unlock()
	case packets.DisconnectType:
		c.handleServerDisconnect(pkt)
	case packets.AuthType:
		c.handleAuth(pkt)
	}
}

func (c *Client) handlePublish(pkt packets.ControlPacket) {
	var msg *Message

	if c.isMQTTv5() {
		p := pkt.(*v5.Publish)

		topic := p.TopicName

		// Handle topic alias
		if c.topicAliases != nil && p.Properties != nil && p.Properties.TopicAlias != nil {
			alias := *p.Properties.TopicAlias

			if topic != "" {
				// Server sent both topic and alias - register the mapping
				c.topicAliases.registerInbound(alias, topic)
			} else {
				// Server sent only alias - resolve it
				var ok bool
				topic, ok = c.topicAliases.resolveInbound(alias)
				if !ok {
					// Unknown alias - protocol error, ignore message
					return
				}
			}
		}

		msg = &Message{
			Topic:    topic,
			Payload:  p.Payload,
			QoS:      p.QoS,
			Retain:   p.Retain,
			Dup:      p.Dup,
			PacketID: p.ID,
		}

		// Parse v5 properties
		if p.Properties != nil {
			msg.PayloadFormat = p.Properties.PayloadFormat
			msg.MessageExpiry = p.Properties.MessageExpiry
			msg.ContentType = p.Properties.ContentType
			msg.ResponseTopic = p.Properties.ResponseTopic
			msg.CorrelationData = p.Properties.CorrelationData

			if len(p.Properties.User) > 0 {
				msg.UserProperties = make(map[string]string, len(p.Properties.User))
				for _, u := range p.Properties.User {
					msg.UserProperties[u.Key] = u.Value
				}
			}

			if p.Properties.SubscriptionID != nil {
				msg.SubscriptionIDs = []uint32{uint32(*p.Properties.SubscriptionID)}
			}
		}
	} else {
		p := pkt.(*v3.Publish)
		msg = &Message{
			Topic:    p.TopicName,
			Payload:  p.Payload,
			QoS:      p.QoS,
			Retain:   p.Retain,
			Dup:      p.Dup,
			PacketID: p.ID,
		}
	}

	switch msg.QoS {
	case 0:
		c.deliverMessage(msg)
	case 1:
		c.deliverMessage(msg)
		c.sendPubAck(msg.PacketID)
	case 2:
		c.qos2IncomingMu.Lock()
		c.qos2Incoming[msg.PacketID] = msg
		c.qos2IncomingMu.Unlock()
		_ = c.store.StoreInbound(msg.PacketID, msg)
		c.sendPubRec(msg.PacketID)
	}
}

func (c *Client) deliverMessage(msg *Message) {
	if msg == nil {
		return
	}
	c.deliverWithSharedDispatcher(msg)
}

// deliverWithSharedDispatcher keeps the current connection-wide dispatch model.
// This seam allows introducing per-subscription delivery without touching callers.
func (c *Client) deliverWithSharedDispatcher(msg *Message) {
	msgCh, msgStop := c.dispatcherChannels()
	if msgCh == nil {
		c.handleDeliveredMessage(msg)
		return
	}

	if c.opts.OrderMatters {
		select {
		case msgCh <- msg:
		case <-msgStop:
		}
		return
	}

	policy := c.opts.SlowConsumerPolicy
	if policy == "" {
		policy = SlowConsumerDropNew
	}

	msgBytes := int64(mqttMessageSize(msg))

	switch policy {
	case SlowConsumerDropOldest:
		c.deliverDropOldest(msgCh, msgStop, msg, msgBytes)
	case SlowConsumerBlockWithTimeout:
		c.deliverBlockWithTimeout(msgCh, msgStop, msg, msgBytes)
	default:
		c.deliverDropNew(msgCh, msgStop, msg, msgBytes)
	}
}

func (c *Client) handleDeliveredMessage(msg *Message) {
	// Check if this is a queue message
	if isQueueTopic(msg.Topic) {
		c.handleQueueMessage(msg)
		return
	}

	// Call OnMessageV2 if set (provides full message context)
	if c.opts.OnMessageV2 != nil {
		c.opts.OnMessageV2(msg)
		return
	}

	// Fall back to OnMessage for backward compatibility
	if c.opts.OnMessage != nil {
		c.opts.OnMessage(msg.Topic, msg.Payload, msg.QoS)
	}
}

func (c *Client) startDispatcher() {
	c.stopDispatcher()

	size := c.opts.MessageChanSize
	if size <= 0 {
		size = DefaultMessageChanSize
	}

	msgCh := make(chan *Message, size)
	msgStop := make(chan struct{})
	c.dispatchMu.Lock()
	c.msgCh = msgCh
	c.msgStop = msgStop
	c.dispatchRT.Store(&dispatchRuntime{
		msgCh:   msgCh,
		msgStop: msgStop,
	})
	c.dispatchMu.Unlock()

	workers := 1
	if !c.opts.OrderMatters {
		workers = runtime.GOMAXPROCS(0)
		if workers < 2 {
			workers = 2
		}
		if workers > 8 {
			workers = 8
		}
	}

	for i := 0; i < workers; i++ {
		c.dispatchWg.Add(1)
		go func(ch <-chan *Message, stop <-chan struct{}) {
			defer c.dispatchWg.Done()
			for {
				select {
				case <-stop:
					return
				case msg, ok := <-ch:
					if !ok {
						return
					}
					c.releasePending(int64(mqttMessageSize(msg)))
					c.handleDeliveredMessage(msg)
				}
			}
		}(msgCh, msgStop)
	}
}

func (c *Client) stopDispatcher() {
	c.dispatchMu.Lock()
	msgStop := c.msgStop
	if msgStop == nil {
		c.dispatchMu.Unlock()
		return
	}
	c.msgStop = nil
	c.msgCh = nil
	c.dispatchRT.Store(nil)
	close(msgStop)
	c.dispatchMu.Unlock()
	c.dispatchWg.Wait()
}

// handleQueueMessage processes a queue message and calls the appropriate handler.
func (c *Client) handleQueueMessage(msg *Message) {
	// Get queue subscription
	sub, ok := c.queueSubs.get(msg.Topic)
	if !ok || sub.handler == nil {
		// No handler registered, fall through to default handlers
		if c.opts.OnMessageV2 != nil {
			c.opts.OnMessageV2(msg)
		} else if c.opts.OnMessage != nil {
			c.opts.OnMessage(msg.Topic, msg.Payload, msg.QoS)
		}
		return
	}

	// Extract queue message metadata from user properties
	var messageID string
	var groupID string
	var offset uint64

	if msg.UserProperties != nil {
		if msgID, ok := msg.UserProperties["message-id"]; ok {
			messageID = msgID
		}
		if gid, ok := msg.UserProperties["group-id"]; ok {
			groupID = gid
		}
		if off, ok := msg.UserProperties["offset"]; ok {
			if parsed, err := strconv.ParseUint(off, 10, 64); err == nil {
				offset = parsed
			}
		} else if seq, ok := msg.UserProperties["sequence"]; ok {
			if parsed, err := strconv.ParseUint(seq, 10, 64); err == nil {
				offset = parsed
			}
		}
	}

	if c.queueAckCache != nil {
		c.queueAckCache.set(messageID, groupID)
	}

	// Create queue message with ack/nack/reject methods
	queueMsg := &QueueMessage{
		Message:   msg,
		MessageID: messageID,
		GroupID:   groupID,
		Offset:    offset,
		Sequence:  offset,
		client:    c,
		queueName: sub.queueName,
	}

	// Call handler
	sub.handler(queueMsg)
}

func (c *Client) handlePubAck(pkt packets.ControlPacket) {
	packetID := packetIDFromControlPacket(pkt)
	c.store.DeleteOutbound(packetID)
	c.pending.complete(packetID, nil, nil)
}

func (c *Client) handlePubRec(pkt packets.ControlPacket) {
	packetID := packetIDFromControlPacket(pkt)

	c.pending.updateQoS2State(packetID, 1) // Now waiting for PUBCOMP
	c.sendPubRel(packetID)
}

func (c *Client) handlePubRel(pkt packets.ControlPacket) {
	packetID := packetIDFromControlPacket(pkt)

	c.qos2IncomingMu.Lock()
	msg, exists := c.qos2Incoming[packetID]
	if exists {
		delete(c.qos2Incoming, packetID)
	}
	c.qos2IncomingMu.Unlock()

	if !exists {
		if storedMsg, ok := c.store.GetInbound(packetID); ok {
			msg = storedMsg
			exists = true
		}
	}

	if exists {
		_ = c.store.DeleteInbound(packetID)
	}

	if exists && msg != nil {
		c.deliverMessage(msg)
	}

	c.sendPubComp(packetID)
}

func (c *Client) handlePubComp(pkt packets.ControlPacket) {
	packetID := packetIDFromControlPacket(pkt)
	c.store.DeleteOutbound(packetID)
	c.pending.complete(packetID, nil, nil)
}

func (c *Client) handleSubAck(pkt packets.ControlPacket) {
	packetID, returnCodes, isV5 := subAckDetails(pkt)

	var err error
	for _, rc := range returnCodes {
		if isV5 {
			if rc >= 0x80 {
				err = ErrSubscribeFailed
				break
			}
			continue
		}
		if rc == 0x80 {
			err = ErrSubscribeFailed
			break
		}
	}

	c.pending.complete(packetID, err, returnCodes)
}

func (c *Client) handleUnsubAck(pkt packets.ControlPacket) {
	packetID := packetIDFromControlPacket(pkt)
	c.pending.complete(packetID, nil, nil)
}

func (c *Client) sendPubAck(packetID uint16) {
	c.sendControlAck(c.encodeQoSControlPacket(packets.PubAckType, packetID, 0))
}

func (c *Client) sendPubRec(packetID uint16) {
	c.sendControlAck(c.encodeQoSControlPacket(packets.PubRecType, packetID, 0))
}

func (c *Client) sendPubRel(packetID uint16) {
	c.sendControlAck(c.encodeQoSControlPacket(packets.PubRelType, packetID, 1))
}

func (c *Client) sendPubComp(packetID uint16) {
	c.sendControlAck(c.encodeQoSControlPacket(packets.PubCompType, packetID, 0))
}

func (c *Client) sendControlAck(data []byte) {
	if err := c.queueControlWrite(data, c.writeDeadline(nil)); err != nil {
		c.reportAsyncError(err)
	}
}

func (c *Client) handleServerDisconnect(pkt packets.ControlPacket) {
	if !c.isMQTTv5() {
		return
	}

	d := pkt.(*v5.Disconnect)
	reason := fmt.Sprintf("server disconnect: reason code 0x%02X", d.ReasonCode)
	if d.Properties != nil && d.Properties.ReasonString != "" {
		reason += " (" + d.Properties.ReasonString + ")"
	}

	c.handleConnectionLost(fmt.Errorf("%w: %s", ErrConnectionLost, reason))
}

// handleConnectAuth handles AUTH packets received during the CONNECT handshake.
func (c *Client) handleConnectAuth(conn net.Conn, auth *v5.Auth) error {
	if c.opts.OnAuth == nil {
		return ErrNoAuthHandler
	}

	method := c.opts.AuthMethod
	var data []byte
	if auth.Properties != nil {
		if auth.Properties.AuthMethod != "" {
			if c.opts.AuthMethod != "" && c.opts.AuthMethod != auth.Properties.AuthMethod {
				return ErrAuthMethodMismatch
			}
			method = auth.Properties.AuthMethod
		}
		data = auth.Properties.AuthData
	}

	if method == "" {
		return ErrAuthMethodMissing
	}

	responseData, err := c.opts.OnAuth(auth.ReasonCode, method, data)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrAuthFailed, err)
	}

	resp := &v5.Auth{
		FixedHeader: packets.FixedHeader{PacketType: packets.AuthType},
		ReasonCode:  0x18, // Continue Authentication
		Properties: &v5.AuthProperties{
			AuthMethod: method,
			AuthData:   responseData,
		},
	}

	return resp.Pack(conn)
}

// handleAuth handles AUTH packets received during an active session (re-authentication).
func (c *Client) handleAuth(pkt packets.ControlPacket) {
	if !c.isMQTTv5() {
		return
	}

	auth := pkt.(*v5.Auth)
	if c.opts.OnAuth == nil {
		return
	}

	method := c.opts.AuthMethod
	var data []byte
	if auth.Properties != nil {
		if auth.Properties.AuthMethod != "" {
			if c.opts.AuthMethod != "" && c.opts.AuthMethod != auth.Properties.AuthMethod {
				c.handleConnectionLost(fmt.Errorf("%w: %s", ErrAuthMethodMismatch, auth.Properties.AuthMethod))
				return
			}
			method = auth.Properties.AuthMethod
		}
		data = auth.Properties.AuthData
	}

	if method == "" {
		c.handleConnectionLost(ErrAuthMethodMissing)
		return
	}

	responseData, err := c.opts.OnAuth(auth.ReasonCode, method, data)
	if err != nil {
		c.handleConnectionLost(fmt.Errorf("%w: %v", ErrAuthFailed, err))
		return
	}

	// Reason code 0x00 means success — no response needed
	if auth.ReasonCode == 0x00 {
		return
	}

	resp := &v5.Auth{
		FixedHeader: packets.FixedHeader{PacketType: packets.AuthType},
		ReasonCode:  0x18, // Continue Authentication
		Properties: &v5.AuthProperties{
			AuthMethod: method,
			AuthData:   responseData,
		},
	}

	c.queueControlWriteNoWait(resp.Encode())
}

// SendAuth initiates re-authentication by sending an AUTH packet to the server (MQTT 5.0).
func (c *Client) SendAuth(reasonCode byte, authData []byte) error {
	if !c.isMQTTv5() {
		return ErrAuthNotV5
	}
	if c.state.get() != StateConnected {
		return ErrNotConnected
	}
	if c.opts.AuthMethod == "" {
		return ErrAuthMethodMissing
	}

	pkt := &v5.Auth{
		FixedHeader: packets.FixedHeader{PacketType: packets.AuthType},
		ReasonCode:  reasonCode,
		Properties: &v5.AuthProperties{
			AuthMethod: c.opts.AuthMethod,
			AuthData:   authData,
		},
	}

	return c.queueControlWrite(pkt.Encode(), time.Time{})
}

func (c *Client) handleConnectionLost(err error) {
	c.lifecycleMu.Lock()
	if !c.state.transition(StateConnected, StateDisconnected) {
		c.lifecycleMu.Unlock()
		return
	}
	c.lifecycleMu.Unlock()

	c.stopKeepAlive()
	c.cleanup(ErrConnectionLost)

	if c.opts.OnConnectionLost != nil {
		go c.opts.OnConnectionLost(err)
	}

	if c.opts.AutoReconnect && !c.state.isClosed() {
		go c.reconnect()
	}
}

func (c *Client) reconnect() {
	c.reconnMu.Lock()
	defer c.reconnMu.Unlock()

	if !c.state.transition(StateDisconnected, StateReconnecting) {
		return
	}

	delay := c.opts.ReconnectBackoff
	attempt := 0

	for !c.state.isClosed() {
		attempt++

		if c.opts.OnReconnecting != nil {
			c.opts.OnReconnecting(attempt)
		}

		err := c.Connect()
		if err == nil {
			return
		}
		if c.opts.MaxReconnectAttempts > 0 && attempt >= c.opts.MaxReconnectAttempts {
			if c.opts.OnReconnectFailed != nil {
				go c.opts.OnReconnectFailed(err)
			}
			return
		}

		sleepFor := delay
		if c.opts.ReconnectJitter > 0 {
			sleepFor += time.Duration(rand.Int64N(int64(c.opts.ReconnectJitter) + 1))
		}

		// Exponential backoff
		time.Sleep(sleepFor)
		delay *= 2
		if delay > c.opts.MaxReconnectWait {
			delay = c.opts.MaxReconnectWait
		}
	}
}

func (c *Client) restoreState() {
	if !c.state.isConnected() {
		return
	}

	c.restoreSubscriptions()
	c.restoreQueueSubscriptions()
	c.restoreOutboundMessages()
}

func (c *Client) restoreSubscriptions() {
	records := c.subscriptions.snapshot()
	if len(records) == 0 {
		return
	}

	for _, rec := range records {
		if rec.opt != nil {
			_ = c.subscribeWithOptions(nil, []*SubscribeOption{rec.opt})
			continue
		}
		_ = c.subscribe(nil, map[string]byte{rec.topic: rec.qos})
	}
}

func (c *Client) restoreQueueSubscriptions() {
	c.queueSubs.mu.RLock()
	subs := make([]*queueSubscription, 0, len(c.queueSubs.subs))
	for _, sub := range c.queueSubs.subs {
		copied := *sub
		subs = append(subs, &copied)
	}
	c.queueSubs.mu.RUnlock()

	for _, sub := range subs {
		if c.isMQTTv5() {
			userProps := make(map[string]string)
			if sub.consumerGroup != "" {
				userProps["consumer-group"] = sub.consumerGroup
			}
			_ = c.subscribeWithUserProperties(nil, "$queue/"+sub.queueName, 1, userProps)
			continue
		}
		_ = c.subscribe(nil, map[string]byte{"$queue/" + sub.queueName: 1})
	}
}

func (c *Client) restoreOutboundMessages() {
	msgs := c.store.GetAllOutbound()
	if len(msgs) == 0 {
		return
	}

	_ = c.store.Reset()

	for _, msg := range msgs {
		if msg == nil {
			continue
		}

		if msg.QoS == 0 {
			_ = c.sendPublish(nil, msg, 0)
			continue
		}

		packetID := c.pending.nextPacketID()
		if packetID == 0 {
			return
		}

		replay := msg.Copy()
		replay.PacketID = packetID
		replay.Dup = true

		if err := c.store.StoreOutbound(packetID, replay); err != nil {
			continue
		}

		if _, err := c.pending.add(packetID, pendingPublish, replay); err != nil {
			_ = c.store.DeleteOutbound(packetID)
			continue
		}

		if err := c.sendPublish(nil, replay, packetID); err != nil {
			c.pending.remove(packetID)
			_ = c.store.DeleteOutbound(packetID)
			continue
		}
	}
}

func (c *Client) handleDisconnectedPublish(msg *Message) error {
	if err := c.bufferDisconnectedPublish(msg); err != nil {
		if err == ErrReconnectBufferFull {
			c.reportAsyncError(err)
			meta := &DroppedMessage{
				Direction: DroppedMessageOutbound,
				Reason:    DroppedReasonReconnectBufferFull,
				Timestamp: time.Now().UTC(),
			}
			if msg != nil {
				meta.PayloadSize = len(msg.Payload)
				meta.Topic = msg.Topic
				meta.QoS = msg.QoS
			}
			c.reportDroppedMessage(c.prepareDroppedMeta(meta, DroppedMessageOutbound, DroppedReasonReconnectBufferFull))
		}
		return err
	}
	return nil
}

func (c *Client) bufferDisconnectedPublish(msg *Message) error {
	if msg == nil {
		return ErrInvalidMessage
	}
	if c.state.isClosed() {
		return ErrClientClosed
	}
	if !c.opts.AutoReconnect || c.opts.ReconnectBufSize <= 0 {
		return ErrNotConnected
	}

	state := c.state.get()
	if state != StateDisconnected && state != StateReconnecting && state != StateConnecting {
		return ErrNotConnected
	}

	wire := c.encodePublishPacket(msg, 0, false, nil)
	msgBytes := len(wire)
	if msgBytes <= 0 {
		msgBytes = 1
	}
	entry := &reconnectBuffered{
		wire:       wire,
		topic:      msg.Topic,
		qos:        msg.QoS,
		payloadSz:  len(msg.Payload),
		bufferedSz: msgBytes,
	}

	c.reconnectBufMu.Lock()
	defer c.reconnectBufMu.Unlock()

	if entry.bufferedSz > c.opts.ReconnectBufSize || c.reconnectBufBytes+entry.bufferedSz > c.opts.ReconnectBufSize {
		return ErrReconnectBufferFull
	}

	c.reconnectBuf = append(c.reconnectBuf, entry)
	c.reconnectBufBytes += entry.bufferedSz

	return nil
}

func (c *Client) flushReconnectBuffer() {
	if !c.state.isConnected() {
		return
	}

	c.reconnectBufMu.Lock()
	entries := c.reconnectBuf
	c.reconnectBuf = nil
	c.reconnectBufBytes = 0
	c.reconnectBufMu.Unlock()

	for i, entry := range entries {
		if entry == nil {
			continue
		}
		if len(entry.wire) == 0 {
			continue
		}

		if entry.qos == 0 {
			if err := c.queuePublishRawWrite(nil, entry.wire, c.writeDeadline(nil), entry.topic, entry.qos, entry.payloadSz, true); err != nil {
				c.prependReconnectBuffer(entries[i:])
				c.reportAsyncError(err)
				return
			}
			continue
		}

		packetID := c.pending.nextPacketID()
		if packetID == 0 {
			c.prependReconnectBuffer(entries[i:])
			c.reportAsyncError(ErrMaxInflight)
			return
		}

		msg, err := c.decodeBufferedPublish(entry.wire)
		if err != nil {
			c.prependReconnectBuffer(entries[i:])
			c.reportAsyncError(err)
			return
		}
		msg.PacketID = packetID

		if err := c.store.StoreOutbound(packetID, msg); err != nil {
			c.prependReconnectBuffer(entries[i:])
			c.reportAsyncError(err)
			return
		}

		if _, err := c.pending.add(packetID, pendingPublish, msg); err != nil {
			_ = c.store.DeleteOutbound(packetID)
			c.prependReconnectBuffer(entries[i:])
			c.reportAsyncError(err)
			return
		}

		if err := patchPublishPacketID(entry.wire, packetID); err != nil {
			c.pending.remove(packetID)
			_ = c.store.DeleteOutbound(packetID)
			c.prependReconnectBuffer(entries[i:])
			c.reportAsyncError(err)
			return
		}

		if err := c.queuePublishRawWrite(nil, entry.wire, c.writeDeadline(nil), entry.topic, entry.qos, entry.payloadSz, true); err != nil {
			c.pending.remove(packetID)
			_ = c.store.DeleteOutbound(packetID)
			c.prependReconnectBuffer(entries[i:])
			c.reportAsyncError(err)
			return
		}
	}
}

func (c *Client) prependReconnectBuffer(entries []*reconnectBuffered) {
	if len(entries) == 0 {
		return
	}

	total := 0
	for _, entry := range entries {
		if entry == nil {
			continue
		}
		sz := entry.bufferedSz
		if sz <= 0 {
			sz = len(entry.wire)
			if sz <= 0 {
				sz = 1
			}
			entry.bufferedSz = sz
		}
		total += sz
	}

	c.reconnectBufMu.Lock()
	defer c.reconnectBufMu.Unlock()

	combined := make([]*reconnectBuffered, 0, len(entries)+len(c.reconnectBuf))
	combined = append(combined, entries...)
	combined = append(combined, c.reconnectBuf...)
	c.reconnectBuf = combined
	c.reconnectBufBytes += total
}

func patchPublishPacketID(wire []byte, packetID uint16) error {
	if len(wire) < 4 {
		return ErrInvalidMessage
	}

	// Skip fixed header and remaining-length VBI.
	i := 1
	for n := 0; n < 4; n++ {
		if i >= len(wire) {
			return ErrInvalidMessage
		}
		b := wire[i]
		i++
		if (b & 0x80) == 0 {
			break
		}
		if n == 3 {
			return ErrInvalidMessage
		}
	}

	// Topic name length and bytes.
	if i+2 > len(wire) {
		return ErrInvalidMessage
	}
	topicLen := int(wire[i])<<8 | int(wire[i+1])
	i += 2 + topicLen
	if i+2 > len(wire) {
		return ErrInvalidMessage
	}

	wire[i] = byte(packetID >> 8)
	wire[i+1] = byte(packetID)
	return nil
}

func (c *Client) decodeBufferedPublish(wire []byte) (*Message, error) {
	if len(wire) == 0 {
		return nil, ErrInvalidMessage
	}

	if c.isMQTTv5() {
		pkt, _, _, err := v5.ReadPacket(bytes.NewReader(wire))
		if err != nil {
			return nil, err
		}
		p, ok := pkt.(*v5.Publish)
		if !ok {
			return nil, ErrUnexpectedPacket
		}
		msg := &Message{
			Topic:    p.TopicName,
			Payload:  p.Payload,
			QoS:      p.QoS,
			Retain:   p.Retain,
			Dup:      p.Dup,
			PacketID: p.ID,
		}
		if p.Properties != nil {
			msg.PayloadFormat = p.Properties.PayloadFormat
			msg.MessageExpiry = p.Properties.MessageExpiry
			msg.ContentType = p.Properties.ContentType
			msg.ResponseTopic = p.Properties.ResponseTopic
			msg.CorrelationData = p.Properties.CorrelationData
			if len(p.Properties.User) > 0 {
				msg.UserProperties = make(map[string]string, len(p.Properties.User))
				for _, u := range p.Properties.User {
					msg.UserProperties[u.Key] = u.Value
				}
			}
		}
		return msg, nil
	}

	pkt, err := v3.ReadPacket(bytes.NewReader(wire))
	if err != nil {
		return nil, err
	}
	p, ok := pkt.(*v3.Publish)
	if !ok {
		return nil, ErrUnexpectedPacket
	}
	return &Message{
		Topic:    p.TopicName,
		Payload:  p.Payload,
		QoS:      p.QoS,
		Retain:   p.Retain,
		Dup:      p.Dup,
		PacketID: p.ID,
	}, nil
}

// Keep-alive management

func (c *Client) startKeepAlive() {
	c.stopKeepAlive()
	interval := c.opts.KeepAlive

	var tick func()
	tick = func() {
		idle := time.Since(time.Unix(0, c.lastActivity.Load()))
		if idle >= interval/2 {
			c.sendPing()
		}
		c.pingMu.Lock()
		if c.pingTimer != nil {
			c.pingTimer.Reset(interval / 2)
		}
		c.pingMu.Unlock()
	}

	c.pingMu.Lock()
	c.pingTimer = time.AfterFunc(interval/2, tick)
	c.pingMu.Unlock()
}

func (c *Client) stopKeepAlive() {
	c.pingMu.Lock()
	if c.pingTimer != nil {
		c.pingTimer.Stop()
		c.pingTimer = nil
	}
	c.pingMu.Unlock()
}

func (c *Client) sendPing() {
	timeout := c.opts.PingTimeout
	if timeout <= 0 {
		timeout = DefaultPingTimeout
	}

	c.pingMu.Lock()
	if c.waitingPing {
		sincePing := time.Since(c.lastPingSent)
		c.pingMu.Unlock()
		if sincePing >= timeout {
			c.handleConnectionLost(ErrPingTimeout)
		}
		return
	}
	c.waitingPing = true
	c.lastPingSent = time.Now().UTC()
	c.pingMu.Unlock()

	pingReq := c.encodePingReqPacket()

	if !c.queueControlWriteNoWait(pingReq) {
		c.pingMu.Lock()
		c.waitingPing = false
		c.lastPingSent = time.Time{}
		c.pingMu.Unlock()
		return
	}

	c.updateActivity()
}

func (c *Client) writeLoop() {
	rt := c.loadWriteRuntime()
	if rt == nil {
		return
	}

	stopCh := rt.stopCh
	writeCh := rt.writeCh
	controlWriteCh := rt.controlWriteCh
	qos0Wake := rt.qos0Wake
	writeDone := rt.writeDone
	conn := rt.conn

	if stopCh == nil || writeCh == nil || controlWriteCh == nil || qos0Wake == nil || writeDone == nil {
		return
	}

	defer close(writeDone)

	const maxWriteBatchItems = 32
	const maxWriteBatchBytes = 256 * 1024

	batch := make([]writeRequest, 0, maxWriteBatchItems)
	buffers := make(net.Buffers, 0, maxWriteBatchItems)

	resetBatch := func() {
		batch = batch[:0]
		buffers = buffers[:0]
	}
	appendBatch := func(req writeRequest) {
		batch = append(batch, req)
		buffers = append(buffers, req.data)
	}

	writeBatch := func() bool {
		if len(batch) == 0 {
			return true
		}

		ackBatch := func(err error) {
			for _, req := range batch {
				if req.trackOutbound {
					c.releaseOutbound(req)
				}
				if req.errCh != nil {
					req.errCh <- err
				}
			}
		}

		if conn == nil {
			ackBatch(ErrNotConnected)
			resetBatch()
			return true
		}

		deadline := time.Time{}
		for _, req := range batch {
			if req.deadline.IsZero() {
				continue
			}
			if deadline.IsZero() || req.deadline.Before(deadline) {
				deadline = req.deadline
			}
		}

		if !deadline.IsZero() {
			conn.SetWriteDeadline(deadline)
		}
		_, err := buffers.WriteTo(conn)
		if !deadline.IsZero() {
			conn.SetWriteDeadline(time.Time{})
		}

		ackBatch(err)
		resetBatch()
		return err == nil
	}

	collectBatch := func(first writeRequest) {
		resetBatch()
		appendBatch(first)
		totalBytes := len(first.data)

		for len(batch) < maxWriteBatchItems && totalBytes < maxWriteBatchBytes {
			select {
			case req := <-controlWriteCh:
				appendBatch(req)
				totalBytes += len(req.data)
				continue
			default:
			}

			select {
			case req := <-controlWriteCh:
				appendBatch(req)
				totalBytes += len(req.data)
			case req := <-writeCh:
				appendBatch(req)
				totalBytes += len(req.data)
			default:
				return
			}
		}
	}

	collectBufferedQoS0Batch := func() bool {
		extra := c.popBufferedQoS0Batch(maxWriteBatchItems, maxWriteBatchBytes)
		if len(extra) == 0 {
			return false
		}

		resetBatch()
		for _, req := range extra {
			appendBatch(req)
		}
		return true
	}

	for {
		// Prefer control frames whenever present.
		select {
		case <-stopCh:
			return
		case req := <-controlWriteCh:
			collectBatch(req)
			if !writeBatch() {
				return
			}
			continue
		default:
		}

		select {
		case <-stopCh:
			return
		case req := <-controlWriteCh:
			collectBatch(req)
			if !writeBatch() {
				return
			}
		case <-qos0Wake:
			if !collectBufferedQoS0Batch() {
				continue
			}
			// Keep draining buffered QoS0 writes until empty.
			if c.hasBufferedQoS0Writes() {
				select {
				case qos0Wake <- struct{}{}:
				default:
				}
			}
			if !writeBatch() {
				return
			}
		case req := <-writeCh:
			collectBatch(req)
			if !writeBatch() {
				return
			}
		}
	}
}

func (c *Client) enqueueWriteTo(req writeRequest, control bool) (retErr error) {
	reservedOutbound := false
	defer func() {
		if retErr != nil && reservedOutbound {
			c.releaseOutbound(req)
		}
	}()
	defer func() {
		if recover() != nil {
			retErr = ErrNotConnected
		}
	}()
	rt := c.loadWriteRuntime()
	if rt == nil {
		return ErrNotConnected
	}
	dataCh := rt.writeCh
	controlCh := rt.controlWriteCh
	sch := rt.stopCh
	done := rt.writeDone
	if dataCh == nil || controlCh == nil || sch == nil || done == nil {
		return ErrNotConnected
	}
	targetCh := dataCh
	if control {
		targetCh = controlCh
	}
	if !control && req.trackOutbound {
		if err := c.reserveOutbound(req); err != nil {
			return err
		}
		reservedOutbound = true
	}

	select {
	case <-sch:
		return ErrNotConnected
	case <-done:
		return ErrNotConnected
	default:
	}

	select {
	case targetCh <- req:
		return nil
	case <-sch:
		return ErrNotConnected
	case <-done:
		return ErrNotConnected
	}
}

func (c *Client) enqueueWrite(req writeRequest) error {
	return c.enqueueWriteTo(req, false)
}

func (c *Client) enqueueControlWrite(req writeRequest) error {
	return c.enqueueWriteTo(req, true)
}

func acquireWriteErrCh() chan error {
	return writeErrChPool.Get().(chan error)
}

func releaseWriteErrCh(ch chan error) {
	if ch == nil {
		return
	}
	select {
	case <-ch:
	default:
	}
	writeErrChPool.Put(ch)
}

func (c *Client) queueWrite(data []byte, deadline time.Time) error {
	return c.queueWriteRequest(writeRequest{
		data:        data,
		deadline:    deadline,
		errCh:       acquireWriteErrCh(),
		pooledErrCh: true,
	}, false)
}

func (c *Client) queuePublishWrite(ctx context.Context, data []byte, deadline time.Time, msg *Message, waitForWrite bool) error {
	dropPayloadSz := len(data)
	dropTopic := ""
	dropQoS := byte(0)
	if msg != nil {
		dropTopic = msg.Topic
		dropQoS = msg.QoS
		dropPayloadSz = len(msg.Payload)
	}

	var errCh chan error
	pooled := false
	if waitForWrite {
		errCh = acquireWriteErrCh()
		pooled = true
	}

	req := writeRequest{
		data:          data,
		deadline:      deadline,
		errCh:         errCh,
		pooledErrCh:   pooled,
		ctx:           ctx,
		trackOutbound: true,
		dropTopic:     dropTopic,
		dropQoS:       dropQoS,
		dropPayloadSz: dropPayloadSz,
	}

	// QoS0 path: enqueue into shared buffered writer and return immediately.
	if !waitForWrite {
		return c.enqueueBufferedQoS0Write(req)
	}

	return c.queueWriteRequest(req, false)
}

func (c *Client) queuePublishRawWrite(ctx context.Context, data []byte, deadline time.Time, topic string, qos byte, payloadSz int, waitForWrite bool) error {
	var errCh chan error
	pooled := false
	if waitForWrite {
		errCh = acquireWriteErrCh()
		pooled = true
	}
	if payloadSz <= 0 {
		payloadSz = len(data)
	}

	req := writeRequest{
		data:          data,
		deadline:      deadline,
		errCh:         errCh,
		pooledErrCh:   pooled,
		ctx:           ctx,
		trackOutbound: true,
		dropTopic:     topic,
		dropQoS:       qos,
		dropPayloadSz: payloadSz,
	}

	if !waitForWrite {
		return c.enqueueBufferedQoS0Write(req)
	}

	return c.queueWriteRequest(req, false)
}

func (c *Client) enqueueBufferedQoS0Write(req writeRequest) (retErr error) {
	if !req.trackOutbound {
		return c.queueWriteRequest(req, false)
	}

	if err := c.reserveOutbound(req); err != nil {
		return err
	}
	defer func() {
		if retErr != nil {
			c.releaseOutbound(req)
		}
	}()

	rt := c.loadWriteRuntime()
	if rt == nil || rt.qos0Wake == nil || rt.stopCh == nil || rt.writeDone == nil {
		return ErrNotConnected
	}

	select {
	case <-rt.stopCh:
		return ErrNotConnected
	case <-rt.writeDone:
		return ErrNotConnected
	default:
	}

	c.qos0BufMu.Lock()
	c.qos0Buf = append(c.qos0Buf, req)
	c.qos0BufMu.Unlock()

	select {
	case rt.qos0Wake <- struct{}{}:
	default:
	}

	return nil
}

func (c *Client) popBufferedQoS0Batch(maxItems, maxBytes int) []writeRequest {
	if maxItems <= 0 || maxBytes <= 0 {
		return nil
	}

	c.qos0BufMu.Lock()
	defer c.qos0BufMu.Unlock()
	if len(c.qos0Buf) == 0 {
		return nil
	}

	total := 0
	n := 0
	for n < len(c.qos0Buf) && n < maxItems {
		sz := len(c.qos0Buf[n].data)
		if sz <= 0 {
			sz = 1
		}
		if n > 0 && total+sz > maxBytes {
			break
		}
		total += sz
		n++
		if total >= maxBytes {
			break
		}
	}
	if n == 0 {
		n = 1
	}

	out := c.qos0Buf[:n:n]
	c.qos0Buf = c.qos0Buf[n:]
	return out
}

func (c *Client) hasBufferedQoS0Writes() bool {
	c.qos0BufMu.Lock()
	defer c.qos0BufMu.Unlock()
	return len(c.qos0Buf) > 0
}

func (c *Client) queueControlWrite(data []byte, deadline time.Time) error {
	return c.queueWriteRequest(writeRequest{
		data:        data,
		deadline:    deadline,
		errCh:       acquireWriteErrCh(),
		pooledErrCh: true,
	}, true)
}

func (c *Client) queueWriteRequest(req writeRequest, control bool) error {
	recycleErrCh := func() {
		if req.pooledErrCh {
			releaseWriteErrCh(req.errCh)
		}
	}

	var err error
	if control {
		err = c.enqueueControlWrite(req)
	} else {
		err = c.enqueueWrite(req)
	}
	if err != nil {
		recycleErrCh()
		return err
	}
	if req.errCh == nil {
		return nil
	}

	rt := c.loadWriteRuntime()
	if rt == nil {
		recycleErrCh()
		return ErrNotConnected
	}
	sch := rt.stopCh
	done := rt.writeDone
	if sch == nil || done == nil {
		recycleErrCh()
		return ErrNotConnected
	}

	select {
	case err := <-req.errCh:
		recycleErrCh()
		return err
	case <-done:
		select {
		case err := <-req.errCh:
			recycleErrCh()
			return err
		default:
		}
		recycleErrCh()
		return ErrNotConnected
	case <-sch:
		// Prefer a completed write result if already available.
		select {
		case err := <-req.errCh:
			recycleErrCh()
			return err
		default:
		}
		// If writer has fully stopped, it is safe to recycle pooled channels.
		// When done hasn't closed yet the writeLoop may still send on errCh,
		// so we intentionally skip recycling to avoid pool corruption.
		select {
		case <-done:
			select {
			case err := <-req.errCh:
				recycleErrCh()
				return err
			default:
			}
			recycleErrCh()
		default:
			// writeLoop still running — channel cannot be safely recycled.
		}
		return ErrNotConnected
	}
}

// queueControlWriteNoWait sends a control frame (ack, ping, auth response)
// without waiting for write completion.
// Returns false if the frame could not be enqueued.
func (c *Client) queueControlWriteNoWait(data []byte) (enqueued bool) {
	defer func() {
		if recover() != nil {
			enqueued = false
		}
	}()
	rt := c.loadWriteRuntime()
	if rt == nil {
		return false
	}
	ch := rt.controlWriteCh
	sch := rt.stopCh
	done := rt.writeDone
	if ch == nil || sch == nil || done == nil {
		return false
	}

	select {
	case <-sch:
		return false
	case <-done:
		return false
	case ch <- writeRequest{data: data}:
		return true
	default:
		return false
	}
}

func (c *Client) isWritePathActive() bool {
	rt := c.loadWriteRuntime()
	if rt == nil {
		return false
	}
	sch := rt.stopCh
	done := rt.writeDone
	if sch == nil || done == nil {
		return false
	}
	select {
	case <-sch:
		return false
	case <-done:
		return false
	default:
		return true
	}
}

func (c *Client) reserveOutbound(req writeRequest) error {
	if c.opts.MaxOutboundPendingMessages <= 0 && c.opts.MaxOutboundPendingBytes <= 0 {
		return nil
	}

	policy := c.opts.OutboundBackpressurePolicy
	if policy == "" {
		policy = OutboundBackpressureBlock
	}
	size := int64(len(req.data))
	if size <= 0 {
		size = 1
	}

	var deadline time.Time
	if policy == OutboundBackpressureBlockWithTimeout {
		if c.opts.OutboundBlockTimeout <= 0 {
			policy = OutboundBackpressureDropNew
		} else {
			deadline = time.Now().Add(c.opts.OutboundBlockTimeout)
		}
	}

	c.outboundMu.Lock()
	defer c.outboundMu.Unlock()

	for !c.canReserveOutboundLocked(size) {
		if req.ctx != nil {
			select {
			case <-req.ctx.Done():
				return req.ctx.Err()
			default:
			}
		}
		c.outboundMu.Unlock()
		active := c.isWritePathActive()
		c.outboundMu.Lock()
		if !active {
			return ErrNotConnected
		}

		switch policy {
		case OutboundBackpressureDropNew:
			c.reportOutboundBackpressureDrop(req)
			return ErrOutboundBackpressure
		case OutboundBackpressureBlockWithTimeout:
			remaining := time.Until(deadline)
			if remaining <= 0 {
				c.reportOutboundBackpressureDrop(req)
				return ErrOutboundBackpressure
			}
			waitFor := remaining
			if waitFor > 100*time.Millisecond {
				waitFor = 100 * time.Millisecond
			}
			timer := time.AfterFunc(waitFor, func() { c.outboundCond.Signal() })
			c.outboundCond.Wait()
			timer.Stop()
		default:
			if req.ctx == nil {
				c.outboundCond.Wait()
			} else {
				timer := time.AfterFunc(100*time.Millisecond, func() { c.outboundCond.Signal() })
				c.outboundCond.Wait()
				timer.Stop()
			}
		}
	}

	c.outboundPendingMessages++
	c.outboundPendingBytes += size
	return nil
}

func (c *Client) reportOutboundBackpressureDrop(req writeRequest) {
	c.reportAsyncError(ErrOutboundBackpressure)

	payloadSz := req.dropPayloadSz
	if payloadSz <= 0 {
		payloadSz = len(req.data)
	}

	c.reportDroppedMessage(&DroppedMessage{
		Direction:   DroppedMessageOutbound,
		Reason:      DroppedReasonOutboundBackpressure,
		Topic:       req.dropTopic,
		QoS:         req.dropQoS,
		PayloadSize: payloadSz,
		Timestamp:   time.Now().UTC(),
	})
}

func (c *Client) canReserveOutboundLocked(size int64) bool {
	if c.opts.MaxOutboundPendingMessages > 0 && c.outboundPendingMessages+1 > c.opts.MaxOutboundPendingMessages {
		return false
	}
	if c.opts.MaxOutboundPendingBytes > 0 && c.outboundPendingBytes+size > c.opts.MaxOutboundPendingBytes {
		return false
	}
	return true
}

func (c *Client) releaseOutbound(req writeRequest) {
	if c.opts.MaxOutboundPendingMessages <= 0 && c.opts.MaxOutboundPendingBytes <= 0 {
		return
	}

	size := int64(len(req.data))
	if size <= 0 {
		size = 1
	}

	c.outboundMu.Lock()
	if c.outboundPendingMessages > 0 {
		c.outboundPendingMessages--
	}
	if c.outboundPendingBytes >= size {
		c.outboundPendingBytes -= size
	} else {
		c.outboundPendingBytes = 0
	}
	c.outboundCond.Signal()
	c.outboundMu.Unlock()
}

func (c *Client) dispatcherChannels() (chan *Message, chan struct{}) {
	if rt := c.dispatchRT.Load(); rt != nil {
		return rt.msgCh, rt.msgStop
	}

	c.dispatchMu.RLock()
	defer c.dispatchMu.RUnlock()
	return c.msgCh, c.msgStop
}

func (c *Client) deliverDropNew(msgCh chan *Message, msgStop chan struct{}, msg *Message, msgBytes int64) {
	if !c.reservePending(msgBytes) {
		c.onSlowConsumer(msg)
		return
	}

	select {
	case msgCh <- msg:
		c.slowConsumerNotified.Store(false)
	case <-msgStop:
		c.releasePending(msgBytes)
	default:
		c.releasePending(msgBytes)
		c.onSlowConsumer(msg)
	}
}

func (c *Client) deliverDropOldest(msgCh chan *Message, msgStop chan struct{}, msg *Message, msgBytes int64) {
	// Try to reserve pending capacity; if full, drop one queued message to make room.
	if !c.reservePending(msgBytes) {
		if !c.dropOldestQueued(msgCh, msgStop) {
			c.onSlowConsumer(msg)
			return
		}
		// Retry once after freeing one slot.
		if !c.reservePending(msgBytes) {
			c.onSlowConsumer(msg)
			return
		}
	}

	select {
	case msgCh <- msg:
		c.slowConsumerNotified.Store(false)
	case <-msgStop:
		c.releasePending(msgBytes)
	default:
		// Channel full — drop oldest to make room.
		c.releasePending(msgBytes)
		if !c.dropOldestQueued(msgCh, msgStop) {
			c.onSlowConsumer(msg)
			return
		}
		// Re-reserve and send.
		if !c.reservePending(msgBytes) {
			c.onSlowConsumer(msg)
			return
		}
		select {
		case msgCh <- msg:
			c.slowConsumerNotified.Store(false)
		case <-msgStop:
			c.releasePending(msgBytes)
		default:
			c.releasePending(msgBytes)
			c.onSlowConsumer(msg)
		}
	}
}

func (c *Client) deliverBlockWithTimeout(msgCh chan *Message, msgStop chan struct{}, msg *Message, msgBytes int64) {
	timeout := c.opts.SlowConsumerBlockTimeout
	if timeout <= 0 {
		c.deliverDropNew(msgCh, msgStop, msg, msgBytes)
		return
	}

	deadline := time.Now().Add(timeout)

	// Wait for pending capacity using condition variable.
	c.pendingMu.Lock()
	for !c.canReservePendingLocked(msgBytes) {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			c.pendingMu.Unlock()
			c.onSlowConsumer(msg)
			return
		}
		// Schedule a wakeup at the deadline in case no release happens.
		timer := time.AfterFunc(remaining, func() { c.pendingCond.Signal() })
		c.pendingCond.Wait()
		timer.Stop()
	}
	c.pendingMessages++
	c.pendingBytes += msgBytes
	c.pendingMu.Unlock()

	remaining := time.Until(deadline)
	if remaining <= 0 {
		c.releasePending(msgBytes)
		c.onSlowConsumer(msg)
		return
	}

	timer := time.NewTimer(remaining)
	select {
	case msgCh <- msg:
		timer.Stop()
		c.slowConsumerNotified.Store(false)
	case <-msgStop:
		timer.Stop()
		c.releasePending(msgBytes)
	case <-timer.C:
		c.releasePending(msgBytes)
		c.onSlowConsumer(msg)
	}
}

func (c *Client) dropOldestQueued(msgCh chan *Message, msgStop chan struct{}) bool {
	select {
	case <-msgStop:
		return false
	case oldest := <-msgCh:
		c.releasePending(int64(mqttMessageSize(oldest)))
		c.onSlowConsumer(oldest)
		return true
	default:
		return false
	}
}

func (c *Client) reservePending(msgBytes int64) bool {
	if c.opts.MaxPendingMessages <= 0 && c.opts.MaxPendingBytes <= 0 {
		return true
	}

	c.pendingMu.Lock()
	defer c.pendingMu.Unlock()

	if !c.canReservePendingLocked(msgBytes) {
		return false
	}

	c.pendingMessages++
	c.pendingBytes += msgBytes

	return true
}

// canReservePendingLocked checks whether there is capacity for msgBytes.
// Must be called with pendingMu held.
func (c *Client) canReservePendingLocked(msgBytes int64) bool {
	if c.opts.MaxPendingMessages > 0 && c.pendingMessages+1 > c.opts.MaxPendingMessages {
		return false
	}
	if c.opts.MaxPendingBytes > 0 && c.pendingBytes+msgBytes > c.opts.MaxPendingBytes {
		return false
	}
	return true
}

func (c *Client) releasePending(msgBytes int64) {
	if c.opts.MaxPendingMessages <= 0 && c.opts.MaxPendingBytes <= 0 {
		return
	}

	c.pendingMu.Lock()
	if c.pendingMessages > 0 {
		c.pendingMessages--
	}
	if c.pendingBytes >= msgBytes {
		c.pendingBytes -= msgBytes
	} else {
		c.pendingBytes = 0
	}
	c.pendingCond.Signal()
	c.pendingMu.Unlock()
}

func (c *Client) onSlowConsumer(msg *Message) {
	meta := &DroppedMessage{
		Direction: DroppedMessageInbound,
		Reason:    DroppedReasonSlowConsumer,
		Timestamp: time.Now().UTC(),
	}
	if msg != nil {
		meta.Topic = msg.Topic
		meta.QoS = msg.QoS
		meta.PayloadSize = len(msg.Payload)
	}

	c.droppedMessages.Add(1)
	c.reportDroppedMessage(c.prepareDroppedMeta(meta, DroppedMessageInbound, DroppedReasonSlowConsumer))
	if c.slowConsumerNotified.CompareAndSwap(false, true) {
		c.reportAsyncError(ErrSlowConsumer)
	}
}

func (c *Client) reportAsyncError(err error) {
	if err == nil || c.opts.OnAsyncError == nil {
		return
	}
	enqueueAsyncCallback(func() {
		c.opts.OnAsyncError(err)
	})
}

func (c *Client) reportDroppedMessage(meta *DroppedMessage) {
	if meta == nil || c.opts.OnDroppedMessage == nil {
		return
	}
	copyMeta := *meta
	enqueueAsyncCallback(func() {
		c.opts.OnDroppedMessage(&copyMeta)
	})
}

func enqueueAsyncCallback(fn func()) {
	if fn == nil {
		return
	}

	callbackQueueOnce.Do(func() {
		callbackQueueCh = make(chan func(), defaultCallbackQueueSize)
		go func(callbackCh <-chan func()) {
			for fn := range callbackCh {
				if fn == nil {
					continue
				}
				func() {
					defer func() { recover() }()
					fn()
				}()
			}
		}(callbackQueueCh)
	})

	select {
	case callbackQueueCh <- fn:
	default:
	}
}

func (c *Client) prepareDroppedMeta(meta *DroppedMessage, dir DroppedMessageDirection, reason DroppedMessageReason) *DroppedMessage {
	if meta == nil {
		return &DroppedMessage{
			Direction: dir,
			Reason:    reason,
			Timestamp: time.Now().UTC(),
		}
	}
	out := *meta
	if out.Direction == "" {
		out.Direction = dir
	}
	if out.Reason == "" {
		out.Reason = reason
	}
	if out.Timestamp.IsZero() {
		out.Timestamp = time.Now().UTC()
	}
	return &out
}

func mqttMessageSize(msg *Message) int {
	if msg == nil {
		return 0
	}

	size := len(msg.Topic) + len(msg.Payload) + len(msg.ContentType) + len(msg.ResponseTopic) + len(msg.CorrelationData) + 32
	for k, v := range msg.UserProperties {
		size += len(k) + len(v)
	}
	size += len(msg.SubscriptionIDs) * 4
	return size
}

// DroppedMessages returns count of messages dropped due to slow consumer pressure.
func (c *Client) DroppedMessages() uint64 {
	return c.droppedMessages.Load()
}

func (c *Client) writeQueueDepth() int {
	rt := c.loadWriteRuntime()
	if rt == nil {
		return 0
	}
	depth := 0
	if rt.writeCh != nil {
		depth += len(rt.writeCh)
	}
	if rt.controlWriteCh != nil {
		depth += len(rt.controlWriteCh)
	}
	c.qos0BufMu.Lock()
	depth += len(c.qos0Buf)
	c.qos0BufMu.Unlock()
	return depth
}

func (c *Client) outboundPendingDepth() int {
	c.outboundMu.Lock()
	defer c.outboundMu.Unlock()
	return c.outboundPendingMessages
}

func (c *Client) reconnectBufferDepth() int {
	c.reconnectBufMu.Lock()
	defer c.reconnectBufMu.Unlock()
	return len(c.reconnectBuf)
}

func (c *Client) updateActivity() {
	c.lastActivity.Store(time.Now().UTC().UnixNano())
}

// ServerCapabilities returns the capabilities advertised by the server
// in the CONNACK packet. This is only available for MQTT 5.0 connections.
// Returns nil if not connected or using MQTT 3.1.1.
func (c *Client) ServerCapabilities() *ServerCapabilities {
	c.serverCapsMu.RLock()
	defer c.serverCapsMu.RUnlock()
	return c.serverCaps
}
