// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package mqtt

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"runtime"
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
}

// Client is a thread-safe MQTT client.
type Client struct {
	opts *Options

	// State management
	state *stateManager

	// Connection — only touched by writeLoop (writes) and readLoop (reads) after Connect().
	conn net.Conn

	// Write serialization
	writeCh   chan writeRequest
	writeDone chan struct{}

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
	msgCh      chan *Message
	msgStop    chan struct{}
	dispatchWg sync.WaitGroup

	// Server index for round-robin
	serverIdx int

	// Cleanup guard to keep teardown idempotent under concurrent lifecycle calls.
	cleanupInProgress uint32
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

	return &Client{
		opts:          opts,
		state:         newStateManager(),
		pending:       newPendingStore(opts.MaxInflight),
		store:         store,
		qos2Incoming:  make(map[uint16]*Message),
		queueSubs:     newQueueSubscriptions(),
		queueAckCache: newQueueAckCache(5 * time.Minute),
		subscriptions: newSubscriptionRegistry(),
	}, nil
}

// Connect establishes a connection to the broker.
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
		if c.conn != nil {
			c.conn.Close()
			c.conn = nil
		}
		if c.state.isClosed() {
			return ErrClientClosed
		}
		return ErrConnectionLost
	}

	// Start background goroutines
	c.stopCh = make(chan struct{})
	c.doneCh = make(chan struct{})

	size := c.opts.MessageChanSize
	if size <= 0 {
		size = DefaultMessageChanSize
	}
	c.writeCh = make(chan writeRequest, size)
	c.writeDone = make(chan struct{})
	go c.writeLoop()

	c.startDispatcher()
	go c.readLoop()

	if c.opts.KeepAlive > 0 {
		c.startKeepAlive()
	}

	// Restore client-side state on reconnect (subscriptions and QoS in-flight).
	c.restoreState()

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

	c.conn = conn
	c.updateActivity()
	return nil
}

func (c *Client) sendConnect(conn net.Conn) error {
	conn.SetWriteDeadline(time.Now().Add(c.opts.WriteTimeout))
	defer conn.SetWriteDeadline(time.Time{})

	keepAlive := uint16(c.opts.KeepAlive.Seconds())

	if c.opts.ProtocolVersion == 5 {
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

	if c.opts.ProtocolVersion == 5 {
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

// sendDisconnectWithReason writes the DISCONNECT directly to conn (bypassing writeLoop)
// because it runs right before cleanup tears down the write infrastructure.
func (c *Client) sendDisconnectWithReason(reasonCode byte, sessionExpiry uint32, reasonString string) {
	conn := c.conn
	if conn == nil {
		return
	}
	conn.SetWriteDeadline(time.Now().Add(c.opts.WriteTimeout))

	if c.opts.ProtocolVersion == 5 {
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

	// Signal readLoop.
	if c.stopCh != nil {
		close(c.stopCh)
	}

	// Signal writeLoop to drain and exit.
	if c.writeCh != nil {
		close(c.writeCh)
	}

	// Close connection to unblock any blocked read/write syscalls.
	if c.conn != nil {
		c.conn.Close()
	}

	// Wait for readLoop to exit.
	if c.doneCh != nil {
		<-c.doneCh
	}

	// Wait for writeLoop to exit.
	if c.writeDone != nil {
		<-c.writeDone
	}

	c.conn = nil
	c.stopCh = nil
	c.doneCh = nil
	c.writeCh = nil
	c.writeDone = nil

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
	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return err
		}
	}
	if !c.state.isConnected() {
		return ErrNotConnected
	}
	if qos > 2 {
		return ErrInvalidQoS
	}
	if topic == "" {
		return ErrInvalidTopic
	}

	msg := NewMessage(topic, payload, qos, retain)

	if qos == 0 {
		return c.sendPublish(ctx, msg, 0)
	}

	packetID := c.pending.nextPacketID()
	if packetID == 0 {
		return ErrMaxInflight
	}
	msg.PacketID = packetID

	if err := c.store.StoreOutbound(packetID, msg); err != nil {
		return err
	}

	op, err := c.pending.add(packetID, pendingPublish, msg)
	if err != nil {
		c.store.DeleteOutbound(packetID)
		return err
	}

	if err := c.sendPublish(ctx, msg, packetID); err != nil {
		c.pending.remove(packetID)
		c.store.DeleteOutbound(packetID)
		return err
	}

	if err := op.waitWithContext(ctx, c.opts.AckTimeout); err != nil {
		c.pending.remove(packetID)
		c.store.DeleteOutbound(packetID)
		return err
	}
	return nil
}

// PublishMessage sends a message with optional MQTT 5.0 publish properties.
// For MQTT 3.1.1, publish properties are ignored.
func (c *Client) PublishMessage(ctx context.Context, msg *Message) error {
	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return err
		}
	}
	if msg == nil {
		return ErrInvalidMessage
	}
	if !c.state.isConnected() {
		return ErrNotConnected
	}
	if msg.QoS > 2 {
		return ErrInvalidQoS
	}
	if msg.Topic == "" {
		return ErrInvalidTopic
	}

	if msg.QoS == 0 {
		return c.sendPublish(ctx, msg, 0)
	}

	packetID := c.pending.nextPacketID()
	if packetID == 0 {
		return ErrMaxInflight
	}

	publishMsg := msg.Copy()
	publishMsg.PacketID = packetID

	if err := c.store.StoreOutbound(packetID, publishMsg); err != nil {
		return err
	}

	op, err := c.pending.add(packetID, pendingPublish, publishMsg)
	if err != nil {
		c.store.DeleteOutbound(packetID)
		return err
	}

	if err := c.sendPublish(ctx, publishMsg, packetID); err != nil {
		c.pending.remove(packetID)
		c.store.DeleteOutbound(packetID)
		return err
	}

	if err := op.waitWithContext(ctx, c.opts.AckTimeout); err != nil {
		c.pending.remove(packetID)
		c.store.DeleteOutbound(packetID)
		return err
	}
	return nil
}

// PublishAsync publishes in a background goroutine and returns a completion token.
func (c *Client) PublishAsync(ctx context.Context, topic string, payload []byte, qos byte, retain bool) *PublishToken {
	tok := &PublishToken{token: newToken()}
	go func() {
		tok.complete(c.Publish(ctx, topic, payload, qos, retain))
	}()
	return tok
}

// PublishMessageAsync publishes a message with properties in a background goroutine.
func (c *Client) PublishMessageAsync(ctx context.Context, msg *Message) *PublishToken {
	tok := &PublishToken{token: newToken()}
	go func() {
		tok.complete(c.PublishMessage(ctx, msg))
	}()
	return tok
}

func (c *Client) sendPublish(ctx context.Context, msg *Message, packetID uint16) error {
	return c.sendPublishWithDeadline(msg, packetID, c.writeDeadline(ctx))
}

func (c *Client) sendPublishWithDeadline(msg *Message, packetID uint16, deadline time.Time) error {
	if c.opts.ProtocolVersion == 5 {
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
			if pkt.Properties == nil {
				pkt.Properties = &v5.PublishProperties{}
			}
		}

		if msg.PayloadFormat != nil {
			pkt.Properties.PayloadFormat = msg.PayloadFormat
		}
		if msg.MessageExpiry != nil {
			pkt.Properties.MessageExpiry = msg.MessageExpiry
		}
		if msg.ContentType != "" {
			pkt.Properties.ContentType = msg.ContentType
		}
		if msg.ResponseTopic != "" {
			pkt.Properties.ResponseTopic = msg.ResponseTopic
		}
		if len(msg.CorrelationData) > 0 {
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

		if c.topicAliases != nil {
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

		c.updateActivity()
		return c.queueWrite(pkt.Encode(), deadline)
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
	c.updateActivity()
	return c.queueWrite(pkt.Encode(), deadline)
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

	if c.opts.ProtocolVersion == 5 {
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

	if c.opts.ProtocolVersion == 5 {
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

	if c.opts.ProtocolVersion == 5 {
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
	defer close(c.doneCh)

	conn := c.conn

	for {
		select {
		case <-c.stopCh:
			return
		default:
		}

		if conn == nil {
			return
		}

		var pkt packets.ControlPacket
		var err error

		if c.opts.ProtocolVersion == 5 {
			pkt, _, _, err = v5.ReadPacket(conn)
		} else {
			pkt, err = v3.ReadPacket(conn)
		}

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

	if c.opts.ProtocolVersion == 5 {
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

	if c.msgCh == nil {
		c.handleDeliveredMessage(msg)
		return
	}

	if c.opts.OrderMatters {
		c.msgCh <- msg
		return
	}

	select {
	case c.msgCh <- msg:
	default:
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

	c.msgCh = make(chan *Message, size)
	c.msgStop = make(chan struct{})

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
		go func() {
			defer c.dispatchWg.Done()
			for {
				select {
				case <-c.msgStop:
					return
				case msg, ok := <-c.msgCh:
					if !ok {
						return
					}
					c.handleDeliveredMessage(msg)
				}
			}
		}()
	}
}

func (c *Client) stopDispatcher() {
	if c.msgStop != nil {
		close(c.msgStop)
		c.msgStop = nil
	}
	if c.msgCh != nil {
		close(c.msgCh)
		c.msgCh = nil
	}
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
			fmt.Sscanf(off, "%d", &offset)
		} else if seq, ok := msg.UserProperties["sequence"]; ok {
			fmt.Sscanf(seq, "%d", &offset)
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
	var packetID uint16
	if c.opts.ProtocolVersion == 5 {
		packetID = pkt.(*v5.PubAck).ID
	} else {
		packetID = pkt.(*v3.PubAck).ID
	}
	c.store.DeleteOutbound(packetID)
	c.pending.complete(packetID, nil, nil)
}

func (c *Client) handlePubRec(pkt packets.ControlPacket) {
	var packetID uint16
	if c.opts.ProtocolVersion == 5 {
		packetID = pkt.(*v5.PubRec).ID
	} else {
		packetID = pkt.(*v3.PubRec).ID
	}

	c.pending.updateQoS2State(packetID, 1) // Now waiting for PUBCOMP
	c.sendPubRel(packetID)
}

func (c *Client) handlePubRel(pkt packets.ControlPacket) {
	var packetID uint16
	if c.opts.ProtocolVersion == 5 {
		packetID = pkt.(*v5.PubRel).ID
	} else {
		packetID = pkt.(*v3.PubRel).ID
	}

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
	var packetID uint16
	if c.opts.ProtocolVersion == 5 {
		packetID = pkt.(*v5.PubComp).ID
	} else {
		packetID = pkt.(*v3.PubComp).ID
	}
	c.store.DeleteOutbound(packetID)
	c.pending.complete(packetID, nil, nil)
}

func (c *Client) handleSubAck(pkt packets.ControlPacket) {
	var packetID uint16
	var returnCodes []byte

	if c.opts.ProtocolVersion == 5 {
		p := pkt.(*v5.SubAck)
		packetID = p.ID
		if p.ReasonCodes != nil {
			returnCodes = *p.ReasonCodes
		}
	} else {
		p := pkt.(*v3.SubAck)
		packetID = p.ID
		returnCodes = p.ReturnCodes
	}

	var err error
	for _, rc := range returnCodes {
		if c.opts.ProtocolVersion == 5 {
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
	var packetID uint16
	if c.opts.ProtocolVersion == 5 {
		packetID = pkt.(*v5.UnsubAck).ID
	} else {
		packetID = pkt.(*v3.UnSubAck).ID
	}
	c.pending.complete(packetID, nil, nil)
}

func (c *Client) sendPubAck(packetID uint16) {
	if c.opts.ProtocolVersion == 5 {
		pkt := &v5.PubAck{
			FixedHeader: packets.FixedHeader{PacketType: packets.PubAckType},
			ID:          packetID,
		}
		c.queueWriteNoWait(pkt.Encode())
	} else {
		pkt := &v3.PubAck{
			FixedHeader: packets.FixedHeader{PacketType: packets.PubAckType},
			ID:          packetID,
		}
		c.queueWriteNoWait(pkt.Encode())
	}
}

func (c *Client) sendPubRec(packetID uint16) {
	if c.opts.ProtocolVersion == 5 {
		pkt := &v5.PubRec{
			FixedHeader: packets.FixedHeader{PacketType: packets.PubRecType},
			ID:          packetID,
		}
		c.queueWriteNoWait(pkt.Encode())
	} else {
		pkt := &v3.PubRec{
			FixedHeader: packets.FixedHeader{PacketType: packets.PubRecType},
			ID:          packetID,
		}
		c.queueWriteNoWait(pkt.Encode())
	}
}

func (c *Client) sendPubRel(packetID uint16) {
	if c.opts.ProtocolVersion == 5 {
		pkt := &v5.PubRel{
			FixedHeader: packets.FixedHeader{PacketType: packets.PubRelType, QoS: 1},
			ID:          packetID,
		}
		c.queueWriteNoWait(pkt.Encode())
	} else {
		pkt := &v3.PubRel{
			FixedHeader: packets.FixedHeader{PacketType: packets.PubRelType, QoS: 1},
			ID:          packetID,
		}
		c.queueWriteNoWait(pkt.Encode())
	}
}

func (c *Client) sendPubComp(packetID uint16) {
	if c.opts.ProtocolVersion == 5 {
		pkt := &v5.PubComp{
			FixedHeader: packets.FixedHeader{PacketType: packets.PubCompType},
			ID:          packetID,
		}
		c.queueWriteNoWait(pkt.Encode())
	} else {
		pkt := &v3.PubComp{
			FixedHeader: packets.FixedHeader{PacketType: packets.PubCompType},
			ID:          packetID,
		}
		c.queueWriteNoWait(pkt.Encode())
	}
}

func (c *Client) handleServerDisconnect(pkt packets.ControlPacket) {
	if c.opts.ProtocolVersion != 5 {
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
	if c.opts.ProtocolVersion != 5 {
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

	c.queueWriteNoWait(resp.Encode())
}

// SendAuth initiates re-authentication by sending an AUTH packet to the server (MQTT 5.0).
func (c *Client) SendAuth(reasonCode byte, authData []byte) error {
	if c.opts.ProtocolVersion != 5 {
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

	return c.queueWrite(pkt.Encode(), time.Time{})
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

		// Exponential backoff
		time.Sleep(delay)
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
		if c.opts.ProtocolVersion == 5 {
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

	if c.opts.ProtocolVersion == 5 {
		pkt := &v5.PingReq{
			FixedHeader: packets.FixedHeader{PacketType: packets.PingReqType},
		}
		c.queueWriteNoWait(pkt.Encode())
	} else {
		pkt := &v3.PingReq{
			FixedHeader: packets.FixedHeader{PacketType: packets.PingReqType},
		}
		c.queueWriteNoWait(pkt.Encode())
	}

	c.updateActivity()
}

func (c *Client) writeLoop() {
	defer close(c.writeDone)
	for req := range c.writeCh {
		if !req.deadline.IsZero() {
			c.conn.SetWriteDeadline(req.deadline)
		}
		_, err := c.conn.Write(req.data)
		if !req.deadline.IsZero() {
			c.conn.SetWriteDeadline(time.Time{})
		}
		if req.errCh != nil {
			req.errCh <- err
		}
		if err != nil {
			return
		}
	}
}

func (c *Client) queueWrite(data []byte, deadline time.Time) (retErr error) {
	defer func() {
		if recover() != nil {
			retErr = ErrNotConnected
		}
	}()
	wch := c.writeCh
	sch := c.stopCh
	if wch == nil || sch == nil {
		return ErrNotConnected
	}
	errCh := make(chan error, 1)
	select {
	case wch <- writeRequest{data: data, deadline: deadline, errCh: errCh}:
		return <-errCh
	case <-sch:
		return ErrNotConnected
	}
}

func (c *Client) queueWriteNoWait(data []byte) {
	wch := c.writeCh
	if wch == nil {
		return
	}
	select {
	case wch <- writeRequest{data: data}:
	default:
	}
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
