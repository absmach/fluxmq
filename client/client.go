// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package client

import (
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/absmach/fluxmq/core/packets"
	v3 "github.com/absmach/fluxmq/core/packets/v3"
	v5 "github.com/absmach/fluxmq/core/packets/v5"
)

var timeZero = time.Time{}

// Client is a thread-safe MQTT client.
type Client struct {
	opts *Options

	// State management
	state *stateManager

	// Connection
	conn   net.Conn
	connMu sync.RWMutex

	// Server capabilities (MQTT 5.0)
	serverCaps   *ServerCapabilities
	serverCapsMu sync.RWMutex

	// Topic aliases (MQTT 5.0)
	topicAliases *topicAliasManager

	// Queue subscriptions
	queueSubs *queueSubscriptions

	// Pending operations
	pending *pendingStore

	// Message store for QoS 1/2
	store MessageStore

	// QoS 2 incoming messages waiting for PUBREL
	qos2Incoming   map[uint16]*Message
	qos2IncomingMu sync.Mutex

	// Lifecycle
	stopCh   chan struct{}
	doneCh   chan struct{}
	reconnMu sync.Mutex

	// Keep-alive
	pingTimer    *time.Timer
	pingStop     chan struct{}
	lastActivity time.Time
	activityMu   sync.Mutex

	// Server index for round-robin
	serverIdx int
}

// New creates a new MQTT client with the given options.
func New(opts *Options) (*Client, error) {
	if err := opts.Validate(); err != nil {
		return nil, err
	}

	store := opts.Store
	if store == nil {
		store = NewMemoryStore()
	}

	return &Client{
		opts:         opts,
		state:        newStateManager(),
		pending:      newPendingStore(opts.MaxInflight),
		store:        store,
		qos2Incoming: make(map[uint16]*Message),
		queueSubs:    newQueueSubscriptions(),
	}, nil
}

// Connect establishes a connection to the broker.
func (c *Client) Connect() error {
	if c.state.isClosed() {
		return ErrClientClosed
	}

	if !c.state.transitionFrom(StateConnecting, StateDisconnected, StateReconnecting) {
		return ErrAlreadyConnected
	}

	err := c.doConnect()
	if err != nil {
		c.state.set(StateDisconnected)
		return err
	}

	c.state.set(StateConnected)

	// Start background goroutines
	c.stopCh = make(chan struct{})
	c.doneCh = make(chan struct{})
	go c.readLoop()

	if c.opts.KeepAlive > 0 {
		c.startKeepAlive()
	}

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

	c.connMu.Lock()
	c.conn = conn
	c.connMu.Unlock()

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
			c.opts.RequestResponseInfo || !c.opts.RequestProblemInfo {
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
		}

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
		pkt, _, _, err := v5.ReadPacket(conn)
		if err != nil {
			return 0, err
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
	if !c.state.transition(StateConnected, StateDisconnecting) {
		return nil
	}

	c.stopKeepAlive()
	c.sendDisconnectWithReason(reasonCode, sessionExpiry, reasonString)
	c.cleanup(nil)
	c.state.set(StateDisconnected)

	return nil
}

func (c *Client) sendDisconnectWithReason(reasonCode byte, sessionExpiry uint32, reasonString string) {
	c.connMu.RLock()
	conn := c.conn
	c.connMu.RUnlock()

	if conn == nil {
		return
	}

	conn.SetWriteDeadline(time.Now().Add(c.opts.WriteTimeout))

	if c.opts.ProtocolVersion == 5 {
		pkt := &v5.Disconnect{
			FixedHeader: packets.FixedHeader{PacketType: packets.DisconnectType},
			ReasonCode:  reasonCode,
		}

		// Add properties if specified
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
	c.state.set(StateClosed)
	c.stopKeepAlive()
	c.cleanup(ErrClientClosed)
	if c.store != nil {
		c.store.Close()
	}
	return nil
}

func (c *Client) cleanup(err error) {
	// Stop read loop
	if c.stopCh != nil {
		close(c.stopCh)
		<-c.doneCh
	}

	// Close connection
	c.connMu.Lock()
	if c.conn != nil {
		c.conn.Close()
		c.conn = nil
	}
	c.connMu.Unlock()

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
func (c *Client) Publish(topic string, payload []byte, qos byte, retain bool) error {
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
		return c.sendPublish(msg, 0)
	}

	// QoS 1 or 2: need packet ID and wait for ack
	packetID := c.pending.nextPacketID()
	if packetID == 0 {
		return ErrMaxInflight
	}
	msg.PacketID = packetID

	// Store message for potential retransmission
	if err := c.store.StoreOutbound(packetID, msg); err != nil {
		return err
	}

	op, err := c.pending.add(packetID, pendingPublish, msg)
	if err != nil {
		c.store.DeleteOutbound(packetID)
		return err
	}

	if err := c.sendPublish(msg, packetID); err != nil {
		c.pending.remove(packetID)
		c.store.DeleteOutbound(packetID)
		return err
	}

	return op.wait(c.opts.AckTimeout)
}

func (c *Client) sendPublish(msg *Message, packetID uint16) error {
	c.connMu.RLock()
	conn := c.conn
	c.connMu.RUnlock()

	if conn == nil {
		return ErrNotConnected
	}

	conn.SetWriteDeadline(time.Now().Add(c.opts.WriteTimeout))
	defer conn.SetWriteDeadline(time.Time{})

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

		// Apply topic alias if available
		if c.topicAliases != nil {
			if alias, isNew, ok := c.topicAliases.getOrAssignOutbound(msg.Topic); ok {
				if pkt.Properties == nil {
					pkt.Properties = &v5.PublishProperties{}
				}
				pkt.Properties.TopicAlias = &alias

				// If using existing alias, clear topic name to save bandwidth
				if !isNew {
					pkt.TopicName = ""
				}
				// If new alias, send both topic and alias for registration
			}
		}

		return pkt.Pack(conn)
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
	return pkt.Pack(conn)
}

// Subscribe subscribes to one or more topics.
func (c *Client) Subscribe(topics map[string]byte) error {
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

	op, err := c.pending.add(packetID, pendingSubscribe, nil)
	if err != nil {
		return err
	}

	if err := c.sendSubscribe(packetID, topics); err != nil {
		c.pending.remove(packetID)
		return err
	}

	return op.wait(c.opts.AckTimeout)
}

// SubscribeSingle is a convenience method for subscribing to a single topic.
func (c *Client) SubscribeSingle(topic string, qos byte) error {
	return c.Subscribe(map[string]byte{topic: qos})
}

// SubscribeWithOptions subscribes with advanced MQTT 5.0 options.
// For MQTT 3.1.1 connections, advanced options are ignored.
func (c *Client) SubscribeWithOptions(opts ...*SubscribeOption) error {
	if !c.state.isConnected() {
		return ErrNotConnected
	}
	if len(opts) == 0 {
		return ErrInvalidTopic
	}

	packetID := c.pending.nextPacketID()
	if packetID == 0 {
		return ErrMaxInflight
	}

	op, err := c.pending.add(packetID, pendingSubscribe, nil)
	if err != nil {
		return err
	}

	if err := c.sendSubscribeWithOptions(packetID, opts); err != nil {
		c.pending.remove(packetID)
		return err
	}

	return op.wait(c.opts.AckTimeout)
}

func (c *Client) sendSubscribe(packetID uint16, topics map[string]byte) error {
	c.connMu.RLock()
	conn := c.conn
	c.connMu.RUnlock()

	if conn == nil {
		return ErrNotConnected
	}

	conn.SetWriteDeadline(time.Now().Add(c.opts.WriteTimeout))
	defer conn.SetWriteDeadline(time.Time{})

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
		return pkt.Pack(conn)
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
	return pkt.Pack(conn)
}

func (c *Client) sendSubscribeWithOptions(packetID uint16, opts []*SubscribeOption) error {
	c.connMu.RLock()
	conn := c.conn
	c.connMu.RUnlock()

	if conn == nil {
		return ErrNotConnected
	}

	conn.SetWriteDeadline(time.Now().Add(c.opts.WriteTimeout))
	defer conn.SetWriteDeadline(time.Time{})

	if c.opts.ProtocolVersion == 5 {
		v5Opts := make([]v5.SubOption, len(opts))
		for i, opt := range opts {
			v5Opts[i] = v5.SubOption{
				Topic:  opt.Topic,
				MaxQoS: opt.QoS,
			}

			// Set advanced options if specified
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

		// Use subscription identifier from first option (applies to all topics in this packet)
		if len(opts) > 0 && opts[0].SubscriptionID > 0 {
			subID := int(opts[0].SubscriptionID)
			pkt.Properties = &v5.SubscribeProperties{
				SubscriptionIdentifier: &subID,
			}
		}

		c.updateActivity()
		return pkt.Pack(conn)
	}

	// For MQTT 3.1.1, ignore advanced options
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
	return pkt.Pack(conn)
}

// Unsubscribe unsubscribes from one or more topics.
func (c *Client) Unsubscribe(topics ...string) error {
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

	if err := c.sendUnsubscribe(packetID, topics); err != nil {
		c.pending.remove(packetID)
		return err
	}

	return op.wait(c.opts.AckTimeout)
}

func (c *Client) sendUnsubscribe(packetID uint16, topics []string) error {
	c.connMu.RLock()
	conn := c.conn
	c.connMu.RUnlock()

	if conn == nil {
		return ErrNotConnected
	}

	conn.SetWriteDeadline(time.Now().Add(c.opts.WriteTimeout))
	defer conn.SetWriteDeadline(time.Time{})

	if c.opts.ProtocolVersion == 5 {
		pkt := &v5.Unsubscribe{
			FixedHeader: packets.FixedHeader{PacketType: packets.UnsubscribeType, QoS: 1},
			ID:          packetID,
			Topics:      topics,
		}
		return pkt.Pack(conn)
	}

	pkt := &v3.Unsubscribe{
		FixedHeader: packets.FixedHeader{PacketType: packets.UnsubscribeType, QoS: 1},
		ID:          packetID,
		Topics:      topics,
	}
	c.updateActivity()
	return pkt.Pack(conn)
}

// readLoop reads packets from the connection.
func (c *Client) readLoop() {
	defer close(c.doneCh)

	for {
		select {
		case <-c.stopCh:
			return
		default:
		}

		c.connMu.RLock()
		conn := c.conn
		c.connMu.RUnlock()

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
		// Keep-alive response, nothing to do
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
		c.sendPubRec(msg.PacketID)
	}
}

func (c *Client) deliverMessage(msg *Message) {
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
	var partitionID int
	var sequence uint64

	if msg.UserProperties != nil {
		if msgID, ok := msg.UserProperties["message-id"]; ok {
			messageID = msgID
		}
		if partID, ok := msg.UserProperties["partition-id"]; ok {
			fmt.Sscanf(partID, "%d", &partitionID)
		}
		if seq, ok := msg.UserProperties["sequence"]; ok {
			fmt.Sscanf(seq, "%d", &sequence)
		}
	}

	// Create queue message with ack/nack/reject methods
	queueMsg := &QueueMessage{
		Message:     msg,
		MessageID:   messageID,
		PartitionID: partitionID,
		Sequence:    sequence,
		client:      c,
		queueName:   sub.queueName,
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
	c.connMu.RLock()
	conn := c.conn
	c.connMu.RUnlock()

	if conn == nil {
		return
	}

	if c.opts.ProtocolVersion == 5 {
		pkt := &v5.PubAck{
			FixedHeader: packets.FixedHeader{PacketType: packets.PubAckType},
			ID:          packetID,
		}
		pkt.Pack(conn)
	} else {
		pkt := &v3.PubAck{
			FixedHeader: packets.FixedHeader{PacketType: packets.PubAckType},
			ID:          packetID,
		}
		pkt.Pack(conn)
	}
}

func (c *Client) sendPubRec(packetID uint16) {
	c.connMu.RLock()
	conn := c.conn
	c.connMu.RUnlock()

	if conn == nil {
		return
	}

	if c.opts.ProtocolVersion == 5 {
		pkt := &v5.PubRec{
			FixedHeader: packets.FixedHeader{PacketType: packets.PubRecType},
			ID:          packetID,
		}
		pkt.Pack(conn)
	} else {
		pkt := &v3.PubRec{
			FixedHeader: packets.FixedHeader{PacketType: packets.PubRecType},
			ID:          packetID,
		}
		pkt.Pack(conn)
	}
}

func (c *Client) sendPubRel(packetID uint16) {
	c.connMu.RLock()
	conn := c.conn
	c.connMu.RUnlock()

	if conn == nil {
		return
	}

	if c.opts.ProtocolVersion == 5 {
		pkt := &v5.PubRel{
			FixedHeader: packets.FixedHeader{PacketType: packets.PubRelType},
			ID:          packetID,
		}
		pkt.Pack(conn)
	} else {
		pkt := &v3.PubRel{
			FixedHeader: packets.FixedHeader{PacketType: packets.PubRelType},
			ID:          packetID,
		}
		pkt.Pack(conn)
	}
}

func (c *Client) sendPubComp(packetID uint16) {
	c.connMu.RLock()
	conn := c.conn
	c.connMu.RUnlock()

	if conn == nil {
		return
	}

	if c.opts.ProtocolVersion == 5 {
		pkt := &v5.PubComp{
			FixedHeader: packets.FixedHeader{PacketType: packets.PubCompType},
			ID:          packetID,
		}
		pkt.Pack(conn)
	} else {
		pkt := &v3.PubComp{
			FixedHeader: packets.FixedHeader{PacketType: packets.PubCompType},
			ID:          packetID,
		}
		pkt.Pack(conn)
	}
}

func (c *Client) handleConnectionLost(err error) {
	if !c.state.transition(StateConnected, StateDisconnected) {
		return
	}

	c.stopKeepAlive()
	c.pending.clear(ErrConnectionLost)

	c.connMu.Lock()
	if c.conn != nil {
		c.conn.Close()
		c.conn = nil
	}
	c.connMu.Unlock()

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

// Keep-alive management

func (c *Client) startKeepAlive() {
	c.pingStop = make(chan struct{})
	interval := c.opts.KeepAlive

	go func() {
		ticker := time.NewTicker(interval / 2)
		defer ticker.Stop()

		for {
			select {
			case <-c.pingStop:
				return
			case <-ticker.C:
				c.activityMu.Lock()
				idle := time.Since(c.lastActivity)
				c.activityMu.Unlock()

				if idle >= interval/2 {
					c.sendPing()
				}
			}
		}
	}()
}

func (c *Client) stopKeepAlive() {
	if c.pingStop != nil {
		close(c.pingStop)
		c.pingStop = nil
	}
}

func (c *Client) sendPing() {
	c.connMu.RLock()
	conn := c.conn
	c.connMu.RUnlock()

	if conn == nil {
		return
	}

	if c.opts.ProtocolVersion == 5 {
		pkt := &v5.PingReq{
			FixedHeader: packets.FixedHeader{PacketType: packets.PingReqType},
		}
		pkt.Pack(conn)
	} else {
		pkt := &v3.PingReq{
			FixedHeader: packets.FixedHeader{PacketType: packets.PingReqType},
		}
		pkt.Pack(conn)
	}
}

func (c *Client) updateActivity() {
	c.activityMu.Lock()
	defer c.activityMu.Unlock()
	c.lastActivity = time.Now().UTC()
}

// ServerCapabilities returns the capabilities advertised by the server
// in the CONNACK packet. This is only available for MQTT 5.0 connections.
// Returns nil if not connected or using MQTT 3.1.1.
func (c *Client) ServerCapabilities() *ServerCapabilities {
	c.serverCapsMu.RLock()
	defer c.serverCapsMu.RUnlock()
	return c.serverCaps
}
