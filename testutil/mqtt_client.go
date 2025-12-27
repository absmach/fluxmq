// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package testutil

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Client state constants.
const (
	StateDisconnected uint32 = iota
	StateConnecting
	StateConnected
	StateDisconnecting
)

// Default timeouts.
const (
	DefaultConnectTimeout = 5 * time.Second
	DefaultReadTimeout    = 100 * time.Millisecond
	DefaultAckTimeout     = 5 * time.Second
)

// Errors.
var (
	ErrNotConnected    = errors.New("client not connected")
	ErrAlreadyConnected = errors.New("client already connected")
	ErrTimeout         = errors.New("operation timed out")
	ErrConnectionLost  = errors.New("connection lost")
	ErrInvalidPacket   = errors.New("invalid packet received")
)

// Message represents a received MQTT message.
type Message struct {
	Topic    string
	Payload  []byte
	QoS      byte
	Retain   bool
	Dup      bool
	PacketID uint16
}

// WillMessage represents a will message for CONNECT.
type WillMessage struct {
	Topic   string
	Payload []byte
	QoS     byte
	Retain  bool
}

// ConnectOptions holds options for Connect.
type ConnectOptions struct {
	CleanSession bool
	KeepAlive    uint16
	Will         *WillMessage
}

// MessageStore interface for storing received messages.
type MessageStore interface {
	Store(msg *Message)
	Get(topic string) []*Message
	GetAll() []*Message
	Clear()
	Count() int
}

// InMemoryStore is a simple in-memory message store.
type InMemoryStore struct {
	messages []*Message
	mu       sync.RWMutex
}

// NewInMemoryStore creates a new in-memory message store.
func NewInMemoryStore() *InMemoryStore {
	return &InMemoryStore{
		messages: make([]*Message, 0),
	}
}

// Store adds a message to the store.
func (s *InMemoryStore) Store(msg *Message) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.messages = append(s.messages, msg)
}

// Get returns all messages for a topic.
func (s *InMemoryStore) Get(topic string) []*Message {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var result []*Message
	for _, msg := range s.messages {
		if msg.Topic == topic {
			result = append(result, msg)
		}
	}
	return result
}

// GetAll returns all stored messages.
func (s *InMemoryStore) GetAll() []*Message {
	s.mu.RLock()
	defer s.mu.RUnlock()

	result := make([]*Message, len(s.messages))
	copy(result, s.messages)
	return result
}

// Clear removes all messages from the store.
func (s *InMemoryStore) Clear() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.messages = s.messages[:0]
}

// Count returns the number of stored messages.
func (s *InMemoryStore) Count() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.messages)
}

// pendingAck tracks a pending acknowledgment.
type pendingAck struct {
	packetID uint16
	done     chan struct{}
	err      error
}

// TestMQTTClient is a robust MQTT client for integration testing.
type TestMQTTClient struct {
	t        *testing.T
	ClientID string
	Node     *TestNode

	conn         net.Conn
	state        uint32
	cleanSession bool

	// Message handling
	messages     chan *Message
	messageStore MessageStore

	// Packet ID management
	nextPacketID uint16
	packetIDMu   sync.Mutex

	// Pending acknowledgments
	pendingAcks   map[uint16]*pendingAck
	pendingAcksMu sync.Mutex

	// QoS 2 state tracking
	qos2Incoming   map[uint16]*Message // Messages waiting for PUBREL
	qos2IncomingMu sync.Mutex

	// Lifecycle
	stopCh   chan struct{}
	doneCh   chan struct{}
	errCh    chan error
	mu       sync.Mutex
	connMu   sync.RWMutex
}

// NewTestMQTTClient creates a new test MQTT client.
func NewTestMQTTClient(t *testing.T, node *TestNode, clientID string) *TestMQTTClient {
	return &TestMQTTClient{
		t:            t,
		ClientID:     clientID,
		Node:         node,
		messages:     make(chan *Message, 100),
		messageStore: NewInMemoryStore(),
		pendingAcks:  make(map[uint16]*pendingAck),
		qos2Incoming: make(map[uint16]*Message),
		errCh:        make(chan error, 1),
	}
}

// SetMessageStore sets a custom message store.
func (c *TestMQTTClient) SetMessageStore(store MessageStore) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.messageStore = store
}

// Messages returns the message store.
func (c *TestMQTTClient) Messages() MessageStore {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.messageStore
}

// State returns the current connection state.
func (c *TestMQTTClient) State() uint32 {
	return atomic.LoadUint32(&c.state)
}

// IsConnected returns true if the client is connected.
func (c *TestMQTTClient) IsConnected() bool {
	return c.State() == StateConnected
}

// Errors returns the error channel for connection errors.
func (c *TestMQTTClient) Errors() <-chan error {
	return c.errCh
}

// Connect connects to the broker with MQTT v3.1.1 protocol.
func (c *TestMQTTClient) Connect(cleanSession bool) error {
	return c.ConnectWithOptions(ConnectOptions{
		CleanSession: cleanSession,
		KeepAlive:    60,
	})
}

// ConnectWithOptions connects with full options including will message.
func (c *TestMQTTClient) ConnectWithOptions(opts ConnectOptions) error {
	if !atomic.CompareAndSwapUint32(&c.state, StateDisconnected, StateConnecting) {
		return ErrAlreadyConnected
	}

	c.mu.Lock()
	c.cleanSession = opts.CleanSession
	c.stopCh = make(chan struct{})
	c.doneCh = make(chan struct{})
	c.nextPacketID = 1
	c.pendingAcks = make(map[uint16]*pendingAck)
	c.qos2Incoming = make(map[uint16]*Message)
	c.mu.Unlock()

	// Connect to TCP port
	conn, err := net.DialTimeout("tcp", c.Node.TCPAddr, DefaultConnectTimeout)
	if err != nil {
		atomic.StoreUint32(&c.state, StateDisconnected)
		return fmt.Errorf("failed to connect: %w", err)
	}

	c.connMu.Lock()
	c.conn = conn
	c.connMu.Unlock()

	// Build and send CONNECT packet
	connectPkt := c.buildConnectPacket(opts)
	if _, err := conn.Write(connectPkt); err != nil {
		c.closeConn()
		atomic.StoreUint32(&c.state, StateDisconnected)
		return fmt.Errorf("failed to send CONNECT: %w", err)
	}

	// Read CONNACK
	conn.SetReadDeadline(time.Now().Add(DefaultConnectTimeout))
	connackPkt := make([]byte, 4)
	if _, err := io.ReadFull(conn, connackPkt); err != nil {
		c.closeConn()
		atomic.StoreUint32(&c.state, StateDisconnected)
		return fmt.Errorf("failed to read CONNACK: %w", err)
	}
	conn.SetReadDeadline(time.Time{})

	// Verify CONNACK
	if connackPkt[0] != 0x20 {
		c.closeConn()
		atomic.StoreUint32(&c.state, StateDisconnected)
		return fmt.Errorf("expected CONNACK, got packet type %d", connackPkt[0]>>4)
	}
	if connackPkt[3] != 0x00 {
		c.closeConn()
		atomic.StoreUint32(&c.state, StateDisconnected)
		return fmt.Errorf("CONNACK rejected with code %d", connackPkt[3])
	}

	atomic.StoreUint32(&c.state, StateConnected)
	c.t.Logf("Client %s connected to %s", c.ClientID, c.Node.TCPAddr)

	// Start read loop
	go c.readLoop()

	return nil
}

func (c *TestMQTTClient) buildConnectPacket(opts ConnectOptions) []byte {
	var buf bytes.Buffer

	// Variable header - Protocol name
	buf.WriteByte(0x00)
	buf.WriteByte(0x04)
	buf.WriteString("MQTT")

	// Protocol level (3.1.1)
	buf.WriteByte(0x04)

	// Connect flags
	flags := byte(0x00)
	if opts.CleanSession {
		flags |= 0x02
	}
	if opts.Will != nil {
		flags |= 0x04 // Will flag
		flags |= (opts.Will.QoS & 0x03) << 3
		if opts.Will.Retain {
			flags |= 0x20
		}
	}
	buf.WriteByte(flags)

	// Keep alive
	buf.WriteByte(byte(opts.KeepAlive >> 8))
	buf.WriteByte(byte(opts.KeepAlive))

	// Payload - Client ID
	c.writeString(&buf, c.ClientID)

	// Will topic and payload
	if opts.Will != nil {
		c.writeString(&buf, opts.Will.Topic)
		c.writeBytes(&buf, opts.Will.Payload)
	}

	// Fixed header
	payload := buf.Bytes()
	var packet bytes.Buffer
	packet.WriteByte(0x10) // CONNECT packet type
	c.writeRemainingLength(&packet, len(payload))
	packet.Write(payload)

	return packet.Bytes()
}

// Subscribe subscribes to a topic filter and waits for SUBACK.
func (c *TestMQTTClient) Subscribe(filter string, qos byte) error {
	return c.SubscribeMultiple(map[string]byte{filter: qos})
}

// SubscribeMultiple subscribes to multiple topic filters.
func (c *TestMQTTClient) SubscribeMultiple(filters map[string]byte) error {
	if !c.IsConnected() {
		return ErrNotConnected
	}

	packetID := c.nextPacketIDLocked()

	var buf bytes.Buffer

	// Variable header - Packet ID
	binary.Write(&buf, binary.BigEndian, packetID)

	// Payload - Topic filters with QoS
	for filter, qos := range filters {
		c.writeString(&buf, filter)
		buf.WriteByte(qos)
	}

	// Fixed header
	payload := buf.Bytes()
	var packet bytes.Buffer
	packet.WriteByte(0x82) // SUBSCRIBE packet type
	c.writeRemainingLength(&packet, len(payload))
	packet.Write(payload)

	// Register pending ack
	pending := c.registerPendingAck(packetID)

	// Send packet
	if err := c.writePacket(packet.Bytes()); err != nil {
		c.unregisterPendingAck(packetID)
		return fmt.Errorf("failed to send SUBSCRIBE: %w", err)
	}

	// Wait for SUBACK
	select {
	case <-pending.done:
		if pending.err != nil {
			return pending.err
		}
	case <-time.After(DefaultAckTimeout):
		c.unregisterPendingAck(packetID)
		return fmt.Errorf("SUBACK timeout for packet %d", packetID)
	}

	for filter, qos := range filters {
		c.t.Logf("Client %s subscribed to %s (QoS %d)", c.ClientID, filter, qos)
	}
	return nil
}

// Publish publishes a message and waits for acknowledgment if QoS > 0.
func (c *TestMQTTClient) Publish(topic string, qos byte, payload []byte, retain bool) error {
	if !c.IsConnected() {
		return ErrNotConnected
	}

	var buf bytes.Buffer

	// Variable header - Topic name
	c.writeString(&buf, topic)

	// Packet ID (only for QoS > 0)
	var packetID uint16
	if qos > 0 {
		packetID = c.nextPacketIDLocked()
		binary.Write(&buf, binary.BigEndian, packetID)
	}

	// Payload
	buf.Write(payload)

	// Fixed header
	payloadData := buf.Bytes()
	var packet bytes.Buffer

	flags := byte(qos << 1)
	if retain {
		flags |= 0x01
	}
	packet.WriteByte(0x30 | flags) // PUBLISH packet type
	c.writeRemainingLength(&packet, len(payloadData))
	packet.Write(payloadData)

	// Register pending ack for QoS > 0
	var pending *pendingAck
	if qos > 0 {
		pending = c.registerPendingAck(packetID)
	}

	// Send packet
	if err := c.writePacket(packet.Bytes()); err != nil {
		if qos > 0 {
			c.unregisterPendingAck(packetID)
		}
		return fmt.Errorf("failed to send PUBLISH: %w", err)
	}

	c.t.Logf("Client %s published to %s (QoS %d, payload: %d bytes)", c.ClientID, topic, qos, len(payload))

	// Wait for acknowledgment if QoS > 0
	if qos > 0 {
		select {
		case <-pending.done:
			if pending.err != nil {
				return pending.err
			}
		case <-time.After(DefaultAckTimeout):
			c.unregisterPendingAck(packetID)
			return fmt.Errorf("publish ack timeout for packet %d", packetID)
		}
	}

	return nil
}

// Disconnect sends a DISCONNECT packet and closes the connection.
func (c *TestMQTTClient) Disconnect() error {
	if !atomic.CompareAndSwapUint32(&c.state, StateConnected, StateDisconnecting) {
		return nil // Already disconnected or disconnecting
	}

	c.mu.Lock()
	stopCh := c.stopCh
	doneCh := c.doneCh
	c.mu.Unlock()

	// Send DISCONNECT packet
	disconnectPkt := []byte{0xE0, 0x00}
	c.writePacket(disconnectPkt)

	// Signal read loop to stop
	if stopCh != nil {
		close(stopCh)
	}

	// Wait for read loop to finish
	if doneCh != nil {
		select {
		case <-doneCh:
		case <-time.After(2 * time.Second):
			c.t.Logf("Client %s read loop stop timeout", c.ClientID)
		}
	}

	c.closeConn()
	atomic.StoreUint32(&c.state, StateDisconnected)
	c.t.Logf("Client %s disconnected", c.ClientID)

	return nil
}

// DisconnectUngracefully closes the connection without sending DISCONNECT.
// This simulates network failures or client crashes.
func (c *TestMQTTClient) DisconnectUngracefully() {
	if !atomic.CompareAndSwapUint32(&c.state, StateConnected, StateDisconnecting) {
		return // Already disconnected or disconnecting
	}

	c.mu.Lock()
	stopCh := c.stopCh
	doneCh := c.doneCh
	c.mu.Unlock()

	// Close connection without sending DISCONNECT
	c.closeConn()

	// Signal read loop to stop
	if stopCh != nil {
		close(stopCh)
	}

	// Wait for read loop to finish
	if doneCh != nil {
		select {
		case <-doneCh:
		case <-time.After(2 * time.Second):
			c.t.Logf("Client %s read loop stop timeout", c.ClientID)
		}
	}

	atomic.StoreUint32(&c.state, StateDisconnected)
	c.t.Logf("Client %s disconnected ungracefully", c.ClientID)
}

// WaitForMessage waits for a message to arrive.
func (c *TestMQTTClient) WaitForMessage(timeout time.Duration) (*Message, error) {
	select {
	case msg := <-c.messages:
		return msg, nil
	case <-time.After(timeout):
		return nil, ErrTimeout
	}
}

// WaitForMessages waits for multiple messages to arrive.
func (c *TestMQTTClient) WaitForMessages(count int, timeout time.Duration) ([]*Message, error) {
	msgs := make([]*Message, 0, count)
	deadline := time.Now().Add(timeout)

	for len(msgs) < count {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return msgs, fmt.Errorf("timeout: received %d of %d messages", len(msgs), count)
		}

		select {
		case msg := <-c.messages:
			msgs = append(msgs, msg)
		case <-time.After(remaining):
			return msgs, fmt.Errorf("timeout: received %d of %d messages", len(msgs), count)
		}
	}

	return msgs, nil
}

// Reconnect reconnects to a different node (for testing session takeover).
func (c *TestMQTTClient) Reconnect(newNode *TestNode) error {
	// Store clean session preference
	c.mu.Lock()
	cleanSession := c.cleanSession
	c.mu.Unlock()

	// Disconnect if connected
	if c.IsConnected() {
		c.Disconnect()
	}

	// Wait for full disconnect
	for c.State() != StateDisconnected {
		time.Sleep(10 * time.Millisecond)
	}

	c.Node = newNode
	return c.Connect(cleanSession)
}

// readLoop reads packets from the connection.
func (c *TestMQTTClient) readLoop() {
	defer func() {
		c.mu.Lock()
		if c.doneCh != nil {
			close(c.doneCh)
		}
		c.mu.Unlock()

		if r := recover(); r != nil {
			c.t.Logf("Client %s read loop panic: %v", c.ClientID, r)
		}
	}()

	for {
		c.mu.Lock()
		stopCh := c.stopCh
		c.mu.Unlock()

		select {
		case <-stopCh:
			return
		default:
		}

		conn := c.getConn()
		if conn == nil {
			return
		}

		// Set read deadline
		conn.SetReadDeadline(time.Now().Add(DefaultReadTimeout))

		// Read fixed header
		header := make([]byte, 1)
		_, err := conn.Read(header)
		if err != nil {
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				continue
			}
			if err != io.EOF && c.State() == StateConnected {
				c.notifyError(fmt.Errorf("read error: %w", err))
			}
			return
		}

		// Read remaining length
		remainingLength, err := c.readRemainingLength(conn)
		if err != nil {
			if c.State() == StateConnected {
				c.notifyError(fmt.Errorf("read remaining length: %w", err))
			}
			return
		}

		// Read packet data
		data := make([]byte, remainingLength)
		if remainingLength > 0 {
			if _, err := io.ReadFull(conn, data); err != nil {
				if c.State() == StateConnected {
					c.notifyError(fmt.Errorf("read packet data: %w", err))
				}
				return
			}
		}

		// Handle packet
		packetType := header[0] >> 4
		switch packetType {
		case 3: // PUBLISH
			c.handlePublish(header[0], data)
		case 4: // PUBACK
			c.handlePubAck(data)
		case 5: // PUBREC
			c.handlePubRec(data)
		case 6: // PUBREL
			c.handlePubRel(data)
		case 7: // PUBCOMP
			c.handlePubComp(data)
		case 9: // SUBACK
			c.handleSubAck(data)
		case 11: // UNSUBACK
			c.handleUnsubAck(data)
		case 13: // PINGRESP
			// Ignore
		}
	}
}

func (c *TestMQTTClient) handlePublish(flags byte, data []byte) {
	qos := (flags >> 1) & 0x03
	retain := (flags & 0x01) != 0
	dup := (flags & 0x08) != 0

	if len(data) < 2 {
		return
	}

	// Parse topic
	topicLen := int(data[0])<<8 | int(data[1])
	if len(data) < 2+topicLen {
		return
	}
	topic := string(data[2 : 2+topicLen])

	offset := 2 + topicLen
	var packetID uint16

	// Parse packet ID if QoS > 0
	if qos > 0 {
		if len(data) < offset+2 {
			return
		}
		packetID = binary.BigEndian.Uint16(data[offset : offset+2])
		offset += 2
	}

	// Payload
	var payload []byte
	if offset < len(data) {
		payload = data[offset:]
	}

	msg := &Message{
		Topic:    topic,
		Payload:  payload,
		QoS:      qos,
		Retain:   retain,
		Dup:      dup,
		PacketID: packetID,
	}

	// Handle based on QoS
	switch qos {
	case 0:
		c.deliverMessage(msg)
	case 1:
		c.deliverMessage(msg)
		c.sendPubAck(packetID)
	case 2:
		// Store for later delivery after PUBREL
		c.qos2IncomingMu.Lock()
		c.qos2Incoming[packetID] = msg
		c.qos2IncomingMu.Unlock()
		c.sendPubRec(packetID)
	}
}

func (c *TestMQTTClient) handlePubAck(data []byte) {
	if len(data) < 2 {
		return
	}
	packetID := binary.BigEndian.Uint16(data[0:2])
	c.completePendingAck(packetID, nil)
}

func (c *TestMQTTClient) handlePubRec(data []byte) {
	if len(data) < 2 {
		return
	}
	packetID := binary.BigEndian.Uint16(data[0:2])
	c.sendPubRel(packetID)
}

func (c *TestMQTTClient) handlePubRel(data []byte) {
	if len(data) < 2 {
		return
	}
	packetID := binary.BigEndian.Uint16(data[0:2])

	// Deliver the stored message
	c.qos2IncomingMu.Lock()
	msg, ok := c.qos2Incoming[packetID]
	if ok {
		delete(c.qos2Incoming, packetID)
	}
	c.qos2IncomingMu.Unlock()

	if ok && msg != nil {
		c.deliverMessage(msg)
	}

	c.sendPubComp(packetID)
}

func (c *TestMQTTClient) handlePubComp(data []byte) {
	if len(data) < 2 {
		return
	}
	packetID := binary.BigEndian.Uint16(data[0:2])
	c.completePendingAck(packetID, nil)
}

func (c *TestMQTTClient) handleSubAck(data []byte) {
	if len(data) < 2 {
		return
	}
	packetID := binary.BigEndian.Uint16(data[0:2])

	// Check return codes
	var err error
	if len(data) > 2 {
		for _, rc := range data[2:] {
			if rc == 0x80 {
				err = fmt.Errorf("subscription failed with return code 0x80")
				break
			}
		}
	}

	c.completePendingAck(packetID, err)
}

func (c *TestMQTTClient) handleUnsubAck(data []byte) {
	if len(data) < 2 {
		return
	}
	packetID := binary.BigEndian.Uint16(data[0:2])
	c.completePendingAck(packetID, nil)
}

func (c *TestMQTTClient) deliverMessage(msg *Message) {
	// Store in message store
	c.mu.Lock()
	store := c.messageStore
	c.mu.Unlock()

	if store != nil {
		store.Store(msg)
	}

	// Send to channel
	select {
	case c.messages <- msg:
	default:
		c.t.Logf("Client %s message queue full, dropping message", c.ClientID)
	}
}

func (c *TestMQTTClient) sendPubAck(packetID uint16) {
	pkt := []byte{0x40, 0x02, byte(packetID >> 8), byte(packetID)}
	c.writePacket(pkt)
}

func (c *TestMQTTClient) sendPubRec(packetID uint16) {
	pkt := []byte{0x50, 0x02, byte(packetID >> 8), byte(packetID)}
	c.writePacket(pkt)
}

func (c *TestMQTTClient) sendPubRel(packetID uint16) {
	pkt := []byte{0x62, 0x02, byte(packetID >> 8), byte(packetID)}
	c.writePacket(pkt)
}

func (c *TestMQTTClient) sendPubComp(packetID uint16) {
	pkt := []byte{0x70, 0x02, byte(packetID >> 8), byte(packetID)}
	c.writePacket(pkt)
}

// Helper methods

func (c *TestMQTTClient) nextPacketIDLocked() uint16 {
	c.packetIDMu.Lock()
	defer c.packetIDMu.Unlock()

	id := c.nextPacketID
	c.nextPacketID++
	if c.nextPacketID == 0 {
		c.nextPacketID = 1
	}
	return id
}

func (c *TestMQTTClient) registerPendingAck(packetID uint16) *pendingAck {
	c.pendingAcksMu.Lock()
	defer c.pendingAcksMu.Unlock()

	pending := &pendingAck{
		packetID: packetID,
		done:     make(chan struct{}),
	}
	c.pendingAcks[packetID] = pending
	return pending
}

func (c *TestMQTTClient) unregisterPendingAck(packetID uint16) {
	c.pendingAcksMu.Lock()
	defer c.pendingAcksMu.Unlock()
	delete(c.pendingAcks, packetID)
}

func (c *TestMQTTClient) completePendingAck(packetID uint16, err error) {
	c.pendingAcksMu.Lock()
	pending, ok := c.pendingAcks[packetID]
	if ok {
		delete(c.pendingAcks, packetID)
	}
	c.pendingAcksMu.Unlock()

	if ok && pending != nil {
		pending.err = err
		close(pending.done)
	}
}

func (c *TestMQTTClient) getConn() net.Conn {
	c.connMu.RLock()
	defer c.connMu.RUnlock()
	return c.conn
}

func (c *TestMQTTClient) closeConn() {
	c.connMu.Lock()
	defer c.connMu.Unlock()
	if c.conn != nil {
		c.conn.Close()
		c.conn = nil
	}
}

func (c *TestMQTTClient) writePacket(data []byte) error {
	conn := c.getConn()
	if conn == nil {
		return ErrNotConnected
	}
	_, err := conn.Write(data)
	return err
}

func (c *TestMQTTClient) notifyError(err error) {
	select {
	case c.errCh <- err:
	default:
	}
}

func (c *TestMQTTClient) readRemainingLength(conn net.Conn) (int, error) {
	multiplier := 1
	value := 0
	for i := 0; i < 4; i++ {
		b := make([]byte, 1)
		if _, err := conn.Read(b); err != nil {
			return 0, err
		}

		value += int(b[0]&0x7F) * multiplier
		if (b[0] & 0x80) == 0 {
			break
		}
		multiplier *= 128
	}
	return value, nil
}

func (c *TestMQTTClient) writeRemainingLength(buf *bytes.Buffer, length int) {
	for {
		encodedByte := byte(length % 128)
		length /= 128
		if length > 0 {
			encodedByte |= 128
		}
		buf.WriteByte(encodedByte)
		if length == 0 {
			break
		}
	}
}

func (c *TestMQTTClient) writeString(buf *bytes.Buffer, s string) {
	data := []byte(s)
	buf.WriteByte(byte(len(data) >> 8))
	buf.WriteByte(byte(len(data)))
	buf.Write(data)
}

func (c *TestMQTTClient) writeBytes(buf *bytes.Buffer, data []byte) {
	buf.WriteByte(byte(len(data) >> 8))
	buf.WriteByte(byte(len(data)))
	buf.Write(data)
}
