// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package testutil

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"sync"
	"testing"
	"time"
)

// TestMQTTClient is a minimal MQTT client for integration testing.
type TestMQTTClient struct {
	t        *testing.T
	ClientID string
	Conn     net.Conn
	Node     *TestNode
	messages chan *Message
	mu       sync.Mutex
	stopCh   chan struct{}
	cleanSession bool
}

// Message represents a received MQTT message.
type Message struct {
	Topic   string
	Payload []byte
	QoS     byte
	Retain  bool
	Dup     bool
}

// NewTestMQTTClient creates a new test MQTT client.
func NewTestMQTTClient(t *testing.T, node *TestNode, clientID string) *TestMQTTClient {
	return &TestMQTTClient{
		t:        t,
		ClientID: clientID,
		Node:     node,
		messages: make(chan *Message, 100),
		stopCh:   make(chan struct{}),
	}
}

// Connect connects to the broker with MQTT v3.1.1 protocol.
func (c *TestMQTTClient) Connect(cleanSession bool) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.cleanSession = cleanSession

	// Connect to TCP port
	conn, err := net.DialTimeout("tcp", c.Node.TCPAddr, 5*time.Second)
	if err != nil {
		return fmt.Errorf("failed to connect: %w", err)
	}
	c.Conn = conn

	// Build CONNECT packet (MQTT 3.1.1)
	connectPkt := c.buildConnectPacket(cleanSession)

	// Send CONNECT
	if _, err := conn.Write(connectPkt); err != nil {
		conn.Close()
		return fmt.Errorf("failed to send CONNECT: %w", err)
	}

	// Read CONNACK
	connackPkt := make([]byte, 4)
	if _, err := io.ReadFull(conn, connackPkt); err != nil {
		conn.Close()
		return fmt.Errorf("failed to read CONNACK: %w", err)
	}

	// Verify CONNACK
	if connackPkt[0] != 0x20 || connackPkt[3] != 0x00 {
		conn.Close()
		return fmt.Errorf("CONNACK failed: %v", connackPkt)
	}

	c.t.Logf("Client %s connected to %s", c.ClientID, c.Node.ID)

	// Start reading messages
	go c.readLoop()

	return nil
}

func (c *TestMQTTClient) buildConnectPacket(cleanSession bool) []byte {
	var buf bytes.Buffer

	// Variable header
	// Protocol name
	buf.WriteByte(0x00)
	buf.WriteByte(0x04)
	buf.WriteString("MQTT")

	// Protocol level (3.1.1)
	buf.WriteByte(0x04)

	// Connect flags
	flags := byte(0x00)
	if cleanSession {
		flags |= 0x02
	}
	buf.WriteByte(flags)

	// Keep alive (60 seconds)
	buf.WriteByte(0x00)
	buf.WriteByte(0x3C)

	// Payload - Client ID
	clientIDBytes := []byte(c.ClientID)
	buf.WriteByte(byte(len(clientIDBytes) >> 8))
	buf.WriteByte(byte(len(clientIDBytes)))
	buf.Write(clientIDBytes)

	// Fixed header
	payload := buf.Bytes()
	var packet bytes.Buffer
	packet.WriteByte(0x10) // CONNECT packet type
	c.writeRemainingLength(&packet, len(payload))
	packet.Write(payload)

	return packet.Bytes()
}

// Subscribe subscribes to a topic filter.
func (c *TestMQTTClient) Subscribe(filter string, qos byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	var buf bytes.Buffer

	// Variable header - Packet ID
	packetID := uint16(1)
	binary.Write(&buf, binary.BigEndian, packetID)

	// Payload - Topic filter + QoS
	filterBytes := []byte(filter)
	buf.WriteByte(byte(len(filterBytes) >> 8))
	buf.WriteByte(byte(len(filterBytes)))
	buf.Write(filterBytes)
	buf.WriteByte(qos)

	// Fixed header
	payload := buf.Bytes()
	var packet bytes.Buffer
	packet.WriteByte(0x82) // SUBSCRIBE packet type
	c.writeRemainingLength(&packet, len(payload))
	packet.Write(payload)

	if _, err := c.Conn.Write(packet.Bytes()); err != nil {
		return fmt.Errorf("failed to send SUBSCRIBE: %w", err)
	}

	c.t.Logf("Client %s subscribed to %s (QoS %d)", c.ClientID, filter, qos)
	return nil
}

// Publish publishes a message.
func (c *TestMQTTClient) Publish(topic string, qos byte, payload []byte, retain bool) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	var buf bytes.Buffer

	// Variable header - Topic name
	topicBytes := []byte(topic)
	buf.WriteByte(byte(len(topicBytes) >> 8))
	buf.WriteByte(byte(len(topicBytes)))
	buf.Write(topicBytes)

	// Packet ID (only for QoS > 0)
	if qos > 0 {
		packetID := uint16(2)
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

	if _, err := c.Conn.Write(packet.Bytes()); err != nil {
		return fmt.Errorf("failed to send PUBLISH: %w", err)
	}

	c.t.Logf("Client %s published to %s (QoS %d, payload: %d bytes)", c.ClientID, topic, qos, len(payload))
	return nil
}

// Disconnect sends a DISCONNECT packet and closes the connection.
func (c *TestMQTTClient) Disconnect() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.Conn == nil {
		return nil
	}

	// Send DISCONNECT packet
	disconnectPkt := []byte{0xE0, 0x00}
	c.Conn.Write(disconnectPkt)

	// Close connection
	c.Conn.Close()
	c.Conn = nil

	close(c.stopCh)
	c.t.Logf("Client %s disconnected", c.ClientID)

	return nil
}

// WaitForMessage waits for a message to arrive.
func (c *TestMQTTClient) WaitForMessage(timeout time.Duration) (*Message, error) {
	select {
	case msg := <-c.messages:
		return msg, nil
	case <-time.After(timeout):
		return nil, fmt.Errorf("timeout waiting for message")
	}
}

// Reconnect reconnects to a different node (for testing session takeover).
func (c *TestMQTTClient) Reconnect(newNode *TestNode) error {
	c.mu.Lock()
	oldConn := c.Conn
	oldStopCh := c.stopCh
	c.Conn = nil
	c.mu.Unlock()

	// Stop old read loop
	if oldStopCh != nil {
		close(oldStopCh)
	}

	if oldConn != nil {
		oldConn.Close()
	}

	// Create new stop channel for new connection
	c.mu.Lock()
	c.stopCh = make(chan struct{})
	c.mu.Unlock()

	c.Node = newNode
	return c.Connect(c.cleanSession)
}

func (c *TestMQTTClient) readLoop() {
	defer func() {
		if r := recover(); r != nil {
			c.t.Logf("Client %s read loop panic: %v", c.ClientID, r)
		}
	}()

	for {
		select {
		case <-c.stopCh:
			return
		default:
		}

		if c.Conn == nil {
			return
		}

		// Set read deadline
		c.Conn.SetReadDeadline(time.Now().Add(100 * time.Millisecond))

		// Read fixed header
		header := make([]byte, 1)
		_, err := c.Conn.Read(header)
		if err != nil {
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				continue
			}
			return
		}

		packetType := header[0] >> 4

		// Read remaining length
		remainingLength, err := c.readRemainingLength()
		if err != nil {
			return
		}

		// Read packet data
		data := make([]byte, remainingLength)
		if remainingLength > 0 {
			if _, err := io.ReadFull(c.Conn, data); err != nil {
				return
			}
		}

		// Handle packet
		switch packetType {
		case 3: // PUBLISH
			c.handlePublish(header[0], data)
		case 9: // SUBACK
			// Ignore for now
		case 13: // PINGRESP
			// Ignore for now
		}
	}
}

func (c *TestMQTTClient) handlePublish(flags byte, data []byte) {
	qos := (flags >> 1) & 0x03
	retain := (flags & 0x01) != 0
	dup := (flags & 0x08) != 0

	// Parse topic
	topicLen := int(data[0])<<8 | int(data[1])
	topic := string(data[2 : 2+topicLen])

	offset := 2 + topicLen

	// Skip packet ID if QoS > 0
	if qos > 0 {
		offset += 2
	}

	// Payload
	payload := data[offset:]

	msg := &Message{
		Topic:   topic,
		Payload: payload,
		QoS:     qos,
		Retain:  retain,
		Dup:     dup,
	}

	select {
	case c.messages <- msg:
	default:
		c.t.Logf("Client %s message queue full, dropping message", c.ClientID)
	}
}

func (c *TestMQTTClient) readRemainingLength() (int, error) {
	multiplier := 1
	value := 0
	for {
		b := make([]byte, 1)
		if _, err := c.Conn.Read(b); err != nil {
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
