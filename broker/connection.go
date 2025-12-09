// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package broker

import (
	"errors"
	"io"
	"net"

	"github.com/dborovcanin/mqtt/packets"
	v3 "github.com/dborovcanin/mqtt/packets/v3"
	v5 "github.com/dborovcanin/mqtt/packets/v5"
)

// mqttCodec wraps a net.Conn and provides MQTT packet-level I/O.
// It auto-detects protocol version on the first packet (CONNECT).
type mqttCodec struct {
	conn    net.Conn
	reader  io.Reader
	version int // 0 = unknown, 3/4 = v3.1/v3.1.1, 5 = v5
}

// newMQTTCodec creates a new MQTT codec wrapping a network connection.
// Returns unexported implementation for better encapsulation.
func newMQTTCodec(conn net.Conn) *mqttCodec {
	return &mqttCodec{
		conn:   conn,
		reader: conn,
	}
}

// ReadPacket reads the next MQTT packet from the connection.
// It automatically detects the protocol version on the first packet.
func (c *mqttCodec) ReadPacket() (packets.ControlPacket, error) {
	if c.version == 0 {
		// Detect protocol version from the first packet (CONNECT)
		ver, restored, err := packets.DetectProtocolVersion(c.reader)
		if err != nil {
			return nil, err
		}
		c.version = ver
		c.reader = restored
	}

	// Dispatch based on version
	switch c.version {
	case 5:
		pkt, _, _, err := v5.ReadPacket(c.reader)
		return pkt, err
	case 3, 4:
		// v4 is MQTT 3.1.1, v3 is MQTT 3.1
		pkt, _, _, err := v3.ReadPacket(c.reader)
		return pkt, err
	default:
		return nil, errors.New("unsupported MQTT protocol version")
	}
}

// WritePacket writes an MQTT packet to the connection.
func (c *mqttCodec) WritePacket(pkt packets.ControlPacket) error {
	if pkt == nil {
		return errors.New("cannot encode nil packet")
	}
	return pkt.Pack(c.conn)
}

// Close closes the underlying connection.
func (c *mqttCodec) Close() error {
	return c.conn.Close()
}

// RemoteAddr returns the remote network address.
func (c *mqttCodec) RemoteAddr() net.Addr {
	return c.conn.RemoteAddr()
}

// NewConnection creates a broker.Connection from a net.Conn.
// This wraps the connection with an MQTT codec for packet-level I/O.
// Returns the Connection interface with unexported implementation.
func NewConnection(conn net.Conn) Connection {
	return newMQTTCodec(conn)
}
