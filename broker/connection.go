package broker

import (
	"errors"
	"io"
	"net"

	packets "github.com/dborovcanin/mqtt/packets"
	v3 "github.com/dborovcanin/mqtt/packets/v3"
	v5 "github.com/dborovcanin/mqtt/packets/v5"
)

// StreamConnection wraps a net.Conn to provide packet-level reading/writing.
// It handles protocol version detection (sniffing) on the first packet.
type StreamConnection struct {
	net.Conn
	reader  io.Reader
	version int // 0 = unknown, 4=v3.1.1, 5=v5
}

// NewStreamConnection creates a new StreamConnection wrapping a net.Conn.
func NewStreamConnection(conn net.Conn) *StreamConnection {
	return &StreamConnection{
		Conn:   conn,
		reader: conn,
	}
}

// ReadPacket reads a packet from the stream.
// It automatically detects the protocol version if not yet known.
func (c *StreamConnection) ReadPacket() (packets.ControlPacket, error) {
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
	if c.version == 5 {
		pkt, _, _, err := v5.ReadPacket(c.reader)
		return pkt, err
	} else if c.version == 4 || c.version == 3 {
		// v4 is 3.1.1, v3 is 3.1
		pkt, _, _, err := v3.ReadPacket(c.reader)
		return pkt, err
	}

	return nil, errors.New("unsupported protocol version")
}

// WritePacket writes an MQTT packet to the connection.
func (c *StreamConnection) WritePacket(p packets.ControlPacket) error {
	return p.Pack(c.Conn)
}
