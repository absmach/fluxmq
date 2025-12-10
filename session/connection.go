package session

import (
	"errors"
	"io"
	"net"
	"time"

	"github.com/dborovcanin/mqtt/packets"
	v3 "github.com/dborovcanin/mqtt/packets/v3"
	v5 "github.com/dborovcanin/mqtt/packets/v5"
)

// Connection represents a network connection that can read/write MQTT packets.
type Connection interface {
	// ReadPacket reads the next MQTT packet from the connection.
	// It returns the packet or an error.
	ReadPacket() (packets.ControlPacket, error)

	// WritePacket writes an MQTT packet to the connection.
	WritePacket(p packets.ControlPacket) error

	// Close terminates the connection.
	Close() error

	// RemoteAddr returns the address of the connected client.
	RemoteAddr() net.Addr

	// SetReadDeadline sets the connection read deadline.
	SetReadDeadline(t time.Time) error

	// SetWriteDeadline sets the connection write deadline.
	SetWriteDeadline(t time.Time) error
}

var _ Connection = (*mqttCodec)(nil)

// mqttCodec wraps a net.Conn and provides MQTT packet-level I/O.
// It auto-detects protocol version on the first packet (CONNECT).
type mqttCodec struct {
	conn    net.Conn
	reader  io.Reader
	version int // 0 = unknown, 3/4 = v3.1/v3.1.1, 5 = v5
}

// newMQTTCodec creates a new MQTT codec wrapping a network connection.
// Returns unexported implementation for better encapsulation.
func NewConnection(conn net.Conn) Connection {
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
		pkt, err := v3.ReadPacket(c.reader)
		return pkt, err
	default:
		return nil, errors.New("unsupported MQTT protocol version")
	}
}

func (c *mqttCodec) WritePacket(pkt packets.ControlPacket) error {
	if pkt == nil {
		return errors.New("cannot encode nil packet")
	}
	return pkt.Pack(c.conn)
}

func (c *mqttCodec) Close() error {
	return c.conn.Close()
}

func (c *mqttCodec) RemoteAddr() net.Addr {
	return c.conn.RemoteAddr()
}

func (c *mqttCodec) SetReadDeadline(t time.Time) error {
	return c.conn.SetReadDeadline(t)
}

func (c *mqttCodec) SetWriteDeadline(t time.Time) error {
	return c.conn.SetWriteDeadline(t)
}
