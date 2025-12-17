package core

import (
	"errors"
	"io"
	"net"
	"sync"
	"time"

	"github.com/absmach/mqtt/core/packets"
	v3 "github.com/absmach/mqtt/core/packets/v3"
	v5 "github.com/absmach/mqtt/core/packets/v5"
)

var _ Connection = (*connection)(nil)

// Connection represents a network connection that can read/write MQTT packets.
// It also manages connection state and keep-alive.
type Connection interface {
	net.Conn
	PacketReader
	PacketWriter
	SetKeepAlive(t time.Duration) error
	SetOnDisconnect(fn func(graceful bool))
	Touch()
}

// PacketWriter is an interface for writing packets.
type PacketWriter interface {
	WritePacket(pkt packets.ControlPacket) error
}

type PacketReader interface {
	ReadPacket() (packets.ControlPacket, error)
}

// connection wraps a net.Conn and provides MQTT packet-level I/O with state management.
type connection struct {
	conn    *net.TCPConn
	reader  io.Reader
	version int // 0 = unknown, 3/4 = v3.1/v3.1.1, 5 = v5

	mu sync.RWMutex

	lastActivity time.Time

	onDisconnect func(graceful bool)
}

// NewConnection creates a new MQTT connection wrapping a network connection.
func NewConnection(conn *net.TCPConn) Connection {
	return &connection{
		conn:   conn,
		reader: conn,
	}
}

// ReadPacket reads the next MQTT packet from the connection.
func (c *connection) ReadPacket() (packets.ControlPacket, error) {
	c.Touch()

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
	var pkt packets.ControlPacket
	var err error

	switch c.version {
	case 5:
		pkt, _, _, err = v5.ReadPacket(c.reader)
	case 3, 4:
		// v4 is MQTT 3.1.1, v3 is MQTT 3.1
		pkt, err = v3.ReadPacket(c.reader)
	default:
		err = errors.New("unsupported MQTT protocol version")
	}

	if err != nil {
		return nil, err
	}
	return pkt, nil
}

func (c *connection) WritePacket(pkt packets.ControlPacket) error {
	if pkt == nil {
		return errors.New("cannot encode nil packet")
	}
	return pkt.Pack(c.conn)
}

func (c *connection) Read(b []byte) (n int, err error) {
	return c.conn.Read(b)
}

func (c *connection) Write(b []byte) (n int, err error) {
	return c.conn.Write(b)
}

func (c *connection) Close() error {
	return c.conn.Close()
}

func (c *connection) LocalAddr() net.Addr {
	return c.conn.LocalAddr()
}

func (c *connection) RemoteAddr() net.Addr {
	return c.conn.RemoteAddr()
}

func (c *connection) SetReadDeadline(t time.Time) error {
	return c.conn.SetReadDeadline(t)
}

func (c *connection) SetWriteDeadline(t time.Time) error {
	return c.conn.SetWriteDeadline(t)
}

func (c *connection) SetDeadline(t time.Time) error {
	return c.conn.SetDeadline(t)
}

func (c *connection) SetKeepAlive(d time.Duration) error {
	cfg := net.KeepAliveConfig{
		Enable:   true,
		Idle:     d,
		Interval: d,
	}
	return c.conn.SetKeepAliveConfig(cfg)
}

func (c *connection) SetOnDisconnect(fn func(graceful bool)) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.onDisconnect = fn
}

func (c *connection) Touch() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.lastActivity = time.Now()
}
