package core

import (
	"errors"
	"io"
	"log/slog"
	"net"
	"sync"
	"time"

	"github.com/dborovcanin/mqtt/core/packets"
	v3 "github.com/dborovcanin/mqtt/core/packets/v3"
	v5 "github.com/dborovcanin/mqtt/core/packets/v5"
)

var _ Connection = (*connection)(nil)

// Connection represents a network connection that can read/write MQTT packets.
// It also manages connection state and keep-alive.
type Connection interface {
	// ReadPacket reads the next MQTT packet from the connection.
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

	// SetKeepAlive sets the keep-alive interval in seconds.
	SetKeepAlive(seconds uint16)

	// SetOnDisconnect sets a callback to be called when the connection is closed or lost.
	SetOnDisconnect(fn func(graceful bool))

	// State returns the current connection state.
	State() State

	// ConnectedAt returns the time when the connection was established.
	ConnectedAt() time.Time

	// DisconnectedAt returns the time when the connection was closed.
	DisconnectedAt() time.Time

	// StopChan returns a channel that is closed when the connection is terminated.
	StopChan() <-chan struct{}

	// TouchActivity updates the last activity timestamp (used by external components if needed).
	TouchActivity()
}

// PacketWriter is an interface for writing packets.
type PacketWriter interface {
	WritePacket(pkt packets.ControlPacket) error
}

// connection wraps a net.Conn and provides MQTT packet-level I/O with state management.
type connection struct {
	conn    net.Conn
	reader  io.Reader
	version int // 0 = unknown, 3/4 = v3.1/v3.1.1, 5 = v5

	mu sync.RWMutex

	// clientID string // Optional, for logging
	state State

	connectedAt    time.Time
	disconnectedAt time.Time
	lastActivity   time.Time

	keepAlive       uint16
	keepAliveExpiry time.Duration
	keepAliveTimer  *time.Timer

	onDisconnect func(graceful bool)
	stopCh       chan struct{}
}

// NewConnection creates a new MQTT connection wrapping a network connection.
func NewConnection(conn net.Conn) Connection {
	return &connection{
		conn:        conn,
		reader:      conn,
		state:       StateConnected,
		connectedAt: time.Now(),
		stopCh:      make(chan struct{}),
	}
}

// ReadPacket reads the next MQTT packet from the connection.
func (c *connection) ReadPacket() (packets.ControlPacket, error) {
	c.TouchActivity()

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

func (c *connection) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.state == StateDisconnected {
		return nil
	}

	c.cleanup(true)
	return nil
}

// cleanup releases resources. Must be called with lock held.
func (c *connection) cleanup(graceful bool) {
	if c.keepAliveTimer != nil {
		c.keepAliveTimer.Stop()
		c.keepAliveTimer = nil
	}

	c.conn.Close()
	c.state = StateDisconnected
	c.disconnectedAt = time.Now()

	select {
	case <-c.stopCh:
	default:
		close(c.stopCh)
	}

	if c.onDisconnect != nil {
		// execute callback in background to avoid holding lock
		cb := c.onDisconnect
		go cb(graceful)
	}
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

func (c *connection) SetKeepAlive(seconds uint16) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.keepAlive = seconds
	if seconds > 0 {
		c.keepAliveExpiry = time.Duration(seconds) * time.Second * 3 / 2
		c.startKeepAliveTimer()
	} else {
		c.keepAliveExpiry = 0
		if c.keepAliveTimer != nil {
			c.keepAliveTimer.Stop()
			c.keepAliveTimer = nil
		}
	}
}

func (c *connection) SetOnDisconnect(fn func(graceful bool)) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.onDisconnect = fn
}

func (c *connection) State() State {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.state
}

func (c *connection) ConnectedAt() time.Time {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.connectedAt
}

func (c *connection) DisconnectedAt() time.Time {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.disconnectedAt
}

func (c *connection) StopChan() <-chan struct{} {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.stopCh
}

func (c *connection) TouchActivity() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.lastActivity = time.Now()
}

// Internal keep-alive logic

func (c *connection) startKeepAliveTimer() {
	if c.keepAliveTimer != nil {
		c.keepAliveTimer.Stop()
	}

	// Make sure we have lastActivity set, otherwise use now
	if c.lastActivity.IsZero() {
		c.lastActivity = time.Now()
	}

	c.keepAliveTimer = time.AfterFunc(c.keepAliveExpiry, func() {
		c.checkKeepAlive()
	})
}

func (c *connection) checkKeepAlive() {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.state != StateConnected {
		return
	}

	elapsed := time.Since(c.lastActivity)
	if elapsed >= c.keepAliveExpiry {
		slog.Info("Keepalive expired, closing connection")
		c.cleanup(false)
		return
	}

	remaining := c.keepAliveExpiry - elapsed
	c.keepAliveTimer = time.AfterFunc(remaining, func() {
		c.checkKeepAlive()
	})
}
