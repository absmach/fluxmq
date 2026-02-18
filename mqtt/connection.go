// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package mqtt

import (
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/absmach/fluxmq/mqtt/packets"
	v3 "github.com/absmach/fluxmq/mqtt/packets/v3"
	v5 "github.com/absmach/fluxmq/mqtt/packets/v5"
)

const controlBurst = 32

var (
	_ Connection = (*connection)(nil)

	ErrUnsupportedProtocolVersion = errors.New("unsupported MQTT protocol version")
	ErrCannotEncodeNilPacket      = errors.New("cannot encode nil packet")
	ErrSendQueueFull              = errors.New("send queue full: client disconnected")
)

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
	WriteControlPacket(pkt packets.ControlPacket, onSent func()) error
	WriteDataPacket(pkt packets.ControlPacket, onSent func()) error
}

type PacketReader interface {
	ReadPacket() (packets.ControlPacket, error)
}

// sendItem is queued for asynchronous socket writes.
type sendItem struct {
	pkt    packets.ControlPacket
	onSent func()
}

// connection wraps a net.Conn and provides MQTT packet-level I/O with state management.
type connection struct {
	conn    net.Conn
	reader  io.Reader
	version int // 0 = unknown, 3/4 = v3.1/v3.1.1, 5 = v5

	mu sync.RWMutex

	sendMu           sync.Mutex
	controlCh        chan sendItem
	dataCh           chan sendItem
	closeCh          chan struct{}
	closeOnce        sync.Once
	sendWg           sync.WaitGroup
	disconnectOnFull bool
	closed           atomic.Bool

	lastActivity time.Time

	onDisconnect func(graceful bool)
}

// NewConnection creates a new MQTT connection wrapping a network connection.
// queueSize <= 0 keeps synchronous writes; queueSize > 0 enables asynchronous queued writes.
func NewConnection(conn net.Conn, queueSize int, disconnectOnFull bool) Connection {
	c := &connection{
		conn:             conn,
		reader:           conn,
		disconnectOnFull: disconnectOnFull,
	}

	if queueSize > 0 {
		controlCap := queueSize / 4
		if controlCap < 1 {
			controlCap = 1
		}
		c.controlCh = make(chan sendItem, controlCap)
		c.dataCh = make(chan sendItem, queueSize)
		c.closeCh = make(chan struct{})

		c.sendWg.Add(1)
		go c.sendLoop()
	}

	return c
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
		err = ErrUnsupportedProtocolVersion
	}

	if err != nil {
		return nil, err
	}
	return pkt, nil
}

func (c *connection) WritePacket(pkt packets.ControlPacket) error {
	return c.WriteControlPacket(pkt, nil)
}

func (c *connection) WriteControlPacket(pkt packets.ControlPacket, onSent func()) error {
	if pkt == nil {
		return ErrCannotEncodeNilPacket
	}

	if c.controlCh == nil {
		return c.writeSync(pkt, onSent)
	}

	if c.closed.Load() {
		return net.ErrClosed
	}

	item := sendItem{pkt: pkt, onSent: onSent}
	select {
	case c.controlCh <- item:
		return nil
	case <-c.closeCh:
		return net.ErrClosed
	}
}

func (c *connection) WriteDataPacket(pkt packets.ControlPacket, onSent func()) error {
	if pkt == nil {
		return ErrCannotEncodeNilPacket
	}

	if c.dataCh == nil {
		return c.writeSync(pkt, onSent)
	}

	if c.closed.Load() {
		return net.ErrClosed
	}

	item := sendItem{pkt: pkt, onSent: onSent}
	if c.disconnectOnFull {
		select {
		case c.dataCh <- item:
			return nil
		case <-c.closeCh:
			return net.ErrClosed
		default:
			c.markClosed()
			_ = c.conn.Close()
			return ErrSendQueueFull
		}
	}

	select {
	case c.dataCh <- item:
		return nil
	case <-c.closeCh:
		return net.ErrClosed
	}
}

func (c *connection) writeSync(pkt packets.ControlPacket, onSent func()) error {
	if c.closed.Load() {
		return net.ErrClosed
	}

	c.sendMu.Lock()
	defer c.sendMu.Unlock()

	if c.closed.Load() {
		return net.ErrClosed
	}

	if err := pkt.Pack(c.conn); err != nil {
		return err
	}
	if onSent != nil {
		onSent()
	}
	return nil
}

func (c *connection) sendLoop() {
	defer c.sendWg.Done()

	for {
		controlCount := 0

		for draining := true; draining && controlCount < controlBurst; {
			select {
			case <-c.closeCh:
				return
			case item := <-c.controlCh:
				if !c.doWrite(item) {
					return
				}
				controlCount++
			default:
				draining = false
			}
		}

		if controlCount == controlBurst {
			select {
			case <-c.closeCh:
				return
			case item := <-c.dataCh:
				if !c.doWrite(item) {
					return
				}
			default:
			}
			continue
		}

		select {
		case <-c.closeCh:
			return
		case item := <-c.controlCh:
			if !c.doWrite(item) {
				return
			}
		default:
			select {
			case <-c.closeCh:
				return
			case item := <-c.controlCh:
				if !c.doWrite(item) {
					return
				}
			case item := <-c.dataCh:
				if !c.doWrite(item) {
					return
				}
			}
		}
	}
}

func (c *connection) doWrite(item sendItem) bool {
	if err := item.pkt.Pack(c.conn); err != nil {
		c.markClosed()
		_ = c.conn.Close()
		return false
	}
	if item.onSent != nil {
		item.onSent()
	}
	return true
}

func (c *connection) markClosed() {
	c.closed.Store(true)
	if c.closeCh != nil {
		c.closeOnce.Do(func() {
			close(c.closeCh)
		})
	}
}

func (c *connection) Read(b []byte) (n int, err error) {
	return c.conn.Read(b)
}

func (c *connection) Write(b []byte) (n int, err error) {
	return c.conn.Write(b)
}

func (c *connection) Close() error {
	c.markClosed()
	err := c.conn.Close()
	if c.controlCh != nil {
		c.sendWg.Wait()
	}
	return err
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
	// SetKeepAliveConfig is only available on *net.TCPConn
	// For TLS connections, we need to check the underlying connection type
	if tcpConn, ok := c.conn.(*net.TCPConn); ok {
		cfg := net.KeepAliveConfig{
			Enable:   true,
			Idle:     d,
			Interval: d,
		}
		return tcpConn.SetKeepAliveConfig(cfg)
	}
	// For other connection types (like TLS), keep-alive might be handled differently
	// or not supported - just return nil
	return nil
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
