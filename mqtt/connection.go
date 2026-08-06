// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package mqtt

import (
	"bufio"
	"crypto/tls"
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
	ErrSendQueueFull              = errors.New("send queue full")
)

const (
	// ProtocolAuto enables protocol detection from the first CONNECT packet.
	ProtocolAuto = 0
	// ProtocolV3 forces MQTT v3/v3.1.1 packet decoding.
	ProtocolV3 = 4
	// ProtocolV5 forces MQTT v5 packet decoding.
	ProtocolV5 = 5
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
//
// Every method takes ownership of pkt: the packet is released on all paths,
// including every error return, and the caller must not touch it (or the
// payload it points into) afterwards. Packets are pooled and may carry a
// reference to a pooled payload buffer, so a caller that also released would
// hand the same packet or buffer to two owners.
type PacketWriter interface {
	WritePacket(pkt packets.ControlPacket) error
	WriteControlPacket(pkt packets.ControlPacket, onSent func()) error
	WriteDataPacket(pkt packets.ControlPacket, onSent func()) error
	// TryWriteDataPacket is a non-blocking variant of WriteDataPacket.
	// It returns ErrSendQueueFull immediately if the send queue is full,
	// without disconnecting or blocking the caller.
	TryWriteDataPacket(pkt packets.ControlPacket, onSent func()) error
}

type PacketReader interface {
	ReadPacket() (packets.ControlPacket, error)
}

// sendItem is queued for asynchronous socket writes.
type sendItem struct {
	pkt    packets.ControlPacket
	onSent func()
}

// sendBufSize is the size of the bufio.Writer used by the sendLoop
// to coalesce multiple packets into fewer syscalls.
const sendBufSize = 32 * 1024

// connection wraps a net.Conn and provides MQTT packet-level I/O with state management.
type connection struct {
	conn net.Conn
	// sock is where packet bytes are written. It is conn itself, or a wrapper
	// that stamps a write deadline on each socket write when a write timeout is
	// configured. Built once so the write path never allocates.
	sock    io.Writer
	reader  io.Reader
	writer  *bufio.Writer // buffered writer for sendLoop; nil in sync mode
	version int           // 0 = unknown, 3/4 = v3.1/v3.1.1, 5 = v5

	mu sync.RWMutex

	sendMu           sync.Mutex
	controlCh        chan sendItem
	dataCh           chan sendItem
	closeCh          chan struct{}
	closeOnce        sync.Once
	sendWg           sync.WaitGroup
	writeTimeout     time.Duration // 0 = no write deadline
	maxPacketSize    int           // 0 = unlimited
	disconnectOnFull bool
	closed           atomic.Bool

	lastActivity atomic.Int64 // UnixNano timestamp; updated lock-free by Touch()

	onDisconnect func(graceful bool)
}

// ConnOption configures optional connection behaviour.
type ConnOption func(*connection)

// WithMaxPacketSize rejects inbound packets whose remaining length exceeds
// maxSize bytes, before any memory is reserved for the body. A value <= 0
// leaves packets unbounded (the protocol ceiling of ~256 MiB).
func WithMaxPacketSize(maxSize int) ConnOption {
	return func(c *connection) {
		if maxSize > 0 {
			c.maxPacketSize = maxSize
		}
	}
}

// WithWriteTimeout bounds how long a single socket write may take. Without it a
// peer that stops reading can park a broker goroutine on a blocked write for as
// long as it likes. A value <= 0 leaves writes without a deadline.
func WithWriteTimeout(d time.Duration) ConnOption {
	return func(c *connection) {
		if d > 0 {
			c.writeTimeout = d
		}
	}
}

// deadlineWriter stamps the connection's write timeout on every socket write.
// The deadline is absolute and refreshed per write, so it never needs clearing.
type deadlineWriter struct {
	c *connection
}

func (w deadlineWriter) Write(p []byte) (int, error) {
	if err := w.c.conn.SetWriteDeadline(time.Now().Add(w.c.writeTimeout)); err != nil {
		return 0, err
	}
	return w.c.conn.Write(p)
}

// NewConnection creates a new MQTT connection wrapping a network connection.
// queueSize <= 0 keeps synchronous writes; queueSize > 0 enables asynchronous queued writes.
func NewConnection(conn net.Conn, queueSize int, disconnectOnFull bool, opts ...ConnOption) Connection {
	return NewConnectionWithVersion(conn, queueSize, disconnectOnFull, ProtocolAuto, opts...)
}

// NewConnectionWithVersion creates a new MQTT connection with an optional forced protocol version.
// version = ProtocolAuto enables detection; ProtocolV3 or ProtocolV5 force decoding mode.
func NewConnectionWithVersion(conn net.Conn, queueSize int, disconnectOnFull bool, version int, opts ...ConnOption) Connection {
	c := &connection{
		conn:             conn,
		reader:           conn,
		version:          version,
		disconnectOnFull: disconnectOnFull,
	}
	for _, opt := range opts {
		opt(c)
	}

	c.sock = conn
	if c.writeTimeout > 0 {
		c.sock = deadlineWriter{c: c}
	}

	if queueSize > 0 {
		controlCap := queueSize / 4
		if controlCap < 1 {
			controlCap = 1
		}
		c.controlCh = make(chan sendItem, controlCap)
		c.dataCh = make(chan sendItem, queueSize)
		c.closeCh = make(chan struct{})
		c.writer = bufio.NewWriterSize(c.sock, sendBufSize)

		c.sendWg.Add(1)
		go c.sendLoop()
	}

	return c
}

// PeerCertificateDER returns a copy of the verified TLS peer leaf.
func (c *connection) PeerCertificateDER() []byte {
	tlsConn, ok := c.conn.(*tls.Conn)
	if !ok {
		return nil
	}
	state := tlsConn.ConnectionState()
	if len(state.VerifiedChains) == 0 || len(state.VerifiedChains[0]) == 0 {
		return nil
	}
	return append([]byte(nil), state.VerifiedChains[0][0].Raw...)
}

// PeerIssuerCertificateDER returns a copy of the verified leaf issuer.
func (c *connection) PeerIssuerCertificateDER() []byte {
	tlsConn, ok := c.conn.(*tls.Conn)
	if !ok {
		return nil
	}
	state := tlsConn.ConnectionState()
	if len(state.VerifiedChains) == 0 || len(state.VerifiedChains[0]) < 2 {
		return nil
	}
	return append([]byte(nil), state.VerifiedChains[0][1].Raw...)
}

// ReadPacket reads the next MQTT packet from the connection.
func (c *connection) ReadPacket() (packets.ControlPacket, error) {
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
		pkt, _, _, err = v5.ReadPacketLimit(c.reader, c.maxPacketSize)
	case 3, 4:
		// v4 is MQTT 3.1.1, v3 is MQTT 3.1
		pkt, err = v3.ReadPacketLimit(c.reader, c.maxPacketSize)
	default:
		err = ErrUnsupportedProtocolVersion
	}

	if err != nil {
		return nil, err
	}

	c.reader = c.conn

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
		pkt.Release()
		return net.ErrClosed
	}

	item := sendItem{pkt: pkt, onSent: onSent}
	select {
	case c.controlCh <- item:
		return nil
	case <-c.closeCh:
		pkt.Release()
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
		pkt.Release()
		return net.ErrClosed
	}

	item := sendItem{pkt: pkt, onSent: onSent}
	if c.disconnectOnFull {
		select {
		case c.dataCh <- item:
			return nil
		case <-c.closeCh:
			pkt.Release()
			return net.ErrClosed
		default:
			c.markClosed()
			_ = c.conn.Close()
			pkt.Release()
			return ErrSendQueueFull
		}
	}

	select {
	case c.dataCh <- item:
		return nil
	case <-c.closeCh:
		pkt.Release()
		return net.ErrClosed
	}
}

func (c *connection) TryWriteDataPacket(pkt packets.ControlPacket, onSent func()) error {
	if pkt == nil {
		return ErrCannotEncodeNilPacket
	}

	if c.dataCh == nil {
		return c.writeSync(pkt, onSent)
	}

	if c.closed.Load() {
		pkt.Release()
		return net.ErrClosed
	}

	item := sendItem{pkt: pkt, onSent: onSent}
	select {
	case c.dataCh <- item:
		return nil
	case <-c.closeCh:
		pkt.Release()
		return net.ErrClosed
	default:
		if c.disconnectOnFull {
			c.markClosed()
			_ = c.conn.Close()
		}
		pkt.Release()
		return ErrSendQueueFull
	}
}

func (c *connection) writeSync(pkt packets.ControlPacket, onSent func()) error {
	if c.closed.Load() {
		pkt.Release()
		return net.ErrClosed
	}

	c.sendMu.Lock()
	defer c.sendMu.Unlock()

	if c.closed.Load() {
		pkt.Release()
		return net.ErrClosed
	}

	if err := pkt.Pack(c.sock); err != nil {
		pkt.Release()
		return err
	}
	pkt.Release()
	if onSent != nil {
		onSent()
	}
	return nil
}

func (c *connection) sendLoop() {
	defer c.sendWg.Done()
	defer c.drainSendQueues()
	w := c.writer
	// Reusable slice for onSent callbacks; fired after flush.
	var pending []func()

	for {
		didWork := false

		// Phase 1: Drain pending control packets (up to burst limit).
		controlCount := 0
		for draining := true; draining && controlCount < controlBurst; {
			select {
			case <-c.closeCh:
				return
			case item := <-c.controlCh:
				if !c.doPack(w, item, &pending) {
					return
				}
				didWork = true
				controlCount++
			default:
				draining = false
			}
		}

		// Phase 2: Process one data packet to prevent starvation.
		if controlCount == controlBurst {
			select {
			case <-c.closeCh:
				return
			case item := <-c.dataCh:
				if !c.doPack(w, item, &pending) {
					return
				}
				didWork = true
			default:
			}
			if !c.flushAndNotify(w, &pending) {
				return
			}
			continue
		}

		// Phase 3: Re-check control channel, then pack at most one data packet.
		// This prevents speculative packing of multiple data packets ahead of
		// control packets that arrive while the socket is blocked.
		packedData := false
		for {
			select {
			case item := <-c.controlCh:
				if !c.doPack(w, item, &pending) {
					return
				}
				didWork = true
				continue
			default:
			}
			if packedData {
				goto flush
			}
			select {
			case item := <-c.dataCh:
				if !c.doPack(w, item, &pending) {
					return
				}
				didWork = true
				packedData = true
			default:
				goto flush
			}
		}
	flush:
		if !c.flushAndNotify(w, &pending) {
			return
		}
		if didWork {
			// Start a fresh scheduling cycle so control packets are checked
			// first again before taking another blocking read.
			continue
		}

		// Phase 4: Block until the next item arrives.
		select {
		case <-c.closeCh:
			return
		case item := <-c.controlCh:
			if !c.doPack(w, item, &pending) {
				return
			}
		case item := <-c.dataCh:
			if !c.doPack(w, item, &pending) {
				return
			}
		}
		// Flush immediately after a blocking receive. This preserves control
		// priority: once one item is taken from an idle loop, we write it out
		// before potentially packing additional queued data.
		if !c.flushAndNotify(w, &pending) {
			return
		}
	}
}

// drainSendQueues releases packets left queued when the send loop exits, so a
// torn-down connection returns its pooled packets and payload buffers instead
// of stranding them in the channels. A producer that wins the race against
// markClosed can still enqueue after this runs; that packet is simply collected
// by the GC rather than pooled.
func (c *connection) drainSendQueues() {
	for {
		select {
		case item := <-c.controlCh:
			item.pkt.Release()
		case item := <-c.dataCh:
			item.pkt.Release()
		default:
			return
		}
	}
}

// doPack serialises a packet into the buffered writer. onSent callbacks are
// deferred into pending and fired after the batch is flushed to the socket.
func (c *connection) doPack(w *bufio.Writer, item sendItem, pending *[]func()) bool {
	if err := item.pkt.Pack(w); err != nil {
		item.pkt.Release()
		c.markClosed()
		_ = c.conn.Close()
		return false
	}
	item.pkt.Release()
	if item.onSent != nil {
		*pending = append(*pending, item.onSent)
	}
	return true
}

// flushAndNotify flushes the buffered writer to the socket, then fires all
// deferred onSent callbacks. If the flush fails the callbacks are discarded
// (the connection is closing; inflight messages will be retried on reconnect).
func (c *connection) flushAndNotify(w *bufio.Writer, pending *[]func()) bool {
	if w.Buffered() > 0 {
		if err := w.Flush(); err != nil {
			*pending = (*pending)[:0]
			c.markClosed()
			_ = c.conn.Close()
			return false
		}
	}
	for _, fn := range *pending {
		fn()
	}
	*pending = (*pending)[:0]
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
	c.lastActivity.Store(time.Now().UnixNano())
}
