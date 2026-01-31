// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package amqp

import (
	"bytes"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/absmach/fluxmq/amqp/frames"
	"github.com/absmach/fluxmq/amqp/performatives"
	"github.com/absmach/fluxmq/amqp/sasl"
	"github.com/absmach/fluxmq/amqp/types"
)

// Connection wraps a net.Conn for AMQP 1.0 frame I/O.
type Connection struct {
	conn         net.Conn
	mu           sync.Mutex // protects writes
	maxFrameSize uint32
	idleTimeout  time.Duration
}

// NewConnection creates a new AMQP connection wrapper.
func NewConnection(conn net.Conn) *Connection {
	return &Connection{
		conn:         conn,
		maxFrameSize: frames.DefaultMaxFrameSize,
	}
}

// SetMaxFrameSize sets the negotiated maximum frame size.
func (c *Connection) SetMaxFrameSize(size uint32) {
	c.maxFrameSize = size
}

// MaxFrameSize returns the current maximum frame size.
func (c *Connection) MaxFrameSize() uint32 {
	return c.maxFrameSize
}

// SetIdleTimeout sets the idle timeout duration.
func (c *Connection) SetIdleTimeout(d time.Duration) {
	c.idleTimeout = d
}

// ReadProtocolHeader reads and validates the 8-byte AMQP protocol header.
func (c *Connection) ReadProtocolHeader() (byte, error) {
	if c.idleTimeout > 0 {
		c.conn.SetReadDeadline(time.Now().Add(c.idleTimeout))
	}
	return frames.ReadProtocolHeader(c.conn)
}

// WriteProtocolHeader writes the AMQP protocol header.
func (c *Connection) WriteProtocolHeader(protoID byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return frames.WriteProtocolHeader(c.conn, protoID)
}

// ReadFrame reads a single AMQP frame.
func (c *Connection) ReadFrame() (*frames.Frame, error) {
	if c.idleTimeout > 0 {
		c.conn.SetReadDeadline(time.Now().Add(c.idleTimeout))
	}
	return frames.ReadFrame(c.conn)
}

// WriteFrame writes a single AMQP frame.
func (c *Connection) WriteFrame(frameType byte, channel uint16, body []byte) error {
	if c.maxFrameSize > 0 && uint32(frames.HeaderSize+len(body)) > c.maxFrameSize {
		return fmt.Errorf("frame size %d exceeds max frame size %d",
			frames.HeaderSize+len(body), c.maxFrameSize)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return frames.WriteFrame(c.conn, frameType, channel, body)
}

// WritePerformative encodes and writes a performative as an AMQP frame.
func (c *Connection) WritePerformative(channel uint16, body []byte) error {
	return c.WriteFrame(frames.FrameTypeAMQP, channel, body)
}

// WriteTransfer writes a transfer frame with payload appended after the performative.
func (c *Connection) WriteTransfer(channel uint16, perfBody []byte, payload []byte) error {
	combined := make([]byte, len(perfBody)+len(payload))
	copy(combined, perfBody)
	copy(combined[len(perfBody):], payload)
	return c.WriteFrame(frames.FrameTypeAMQP, channel, combined)
}

// WriteSASLFrame writes a SASL frame.
func (c *Connection) WriteSASLFrame(body []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return frames.WriteFrame(c.conn, frames.FrameTypeSASL, 0, body)
}

// SendHeartbeat writes an empty frame (heartbeat).
func (c *Connection) SendHeartbeat() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return frames.WriteEmptyFrame(c.conn)
}

// Close closes the underlying connection.
func (c *Connection) Close() error {
	return c.conn.Close()
}

// RemoteAddr returns the remote network address.
func (c *Connection) RemoteAddr() net.Addr {
	return c.conn.RemoteAddr()
}

// ReadSASLFrame reads a frame and decodes it as a SASL frame.
func (c *Connection) ReadSASLFrame() (uint64, any, error) {
	f, err := c.ReadFrame()
	if err != nil {
		return 0, nil, err
	}
	if f.Type != frames.FrameTypeSASL {
		return 0, nil, fmt.Errorf("expected SASL frame, got type 0x%02x", f.Type)
	}
	return sasl.DecodeSASL(f.Body)
}

// ReadPerformative reads a frame and decodes the performative.
// Returns channel, descriptor, decoded performative, remaining payload (for Transfer), and error.
// A nil performative with zero descriptor indicates a heartbeat.
func (c *Connection) ReadPerformative() (uint16, uint64, any, []byte, error) {
	f, err := c.ReadFrame()
	if err != nil {
		return 0, 0, nil, nil, err
	}
	if f.IsEmpty() {
		return 0, 0, nil, nil, nil // heartbeat
	}

	r := bytes.NewReader(f.Body)

	// Read described type: descriptor constructor + ulong + list
	descriptor, fields, err := types.ReadListFields(r)
	if err != nil {
		return 0, 0, nil, nil, err
	}

	var perf any
	switch descriptor {
	case performatives.DescriptorOpen:
		perf = performatives.DecodeOpen(fields)
	case performatives.DescriptorBegin:
		perf = performatives.DecodeBegin(fields)
	case performatives.DescriptorAttach:
		perf = performatives.DecodeAttach(fields)
	case performatives.DescriptorFlow:
		perf = performatives.DecodeFlow(fields)
	case performatives.DescriptorTransfer:
		perf = performatives.DecodeTransfer(fields)
	case performatives.DescriptorDisposition:
		perf = performatives.DecodeDisposition(fields)
	case performatives.DescriptorDetach:
		perf = performatives.DecodeDetach(fields)
	case performatives.DescriptorEnd:
		perf = performatives.DecodeEnd(fields)
	case performatives.DescriptorClose:
		perf = performatives.DecodeClose(fields)
	default:
		return f.Channel, descriptor, nil, nil, fmt.Errorf("unknown performative: 0x%02x", descriptor)
	}

	// Remaining bytes are transfer payload
	var payload []byte
	if remaining := r.Len(); remaining > 0 {
		payload = make([]byte, remaining)
		r.Read(payload)
	}

	return f.Channel, descriptor, perf, payload, nil
}
