// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package amqp1

import (
	"bytes"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/absmach/fluxmq/amqp1/frames"
	"github.com/absmach/fluxmq/amqp1/performatives"
	"github.com/absmach/fluxmq/amqp1/sasl"
	"github.com/absmach/fluxmq/amqp1/types"
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
// If the combined size exceeds maxFrameSize, the payload is split across multiple
// frames using the Transfer.More flag per AMQP 1.0 spec.
func (c *Connection) WriteTransfer(channel uint16, transfer *performatives.Transfer, payload []byte) error {
	perfBody, err := transfer.Encode()
	if err != nil {
		return err
	}

	maxBody := int(c.maxFrameSize) - frames.HeaderSize
	combined := len(perfBody) + len(payload)

	if maxBody <= 0 || combined <= maxBody {
		buf := make([]byte, combined)
		copy(buf, perfBody)
		copy(buf[len(perfBody):], payload)
		c.mu.Lock()
		defer c.mu.Unlock()
		return frames.WriteFrame(c.conn, frames.FrameTypeAMQP, channel, buf)
	}

	// Multi-frame: re-encode first transfer with More=true
	origMore := transfer.More
	transfer.More = true
	perfBody, err = transfer.Encode()
	if err != nil {
		return err
	}
	transfer.More = origMore

	firstChunk := maxBody - len(perfBody)
	if firstChunk <= 0 {
		return fmt.Errorf("transfer performative exceeds max frame size")
	}

	// Pre-encode continuation transfer (handle + more=true)
	contTransfer := &performatives.Transfer{Handle: transfer.Handle, More: true}
	contBody, err := contTransfer.Encode()
	if err != nil {
		return err
	}
	contChunk := maxBody - len(contBody)

	// Pre-encode last continuation (handle + more=false)
	lastTransfer := &performatives.Transfer{Handle: transfer.Handle, More: false}
	lastBody, err := lastTransfer.Encode()
	if err != nil {
		return err
	}
	lastChunk := maxBody - len(lastBody)

	// Hold lock for entire multi-frame sequence to prevent interleaving
	c.mu.Lock()
	defer c.mu.Unlock()

	// First frame
	buf := make([]byte, len(perfBody)+firstChunk)
	copy(buf, perfBody)
	copy(buf[len(perfBody):], payload[:firstChunk])
	if err := frames.WriteFrame(c.conn, frames.FrameTypeAMQP, channel, buf); err != nil {
		return err
	}

	offset := firstChunk
	remaining := len(payload) - offset

	for remaining > 0 {
		var framePerf []byte
		var chunkSize int

		if remaining <= lastChunk {
			framePerf = lastBody
			chunkSize = remaining
		} else {
			framePerf = contBody
			chunkSize = contChunk
			if chunkSize > remaining {
				chunkSize = remaining
			}
		}

		buf := make([]byte, len(framePerf)+chunkSize)
		copy(buf, framePerf)
		copy(buf[len(framePerf):], payload[offset:offset+chunkSize])
		if err := frames.WriteFrame(c.conn, frames.FrameTypeAMQP, channel, buf); err != nil {
			return err
		}

		offset += chunkSize
		remaining -= chunkSize
	}

	return nil
}

// WriteTransferRaw writes a pre-encoded transfer frame with payload.
// Does NOT handle multi-frame splitting — use WriteTransfer for that.
func (c *Connection) WriteTransferRaw(channel uint16, perfBody []byte, payload []byte) error {
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
