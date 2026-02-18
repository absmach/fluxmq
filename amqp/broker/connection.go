// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package broker

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/absmach/fluxmq/amqp/codec"
	"github.com/absmach/fluxmq/internal/bufpool"
)

// AMQP 0.9.1 protocol header: "AMQP" followed by 0, 0, 9, 1.
var protocolHeader = []byte{'A', 'M', 'Q', 'P', 0, 0, 9, 1}

const (
	defaultFrameMax   = uint32(131072)
	defaultChannelMax = uint16(2047)
	defaultHeartbeat  = uint16(60)
)

// Connection represents a single AMQP 0.9.1 client connection.
type Connection struct {
	broker *Broker
	conn   net.Conn
	reader *bufio.Reader
	writer *bufio.Writer

	connID     string
	frameMax   uint32
	channelMax uint16
	heartbeat  uint16

	channels   map[uint16]*Channel
	channelsMu sync.RWMutex

	writeMu   sync.Mutex
	closed    atomic.Bool
	closeCh   chan struct{}
	closeOnce sync.Once

	deliveryTag atomic.Uint64

	logger *slog.Logger
}

func newConnection(b *Broker, netConn net.Conn) *Connection {
	return &Connection{
		broker:     b,
		conn:       netConn,
		reader:     bufio.NewReaderSize(netConn, 65536),
		writer:     bufio.NewWriterSize(netConn, 65536),
		frameMax:   defaultFrameMax,
		channelMax: defaultChannelMax,
		heartbeat:  defaultHeartbeat,
		channels:   make(map[uint16]*Channel),
		closeCh:    make(chan struct{}),
		logger:     b.logger,
	}
}

func (c *Connection) nextDeliveryTag() uint64 {
	return c.deliveryTag.Add(1)
}

// run executes the full connection lifecycle.
func (c *Connection) run() error {
	defer c.cleanup()

	if err := c.negotiateProtocol(); err != nil {
		return fmt.Errorf("protocol negotiation: %w", err)
	}

	if err := c.connectionHandshake(); err != nil {
		return fmt.Errorf("connection handshake: %w", err)
	}

	c.connID = c.conn.RemoteAddr().String()
	c.broker.registerConnection(c.connID, c)
	c.broker.stats.IncrementConnections()
	if cl := c.broker.cluster; cl != nil {
		clientID := PrefixedClientID(c.connID)
		if err := cl.AcquireSession(context.Background(), clientID, cl.NodeID()); err != nil {
			c.logger.Warn("AMQP 0.9.1 acquire session ownership failed", "client_id", clientID, "error", err)
		}
	}
	c.logger.Info("AMQP 0.9.1 connection opened", "remote", c.connID)

	if c.heartbeat > 0 {
		go c.heartbeatSender()
		go c.heartbeatMonitor()
	}

	return c.processFrames()
}

// negotiateProtocol reads and validates the AMQP 0.9.1 protocol header.
func (c *Connection) negotiateProtocol() error {
	header := make([]byte, 8)
	if _, err := io.ReadFull(c.reader, header); err != nil {
		return fmt.Errorf("reading protocol header: %w", err)
	}

	if !bytes.Equal(header, protocolHeader) {
		// Send correct protocol header and close
		c.conn.Write(protocolHeader)
		return fmt.Errorf("unsupported protocol header: %x", header)
	}

	return nil
}

// connectionHandshake performs the Connection.Start → TuneOk → Open handshake.
func (c *Connection) connectionHandshake() error {
	// Send Connection.Start
	start := &codec.ConnectionStart{
		VersionMajor: 0,
		VersionMinor: 9,
		ServerProperties: map[string]interface{}{
			"product":     "FluxMQ",
			"version":     "0.1.0",
			"platform":    "Go",
			"information": "https://github.com/absmach/fluxmq",
			"capabilities": map[string]interface{}{
				"basic.nack":                 true,
				"publisher_confirms":         true,
				"consumer_cancel_notify":     true,
				"exchange_exchange_bindings": true,
				"connection.blocked":         true,
			},
		},
		Mechanisms: "PLAIN",
		Locales:    "en_US",
	}
	if err := c.writeMethod(0, start); err != nil {
		return err
	}

	// Read Connection.StartOk
	frame, err := codec.ReadFrame(c.reader)
	if err != nil {
		return err
	}
	decoded, err := frame.Decode()
	if err != nil {
		return err
	}
	_, ok := decoded.(*codec.ConnectionStartOk)
	if !ok {
		return fmt.Errorf("expected Connection.StartOk, got %T", decoded)
	}

	// Send Connection.Tune
	tune := &codec.ConnectionTune{
		ChannelMax: c.channelMax,
		FrameMax:   c.frameMax,
		Heartbeat:  c.heartbeat,
	}
	if err := c.writeMethod(0, tune); err != nil {
		return err
	}

	// Read Connection.TuneOk
	frame, err = codec.ReadFrame(c.reader)
	if err != nil {
		return err
	}
	decoded, err = frame.Decode()
	if err != nil {
		return err
	}
	tuneOk, ok := decoded.(*codec.ConnectionTuneOk)
	if !ok {
		return fmt.Errorf("expected Connection.TuneOk, got %T", decoded)
	}

	// Apply negotiated values (minimum of client/server)
	if tuneOk.ChannelMax > 0 && tuneOk.ChannelMax < c.channelMax {
		c.channelMax = tuneOk.ChannelMax
	}
	if tuneOk.FrameMax > 0 && tuneOk.FrameMax < c.frameMax {
		c.frameMax = tuneOk.FrameMax
	}
	if tuneOk.Heartbeat < c.heartbeat {
		c.heartbeat = tuneOk.Heartbeat
	}

	// Read Connection.Open
	frame, err = codec.ReadFrame(c.reader)
	if err != nil {
		return err
	}
	decoded, err = frame.Decode()
	if err != nil {
		return err
	}
	_, ok = decoded.(*codec.ConnectionOpen)
	if !ok {
		return fmt.Errorf("expected Connection.Open, got %T", decoded)
	}

	// Send Connection.OpenOk
	openOk := &codec.ConnectionOpenOk{}
	return c.writeMethod(0, openOk)
}

// processFrames is the main frame processing loop.
func (c *Connection) processFrames() error {
	for {
		select {
		case <-c.closeCh:
			return nil
		default:
		}

		if c.heartbeat > 0 {
			deadline := time.Now().Add(time.Duration(c.heartbeat*2) * time.Second)
			c.conn.SetReadDeadline(deadline)
		}

		frame, err := codec.ReadFrame(c.reader)
		if err != nil {
			if c.closed.Load() {
				return nil
			}
			return fmt.Errorf("reading frame: %w", err)
		}

		switch frame.Type {
		case codec.FrameMethod:
			if err := c.handleMethodFrame(frame); err != nil {
				return err
			}
		case codec.FrameHeader:
			ch := c.getChannel(frame.Channel)
			if ch == nil {
				continue
			}
			ch.handleHeaderFrame(frame)
		case codec.FrameBody:
			ch := c.getChannel(frame.Channel)
			if ch == nil {
				continue
			}
			ch.handleBodyFrame(frame)
		case codec.FrameHeartbeat:
			// Heartbeat received, deadline already reset
		default:
			c.logger.Warn("unknown frame type", "type", frame.Type)
		}
	}
}

func (c *Connection) handleMethodFrame(frame *codec.Frame) error {
	decoded, err := frame.Decode()
	if err != nil {
		return fmt.Errorf("decoding method: %w", err)
	}

	// Connection-level methods (channel 0)
	if frame.Channel == 0 {
		switch m := decoded.(type) {
		case *codec.ConnectionClose:
			closeOk := &codec.ConnectionCloseOk{}
			c.writeMethod(0, closeOk)
			c.close()
			return nil
		case *codec.ConnectionCloseOk:
			c.close()
			return nil
		default:
			return fmt.Errorf("unexpected method on channel 0: %T (%+v)", m, m)
		}
	}

	// Channel-level methods
	switch m := decoded.(type) {
	case *codec.ChannelOpen:
		return c.handleChannelOpen(frame.Channel)
	default:
		ch := c.getChannel(frame.Channel)
		if ch == nil {
			return fmt.Errorf("method on unknown channel %d: %T", frame.Channel, m)
		}
		return ch.handleMethod(decoded)
	}
}

func (c *Connection) handleChannelOpen(chID uint16) error {
	c.channelsMu.Lock()
	defer c.channelsMu.Unlock()

	if uint16(len(c.channels)) >= c.channelMax {
		return c.sendConnectionClose(codec.ChannelError, "channel limit exceeded", 0, 0)
	}

	if _, exists := c.channels[chID]; exists {
		return c.sendConnectionClose(codec.ChannelError,
			fmt.Sprintf("channel %d already open", chID), 0, 0)
	}

	ch := newChannel(c, chID)
	c.channels[chID] = ch
	c.broker.stats.IncrementChannels()

	openOk := &codec.ChannelOpenOk{}
	return c.writeMethod(chID, openOk)
}

func (c *Connection) closeChannel(chID uint16) {
	c.channelsMu.Lock()
	ch, exists := c.channels[chID]
	delete(c.channels, chID)
	c.channelsMu.Unlock()

	if exists {
		ch.cleanup()
		c.broker.stats.DecrementChannels()
	}
}

func (c *Connection) getChannel(chID uint16) *Channel {
	c.channelsMu.RLock()
	defer c.channelsMu.RUnlock()
	return c.channels[chID]
}

// deliverMessage delivers a message to all channels that have matching consumers.
func (c *Connection) deliverMessage(topic string, payload []byte, props map[string]string) {
	c.channelsMu.RLock()
	defer c.channelsMu.RUnlock()

	for _, ch := range c.channels {
		ch.deliverMessage(topic, payload, props)
	}
}

// writeMethod serializes a method and sends it as a FrameMethod.
func (c *Connection) writeMethod(channel uint16, method interface{ Write(io.Writer) error }) error {
	buf := bufpool.Get()
	defer bufpool.Put(buf)
	if err := method.Write(buf); err != nil {
		return err
	}
	return c.writeFrame(&codec.Frame{
		Type:    codec.FrameMethod,
		Channel: channel,
		Payload: buf.Bytes(),
	})
}

// writeFrame writes a frame to the connection, thread-safe.
func (c *Connection) writeFrame(frame *codec.Frame) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()

	if err := frame.WriteFrame(c.writer); err != nil {
		return err
	}
	return c.writer.Flush()
}

// writeFrames writes multiple frames to the connection, thread-safe, flushing once.
func (c *Connection) writeFrames(frames ...*codec.Frame) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()

	for _, frame := range frames {
		if frame == nil {
			continue
		}
		if err := frame.WriteFrame(c.writer); err != nil {
			return err
		}
	}
	return c.writer.Flush()
}

func (c *Connection) sendConnectionClose(code uint16, text string, classID, methodID uint16) error {
	cl := &codec.ConnectionClose{
		ReplyCode: code,
		ReplyText: text,
		ClassID:   classID,
		MethodID:  methodID,
	}
	return c.writeMethod(0, cl)
}

func (c *Connection) sendChannelClose(chID, code uint16, text string, classID, methodID uint16) error {
	cl := &codec.ChannelClose{
		ReplyCode: code,
		ReplyText: text,
		ClassID:   classID,
		MethodID:  methodID,
	}
	return c.writeMethod(chID, cl)
}

func (c *Connection) heartbeatSender() {
	if c.heartbeat == 0 {
		return
	}
	interval := time.Duration(c.heartbeat) * time.Second
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-c.closeCh:
			return
		case <-ticker.C:
			hb := &codec.Frame{
				Type:    codec.FrameHeartbeat,
				Channel: 0,
				Payload: nil,
			}
			if err := c.writeFrame(hb); err != nil {
				c.logger.Debug("heartbeat send failed", "error", err)
				return
			}
		}
	}
}

func (c *Connection) heartbeatMonitor() {
	// Read deadline is set in processFrames before each read.
	// If no data arrives within 2x heartbeat, the read will timeout
	// and processFrames will return an error, closing the connection.
}

func (c *Connection) close() {
	c.closeOnce.Do(func() {
		c.closed.Store(true)
		close(c.closeCh)
	})
}

func (c *Connection) cleanup() {
	c.close()

	c.channelsMu.Lock()
	channels := make([]*Channel, 0, len(c.channels))
	for _, ch := range c.channels {
		channels = append(channels, ch)
	}
	c.channels = make(map[uint16]*Channel)
	c.channelsMu.Unlock()

	for _, ch := range channels {
		ch.cleanup()
	}

	c.broker.stats.DecrementConnections()
	if c.connID != "" {
		if cl := c.broker.cluster; cl != nil {
			clientID := PrefixedClientID(c.connID)
			if err := cl.RemoveAllSubscriptions(context.Background(), clientID); err != nil {
				c.logger.Warn("AMQP 0.9.1 remove all subscriptions failed", "client_id", clientID, "error", err)
			}
			if err := cl.ReleaseSession(context.Background(), clientID); err != nil {
				c.logger.Warn("AMQP 0.9.1 release session ownership failed", "client_id", clientID, "error", err)
			}
		}
	}
	c.broker.unregisterConnection(c.connID)
	c.conn.Close()
	c.logger.Info("AMQP 0.9.1 connection closed", "remote", c.connID)
}
