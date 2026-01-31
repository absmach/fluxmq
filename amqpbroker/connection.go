// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package amqpbroker

import (
	"fmt"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"time"

	amqpconn "github.com/absmach/fluxmq/amqp"
	"github.com/absmach/fluxmq/amqp/frames"
	"github.com/absmach/fluxmq/amqp/performatives"
	"github.com/absmach/fluxmq/amqp/sasl"
	"github.com/absmach/fluxmq/amqp/types"
)

// Connection represents a single AMQP client connection.
type Connection struct {
	broker       *Broker
	conn         *amqpconn.Connection
	containerID  string
	maxFrameSize uint32
	channelMax   uint16
	idleTimeout  time.Duration

	sessions   map[uint16]*Session
	sessionsMu sync.RWMutex
	nextCh     uint16

	closed    atomic.Bool
	closeCh   chan struct{}
	closeOnce sync.Once

	logger *slog.Logger
}

func newConnection(b *Broker, raw net.Conn) *Connection {
	return &Connection{
		broker:       b,
		conn:         amqpconn.NewConnection(raw),
		maxFrameSize: frames.DefaultMaxFrameSize,
		channelMax:   65535,
		sessions:     make(map[uint16]*Session),
		closeCh:      make(chan struct{}),
		logger:       b.logger,
	}
}

// run executes the full connection lifecycle:
// SASL negotiation → OPEN exchange → frame loop → cleanup.
func (c *Connection) run() error {
	defer c.cleanup()

	// Phase 1: Read protocol header
	protoID, err := c.conn.ReadProtocolHeader()
	if err != nil {
		return fmt.Errorf("reading protocol header: %w", err)
	}

	// Phase 2: SASL negotiation if client sends SASL header
	if protoID == frames.ProtoIDSASL {
		if err := c.handleSASL(); err != nil {
			return fmt.Errorf("SASL negotiation: %w", err)
		}

		// After SASL, client must send AMQP protocol header
		protoID, err = c.conn.ReadProtocolHeader()
		if err != nil {
			return fmt.Errorf("reading AMQP header after SASL: %w", err)
		}
	}

	if protoID != frames.ProtoIDAMQP {
		c.conn.WriteProtocolHeader(frames.ProtoIDAMQP)
		return fmt.Errorf("unexpected protocol ID: 0x%02x", protoID)
	}

	// Phase 3: Respond with AMQP header
	if err := c.conn.WriteProtocolHeader(frames.ProtoIDAMQP); err != nil {
		return fmt.Errorf("writing AMQP header: %w", err)
	}

	// Phase 4: OPEN exchange
	if err := c.handleOpen(); err != nil {
		return fmt.Errorf("OPEN exchange: %w", err)
	}

	c.broker.registerConnection(c.containerID, c)
	c.logger.Info("AMQP connection opened", "container_id", c.containerID, "remote", c.conn.RemoteAddr())

	// Phase 5: Start heartbeat sender if idle timeout is set
	if c.idleTimeout > 0 {
		go c.heartbeatLoop()
	}

	// Phase 6: Frame processing loop
	return c.processFrames()
}

func (c *Connection) handleSASL() error {
	// Send SASL protocol header
	if err := c.conn.WriteProtocolHeader(frames.ProtoIDSASL); err != nil {
		return err
	}

	// Send SASL mechanisms
	mechs := &sasl.Mechanisms{
		Mechanisms: []types.Symbol{sasl.MechPLAIN, sasl.MechANONYMOUS},
	}
	body, err := mechs.Encode()
	if err != nil {
		return err
	}
	if err := c.conn.WriteSASLFrame(body); err != nil {
		return err
	}

	// Read SASL Init
	desc, val, err := c.conn.ReadSASLFrame()
	if err != nil {
		return err
	}
	if desc != sasl.DescriptorInit {
		return fmt.Errorf("expected SASL-Init, got descriptor 0x%02x", desc)
	}

	init := val.(*sasl.Init)

	// Validate mechanism
	switch init.Mechanism {
	case sasl.MechPLAIN:
		// Parse and validate credentials
		_, _, _, err := sasl.ParsePLAIN(init.InitialResponse)
		if err != nil {
			outcome := &sasl.Outcome{Code: sasl.CodeAuth}
			body, _ := outcome.Encode()
			c.conn.WriteSASLFrame(body)
			return fmt.Errorf("PLAIN auth failed: %w", err)
		}
		// TODO: integrate with AuthEngine for actual credential validation
	case sasl.MechANONYMOUS:
		// Anonymous is always accepted
	default:
		outcome := &sasl.Outcome{Code: sasl.CodeAuth}
		body, _ := outcome.Encode()
		c.conn.WriteSASLFrame(body)
		return fmt.Errorf("unsupported SASL mechanism: %s", init.Mechanism)
	}

	// Send SASL outcome OK
	outcome := &sasl.Outcome{Code: sasl.CodeOK}
	body, err = outcome.Encode()
	if err != nil {
		return err
	}
	return c.conn.WriteSASLFrame(body)
}


func (c *Connection) handleOpen() error {
	// Read client OPEN
	ch, desc, perf, _, err := c.conn.ReadPerformative()
	if err != nil {
		return err
	}
	if desc != performatives.DescriptorOpen {
		return fmt.Errorf("expected OPEN, got descriptor 0x%02x on channel %d", desc, ch)
	}

	clientOpen := perf.(*performatives.Open)
	c.containerID = clientOpen.ContainerID

	// Negotiate max frame size (minimum of both)
	if clientOpen.MaxFrameSize > 0 && clientOpen.MaxFrameSize < c.maxFrameSize {
		c.maxFrameSize = clientOpen.MaxFrameSize
	}
	c.conn.SetMaxFrameSize(c.maxFrameSize)

	if clientOpen.ChannelMax > 0 && clientOpen.ChannelMax < c.channelMax {
		c.channelMax = clientOpen.ChannelMax
	}

	// Set idle timeout from client's preference
	if clientOpen.IdleTimeOut > 0 {
		c.idleTimeout = time.Duration(clientOpen.IdleTimeOut) * time.Millisecond
		c.conn.SetIdleTimeout(c.idleTimeout * 2) // read deadline is 2x the timeout
	}

	// Send our OPEN
	serverOpen := &performatives.Open{
		ContainerID:  "fluxmq",
		MaxFrameSize: c.maxFrameSize,
		ChannelMax:   c.channelMax,
		IdleTimeOut:  uint32(c.idleTimeout.Milliseconds()),
	}

	body, err := serverOpen.Encode()
	if err != nil {
		return err
	}
	return c.conn.WritePerformative(0, body)
}

func (c *Connection) processFrames() error {
	for {
		select {
		case <-c.closeCh:
			return nil
		default:
		}

		ch, desc, perf, payload, err := c.conn.ReadPerformative()
		if err != nil {
			return err
		}

		// Heartbeat
		if perf == nil && desc == 0 {
			continue
		}

		switch desc {
		case performatives.DescriptorBegin:
			if err := c.handleBegin(ch, perf.(*performatives.Begin)); err != nil {
				return err
			}
		case performatives.DescriptorAttach:
			c.sessionsMu.RLock()
			s := c.sessions[ch]
			c.sessionsMu.RUnlock()
			if s == nil {
				return fmt.Errorf("attach on unknown channel %d", ch)
			}
			if err := s.handleAttach(perf.(*performatives.Attach)); err != nil {
				return err
			}
		case performatives.DescriptorFlow:
			c.sessionsMu.RLock()
			s := c.sessions[ch]
			c.sessionsMu.RUnlock()
			if s == nil {
				continue
			}
			s.handleFlow(perf.(*performatives.Flow))
		case performatives.DescriptorTransfer:
			c.sessionsMu.RLock()
			s := c.sessions[ch]
			c.sessionsMu.RUnlock()
			if s == nil {
				continue
			}
			s.handleTransfer(perf.(*performatives.Transfer), payload)
		case performatives.DescriptorDisposition:
			c.sessionsMu.RLock()
			s := c.sessions[ch]
			c.sessionsMu.RUnlock()
			if s == nil {
				continue
			}
			s.handleDisposition(perf.(*performatives.Disposition))
		case performatives.DescriptorDetach:
			c.sessionsMu.RLock()
			s := c.sessions[ch]
			c.sessionsMu.RUnlock()
			if s == nil {
				continue
			}
			s.handleDetach(perf.(*performatives.Detach))
		case performatives.DescriptorEnd:
			if err := c.handleEnd(ch, perf.(*performatives.End)); err != nil {
				return err
			}
		case performatives.DescriptorClose:
			c.handleClose(perf.(*performatives.Close))
			return nil
		default:
			c.logger.Warn("unknown performative", "descriptor", desc, "channel", ch)
		}
	}
}

func (c *Connection) handleBegin(ch uint16, begin *performatives.Begin) error {
	c.sessionsMu.Lock()
	defer c.sessionsMu.Unlock()

	localCh := c.nextCh
	c.nextCh++

	s := newSession(c, localCh, ch)
	s.remoteIncomingWindow = begin.IncomingWindow
	s.remoteOutgoingWindow = begin.OutgoingWindow
	c.sessions[ch] = s

	// Send Begin response
	resp := &performatives.Begin{
		RemoteChannel:  &ch,
		NextOutgoingID: 0,
		IncomingWindow: 65535,
		OutgoingWindow: 65535,
		HandleMax:      255,
	}
	body, err := resp.Encode()
	if err != nil {
		return err
	}
	return c.conn.WritePerformative(localCh, body)
}

func (c *Connection) handleEnd(ch uint16, end *performatives.End) error {
	c.sessionsMu.Lock()
	s := c.sessions[ch]
	delete(c.sessions, ch)
	c.sessionsMu.Unlock()

	if s != nil {
		s.cleanup()
	}

	resp := &performatives.End{}
	body, err := resp.Encode()
	if err != nil {
		return err
	}
	return c.conn.WritePerformative(ch, body)
}

func (c *Connection) handleClose(cl *performatives.Close) {
	resp := &performatives.Close{}
	body, _ := resp.Encode()
	c.conn.WritePerformative(0, body)
}

func (c *Connection) heartbeatLoop() {
	if c.idleTimeout <= 0 {
		return
	}

	// Send heartbeat at half the idle timeout period
	interval := c.idleTimeout / 2
	if interval < time.Second {
		interval = time.Second
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-c.closeCh:
			return
		case <-ticker.C:
			if err := c.conn.SendHeartbeat(); err != nil {
				c.logger.Debug("heartbeat send failed", "error", err)
				return
			}
		}
	}
}

func (c *Connection) close(amqpErr *performatives.Error) {
	c.closeOnce.Do(func() {
		c.closed.Store(true)
		close(c.closeCh)
	})
}

func (c *Connection) cleanup() {
	c.close(nil)
	c.broker.unregisterConnection(c.containerID)

	c.sessionsMu.Lock()
	sessions := make([]*Session, 0, len(c.sessions))
	for _, s := range c.sessions {
		sessions = append(sessions, s)
	}
	c.sessions = make(map[uint16]*Session)
	c.sessionsMu.Unlock()

	for _, s := range sessions {
		s.cleanup()
	}

	c.conn.Close()
	c.logger.Info("AMQP connection closed", "container_id", c.containerID)
}

// deliverMessage delivers a pub/sub message to the appropriate receiver link.
func (c *Connection) deliverMessage(topic string, payload []byte, props map[string]string, qos byte) {
	c.sessionsMu.RLock()
	defer c.sessionsMu.RUnlock()

	for _, s := range c.sessions {
		s.deliverToMatchingLinks(topic, payload, props, qos)
	}
}

// deliverAMQPMessage delivers a pre-built AMQP message to receiver links matching the topic.
func (c *Connection) deliverAMQPMessage(topic string, msg interface{}, qos byte) {
	c.sessionsMu.RLock()
	defer c.sessionsMu.RUnlock()

	for _, s := range c.sessions {
		s.deliverAMQPMessageToLinks(topic, msg, qos)
	}
}
