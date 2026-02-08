// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package broker

import (
	"fmt"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"time"

	amqpconn "github.com/absmach/fluxmq/amqp1"
	"github.com/absmach/fluxmq/amqp1/frames"
	"github.com/absmach/fluxmq/amqp1/performatives"
	"github.com/absmach/fluxmq/amqp1/sasl"
	"github.com/absmach/fluxmq/amqp1/types"
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

	// Multi-frame transfer reassembly per channel
	partialTransfers   map[uint16]*partialTransfer
	partialTransfersMu sync.Mutex

	logger *slog.Logger
}

type partialTransfer struct {
	transfer *performatives.Transfer
	payload  []byte
}

func newConnection(b *Broker, raw net.Conn) *Connection {
	return &Connection{
		broker:           b,
		conn:             amqpconn.NewConnection(raw),
		maxFrameSize:     frames.DefaultMaxFrameSize,
		channelMax:       65535,
		sessions:         make(map[uint16]*Session),
		partialTransfers: make(map[uint16]*partialTransfer),
		closeCh:          make(chan struct{}),
		logger:           b.logger,
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
	c.broker.stats.IncrementConnections()
	if m := c.broker.getMetrics(); m != nil {
		m.RecordConnection()
	}
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
		_, username, password, err := sasl.ParsePLAIN(init.InitialResponse)
		if err != nil {
			outcome := &sasl.Outcome{Code: sasl.CodeAuth}
			body, _ := outcome.Encode()
			c.conn.WriteSASLFrame(body)
			return fmt.Errorf("PLAIN auth failed: %w", err)
		}
		if auth := c.broker.getAuth(); auth != nil {
			ok, authErr := auth.Authenticate(username, username, password)
			if authErr != nil || !ok {
				c.broker.stats.IncrementAuthErrors()
				if m := c.broker.getMetrics(); m != nil {
					m.RecordError("auth")
				}
				outcome := &sasl.Outcome{Code: sasl.CodeAuth}
				body, _ := outcome.Encode()
				c.conn.WriteSASLFrame(body)
				return fmt.Errorf("PLAIN auth rejected for user %q", username)
			}
		}
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
			transfer := perf.(*performatives.Transfer)

			c.partialTransfersMu.Lock()
			partial := c.partialTransfers[ch]

			if partial != nil {
				// Continuation frame: append payload to existing partial
				partial.payload = append(partial.payload, payload...)
				if transfer.More {
					c.partialTransfersMu.Unlock()
					continue
				}
				// Final frame: deliver the reassembled transfer
				transfer = partial.transfer
				payload = partial.payload
				delete(c.partialTransfers, ch)
				c.partialTransfersMu.Unlock()
			} else if transfer.More {
				// First frame of multi-frame transfer
				c.partialTransfers[ch] = &partialTransfer{
					transfer: transfer,
					payload:  append([]byte(nil), payload...),
				}
				c.partialTransfersMu.Unlock()
				continue
			} else {
				c.partialTransfersMu.Unlock()
			}

			c.sessionsMu.RLock()
			s := c.sessions[ch]
			c.sessionsMu.RUnlock()
			if s == nil {
				continue
			}
			s.handleTransfer(transfer, payload)
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

	// Check channel limit
	if uint16(len(c.sessions)) >= c.channelMax {
		c.logger.Warn("channel limit exceeded", "channel", ch, "max", c.channelMax)
		return c.sendClose(&performatives.Error{
			Condition:   performatives.ErrResourceLimitExceeded,
			Description: "channel limit exceeded",
		})
	}

	// Reject duplicate channel
	if _, exists := c.sessions[ch]; exists {
		c.logger.Warn("duplicate channel", "channel", ch)
		return c.sendClose(&performatives.Error{
			Condition:   performatives.ErrFramingError,
			Description: fmt.Sprintf("channel %d already in use", ch),
		})
	}

	localCh := c.nextCh
	c.nextCh++

	handleMax := uint32(255)
	if begin.HandleMax > 0 && begin.HandleMax < handleMax {
		handleMax = begin.HandleMax
	}

	s := newSession(c, localCh, ch, handleMax)
	s.initWindows(begin)
	c.sessions[ch] = s

	c.broker.stats.IncrementSessions()
	if m := c.broker.getMetrics(); m != nil {
		m.RecordSessionOpened()
	}

	// Send Begin response with our actual window state
	_, inWin, nextOut, outWin := s.sessionFlowState()
	resp := &performatives.Begin{
		RemoteChannel:  &ch,
		NextOutgoingID: nextOut,
		IncomingWindow: inWin,
		OutgoingWindow: outWin,
		HandleMax:      handleMax,
	}
	body, err := resp.Encode()
	if err != nil {
		return err
	}
	return c.conn.WritePerformative(localCh, body)
}

// sendClose sends a Close frame with an optional error and signals shutdown.
func (c *Connection) sendClose(amqpErr *performatives.Error) error {
	resp := &performatives.Close{Error: amqpErr}
	body, err := resp.Encode()
	if err != nil {
		return err
	}
	if err := c.conn.WritePerformative(0, body); err != nil {
		c.logger.Error("failed to send close", "error", err)
	}
	c.close(nil)
	return nil
}


func (c *Connection) handleEnd(ch uint16, end *performatives.End) error {
	c.sessionsMu.Lock()
	s := c.sessions[ch]
	delete(c.sessions, ch)
	c.sessionsMu.Unlock()

	if s != nil {
		s.cleanup()
		c.broker.stats.DecrementSessions()
		if m := c.broker.getMetrics(); m != nil {
			m.RecordSessionClosed()
		}
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
	body, err := resp.Encode()
	if err != nil {
		c.logger.Error("failed to encode close response", "error", err)
		return
	}
	if err := c.conn.WritePerformative(0, body); err != nil {
		c.logger.Error("failed to send close response", "error", err)
	}
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
	c.broker.stats.DecrementConnections()
	if m := c.broker.getMetrics(); m != nil {
		m.RecordDisconnection()
	}
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
