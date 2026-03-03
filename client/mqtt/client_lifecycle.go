// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package mqtt

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"sync/atomic"
	"time"

	v3 "github.com/absmach/fluxmq/mqtt/packets/v3"
	v5 "github.com/absmach/fluxmq/mqtt/packets/v5"
)

func (c *Client) Connect(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}

	// Phase 1: guard state transition only — no I/O inside the lock.
	c.lifecycleMu.Lock()
	if c.state.isClosed() {
		c.lifecycleMu.Unlock()
		return ErrClientClosed
	}
	if !c.state.transitionFrom(StateConnecting, StateDisconnected, StateReconnecting) {
		c.lifecycleMu.Unlock()
		return ErrAlreadyConnected
	}
	c.lifecycleMu.Unlock()

	// Phase 2: network I/O with no lock held.
	err := c.doConnect(ctx)
	if err != nil {
		if c.state.transition(StateConnecting, StateDisconnected) {
			return err
		}
		if c.state.isClosed() {
			return ErrClientClosed
		}
		return err
	}

	// Phase 3: re-acquire to commit result and start goroutines.
	c.lifecycleMu.Lock()
	defer c.lifecycleMu.Unlock()

	// If another goroutine closed the client while connecting, do not transition to connected.
	if !c.state.transition(StateConnecting, StateConnected) {
		c.writeStateMu.Lock()
		if c.conn != nil {
			c.conn.Close()
			c.conn = nil
		}
		c.writeRT.Store(nil)
		c.writeStateMu.Unlock()
		if c.state.isClosed() {
			return ErrClientClosed
		}
		return ErrConnectionLost
	}

	// Start background goroutines
	stopCh := make(chan struct{})
	doneCh := make(chan struct{})

	size := c.opts.MessageChanSize
	if size <= 0 {
		size = DefaultMessageChanSize
	}
	writeCh := make(chan writeRequest, size)
	controlSize := size / 4
	if controlSize < defaultControlWriteChanSize {
		controlSize = defaultControlWriteChanSize
	}
	controlWriteCh := make(chan writeRequest, controlSize)
	qos0Wake := make(chan struct{}, 1)
	writeDone := make(chan struct{})
	c.writeStateMu.Lock()
	c.stopCh = stopCh
	c.doneCh = doneCh
	c.writeCh = writeCh
	c.controlWriteCh = controlWriteCh
	c.qos0Wake = qos0Wake
	c.writeDone = writeDone
	c.writeRT.Store(&writeRuntime{
		conn:           c.conn,
		stopCh:         stopCh,
		doneCh:         doneCh,
		writeCh:        writeCh,
		controlWriteCh: controlWriteCh,
		qos0Wake:       qos0Wake,
		writeDone:      writeDone,
	})
	c.writeStateMu.Unlock()

	go c.writeLoop()

	c.startDispatcher()
	go c.readLoop()

	if c.opts.KeepAlive > 0 {
		c.startKeepAlive()
	}

	// Restore client-side state on reconnect (subscriptions and QoS in-flight).
	c.restoreState()
	c.flushReconnectBuffer()

	// Callback
	if c.opts.OnConnect != nil {
		go c.opts.OnConnect()
	}

	return nil
}

func (c *Client) doConnect(ctx context.Context) error {
	// Try each server in order
	var lastErr error
	for i := 0; i < len(c.opts.Servers); i++ {
		idx := (c.serverIdx + i) % len(c.opts.Servers)
		addr := c.opts.Servers[idx]

		err := c.connectToServer(ctx, addr)
		if err == nil {
			c.serverIdx = idx
			return nil
		}
		lastErr = err
	}

	if lastErr != nil {
		return fmt.Errorf("%w: %v", ErrConnectFailed, lastErr)
	}
	return ErrConnectFailed
}

func (c *Client) connectToServer(ctx context.Context, addr string) error {
	// Establish TCP connection
	dialer := &net.Dialer{Timeout: c.opts.ConnectTimeout}

	var conn net.Conn
	var err error
	if c.opts.TLSConfig != nil {
		rawConn, dialErr := dialer.DialContext(ctx, "tcp", addr)
		if dialErr != nil {
			return dialErr
		}
		tlsConn := tls.Client(rawConn, c.opts.TLSConfig)
		if deadline, ok := ctx.Deadline(); ok && (c.opts.ConnectTimeout <= 0 || time.Until(deadline) < c.opts.ConnectTimeout) {
			tlsConn.SetDeadline(deadline)
		} else if c.opts.ConnectTimeout > 0 {
			tlsConn.SetDeadline(time.Now().Add(c.opts.ConnectTimeout))
		}
		if err = tlsConn.Handshake(); err != nil {
			tlsConn.Close()
			return err
		}
		tlsConn.SetDeadline(time.Time{})
		conn = tlsConn
	} else {
		conn, err = dialer.DialContext(ctx, "tcp", addr)
		if err != nil {
			return err
		}
	}

	// Send CONNECT packet
	if err := c.sendConnect(conn); err != nil {
		conn.Close()
		return err
	}

	// Read CONNACK
	code, err := c.readConnAck(ctx, conn)
	if err != nil {
		conn.Close()
		return err
	}
	if code != ConnAccepted {
		conn.Close()
		return code
	}

	c.writeStateMu.Lock()
	c.conn = conn
	c.writeStateMu.Unlock()
	c.updateActivity()
	return nil
}

func (c *Client) sendConnect(conn net.Conn) error {
	conn.SetWriteDeadline(time.Now().Add(c.opts.WriteTimeout))
	defer conn.SetWriteDeadline(time.Time{})

	keepAlive := uint16(c.opts.KeepAlive.Seconds())
	return c.packConnectPacket(conn, keepAlive)
}

func (c *Client) readConnAck(ctx context.Context, conn net.Conn) (ConnAckCode, error) {
	deadline := time.Now().Add(c.opts.ConnectTimeout)
	if ctxDeadline, ok := ctx.Deadline(); ok && ctxDeadline.Before(deadline) {
		deadline = ctxDeadline
	}
	conn.SetReadDeadline(deadline)
	defer conn.SetReadDeadline(time.Time{})

	if c.isMQTTv5() {
		// Enhanced auth may require multiple AUTH packet exchanges
		// before the final CONNACK arrives.
		for {
			pkt, _, _, err := v5.ReadPacket(conn)
			if err != nil {
				return 0, err
			}

			if auth, ok := pkt.(*v5.Auth); ok {
				if err := c.handleConnectAuth(conn, auth); err != nil {
					return 0, err
				}
				continue
			}

			ack, ok := pkt.(*v5.ConnAck)
			if !ok {
				return 0, ErrUnexpectedPacket
			}

			// Parse and store server capabilities
			caps := parseConnAckProperties(ack.Properties)
			c.serverCapsMu.Lock()
			c.serverCaps = caps
			c.serverCapsMu.Unlock()

			// Initialize topic alias manager with server's limits
			c.topicAliases = newTopicAliasManager(
				c.opts.TopicAliasMaximum, // client accepts from server
				caps.TopicAliasMaximum,   // server accepts from client
			)

			// Invoke callback if set
			if c.opts.OnServerCapabilities != nil {
				c.opts.OnServerCapabilities(caps)
			}

			return ConnAckCode(ack.ReasonCode), nil
		}
	}

	pkt, err := v3.ReadPacket(conn)
	if err != nil {
		return 0, err
	}
	ack, ok := pkt.(*v3.ConnAck)
	if !ok {
		return 0, ErrUnexpectedPacket
	}
	return ConnAckCode(ack.ReturnCode), nil
}

// Disconnect gracefully disconnects from the broker.

func (c *Client) Disconnect(ctx context.Context) error {
	return c.DisconnectWithReason(ctx, 0, 0, "")
}

// DisconnectWithReason disconnects with an optional reason code (MQTT 5.0).
// For MQTT 3.1.1, reasonCode and sessionExpiry are ignored.
// Parameters:
//   - ctx: bounds how long the graceful teardown may wait for background goroutines to exit
//   - reasonCode: MQTT 5.0 disconnect reason (0 = normal disconnect)
//   - sessionExpiry: Update session expiry interval (0 = use current value, ignored if 0)
//   - reasonString: Human-readable reason (empty = no reason string)

func (c *Client) DisconnectWithReason(ctx context.Context, reasonCode byte, sessionExpiry uint32, reasonString string) error {
	if ctx == nil {
		ctx = context.Background()
	}

	c.lifecycleMu.Lock()
	if !c.state.transition(StateConnected, StateDisconnecting) {
		c.lifecycleMu.Unlock()
		return nil
	}
	c.notifyDrain()

	c.stopKeepAlive()
	c.sendDisconnectWithReason(reasonCode, sessionExpiry, reasonString)
	c.lifecycleMu.Unlock()

	err := c.cleanup(ctx, nil)
	c.lifecycleMu.Lock()
	c.state.transition(StateDisconnecting, StateDisconnected)
	c.lifecycleMu.Unlock()

	return err
}

// Drain stops accepting new publishes, waits for in-flight publishes to finish,
// and then disconnects gracefully.

func (c *Client) Drain(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if c.state.get() != StateConnected {
		return ErrNotConnected
	}
	if !c.draining.CompareAndSwap(false, true) {
		return ErrDraining
	}
	defer c.draining.Store(false)

	for {
		if !c.state.isConnected() {
			return ErrConnectionLost
		}

		if c.pending.countByType(pendingPublish) == 0 &&
			c.writeQueueDepth() == 0 &&
			c.outboundPendingDepth() == 0 &&
			c.reconnectBufferDepth() == 0 {
			break
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-c.drainCh:
		}
	}

	return c.DisconnectWithReason(ctx, 0, 0, "")
}

// sendDisconnectWithReason writes the DISCONNECT directly to conn (bypassing writeLoop)
// because it runs right before cleanup tears down the write infrastructure.

func (c *Client) sendDisconnectWithReason(reasonCode byte, sessionExpiry uint32, reasonString string) {
	rt := c.loadWriteRuntime()
	if rt == nil {
		return
	}
	conn := rt.conn
	if conn == nil {
		return
	}
	conn.SetWriteDeadline(time.Now().Add(c.opts.WriteTimeout))
	_, _ = conn.Write(c.encodeDisconnectPacket(reasonCode, sessionExpiry, reasonString))
}

// Close permanently closes the client.

func (c *Client) Close(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}

	c.lifecycleMu.Lock()
	c.state.set(StateClosed)
	c.lifecycleMu.Unlock()
	c.notifyDrain()

	c.stopKeepAlive()
	err := c.cleanup(ctx, ErrClientClosed)
	if c.store != nil {
		c.store.Close()
	}
	return err
}

func (c *Client) cleanup(ctx context.Context, err error) error {
	if !atomic.CompareAndSwapUint32(&c.cleanupInProgress, 0, 1) {
		return nil
	}
	defer atomic.StoreUint32(&c.cleanupInProgress, 0)

	rt := c.loadWriteRuntime()
	var stopCh chan struct{}
	var doneCh chan struct{}
	var writeDone chan struct{}
	var conn net.Conn
	if rt != nil {
		stopCh = rt.stopCh
		doneCh = rt.doneCh
		writeDone = rt.writeDone
		conn = rt.conn
	}

	// Signal read/write loops.
	if stopCh != nil {
		close(stopCh)
	}

	// Close connection to unblock any blocked read/write syscalls.
	if conn != nil {
		conn.Close()
	}

	var ctxErr error

	// Wait for readLoop to exit.
	if doneCh != nil {
		select {
		case <-doneCh:
		case <-ctx.Done():
			ctxErr = ctx.Err()
			<-doneCh // conn is already closed; exits imminently
		}
	}

	// Wait for writeLoop to exit.
	if writeDone != nil {
		select {
		case <-writeDone:
		case <-ctx.Done():
			if ctxErr == nil {
				ctxErr = ctx.Err()
			}
			<-writeDone // conn is already closed; exits imminently
		}
	}

	c.writeStateMu.Lock()
	c.conn = nil
	c.stopCh = nil
	c.doneCh = nil
	c.writeCh = nil
	c.controlWriteCh = nil
	c.qos0Wake = nil
	c.writeDone = nil
	c.writeRT.Store(nil)
	c.writeStateMu.Unlock()

	c.qos0BufMu.Lock()
	c.qos0Buf = nil
	c.qos0BufMu.Unlock()

	c.pendingMu.Lock()
	c.pendingMessages = 0
	c.pendingBytes = 0
	c.pendingCond.Broadcast()
	c.pendingMu.Unlock()
	c.slowConsumerNotified.Store(false)
	c.draining.Store(false)

	c.outboundMu.Lock()
	c.outboundPendingMessages = 0
	c.outboundPendingBytes = 0
	c.outboundCond.Broadcast()
	c.outboundMu.Unlock()

	c.stopDispatcher()

	// Clear pending operations
	c.pending.clear(err)

	// Clear QoS 2 state
	c.qos2IncomingMu.Lock()
	c.qos2Incoming = make(map[uint16]*Message)
	c.qos2IncomingMu.Unlock()

	// Reset topic aliases
	if c.topicAliases != nil {
		c.topicAliases.reset()
	}

	c.pingMu.Lock()
	c.waitingPing = false
	c.lastPingSent = time.Time{}
	c.pingTimer = nil
	c.pingMu.Unlock()
	c.notifyDrain()

	return ctxErr
}

// IsConnected returns true if the client is connected.

func (c *Client) IsConnected() bool {
	return c.state.isConnected()
}

// State returns the current client state.

func (c *Client) State() State {
	return c.state.get()
}

// Publish sends a message to the broker.

func (c *Client) writeDeadline(ctx context.Context) time.Time {
	if ctx != nil {
		if deadline, ok := ctx.Deadline(); ok {
			if c.opts.WriteTimeout > 0 {
				fallback := time.Now().Add(c.opts.WriteTimeout)
				if fallback.Before(deadline) {
					return fallback
				}
			}
			return deadline
		}
	}
	if c.opts.WriteTimeout > 0 {
		return time.Now().Add(c.opts.WriteTimeout)
	}
	return time.Time{}
}

// Subscribe subscribes to one or more topics.

func (c *Client) startKeepAlive() {
	c.stopKeepAlive()
	interval := c.opts.KeepAlive

	var tick func()
	tick = func() {
		idle := time.Since(time.Unix(0, c.lastActivity.Load()))
		if idle >= interval/2 {
			c.sendPing()
		}
		c.pingMu.Lock()
		if c.pingTimer != nil {
			c.pingTimer.Reset(interval / 2)
		}
		c.pingMu.Unlock()
	}

	c.pingMu.Lock()
	c.pingTimer = time.AfterFunc(interval/2, tick)
	c.pingMu.Unlock()
}

func (c *Client) stopKeepAlive() {
	c.pingMu.Lock()
	if c.pingTimer != nil {
		c.pingTimer.Stop()
		c.pingTimer = nil
	}
	c.pingMu.Unlock()
}

func (c *Client) sendPing() {
	timeout := c.opts.PingTimeout
	if timeout <= 0 {
		timeout = DefaultPingTimeout
	}

	c.pingMu.Lock()
	if c.waitingPing {
		sincePing := time.Since(c.lastPingSent)
		c.pingMu.Unlock()
		if sincePing >= timeout {
			c.handleConnectionLost(ErrPingTimeout)
		}
		return
	}
	c.waitingPing = true
	c.lastPingSent = time.Now().UTC()
	c.pingMu.Unlock()

	pingReq := c.encodePingReqPacket()

	if !c.queueControlWriteNoWait(pingReq) {
		c.pingMu.Lock()
		c.waitingPing = false
		c.lastPingSent = time.Time{}
		c.pingMu.Unlock()
		return
	}

	c.updateActivity()
}

func (c *Client) updateActivity() {
	c.lastActivity.Store(time.Now().UTC().UnixNano())
}

// ServerCapabilities returns the capabilities advertised by the server
// in the CONNACK packet. This is only available for MQTT 5.0 connections.
// Returns nil if not connected or using MQTT 3.1.1.

func (c *Client) ServerCapabilities() *ServerCapabilities {
	c.serverCapsMu.RLock()
	defer c.serverCapsMu.RUnlock()
	return c.serverCaps
}
