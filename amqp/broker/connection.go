// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package broker

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"log/slog"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/absmach/fluxmq/amqp/codec"
	"github.com/absmach/fluxmq/amqp1/sasl"
	corebroker "github.com/absmach/fluxmq/broker"
	"github.com/absmach/fluxmq/internal/bufpool"
)

// AMQP 0.9.1 protocol header: "AMQP" followed by 0, 0, 9, 1.
var protocolHeader = []byte{'A', 'M', 'Q', 'P', 0, 0, 9, 1}

// localPublishTimeout bounds how long a local publisher waits for one exact
// durable-stream append and its fsync. It is a variable only so tests can
// shorten it; production code must not reassign it.
var localPublishTimeout = 10 * time.Second

const (
	defaultFrameMax   = uint32(131072)
	defaultChannelMax = uint16(2047)
	defaultHeartbeat  = uint16(60)
	frameOverhead     = uint64(8)

	// clusterOpTimeout prevents a slow/partitioned peer from blocking setup or shutdown.
	clusterOpTimeout       = 5 * time.Second
	disconnectWriteTimeout = time.Second
	saslMechanismPlain     = "PLAIN"
	saslMechanismAMQPlain  = "AMQPLAIN"
)

// Connection represents a single AMQP 0.9.1 client connection.
type Connection struct {
	broker *Broker
	conn   net.Conn
	reader *bufio.Reader
	writer *bufio.Writer
	ctx    context.Context
	policy *ConnectionPolicy
	peer   VerifiedPeerIdentity

	connID         string
	connectionName string // human-readable label from AMQP ClientProperties "connection_name"
	localIdentity  *LocalSessionIdentity
	registered     bool
	frameMax       uint32
	channelMax     uint16
	heartbeat      uint16

	channels   map[uint16]*Channel
	channelsMu sync.RWMutex

	writeMu   sync.Mutex
	closed    atomic.Bool
	closeCh   chan struct{}
	closeOnce sync.Once

	deliveryTag atomic.Uint64

	logger *slog.Logger
}

func newConnection(ctx context.Context, b *Broker, netConn net.Conn, policy *ConnectionPolicy) *Connection {
	c := &Connection{
		broker:     b,
		conn:       netConn,
		reader:     bufio.NewReaderSize(netConn, 65536),
		writer:     bufio.NewWriterSize(netConn, 65536),
		ctx:        ctx,
		policy:     policy,
		frameMax:   defaultFrameMax,
		channelMax: defaultChannelMax,
		heartbeat:  defaultHeartbeat,
		channels:   make(map[uint16]*Channel),
		closeCh:    make(chan struct{}),
		logger:     b.logger,
	}
	c.connID = b.nextConnectionID(netConn.RemoteAddr())
	if policy.usesLocalPrincipalAuth() {
		if tlsConn, ok := netConn.(*tls.Conn); ok {
			c.peer = verifiedPeerIdentity(tlsConn)
		}
	}
	return c
}

func (c *Connection) connectionPolicy() *ConnectionPolicy {
	if c.policy != nil {
		return c.policy
	}
	// A nil per-connection policy is the compatibility path used by existing
	// embedded callers and tests. Listener-aware servers always pass a policy.
	return &ConnectionPolicy{
		mode:         ConnectionPolicyExternal,
		externalAuth: c.broker.auth,
		hooks:        c.broker.hooks,
	}
}

func (c *Connection) externalID(clientID string) string {
	if c.localIdentity != nil {
		return c.localIdentity.PrincipalID
	}
	auth := c.connectionPolicy().externalAuth
	if auth == nil {
		return ""
	}
	return auth.ExternalID(clientID)
}

func (c *Connection) applyHook(ctx context.Context, req corebroker.BlockingHookRequest) (corebroker.BlockingHookRequest, bool) {
	hooks := c.connectionPolicy().hooks
	if hooks == nil {
		return req, true
	}
	req.Protocol = corebroker.HookProtocolAMQP091
	return hooks.Handle(ctx, req)
}

// publishContext returns the context that bounds one publish. Embedded callers
// may construct a connection without one, so fall back to a background context
// rather than panicking on a nil parent.
func (c *Connection) publishContext() context.Context {
	if c.ctx == nil {
		return context.Background()
	}
	return c.ctx
}

func (c *Connection) localSessionIdentity() (LocalSessionIdentity, bool) {
	if c.localIdentity == nil {
		return LocalSessionIdentity{}, false
	}
	return *c.localIdentity, true
}

// canPublishLocal authorizes the exchange name the router will actually use.
// Authorizing the raw wire value would let the ACL and the routing decision
// disagree about which exchange a publication targets.
func (c *Connection) canPublishLocal(exchange, routingKey string) LocalPublishGrant {
	policy := c.connectionPolicy()
	if !policy.usesLocalPrincipalAuth() || policy.localAuthz == nil || c.localIdentity == nil {
		return LocalPublishGrantNone
	}
	return policy.localAuthz.CanPublishLocal(*c.localIdentity, normalizeExchange(exchange), routingKey)
}

// localRole returns the capability bound to this session at authentication.
// An unauthenticated connection gets the zero value, the least privileged role.
func (c *Connection) localRole() LocalPrincipalRole {
	if c.localIdentity == nil {
		return LocalRolePublisher
	}
	return c.localIdentity.Role
}

// permitsConsumers reports whether this session's principal may run consumers
// at all. It is a property of the authenticated principal, not of the listener,
// so the same principal has the same answer on every local listener.
func (c *Connection) permitsConsumers() bool {
	return c.connectionPolicy().usesLocalPrincipalAuth() && c.localRole().PermitsConsumers()
}

// propagatesOriginIdentity reports whether this session may relay the origin
// identity of a message rather than having its own stamped on it.
func (c *Connection) propagatesOriginIdentity() bool {
	return c.connectionPolicy().carriesReservedProperties() &&
		(!c.connectionPolicy().usesLocalPrincipalAuth() || c.localRole().PropagatesOriginIdentity())
}

// canSubscribeLocal authorizes a consumer for a local-principal session. A
// principal whose role permits consumers is still refused a queue its own
// subscribe ACL does not name.
func (c *Connection) canSubscribeLocal(queue string) bool {
	policy := c.connectionPolicy()
	if !c.permitsConsumers() || policy.localAuthz == nil || c.localIdentity == nil {
		return false
	}
	return policy.localAuthz.CanSubscribeLocal(*c.localIdentity, queue)
}

func (c *Connection) registerAndValidate() error {
	c.broker.registerConnection(c.connID, c)
	c.registered = true
	c.broker.stats.IncrementConnections()

	if !c.connectionPolicy().usesLocalPrincipalAuth() {
		return nil
	}
	c.broker.stats.IncrementLocalConnections()
	identity, bound := c.localSessionIdentity()
	validator := c.connectionPolicy().localSessions
	if !bound || validator == nil || !validator.IsSessionActive(identity) {
		c.broker.stats.AddLocalForcedDisconnects(1)
		c.logger.Warn("amqp091_local_connection",
			"auth_mode", "local",
			"outcome", "denied",
			"reason", "credentials_retired_before_registration",
			"principal_id", identity.PrincipalID)
		c.disconnect(codec.AccessRefused, "local principal credentials revoked")
		return fmt.Errorf("local principal session became inactive during handshake")
	}
	return nil
}

func (c *Connection) disconnect(code uint16, reason string) {
	if c.closed.Load() {
		return
	}
	_ = c.conn.SetWriteDeadline(time.Now().Add(disconnectWriteTimeout))
	_ = c.sendConnectionClose(code, reason, codec.ClassConnection, codec.MethodConnectionClose)
	c.close()
	_ = c.conn.Close()
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
	if err := c.conn.SetDeadline(time.Time{}); err != nil {
		return fmt.Errorf("clearing handshake deadline: %w", err)
	}

	if err := c.registerAndValidate(); err != nil {
		return err
	}
	if c.localIdentity != nil {
		c.logger.Info("amqp091_local_connection",
			"auth_mode", "local",
			"outcome", "opened",
			"principal_id", c.localIdentity.PrincipalID,
			"active_connections", c.broker.stats.GetLocalConnections())
	}
	if cl := c.broker.cluster; cl != nil {
		clientID := PrefixedClientID(c.connID)
		ctx, cancel := context.WithTimeout(context.Background(), clusterOpTimeout)
		err := cl.AcquireSession(ctx, clientID, cl.NodeID())
		cancel()
		if err != nil {
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
		c.conn.Write(protocolHeader) //nolint:errcheck // best-effort protocol redirect before closing
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
		ServerProperties: map[string]any{
			"product":     "FluxMQ",
			"version":     "0.1.0",
			"platform":    "Go",
			"information": "https://github.com/absmach/fluxmq",
			"capabilities": map[string]any{
				"basic.nack":                 true,
				"publisher_confirms":         true,
				"consumer_cancel_notify":     true,
				"exchange_exchange_bindings": true,
				"connection.blocked":         true,
			},
		},
		Mechanisms: saslMechanismPlain,
		Locales:    "en_US",
	}
	if err := c.writeMethod(0, start); err != nil {
		return err
	}

	// Read Connection.StartOk
	frame, err := c.readFrame()
	if err != nil {
		return err
	}
	decoded, err := frame.Decode()
	if err != nil {
		return err
	}
	startOK, ok := decoded.(*codec.ConnectionStartOk)
	if !ok {
		return fmt.Errorf("expected Connection.StartOk, got %T", decoded)
	}
	if err := c.authenticate(startOK); err != nil {
		return err
	}
	if name, ok := startOK.ClientProperties["connection_name"].(string); ok {
		c.connectionName = name
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
	frame, err = c.readFrame()
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
	frame, err = c.readFrame()
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

func (c *Connection) authenticate(start *codec.ConnectionStartOk) error {
	policy := c.connectionPolicy()
	if policy.mode == ConnectionPolicyExternal && policy.externalAuth == nil {
		// Preserve the existing unauthenticated-listener behavior.
		return nil
	}

	mechanism := strings.ToUpper(strings.TrimSpace(start.Mechanism))
	switch mechanism {
	case saslMechanismPlain, saslMechanismAMQPlain:
		_, username, password, err := sasl.ParsePLAIN([]byte(start.Response))
		if err != nil {
			if policy.usesLocalPrincipalAuth() {
				c.recordLocalAuthFailure("invalid_sasl_response")
			}
			_ = c.sendConnectionClose(codec.AccessRefused, "invalid auth response", codec.ClassConnection, codec.MethodConnectionStartOk)
			return fmt.Errorf("invalid %s auth response: %w", mechanism, err)
		}

		return c.authenticateCredentials(mechanism, username, password)
	default:
		if policy.usesLocalPrincipalAuth() {
			c.recordLocalAuthFailure("unsupported_sasl_mechanism")
		}
		_ = c.sendConnectionClose(codec.CommandInvalid, "unsupported auth mechanism", codec.ClassConnection, codec.MethodConnectionStartOk)
		return fmt.Errorf("unsupported auth mechanism %q", start.Mechanism)
	}
}

func (c *Connection) authenticateCredentials(mechanism, username, password string) error {
	policy := c.connectionPolicy()
	clientID := PrefixedClientID(c.connID)
	if policy.usesLocalPrincipalAuth() {
		if policy.localAuth == nil {
			c.recordLocalAuthFailure("authenticator_unavailable")
			_ = c.sendConnectionClose(codec.AccessRefused, "authentication failed", codec.ClassConnection, codec.MethodConnectionStartOk)
			return fmt.Errorf("%s local auth unavailable", mechanism)
		}
		authentication, ok, err := policy.localAuth.AuthenticateLocal(
			c.ctx, clientID, username, password, c.peer,
		)
		if err != nil {
			c.recordLocalAuthFailure("authenticator_error")
			_ = c.sendConnectionClose(codec.AccessRefused, "authentication failed", codec.ClassConnection, codec.MethodConnectionStartOk)
			return fmt.Errorf("%s local auth failed for user %q", mechanism, username)
		}
		if !ok {
			c.recordLocalAuthFailure("credentials_rejected")
			_ = c.sendConnectionClose(codec.AccessRefused, "authentication failed", codec.ClassConnection, codec.MethodConnectionStartOk)
			return fmt.Errorf("%s local auth rejected for user %q", mechanism, username)
		}
		if authentication.PrincipalID == "" || authentication.CredentialFingerprint == "" ||
			authentication.PermissionsFingerprint == "" || authentication.CertificateURI == "" ||
			c.peer.CertificateFingerprint == "" || !containsURISAN(c.peer, authentication.CertificateURI) {
			c.recordLocalAuthFailure("identity_binding_rejected")
			_ = c.sendConnectionClose(codec.AccessRefused, "authentication failed", codec.ClassConnection, codec.MethodConnectionStartOk)
			return fmt.Errorf("%s local auth rejected for user %q", mechanism, username)
		}
		c.localIdentity = &LocalSessionIdentity{
			PrincipalID:            authentication.PrincipalID,
			Role:                   authentication.Role,
			CredentialFingerprint:  authentication.CredentialFingerprint,
			PermissionsFingerprint: authentication.PermissionsFingerprint,
			CertificateURI:         authentication.CertificateURI,
			CertificateFingerprint: c.peer.CertificateFingerprint,
		}
		c.broker.stats.IncrementLocalAuthSuccess()
		c.logger.Info("amqp091_local_authentication",
			"auth_mode", "local",
			"outcome", "success",
			"reason", "credentials_and_certificate_verified",
			"client_id", clientID,
			"principal_id", authentication.PrincipalID,
			"role", authentication.Role.String())
		return nil
	}

	if policy.externalAuth == nil {
		return nil
	}
	ok, _, err := policy.externalAuth.Authenticate(clientID, username, password)
	if err != nil || !ok {
		_ = c.sendConnectionClose(codec.AccessRefused, "authentication failed", codec.ClassConnection, codec.MethodConnectionStartOk)
		return fmt.Errorf("%s auth rejected for user %q", mechanism, username)
	}
	return nil
}

func (c *Connection) recordLocalAuthFailure(reason string) {
	c.broker.stats.IncrementLocalAuthFailures()
	c.logger.Warn("amqp091_local_authentication",
		"auth_mode", "local",
		"outcome", "failure",
		"reason", reason,
		"client_id", PrefixedClientID(c.connID))
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
			c.conn.SetReadDeadline(deadline) //nolint:errcheck // fails only on closed connection
		}

		frame, err := c.readFrame()
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

// readFrame enforces the negotiated frame maximum before allocating the frame
// payload. codec.ReadFrame cannot apply a per-connection limit because it does
// not know the AMQP tune result.
func (c *Connection) readFrame() (*codec.Frame, error) {
	frameType, err := codec.ReadOctet(c.reader)
	if err != nil {
		return nil, err
	}
	channel, err := codec.ReadShort(c.reader)
	if err != nil {
		return nil, err
	}
	size, err := codec.ReadLong(c.reader)
	if err != nil {
		return nil, err
	}
	if c.frameMax > 0 && uint64(size)+frameOverhead > uint64(c.frameMax) {
		return nil, codec.NewErr(codec.FrameError, "frame exceeds negotiated maximum", nil)
	}
	payload := make([]byte, size)
	if _, err := io.ReadFull(c.reader, payload); err != nil {
		return nil, err
	}
	frameEnd, err := codec.ReadOctet(c.reader)
	if err != nil {
		return nil, err
	}
	if frameEnd != codec.FrameEnd {
		return nil, codec.NewErr(codec.FrameError, "malformed frame: incorrect frame-end marker", nil)
	}
	return &codec.Frame{Type: frameType, Channel: channel, Payload: payload}, nil
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
			c.writeMethod(0, closeOk) //nolint:errcheck // best-effort reply during connection close
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

	if chID == 0 || chID > c.channelMax {
		return c.sendConnectionClose(codec.ChannelError,
			fmt.Sprintf("channel %d exceeds negotiated channel maximum %d", chID, c.channelMax), 0, 0)
	}
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

// cancelConsumerByQueue sends a server-initiated basic.cancel to any channel
// on this connection that has a consumer matching the given queue and group.
func (c *Connection) cancelConsumerByQueue(queueName, groupID string) {
	if c.closed.Load() {
		return
	}

	c.channelsMu.RLock()
	channels := make([]*Channel, 0, len(c.channels))
	for _, ch := range c.channels {
		channels = append(channels, ch)
	}
	c.channelsMu.RUnlock()

	for _, ch := range channels {
		ch.cancelConsumerByQueue(queueName, groupID)
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

	if c.registered {
		c.broker.stats.DecrementConnections()
		if c.localIdentity != nil {
			c.broker.stats.DecrementLocalConnections()
			c.logger.Info("amqp091_local_connection",
				"auth_mode", "local",
				"outcome", "closed",
				"principal_id", c.localIdentity.PrincipalID,
				"active_connections", c.broker.stats.GetLocalConnections())
		}
	}
	if c.connID != "" {
		clientID := PrefixedClientID(c.connID)
		if auth := c.connectionPolicy().externalAuth; auth != nil {
			auth.Forget(clientID)
		}
		if cl := c.broker.cluster; cl != nil {
			ctx, cancel := context.WithTimeout(context.Background(), clusterOpTimeout)
			defer cancel()
			if err := cl.RemoveAllSubscriptions(ctx, clientID); err != nil {
				c.logger.Warn("AMQP 0.9.1 remove all subscriptions failed", "client_id", clientID, "error", err)
			}
			if err := cl.ReleaseSession(ctx, clientID); err != nil {
				c.logger.Warn("AMQP 0.9.1 release session ownership failed", "client_id", clientID, "error", err)
			}
		}
	}
	c.broker.unregisterConnection(c.connID)
	c.conn.Close()
	c.logger.Info("AMQP 0.9.1 connection closed", "remote", c.connID)
}
