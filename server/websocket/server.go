// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package websocket

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/absmach/fluxmq/internal/connguard"
	core "github.com/absmach/fluxmq/mqtt"
	"github.com/absmach/fluxmq/mqtt/broker"
	"github.com/absmach/fluxmq/mqtt/packets"
	v3 "github.com/absmach/fluxmq/mqtt/packets/v3"
	v5 "github.com/absmach/fluxmq/mqtt/packets/v5"
	"github.com/gorilla/websocket"
)

// subprotocolMQTT is the WebSocket subprotocol name for MQTT.
const subprotocolMQTT = "mqtt"

var (
	ErrExpectedBinaryMessage      = errors.New("expected binary message")
	ErrUnsupportedProtocolVersion = errors.New("unsupported MQTT protocol version")
	ErrCannotEncodeNilPacket      = errors.New("cannot encode nil packet")
	ErrReadNotSupported           = errors.New("Read not supported on WebSocket connection")
	ErrWriteNotSupported          = errors.New("Write not supported on WebSocket connection")
)

// IPRateLimiter is the interface for IP-based rate limiting.
type IPRateLimiter interface {
	Allow(addr net.Addr) bool
}

type Config struct {
	Address         string
	Path            string
	ShutdownTimeout time.Duration
	TLSConfig       *tls.Config
	ProtocolVersion int
	AllowedOrigins  []string      // Allowed origins for CORS (empty = allow all, use "*" for explicit wildcard)
	IPRateLimiter   IPRateLimiter // Optional IP-based rate limiter
}

type Server struct {
	config         Config
	broker         *broker.Broker
	logger         *slog.Logger
	server         *http.Server
	upgrader       websocket.Upgrader
	allowedOrigins map[string]bool
	allowAll       bool
	ipRateLimiter  IPRateLimiter
}

func New(cfg Config, b *broker.Broker, logger *slog.Logger) *Server {
	if logger == nil {
		logger = slog.Default()
	}

	if cfg.Path == "" {
		cfg.Path = "/mqtt"
	}

	s := &Server{
		config:         cfg,
		broker:         b,
		logger:         logger,
		allowedOrigins: make(map[string]bool),
		ipRateLimiter:  cfg.IPRateLimiter,
	}

	// Build allowed origins lookup
	if len(cfg.AllowedOrigins) == 0 {
		// No origins configured - allow all (development mode)
		s.allowAll = true
		logger.Warn("websocket origin validation disabled - allowing all origins (development mode only)")
	} else {
		for _, origin := range cfg.AllowedOrigins {
			if origin == "*" {
				s.allowAll = true
				break
			}
			// Normalize origin to lowercase
			s.allowedOrigins[strings.ToLower(origin)] = true
		}
		if !s.allowAll {
			logger.Info("websocket origin validation enabled", slog.Int("allowed_origins", len(cfg.AllowedOrigins)))
		}
	}

	s.upgrader = websocket.Upgrader{
		CheckOrigin:  s.checkOrigin,
		Subprotocols: []string{subprotocolMQTT},
	}

	mux := http.NewServeMux()
	mux.HandleFunc(cfg.Path, s.handleWebSocket)

	s.server = &http.Server{
		Addr:    cfg.Address,
		Handler: mux,
	}

	return s
}

// checkOrigin validates the Origin header against the allowed origins list.
func (s *Server) checkOrigin(r *http.Request) bool {
	if s.allowAll {
		return true
	}

	origin := r.Header.Get("Origin")
	if origin == "" {
		// No origin header - allow (same-origin request or non-browser client)
		return true
	}

	// Parse and normalize origin
	parsedOrigin, err := url.Parse(origin)
	if err != nil {
		s.logger.Warn("invalid origin header", slog.String("origin", origin))
		return false
	}

	// Build normalized origin (scheme://host)
	normalizedOrigin := strings.ToLower(parsedOrigin.Scheme + "://" + parsedOrigin.Host)

	// Check exact match
	if s.allowedOrigins[normalizedOrigin] {
		return true
	}

	// Check wildcard subdomain patterns (e.g., "*.example.com")
	for allowedOrigin := range s.allowedOrigins {
		if strings.HasPrefix(allowedOrigin, "*.") {
			// Extract domain part after "*."
			domain := allowedOrigin[1:] // e.g., ".example.com"
			hostWithScheme := parsedOrigin.Scheme + "://" + parsedOrigin.Host
			// Check if origin ends with the domain pattern
			if strings.HasSuffix(strings.ToLower(hostWithScheme), domain) ||
				strings.ToLower(hostWithScheme) == parsedOrigin.Scheme+"://"+allowedOrigin[2:] {
				return true
			}
		}
	}

	s.logger.Warn("origin not allowed",
		slog.String("origin", origin),
		slog.String("remote_addr", r.RemoteAddr))
	return false
}

func (s *Server) Listen(ctx context.Context) error {
	tlsEnabled := s.config.TLSConfig != nil
	s.logger.Info("websocket_server_starting",
		slog.String("addr", s.config.Address),
		slog.String("path", s.config.Path),
		slog.Bool("tls_enabled", tlsEnabled))

	errCh := make(chan error, 1)
	go func() {
		var err error
		if s.config.TLSConfig != nil {
			s.server.TLSConfig = s.config.TLSConfig
			// ListenAndServeTLS with empty cert/key paths because TLS config is already set
			err = s.server.ListenAndServeTLS("", "")
		} else {
			err = s.server.ListenAndServe()
		}
		if err != nil && err != http.ErrServerClosed {
			errCh <- err
		}
	}()

	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
		s.logger.Info("websocket_server_shutdown_initiated")
		shutdownCtx, cancel := context.WithTimeout(context.Background(), s.config.ShutdownTimeout) //nolint:contextcheck // intentionally creates new context for graceful shutdown
		defer cancel()

		if err := s.server.Shutdown(shutdownCtx); err != nil { //nolint:contextcheck // intentionally creates new context for graceful shutdown
			s.logger.Error("websocket_server_shutdown_error", slog.String("error", err.Error()))
			return err
		}

		s.logger.Info("websocket_server_stopped")
		return nil
	}
}

func (s *Server) handleWebSocket(w http.ResponseWriter, r *http.Request) {
	// Check IP rate limit before upgrade
	if s.ipRateLimiter != nil {
		// Create a temporary addr for rate limiting
		addr := &wsAddr{addr: r.RemoteAddr}
		if !s.ipRateLimiter.Allow(addr) {
			s.logger.Warn("websocket_rate_limit_exceeded",
				slog.String("remote_addr", r.RemoteAddr))
			http.Error(w, "rate limit exceeded", http.StatusTooManyRequests)
			return
		}
	}

	ws, err := s.upgrader.Upgrade(w, r, nil)
	if err != nil {
		s.logger.Warn("websocket_upgrade_failed", slog.String("error", err.Error()))
		return
	}

	s.logger.Debug("websocket_connection_accepted", slog.String("remote_addr", r.RemoteAddr))

	conn := newWSConnection(ws, r.RemoteAddr, s.config.ProtocolVersion)
	defer connguard.Recover(s.logger, "mqtt-ws", r.RemoteAddr)
	broker.HandleConnection(r.Context(), s.broker, conn)
}

// wsConnection implements core.Connection for WebSocket transport.
type wsConnection struct {
	ws           *websocket.Conn
	remoteAddr   string
	reader       io.Reader
	frameReader  *wsFrameReader
	version      int
	mu           sync.RWMutex
	closeOnce    sync.Once
	readMu       sync.RWMutex
	writeMu      sync.Mutex
	closed       bool
	closeCh      chan struct{}
	readDeadline time.Time
	lastActivity time.Time
	onDisconnect func(graceful bool)
	pingStop     chan struct{}
	pingOnce     sync.Once
}

func newWSConnection(ws *websocket.Conn, remoteAddr string, protocolVersion int) core.Connection {
	return &wsConnection{
		ws:         ws,
		remoteAddr: remoteAddr,
		version:    protocolVersion,
		closed:     false,
		closeCh:    make(chan struct{}),
	}
}

func (c *wsConnection) ReadPacket() (packets.ControlPacket, error) {
	c.Touch()

	if c.frameReader == nil {
		c.frameReader = &wsFrameReader{conn: c}
	}
	if c.reader == nil {
		c.reader = c.frameReader
	}

	if c.version == 0 {
		ver, restored, err := packets.DetectProtocolVersion(c.reader)
		if err != nil {
			return nil, err
		}
		c.version = ver
		c.reader = restored
	}

	var pkt packets.ControlPacket
	var err error
	switch c.version {
	case 5:
		pkt, _, _, err = v5.ReadPacket(c.reader)
	case 3, 4:
		pkt, err = v3.ReadPacket(c.reader)
	default:
		err = ErrUnsupportedProtocolVersion
	}

	if err != nil {
		return nil, err
	}
	return pkt, nil
}

type wsFrameReader struct {
	conn     *wsConnection
	current  *bytes.Reader
	reads    chan wsReadResult
	requests chan struct{}
	done     chan struct{}
	once     sync.Once
	stateMu  sync.Mutex
	reading  bool
	errMu    sync.RWMutex
	err      error
}

type wsReadResult struct {
	messageType int
	data        []byte
	err         error
}

func (r *wsFrameReader) Read(p []byte) (int, error) {
	r.once.Do(r.start)

	for {
		if r.current != nil && r.current.Len() > 0 {
			return r.current.Read(p)
		}

		result, err := r.nextMessage()
		if err != nil {
			return 0, err
		}
		if result.messageType != websocket.BinaryMessage {
			return 0, ErrExpectedBinaryMessage
		}
		if len(result.data) == 0 {
			continue
		}

		r.conn.Touch()
		r.current = bytes.NewReader(result.data)
	}
}

func (r *wsFrameReader) start() {
	r.reads = make(chan wsReadResult, 1)
	r.requests = make(chan struct{}, 1)
	r.done = make(chan struct{})
	go func() {
		defer close(r.done)
		defer close(r.reads)
		for {
			select {
			case <-r.conn.done():
				return
			case <-r.requests:
			}

			messageType, data, err := r.conn.ws.ReadMessage()
			result := wsReadResult{messageType: messageType, data: data, err: err}
			if err != nil {
				r.setErr(err)
			}

			// Clear the in-flight flag before delivering the result. A consumer
			// that receives this result must observe reading==false on its next
			// requestRead, otherwise it would reuse a read that is already done
			// and block waiting for a request that is never sent.
			r.finishRead()

			select {
			case r.reads <- result:
			case <-r.conn.done():
				return
			}

			if err != nil {
				return
			}
		}
	}()
}

func (r *wsFrameReader) nextMessage() (wsReadResult, error) {
	select {
	case result, ok := <-r.reads:
		if !ok {
			r.finishRead()
			return wsReadResult{}, r.getErr()
		}
		return result, result.err
	default:
	}

	if err := r.requestRead(); err != nil {
		return wsReadResult{}, err
	}

	deadline := r.conn.getReadDeadline()
	if deadline.IsZero() {
		select {
		case result, ok := <-r.reads:
			if !ok {
				r.finishRead()
				return wsReadResult{}, r.getErr()
			}
			return result, result.err
		case <-r.conn.done():
			return wsReadResult{}, r.getErr()
		}
	}

	timeout := time.Until(deadline)
	if timeout <= 0 {
		return wsReadResult{}, wsReadTimeoutError{}
	}

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case result, ok := <-r.reads:
		if !ok {
			r.finishRead()
			return wsReadResult{}, r.getErr()
		}
		return result, result.err
	case <-timer.C:
		return wsReadResult{}, wsReadTimeoutError{}
	case <-r.conn.done():
		return wsReadResult{}, r.getErr()
	}
}

func (r *wsFrameReader) requestRead() error {
	r.stateMu.Lock()
	if r.reading {
		r.stateMu.Unlock()
		return nil
	}
	r.reading = true
	r.stateMu.Unlock()

	select {
	case r.requests <- struct{}{}:
		return nil
	case <-r.conn.done():
		r.finishRead()
		return r.getErr()
	}
}

func (r *wsFrameReader) finishRead() {
	r.stateMu.Lock()
	defer r.stateMu.Unlock()
	r.reading = false
}

func (r *wsFrameReader) setErr(err error) {
	r.errMu.Lock()
	defer r.errMu.Unlock()
	r.err = err
}

func (r *wsFrameReader) getErr() error {
	r.errMu.RLock()
	defer r.errMu.RUnlock()
	if r.err != nil {
		return r.err
	}
	select {
	case <-r.conn.done():
		return net.ErrClosed
	default:
	}
	return io.EOF
}

type wsReadTimeoutError struct{}

func (wsReadTimeoutError) Error() string {
	return "websocket read timeout"
}

func (wsReadTimeoutError) Timeout() bool {
	return true
}

func (wsReadTimeoutError) Temporary() bool {
	return true
}

func (c *wsConnection) WritePacket(pkt packets.ControlPacket) error {
	return c.WriteControlPacket(pkt, nil)
}

func (c *wsConnection) WriteControlPacket(pkt packets.ControlPacket, onSent func()) error {
	return c.writePacket(pkt, onSent)
}

func (c *wsConnection) WriteDataPacket(pkt packets.ControlPacket, onSent func()) error {
	return c.writePacket(pkt, onSent)
}

func (c *wsConnection) TryWriteDataPacket(pkt packets.ControlPacket, onSent func()) error {
	return c.writePacket(pkt, onSent)
}

func (c *wsConnection) writePacket(pkt packets.ControlPacket, onSent func()) error {
	if pkt == nil {
		return ErrCannotEncodeNilPacket
	}

	buf := &bytes.Buffer{}
	if err := pkt.Pack(buf); err != nil {
		pkt.Release()
		return err
	}
	pkt.Release()

	c.writeMu.Lock()
	err := c.ws.WriteMessage(websocket.BinaryMessage, buf.Bytes())
	c.writeMu.Unlock()
	if err != nil {
		return err
	}

	if onSent != nil {
		onSent()
	}
	return nil
}

func (c *wsConnection) Read(b []byte) (n int, err error) {
	return 0, ErrReadNotSupported
}

func (c *wsConnection) Write(b []byte) (n int, err error) {
	return 0, ErrWriteNotSupported
}

func (c *wsConnection) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return nil
	}

	c.closed = true
	c.closeOnce.Do(func() {
		if c.closeCh != nil {
			close(c.closeCh)
		}
	})
	c.pingOnce.Do(func() {
		if c.pingStop != nil {
			close(c.pingStop)
		}
	})
	if c.onDisconnect != nil {
		go c.onDisconnect(false)
	}

	_ = c.ws.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""), time.Now().Add(100*time.Millisecond))
	_ = c.ws.UnderlyingConn().Close()
	return nil
}

func (c *wsConnection) done() <-chan struct{} {
	if c.closeCh == nil {
		return nil
	}
	return c.closeCh
}

func (c *wsConnection) LocalAddr() net.Addr {
	return c.ws.LocalAddr()
}

func (c *wsConnection) RemoteAddr() net.Addr {
	return &wsAddr{addr: c.remoteAddr}
}

func (c *wsConnection) SetReadDeadline(t time.Time) error {
	c.readMu.Lock()
	defer c.readMu.Unlock()
	c.readDeadline = t
	return nil
}

func (c *wsConnection) SetWriteDeadline(t time.Time) error {
	return c.ws.SetWriteDeadline(t)
}

func (c *wsConnection) SetDeadline(t time.Time) error {
	c.SetReadDeadline(t) //nolint:errcheck // local state update cannot fail
	return c.ws.SetWriteDeadline(t)
}

func (c *wsConnection) getReadDeadline() time.Time {
	c.readMu.RLock()
	defer c.readMu.RUnlock()
	return c.readDeadline
}

func (c *wsConnection) SetKeepAlive(d time.Duration) error {
	if d <= 0 {
		return nil
	}

	c.pingStop = make(chan struct{})

	c.ws.SetPongHandler(func(string) error {
		c.Touch()
		return c.SetReadDeadline(time.Now().Add(d + d/2))
	})

	pingInterval := d / 2
	go func() {
		ticker := time.NewTicker(pingInterval)
		defer ticker.Stop()
		for {
			select {
			case <-c.pingStop:
				return
			case <-ticker.C:
				c.writeMu.Lock()
				err := c.ws.WriteControl(
					websocket.PingMessage, nil,
					time.Now().Add(10*time.Second),
				)
				c.writeMu.Unlock()
				if err != nil {
					return
				}
			}
		}
	}()

	return nil
}

func (c *wsConnection) SetOnDisconnect(fn func(graceful bool)) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.onDisconnect = fn
}

func (c *wsConnection) Touch() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.lastActivity = time.Now()
}

// wsAddr implements net.Addr for WebSocket connections.
type wsAddr struct {
	addr string
}

func (a *wsAddr) Network() string {
	return "websocket"
}

func (a *wsAddr) String() string {
	return a.addr
}
