// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package websocket

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	corebroker "github.com/absmach/fluxmq/broker"
	"github.com/absmach/fluxmq/internal/connguard"
	core "github.com/absmach/fluxmq/mqtt"
	"github.com/absmach/fluxmq/mqtt/broker"
	"github.com/absmach/fluxmq/mqtt/packets"
	v3 "github.com/absmach/fluxmq/mqtt/packets/v3"
	v5 "github.com/absmach/fluxmq/mqtt/packets/v5"
	"github.com/gorilla/websocket"
	"golang.org/x/net/netutil"
)

// subprotocolMQTT is the WebSocket subprotocol name for MQTT.
const subprotocolMQTT = "mqtt"

// defaultPath is the URL path served when the configuration leaves it empty.
const defaultPath = "/mqtt"

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
	// MaxPacketSize bounds an inbound MQTT packet's remaining length, and the
	// WebSocket message that carries it. 0 leaves both unbounded.
	MaxPacketSize int
	// ReadTimeout bounds the pre-session phase of an upgraded connection: the
	// protocol version sniff and the CONNECT packet. Once the session starts it
	// sets its own read deadlines from the negotiated keep-alive.
	ReadTimeout time.Duration
	// WriteTimeout bounds a single socket write for the life of the connection.
	WriteTimeout time.Duration
	// MaxConnections caps concurrently upgraded connections. 0 means unlimited.
	MaxConnections int
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

	// activeMu guards active, the set of upgraded connections. http.Server's
	// Shutdown neither closes nor waits for hijacked connections, so these are
	// tracked and closed explicitly.
	activeMu sync.Mutex
	active   map[*websocket.Conn]struct{}
}

func New(cfg Config, b *broker.Broker, logger *slog.Logger) *Server {
	if logger == nil {
		logger = slog.Default()
	}

	if cfg.Path == "" {
		cfg.Path = defaultPath
	}
	if cfg.ReadTimeout == 0 {
		cfg.ReadTimeout = 60 * time.Second
	}
	if cfg.WriteTimeout == 0 {
		cfg.WriteTimeout = 60 * time.Second
	}

	s := &Server{
		config:         cfg,
		broker:         b,
		logger:         logger,
		allowedOrigins: make(map[string]bool),
		ipRateLimiter:  cfg.IPRateLimiter,
		active:         make(map[*websocket.Conn]struct{}),
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
		// Bound the request phase that precedes the upgrade. This also bounds
		// the TLS handshake, which net/http deadlines from the longest of the
		// server's read and write timeouts. Without it a peer can hold a socket
		// open indefinitely by dribbling headers or stalling the handshake.
		// gorilla clears the deadline once it hijacks, so a live WebSocket
		// session is unaffected.
		ReadHeaderTimeout: cfg.ReadTimeout,
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
	listener, err := net.Listen("tcp", s.config.Address)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", s.config.Address, err)
	}
	return s.serveListener(ctx, s.limitListener(listener))
}

// limitListener caps concurrently accepted sockets. The cap belongs on the
// listener rather than on the upgrade handler: a peer that opens a socket and
// never completes the HTTP request would otherwise consume no quota at all
// while still holding a connection.
func (s *Server) limitListener(listener net.Listener) net.Listener {
	if s.config.MaxConnections <= 0 {
		return listener
	}
	return netutil.LimitListener(listener, s.config.MaxConnections)
}

// serveListener serves upgrades on an already-bound listener and shuts down when
// ctx is cancelled.
func (s *Server) serveListener(ctx context.Context, listener net.Listener) error {
	tlsEnabled := s.config.TLSConfig != nil
	s.logger.Info("websocket_server_starting",
		slog.String("addr", listener.Addr().String()),
		slog.String("path", s.config.Path),
		slog.Bool("tls_enabled", tlsEnabled))

	errCh := make(chan error, 1)
	go func() {
		var err error
		if s.config.TLSConfig != nil {
			s.server.TLSConfig = s.config.TLSConfig
			// ServeTLS with empty cert/key paths because TLS config is already set
			err = s.server.ServeTLS(listener, "", "")
		} else {
			err = s.server.Serve(listener)
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

		err := s.server.Shutdown(shutdownCtx) //nolint:contextcheck // intentionally creates new context for graceful shutdown

		// Shutdown neither closes nor waits for hijacked connections, so an
		// upgraded WebSocket outlives it. Close them so their goroutines, which
		// are parked in a socket read, can exit.
		if closed := s.closeActiveConns(); closed > 0 {
			s.logger.Warn("closing upgraded websocket connections", slog.Int("connections", closed))
		}

		if err != nil {
			s.logger.Error("websocket_server_shutdown_error", slog.String("error", err.Error()))
			return err
		}

		s.logger.Info("websocket_server_stopped")
		return nil
	}
}

// trackConn registers an upgraded connection so shutdown can close it.
func (s *Server) trackConn(ws *websocket.Conn) {
	s.activeMu.Lock()
	defer s.activeMu.Unlock()
	s.active[ws] = struct{}{}
}

func (s *Server) untrackConn(ws *websocket.Conn) {
	s.activeMu.Lock()
	defer s.activeMu.Unlock()
	delete(s.active, ws)
}

// closeActiveConns closes every upgraded connection and reports how many.
func (s *Server) closeActiveConns() int {
	s.activeMu.Lock()
	conns := make([]*websocket.Conn, 0, len(s.active))
	for ws := range s.active {
		conns = append(conns, ws)
	}
	s.activeMu.Unlock()

	for _, ws := range conns {
		ws.Close() //nolint:errcheck // forced shutdown; the connection goroutine reports the resulting error
	}
	return len(conns)
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
	// Closing the hijacked connection also releases its listener slot.
	defer ws.Close() //nolint:errcheck // best-effort close once the session ends

	s.trackConn(ws)
	defer s.untrackConn(ws)

	s.logger.Debug("websocket_connection_accepted", slog.String("remote_addr", r.RemoteAddr))

	var peerCertificate corebroker.PeerCertificate
	if r.TLS != nil && len(r.TLS.VerifiedChains) != 0 && len(r.TLS.VerifiedChains[0]) != 0 {
		peerCertificate.LeafDER = append([]byte(nil), r.TLS.VerifiedChains[0][0].Raw...)
		if len(r.TLS.VerifiedChains[0]) > 1 {
			peerCertificate.IssuerDER = append([]byte(nil), r.TLS.VerifiedChains[0][1].Raw...)
		}
	}
	conn := newWSConnection(ws, r.RemoteAddr, s.config.ProtocolVersion, s.config.MaxPacketSize, s.config.WriteTimeout, peerCertificate)

	// Bound the pre-session phase: an upgraded client that never sends a CONNECT
	// would otherwise hold a goroutine and a connection slot indefinitely. The
	// session replaces this deadline on every packet read once it starts.
	if s.config.ReadTimeout > 0 {
		conn.SetReadDeadline(time.Now().Add(s.config.ReadTimeout)) //nolint:errcheck // local state update cannot fail
	}

	defer connguard.Recover(s.logger, "mqtt-ws", r.RemoteAddr)
	broker.HandleConnection(r.Context(), s.broker, conn)
}

// wsConnection implements core.Connection for WebSocket transport.
type wsConnection struct {
	ws              *websocket.Conn
	remoteAddr      string
	reader          io.Reader
	frameReader     *wsFrameReader
	version         int
	mu              sync.RWMutex
	closeOnce       sync.Once
	readMu          sync.RWMutex
	writeMu         sync.Mutex
	closed          bool
	closeCh         chan struct{}
	readDeadline    time.Time
	lastActivity    time.Time
	onDisconnect    func(graceful bool)
	pingStop        chan struct{}
	pingOnce        sync.Once
	maxPacketSize   int           // 0 = unlimited
	writeTimeout    time.Duration // 0 = no write deadline
	peerCertificate corebroker.PeerCertificate
}

// wsFrameOverhead allows an MQTT packet's fixed header (up to 5 bytes) to fit
// inside the WebSocket read limit derived from the packet-size limit, so the
// packet-level check reports the oversized packet rather than the frame reader
// tearing the connection down first.
const wsFrameOverhead = 5

func newWSConnection(ws *websocket.Conn, remoteAddr string, protocolVersion, maxPacketSize int, writeTimeout time.Duration, peerCertificate ...corebroker.PeerCertificate) core.Connection {
	if ws != nil && maxPacketSize > 0 {
		ws.SetReadLimit(int64(maxPacketSize) + wsFrameOverhead)
	}
	conn := &wsConnection{
		ws:            ws,
		remoteAddr:    remoteAddr,
		version:       protocolVersion,
		closed:        false,
		closeCh:       make(chan struct{}),
		maxPacketSize: maxPacketSize,
		writeTimeout:  writeTimeout,
	}
	if len(peerCertificate) != 0 {
		conn.peerCertificate.LeafDER = append([]byte(nil), peerCertificate[0].LeafDER...)
		conn.peerCertificate.IssuerDER = append([]byte(nil), peerCertificate[0].IssuerDER...)
	}
	return conn
}

// PeerCertificateDER returns a copy of the verified WebSocket TLS peer leaf.
func (c *wsConnection) PeerCertificateDER() []byte {
	return append([]byte(nil), c.peerCertificate.LeafDER...)
}

// PeerIssuerCertificateDER returns a copy of the verified leaf issuer.
func (c *wsConnection) PeerIssuerCertificateDER() []byte {
	return append([]byte(nil), c.peerCertificate.IssuerDER...)
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
		pkt, _, _, err = v5.ReadPacketLimit(c.reader, c.maxPacketSize)
	case 3, 4:
		pkt, err = v3.ReadPacketLimit(c.reader, c.maxPacketSize)
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
	// Refresh the deadline per write: without it a peer that stops reading parks
	// this goroutine on the socket write for as long as it likes.
	if c.writeTimeout > 0 {
		if err := c.ws.SetWriteDeadline(time.Now().Add(c.writeTimeout)); err != nil {
			c.writeMu.Unlock()
			return err
		}
	}
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
