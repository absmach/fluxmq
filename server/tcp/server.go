// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package tcp

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"time"

	"github.com/absmach/fluxmq/internal/connguard"
	core "github.com/absmach/fluxmq/mqtt"
	"github.com/absmach/fluxmq/mqtt/broker"
)

// ErrShutdownTimeout is returned when graceful shutdown exceeds the configured timeout.
var ErrShutdownTimeout = errors.New("shutdown timeout exceeded")

// IPRateLimiter is the interface for IP-based rate limiting.
type IPRateLimiter interface {
	Allow(addr net.Addr) bool
}

// Config holds the TCP server configuration.
type Config struct {
	Address         string
	TLSConfig       *tls.Config
	Logger          *slog.Logger
	ShutdownTimeout time.Duration
	// ReadTimeout bounds the pre-session phase of a connection: the TLS
	// handshake, protocol version sniff, and CONNECT packet. Once the session
	// starts it sets its own read deadlines from the negotiated keep-alive.
	ReadTimeout time.Duration
	// WriteTimeout bounds a single socket write for the life of the connection.
	WriteTimeout     time.Duration
	TCPKeepAlive     time.Duration
	MaxConnections   int
	BufferSize       int
	DisableNoDelay   bool
	IPRateLimiter    IPRateLimiter // Optional IP-based rate limiter
	SendQueueSize    int
	DisconnectOnFull bool
	ProtocolVersion  int
	// CertificateAuthentication marks this listener as the Atom-backed mTLS
	// authentication boundary. Ordinary TLS listeners leave it false even when
	// they request or verify optional client certificates.
	CertificateAuthentication bool
	// MaxPacketSize bounds an inbound MQTT packet's remaining length. The limit
	// is applied from the fixed header, before the body is buffered, so an
	// unauthenticated peer cannot reserve memory by advertising a large length.
	// 0 leaves packets unbounded (the protocol ceiling of ~256 MiB).
	MaxPacketSize int
}

// Server is a TCP server that accepts connections and delegates them to a broker.
// It provides robust connection handling, graceful shutdown, and production-ready features.
type Server struct {
	mu            sync.Mutex
	wg            sync.WaitGroup
	config        Config
	handler       *broker.Broker
	listener      net.Listener
	connSem       chan struct{}
	ipRateLimiter IPRateLimiter

	// activeMu guards active, the set of accepted sockets. Cancelling the
	// connection context does not interrupt a goroutine blocked in a socket
	// read, so a forced shutdown closes these directly.
	activeMu sync.Mutex
	active   map[net.Conn]struct{}
}

// New creates a new TCP server with the given configuration and broker.
func New(cfg Config, h *broker.Broker) *Server {
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if cfg.ShutdownTimeout == 0 {
		cfg.ShutdownTimeout = 30 * time.Second
	}
	if cfg.ReadTimeout == 0 {
		cfg.ReadTimeout = 60 * time.Second
	}
	if cfg.WriteTimeout == 0 {
		cfg.WriteTimeout = 60 * time.Second
	}
	if cfg.BufferSize == 0 {
		cfg.BufferSize = 8192 // 8KB default for performance
	}
	if cfg.TCPKeepAlive == 0 {
		cfg.TCPKeepAlive = 15 * time.Second
	}

	var connSem chan struct{}
	if cfg.MaxConnections > 0 {
		connSem = make(chan struct{}, cfg.MaxConnections)
	}

	return &Server{
		config:        cfg,
		handler:       h,
		connSem:       connSem,
		ipRateLimiter: cfg.IPRateLimiter,
		active:        make(map[net.Conn]struct{}),
	}
}

// trackConn registers an accepted socket so a forced shutdown can close it.
func (s *Server) trackConn(conn net.Conn) {
	s.activeMu.Lock()
	defer s.activeMu.Unlock()
	s.active[conn] = struct{}{}
}

func (s *Server) untrackConn(conn net.Conn) {
	s.activeMu.Lock()
	defer s.activeMu.Unlock()
	delete(s.active, conn)
}

// closeActiveConns closes every tracked socket, unblocking connection
// goroutines parked in a read or write that context cancellation cannot reach.
func (s *Server) closeActiveConns() int {
	s.activeMu.Lock()
	conns := make([]net.Conn, 0, len(s.active))
	for conn := range s.active {
		conns = append(conns, conn)
	}
	s.activeMu.Unlock()

	for _, conn := range conns {
		conn.Close() //nolint:errcheck // forced shutdown; the goroutine reports the resulting error
	}
	return len(conns)
}

// Listen starts the TCP server and blocks until the context is cancelled.
// It implements graceful shutdown with connection draining.
func (s *Server) Listen(ctx context.Context) error {
	listener, err := s.createListener()
	if err != nil {
		return err
	}

	connCtx, connCancel := context.WithCancel(context.Background())
	defer connCancel()

	acceptDone := s.runAcceptLoop(ctx, connCtx, listener)

	<-ctx.Done()
	return s.gracefulShutdown(listener, acceptDone, connCancel)
}

// createListener creates and configures the TCP listener.
func (s *Server) createListener() (net.Listener, error) {
	listener, err := net.Listen("tcp", s.config.Address)
	if err != nil {
		return nil, fmt.Errorf("failed to listen on %s: %w", s.config.Address, err)
	}

	s.mu.Lock()
	s.listener = listener
	s.mu.Unlock()

	if s.config.TLSConfig != nil {
		listener = tls.NewListener(listener, s.config.TLSConfig)
		s.config.Logger.Info("TLS enabled", slog.String("address", s.config.Address))
	}

	s.config.Logger.Info("TCP server started", slog.String("address", s.config.Address))
	return listener, nil
}

// runAcceptLoop runs the connection accept loop in a separate goroutine.
func (s *Server) runAcceptLoop(ctx, connCtx context.Context, listener net.Listener) <-chan struct{} {
	acceptDone := make(chan struct{})
	go func() {
		defer close(acceptDone)
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}

			conn, err := listener.Accept()
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				s.config.Logger.Error("failed to accept connection", slog.String("error", err.Error()))
				continue
			}

			// Check IP rate limit before acquiring connection slot
			if s.ipRateLimiter != nil && !s.ipRateLimiter.Allow(conn.RemoteAddr()) {
				s.config.Logger.Warn("connection rate limit exceeded",
					slog.String("remote", conn.RemoteAddr().String()))
				conn.Close()
				continue
			}

			if !s.tryAcquireConnectionSlot(ctx, conn) {
				continue
			}

			if tcpConn, ok := conn.(*net.TCPConn); ok {
				if err := s.configureTCPConn(tcpConn); err != nil {
					s.config.Logger.Error("failed to configure TCP connection",
						slog.String("error", err.Error()))
					s.releaseConnectionSlot()
					conn.Close()
					continue
				}
			}

			s.wg.Add(1)
			go s.handleConnection(connCtx, conn)
		}
	}()
	return acceptDone
}

// tryAcquireConnectionSlot attempts to acquire a connection slot within the configured limit.
func (s *Server) tryAcquireConnectionSlot(ctx context.Context, conn net.Conn) bool {
	if s.connSem == nil {
		return true
	}

	select {
	case s.connSem <- struct{}{}:
		return true
	case <-ctx.Done():
		conn.Close()
		return false
	default:
		s.config.Logger.Warn("connection limit reached, rejecting connection",
			slog.String("remote", conn.RemoteAddr().String()))
		conn.Close()
		return false
	}
}

// releaseConnectionSlot releases a connection slot.
func (s *Server) releaseConnectionSlot() {
	if s.connSem != nil {
		<-s.connSem
	}
}

// handleConnection handles a single connection in a goroutine.
func (s *Server) handleConnection(connCtx context.Context, conn net.Conn) {
	defer s.wg.Done()
	defer s.releaseConnectionSlot()

	defer conn.Close()
	defer connguard.Recover(s.config.Logger, "mqtt-tcp", conn.RemoteAddr().String())

	s.trackConn(conn)
	defer s.untrackConn(conn)

	s.config.Logger.Debug("connection established",
		slog.String("remote", conn.RemoteAddr().String()))

	// Bound everything that happens before the session's own read deadlines take
	// over: the TLS handshake, the protocol version sniff, and the CONNECT
	// packet. Without it an unauthenticated peer can hold a connection slot open
	// indefinitely by opening a socket and sending nothing.
	if s.config.ReadTimeout > 0 {
		if err := conn.SetReadDeadline(time.Now().Add(s.config.ReadTimeout)); err != nil {
			s.config.Logger.Error("failed to set connect deadline", slog.String("error", err.Error()))
			return
		}
	}

	// For TLS connections, the handshake happens during the first Read/Write
	// from the TLS listener, but we need to ensure it's complete before using
	// the connection. HandshakeContext aborts it when the server is shutting
	// down rather than waiting out the read deadline.
	if tlsConn, ok := conn.(*tls.Conn); ok {
		if err := tlsConn.HandshakeContext(connCtx); err != nil {
			s.config.Logger.Error("TLS handshake failed", slog.String("error", err.Error()))
			return
		}
		s.config.Logger.Debug("TLS handshake successful")
	}

	// core.NewConnection accepts any net.Conn (TCP or TLS). The session replaces
	// the read deadline on every packet read; the write deadline is applied per
	// socket write by the connection itself.
	hc := core.NewConnectionWithVersion(conn, s.config.SendQueueSize, s.config.DisconnectOnFull, s.config.ProtocolVersion,
		core.WithMaxPacketSize(s.config.MaxPacketSize),
		core.WithWriteTimeout(s.config.WriteTimeout),
		core.WithCertificateAuthentication(s.config.CertificateAuthentication))
	broker.HandleConnection(connCtx, s.handler, hc)

	s.config.Logger.Debug("connection closed",
		slog.String("remote", conn.RemoteAddr().String()))
}

// gracefulShutdown performs graceful shutdown with connection draining.
func (s *Server) gracefulShutdown(listener net.Listener, acceptDone <-chan struct{}, connCancel context.CancelFunc) error {
	s.config.Logger.Info("shutdown signal received, closing listener")

	if err := listener.Close(); err != nil {
		s.config.Logger.Error("error closing listener", slog.String("error", err.Error()))
	}

	<-acceptDone

	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		s.config.Logger.Info("all connections closed gracefully")
		return nil
	case <-time.After(s.config.ShutdownTimeout):
		s.config.Logger.Warn("shutdown timeout exceeded, forcing connection closure")
		connCancel()
		// Cancellation alone does not interrupt a goroutine blocked in a socket
		// read, so close the sockets out from under them.
		s.config.Logger.Warn("closing active connections",
			slog.Int("connections", s.closeActiveConns()))

		select {
		case <-done:
			return ErrShutdownTimeout
		case <-time.After(1 * time.Second):
			return ErrShutdownTimeout
		}
	}
}

// configureTCPConn sets TCP socket options for optimal performance and resilience.
func (s *Server) configureTCPConn(conn *net.TCPConn) error {
	if s.config.TCPKeepAlive > 0 {
		if err := conn.SetKeepAlive(true); err != nil {
			return fmt.Errorf("failed to enable keepalive: %w", err)
		}
		if err := conn.SetKeepAlivePeriod(s.config.TCPKeepAlive); err != nil {
			return fmt.Errorf("failed to set keepalive period: %w", err)
		}
	}

	if !s.config.DisableNoDelay {
		if err := conn.SetNoDelay(true); err != nil {
			return fmt.Errorf("failed to set TCP_NODELAY: %w", err)
		}
	}

	return nil
}

// Addr returns the listener's network address.

func (s *Server) Addr() net.Addr {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.listener == nil {
		return nil
	}
	return s.listener.Addr()
}
