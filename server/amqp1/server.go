// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package amqp1

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"time"

	amqpbroker "github.com/absmach/fluxmq/amqp1/broker"
	"github.com/absmach/fluxmq/internal/connguard"
)

// ErrShutdownTimeout is returned when graceful shutdown exceeds the configured timeout.
var ErrShutdownTimeout = errors.New("shutdown timeout exceeded")

// Config holds the AMQP server configuration.
type Config struct {
	Address   string
	TLSConfig *tls.Config
	Logger    *slog.Logger
	// HandshakeTimeout bounds the transport, SASL, and AMQP OPEN exchange for
	// one connection. A successful OPEN clears the deadline. Zero disables it,
	// which lets a peer hold a connection slot indefinitely without speaking.
	HandshakeTimeout time.Duration
	ShutdownTimeout  time.Duration
	MaxConnections   int
}

// Server is a TCP server that accepts AMQP 1.0 connections.
type Server struct {
	mu       sync.Mutex
	wg       sync.WaitGroup
	config   Config
	handler  *amqpbroker.Broker
	listener net.Listener
	connSem  chan struct{}
}

// New creates a new AMQP server.
func New(cfg Config, broker *amqpbroker.Broker) *Server {
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if cfg.ShutdownTimeout == 0 {
		cfg.ShutdownTimeout = 30 * time.Second
	}

	var connSem chan struct{}
	if cfg.MaxConnections > 0 {
		connSem = make(chan struct{}, cfg.MaxConnections)
	}

	return &Server{
		config:  cfg,
		handler: broker,
		connSem: connSem,
	}
}

// Listen starts the server and blocks until context is cancelled.
func (s *Server) Listen(ctx context.Context) error {
	listener, err := net.Listen("tcp", s.config.Address)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", s.config.Address, err)
	}

	s.mu.Lock()
	s.listener = listener
	s.mu.Unlock()

	if s.config.TLSConfig != nil {
		listener = tls.NewListener(listener, s.config.TLSConfig)
		s.config.Logger.Info("TLS enabled for AMQP", slog.String("address", s.config.Address))
	}

	s.config.Logger.Info("AMQP server started", slog.String("address", s.config.Address))

	connCtx, connCancel := context.WithCancel(context.Background())
	defer connCancel()

	acceptDone := s.runAcceptLoop(ctx, connCtx, listener)

	<-ctx.Done()
	return s.gracefulShutdown(listener, acceptDone, connCancel)
}

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
				s.config.Logger.Error("failed to accept AMQP connection", slog.String("error", err.Error()))
				continue
			}

			if !s.tryAcquireSlot(ctx, conn) {
				continue
			}

			if tcpConn, ok := conn.(*net.TCPConn); ok {
				tcpConn.SetKeepAlive(true)                   //nolint:errcheck // TCP socket options; fails only on closed connection
				tcpConn.SetKeepAlivePeriod(15 * time.Second) //nolint:errcheck // TCP socket options; fails only on closed connection
				tcpConn.SetNoDelay(true)                     //nolint:errcheck // TCP socket options; fails only on closed connection
			}

			s.wg.Add(1)
			go func(c net.Conn) {
				defer s.wg.Done()
				defer s.releaseSlot()
				defer c.Close()
				defer connguard.Recover(s.config.Logger, "amqp10", c.RemoteAddr().String())

				s.config.Logger.Debug("AMQP connection accepted", slog.String("remote", c.RemoteAddr().String()))
				if !s.handshake(connCtx, c) {
					return
				}
				s.handler.HandleConnection(connCtx, c)
			}(conn)
		}
	}()
	return acceptDone
}

// handshake bounds everything that happens before the connection is
// established and runs it on the connection's own goroutine. The TLS handshake
// used to run inline in the accept loop, where one unresponsive peer stalled
// every pending connection; the deadline covers TLS, SASL, and OPEN, and
// amqp1/broker clears it once OPEN succeeds.
func (s *Server) handshake(ctx context.Context, conn net.Conn) bool {
	if s.config.HandshakeTimeout > 0 {
		if err := conn.SetDeadline(time.Now().Add(s.config.HandshakeTimeout)); err != nil {
			s.config.Logger.Warn("AMQP 1.0 handshake deadline failed",
				slog.String("remote", conn.RemoteAddr().String()),
				slog.String("error", err.Error()))
			return false
		}
	}

	tlsConn, ok := conn.(*tls.Conn)
	if !ok {
		return true
	}
	if err := tlsConn.HandshakeContext(ctx); err != nil {
		s.config.Logger.Error("TLS handshake failed",
			slog.String("remote", conn.RemoteAddr().String()),
			slog.String("error", err.Error()))
		return false
	}
	return true
}

func (s *Server) tryAcquireSlot(ctx context.Context, conn net.Conn) bool {
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
		s.config.Logger.Warn("AMQP connection limit reached",
			slog.String("remote", conn.RemoteAddr().String()))
		conn.Close()
		return false
	}
}

func (s *Server) releaseSlot() {
	if s.connSem != nil {
		<-s.connSem
	}
}

func (s *Server) gracefulShutdown(listener net.Listener, acceptDone <-chan struct{}, connCancel context.CancelFunc) error {
	s.config.Logger.Info("AMQP shutdown signal received, closing listener")

	if err := listener.Close(); err != nil {
		s.config.Logger.Error("error closing AMQP listener", slog.String("error", err.Error()))
	}

	<-acceptDone

	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		s.config.Logger.Info("all AMQP connections closed gracefully")
		return nil
	case <-time.After(s.config.ShutdownTimeout):
		s.config.Logger.Warn("AMQP shutdown timeout exceeded, forcing closure")
		connCancel()
		select {
		case <-done:
		case <-time.After(time.Second):
		}
		return ErrShutdownTimeout
	}
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
