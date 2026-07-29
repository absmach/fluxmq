// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package amqp

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"time"

	"github.com/absmach/fluxmq/amqp/broker"
	"github.com/absmach/fluxmq/internal/connguard"
)

// Config represents the configuration for the AMQP 0.9.1 server.
type Config struct {
	Address   string
	TLSConfig *tls.Config
	// HandshakeTimeout bounds the complete transport and AMQP handshake through
	// Connection.Open. A successful AMQP handshake clears the deadline.
	HandshakeTimeout time.Duration
	// TLSHandshakeTimeout is retained for source compatibility and is used when
	// HandshakeTimeout is unset.
	TLSHandshakeTimeout time.Duration
	ShutdownTimeout     time.Duration
	MaxConnections      int
	ConnectionPolicy    *broker.ConnectionPolicy
	Logger              *slog.Logger
}

// Server is an AMQP 0.9.1 server.
type Server struct {
	cfg       Config
	broker    *broker.Broker
	mu        sync.RWMutex
	listener  net.Listener
	connSem   chan struct{}
	ready     chan struct{}
	readyOnce sync.Once
}

// New creates a new AMQP 0.9.1 server.
func New(cfg Config, b *broker.Broker) *Server {
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if cfg.HandshakeTimeout <= 0 {
		cfg.HandshakeTimeout = cfg.TLSHandshakeTimeout
	}
	if cfg.HandshakeTimeout <= 0 {
		cfg.HandshakeTimeout = 10 * time.Second
	}
	var connSem chan struct{}
	if cfg.MaxConnections > 0 {
		connSem = make(chan struct{}, cfg.MaxConnections)
	}
	return &Server{
		cfg:     cfg,
		broker:  b,
		connSem: connSem,
		ready:   make(chan struct{}),
	}
}

// Ready is closed after the listener has bound successfully. It can be used by
// process readiness wiring to ensure every configured listener is accepting.
func (s *Server) Ready() <-chan struct{} {
	return s.ready
}

// Addr returns the bound listener address, or nil before Listen has bound.
func (s *Server) Addr() net.Addr {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.listener == nil {
		return nil
	}
	return s.listener.Addr()
}

// Listen starts the AMQP 0.9.1 server.
func (s *Server) Listen(ctx context.Context) error {
	var lc net.ListenConfig
	ln, err := lc.Listen(ctx, "tcp", s.cfg.Address)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", s.cfg.Address, err)
	}
	defer ln.Close()

	if s.cfg.TLSConfig != nil {
		ln = tls.NewListener(ln, s.cfg.TLSConfig)
	}
	s.mu.Lock()
	s.listener = ln
	s.mu.Unlock()
	s.readyOnce.Do(func() { close(s.ready) })

	s.cfg.Logger.Info("AMQP 0.9.1 server listening", "address", s.cfg.Address)

	go func() {
		<-ctx.Done()
		s.cfg.Logger.Info("AMQP 0.9.1 server shutting down")
		ln.Close()
	}()

	for {
		conn, err := ln.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return nil
			}
			s.cfg.Logger.Error("Failed to accept new connection", "error", err)
			continue
		}
		if !s.tryAcquireConnectionSlot(ctx, conn) {
			continue
		}
		go s.handleConnection(ctx, conn)
	}
}

func (s *Server) tryAcquireConnectionSlot(ctx context.Context, conn net.Conn) bool {
	if s.connSem == nil {
		return true
	}
	select {
	case s.connSem <- struct{}{}:
		return true
	case <-ctx.Done():
		_ = conn.Close()
		return false
	default:
		s.cfg.Logger.Warn("AMQP 0.9.1 connection limit reached", "remote", conn.RemoteAddr().String())
		_ = conn.Close()
		return false
	}
}

func (s *Server) releaseConnectionSlot() {
	if s.connSem != nil {
		<-s.connSem
	}
}

func (s *Server) handleConnection(ctx context.Context, conn net.Conn) {
	defer s.releaseConnectionSlot()
	defer conn.Close()
	defer connguard.Recover(s.cfg.Logger, "amqp091", conn.RemoteAddr().String())
	deadline := time.Now().Add(s.cfg.HandshakeTimeout)
	if err := conn.SetDeadline(deadline); err != nil {
		s.cfg.Logger.Warn("AMQP 0.9.1 handshake deadline failed", "remote", conn.RemoteAddr().String(), "error", err)
		return
	}
	if tlsConn, ok := conn.(*tls.Conn); ok {
		if err := tlsConn.HandshakeContext(ctx); err != nil {
			s.cfg.Logger.Warn("AMQP 0.9.1 TLS handshake failed", "remote", conn.RemoteAddr().String(), "error", err)
			return
		}
	}
	if s.cfg.ConnectionPolicy != nil {
		s.broker.HandleConnectionWithPolicy(ctx, conn, s.cfg.ConnectionPolicy)
		return
	}
	s.broker.HandleConnection(ctx, conn)
}
