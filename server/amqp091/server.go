// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package amqp091

import (
	"context"
	"crypto/tls"
	"errors"
	"log/slog"
	"net"
	"time"

	"github.com/absmach/fluxmq/amqp091/broker"
)

// Config represents the configuration for the AMQP 0.9.1 server.
type Config struct {
	Address         string
	TLSConfig       *tls.Config
	ShutdownTimeout time.Duration
	MaxConnections  int
	Logger          *slog.Logger
}

// Server is an AMQP 0.9.1 server.
type Server struct {
	cfg    Config
	broker *broker.Broker
}

// New creates a new AMQP 0.9.1 server.
func New(cfg Config, b *broker.Broker) *Server {
	return &Server{
		cfg:    cfg,
		broker: b,
	}
}

// Listen starts the AMQP 0.9.1 server.
func (s *Server) Listen(ctx context.Context) error {
	var lc net.ListenConfig
	ln, err := lc.Listen(ctx, "tcp", s.cfg.Address)
	if err != nil {
		return err
	}
	defer ln.Close()

	if s.cfg.TLSConfig != nil {
		ln = tls.NewListener(ln, s.cfg.TLSConfig)
	}

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
		go s.handleConnection(ctx, conn)
	}
}

func (s *Server) handleConnection(ctx context.Context, conn net.Conn) {
	s.broker.HandleConnection(conn)
}
