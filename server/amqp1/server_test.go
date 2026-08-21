// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package amqp1

import (
	"context"
	"io"
	"log/slog"
	"net"
	"os"
	"testing"
	"time"

	amqpbroker "github.com/absmach/fluxmq/amqp1/broker"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func startTestServer(t *testing.T, cfg Config) *Server {
	t.Helper()
	if cfg.Address == "" {
		cfg.Address = "127.0.0.1:0"
	}
	cfg.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))

	srv := New(cfg, amqpbroker.New(nil, nil, cfg.Logger))
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- srv.Listen(ctx) }()

	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("server did not shut down")
		}
	})

	require.Eventually(t, func() bool { return srv.Addr() != nil }, 3*time.Second, 5*time.Millisecond,
		"server never started listening")
	return srv
}

// TestHandshakeTimeoutClosesSilentConnection is the reason the timeout exists:
// a peer that connects and then says nothing used to hold its connection slot
// for as long as it liked, because nothing bounded the phase before OPEN.
func TestHandshakeTimeoutClosesSilentConnection(t *testing.T) {
	srv := startTestServer(t, Config{HandshakeTimeout: 200 * time.Millisecond})

	conn, err := net.Dial("tcp", srv.Addr().String())
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })

	// Never send the protocol header. The server must give up on its own.
	require.NoError(t, conn.SetReadDeadline(time.Now().Add(3*time.Second)))
	start := time.Now()
	_, err = conn.Read(make([]byte, 1))

	require.Error(t, err, "server kept a silent connection open")
	assert.NotErrorIs(t, err, os.ErrDeadlineExceeded,
		"the client's own deadline fired first; the server never closed the connection")
	assert.Less(t, time.Since(start), 2*time.Second)
}

// TestHandshakeTimeoutZeroLeavesConnectionOpen documents the escape hatch: an
// explicit zero disables the deadline, which is what absent-versus-zero means
// for this key.
func TestHandshakeTimeoutZeroLeavesConnectionOpen(t *testing.T) {
	srv := startTestServer(t, Config{HandshakeTimeout: 0})

	conn, err := net.Dial("tcp", srv.Addr().String())
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })

	require.NoError(t, conn.SetReadDeadline(time.Now().Add(500*time.Millisecond)))
	_, err = conn.Read(make([]byte, 1))
	assert.ErrorIs(t, err, os.ErrDeadlineExceeded,
		"with the deadline disabled the server must not close a silent connection")
}
