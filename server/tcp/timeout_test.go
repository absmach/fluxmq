// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package tcp

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/absmach/fluxmq/mqtt/broker"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestConnectDeadlineDropsSilentClient covers the pre-authentication window: a
// peer that connects and sends nothing must be dropped by the connect deadline
// instead of holding its connection slot indefinitely.
func TestConnectDeadlineDropsSilentClient(t *testing.T) {
	b := broker.NewBroker(nil, nil)
	t.Cleanup(func() { b.Close() }) //nolint:errcheck // best-effort teardown

	server := New(Config{
		ShutdownTimeout: 5 * time.Second,
		ReadTimeout:     100 * time.Millisecond,
	}, b)

	serverConn, clientConn := net.Pipe()
	t.Cleanup(func() { clientConn.Close() }) //nolint:errcheck // best-effort teardown

	done := make(chan struct{})
	server.wg.Add(1)
	go func() {
		defer close(done)
		server.handleConnection(context.Background(), serverConn)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("silent client was never dropped: no connect deadline applied")
	}

	// The handler owns the socket and must have closed it on the way out.
	_, err := clientConn.Read(make([]byte, 1))
	assert.Error(t, err, "server side of a timed-out connection must be closed")
}

// TestForcedShutdownClosesBlockedConnections covers the shutdown path: once the
// grace period expires, cancelling the connection context is not enough because
// a goroutine parked in a socket read never observes it. The sockets themselves
// must be closed.
func TestForcedShutdownClosesBlockedConnections(t *testing.T) {
	b := broker.NewBroker(nil, nil)
	t.Cleanup(func() { b.Close() }) //nolint:errcheck // best-effort teardown

	// No read timeout: the only thing that can unblock the connection goroutine
	// is the forced close.
	server := New(Config{ShutdownTimeout: 50 * time.Millisecond}, b)

	ctx, cancel := context.WithCancel(context.Background())
	connCtx, connCancel := context.WithCancel(context.Background())
	listener := newStubListener()

	server.mu.Lock()
	server.listener = listener
	server.mu.Unlock()

	acceptDone := server.runAcceptLoop(ctx, connCtx, listener)

	serverConn, clientConn := net.Pipe()
	t.Cleanup(func() { clientConn.Close() }) //nolint:errcheck // best-effort teardown
	require.NoError(t, listener.push(serverConn))

	// Wait until the connection is tracked, so shutdown cannot race past it.
	require.Eventually(t, func() bool {
		server.activeMu.Lock()
		defer server.activeMu.Unlock()
		return len(server.active) == 1
	}, 5*time.Second, time.Millisecond, "accepted connection was never tracked")

	cancel()

	shutdownDone := make(chan error, 1)
	go func() { shutdownDone <- server.gracefulShutdown(listener, acceptDone, connCancel) }()

	select {
	case err := <-shutdownDone:
		// The connection is still blocked when the grace period expires, so a
		// timeout is expected; what matters is that shutdown returns at all and
		// leaves no tracked sockets behind.
		assert.ErrorIs(t, err, ErrShutdownTimeout)
	case <-time.After(10 * time.Second):
		t.Fatal("shutdown never returned: blocked connection was not force-closed")
	}

	require.Eventually(t, func() bool {
		server.activeMu.Lock()
		defer server.activeMu.Unlock()
		return len(server.active) == 0
	}, 5*time.Second, time.Millisecond, "force-closed connections must be untracked")
}
