// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package tcp

import (
	"context"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/absmach/fluxmq/config"
	"github.com/absmach/fluxmq/mqtt/broker"
)

type stubListener struct {
	conns  chan net.Conn
	closed chan struct{}
	addr   net.Addr
}

func newStubListener() *stubListener {
	return &stubListener{
		conns:  make(chan net.Conn, 16),
		closed: make(chan struct{}),
		addr:   stubAddr("in-memory"),
	}
}

func (l *stubListener) Accept() (net.Conn, error) {
	select {
	case <-l.closed:
		return nil, net.ErrClosed
	case conn, ok := <-l.conns:
		if !ok {
			return nil, net.ErrClosed
		}
		return conn, nil
	}
}

func (l *stubListener) Close() error {
	select {
	case <-l.closed:
		return nil
	default:
		close(l.closed)
		close(l.conns)
		return nil
	}
}

func (l *stubListener) Addr() net.Addr { return l.addr }

func (l *stubListener) push(conn net.Conn) error {
	select {
	case <-l.closed:
		return net.ErrClosed
	default:
		l.conns <- conn
		return nil
	}
}

type stubAddr string

func (a stubAddr) Network() string { return "stub" }
func (a stubAddr) String() string  { return string(a) }

type trackingConn struct {
	net.Conn
	closed atomic.Bool
}

func (c *trackingConn) Close() error {
	c.closed.Store(true)
	if c.Conn != nil {
		return c.Conn.Close()
	}
	return nil
}

func TestServerStartStop(t *testing.T) {
	b := broker.NewBroker(nil, nil, nil, nil, nil, nil, nil, config.SessionConfig{}, config.TransportConfig{})
	defer b.Close()

	cfg := Config{
		ShutdownTimeout: 1 * time.Second,
	}

	server := New(cfg, b)

	ctx, cancel := context.WithCancel(context.Background())
	connCtx, connCancel := context.WithCancel(context.Background())
	listener := newStubListener()

	server.mu.Lock()
	server.listener = listener
	server.mu.Unlock()

	acceptDone := server.runAcceptLoop(ctx, connCtx, listener)
	cancel()

	if err := server.gracefulShutdown(listener, acceptDone, connCancel); err != nil {
		t.Fatalf("unexpected shutdown error: %v", err)
	}
}

func TestShutdown(t *testing.T) {
	b := broker.NewBroker(nil, nil, nil, nil, nil, nil, nil, config.SessionConfig{}, config.TransportConfig{})
	defer b.Close()

	cfg := Config{
		ShutdownTimeout: 5 * time.Second,
	}

	server := New(cfg, b)

	ctx, cancel := context.WithCancel(context.Background())
	connCtx, connCancel := context.WithCancel(context.Background())
	listener := newStubListener()

	server.mu.Lock()
	server.listener = listener
	server.mu.Unlock()

	acceptDone := server.runAcceptLoop(ctx, connCtx, listener)

	serverConn, clientConn := net.Pipe()
	if err := listener.push(serverConn); err != nil {
		t.Fatalf("failed to push connection: %v", err)
	}
	clientConn.Close()

	cancel()

	// Server should stop gracefully
	if err := server.gracefulShutdown(listener, acceptDone, connCancel); err != nil {
		t.Fatalf("unexpected shutdown error: %v", err)
	}
}

func TestConnectionLimit(t *testing.T) {
	b := broker.NewBroker(nil, nil, nil, nil, nil, nil, nil, config.SessionConfig{}, config.TransportConfig{})
	defer b.Close()

	maxConns := 1
	cfg := Config{
		MaxConnections:  maxConns,
		ShutdownTimeout: 1 * time.Second,
	}

	server := New(cfg, b)

	ctx := context.Background()

	s1, c1 := net.Pipe()
	conn1 := &trackingConn{Conn: s1}
	if !server.tryAcquireConnectionSlot(ctx, conn1) {
		t.Fatal("expected first connection to be accepted")
	}

	s2, c2 := net.Pipe()
	conn2 := &trackingConn{Conn: s2}
	if server.tryAcquireConnectionSlot(ctx, conn2) {
		t.Fatal("expected second connection to be rejected")
	}
	if !conn2.closed.Load() {
		t.Fatal("expected rejected connection to be closed")
	}

	c1.Close()
	c2.Close()
	server.releaseConnectionSlot()
}

func TestConcurrentConnections(t *testing.T) {
	b := broker.NewBroker(nil, nil, nil, nil, nil, nil, nil, config.SessionConfig{}, config.TransportConfig{})
	defer b.Close()

	cfg := Config{
		ShutdownTimeout: 2 * time.Second,
	}

	server := New(cfg, b)

	ctx, cancel := context.WithCancel(context.Background())
	connCtx, connCancel := context.WithCancel(context.Background())
	listener := newStubListener()

	server.mu.Lock()
	server.listener = listener
	server.mu.Unlock()

	acceptDone := server.runAcceptLoop(ctx, connCtx, listener)

	// Create many concurrent connections
	numConns := 20
	var wg sync.WaitGroup
	wg.Add(numConns)

	for i := 0; i < numConns; i++ {
		go func() {
			defer wg.Done()
			serverConn, clientConn := net.Pipe()
			if err := listener.push(serverConn); err != nil {
				return
			}
			clientConn.Close()
		}()
	}

	wg.Wait()
	cancel()
	if err := server.gracefulShutdown(listener, acceptDone, connCancel); err != nil {
		t.Fatalf("unexpected shutdown error: %v", err)
	}
}

func TestDefaultConfigApplied(t *testing.T) {
	b := broker.NewBroker(nil, nil, nil, nil, nil, nil, nil, config.SessionConfig{}, config.TransportConfig{})
	defer b.Close()

	server := New(Config{}, b)

	if server.config.ShutdownTimeout == 0 {
		t.Fatal("expected default ShutdownTimeout to be set")
	}
	if server.config.ReadTimeout == 0 {
		t.Fatal("expected default ReadTimeout to be set")
	}
	if server.config.WriteTimeout == 0 {
		t.Fatal("expected default WriteTimeout to be set")
	}
	if server.config.IdleTimeout == 0 {
		t.Fatal("expected default IdleTimeout to be set")
	}
	if server.config.BufferSize == 0 {
		t.Fatal("expected default BufferSize to be set")
	}
	if server.config.TCPKeepAlive == 0 {
		t.Fatal("expected default TCPKeepAlive to be set")
	}
}
