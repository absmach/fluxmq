// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package tcp

import (
	"context"
	"io"
	"net"
	"sync"
	"testing"
	"time"
)

// mockHandler is a simple handler for testing.
type mockHandler struct {
	mu          sync.Mutex
	connections int
	onHandle    func(conn net.Conn)
}

func (h *mockHandler) HandleConnection(conn net.Conn) {
	h.mu.Lock()
	h.connections++
	h.mu.Unlock()

	if h.onHandle != nil {
		h.onHandle(conn)
	}
}

func (h *mockHandler) getConnections() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.connections
}

func TestServerStartStop(t *testing.T) {
	handler := &mockHandler{}
	cfg := Config{
		Address:         "localhost:0",
		ShutdownTimeout: 1 * time.Second,
	}

	server := New(cfg, handler)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start server in goroutine
	errCh := make(chan error, 1)
	go func() {
		errCh <- server.Listen(ctx)
	}()

	// Wait a bit for server to start
	time.Sleep(100 * time.Millisecond)

	// Verify server started
	if server.Addr() == nil {
		t.Fatal("server address is nil after start")
	}

	// Cancel context to stop server
	cancel()

	// Wait for server to stop
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("server did not stop in time")
	}
}

func TestGracefulShutdown(t *testing.T) {
	closed := make(chan struct{})
	handler := &mockHandler{
		onHandle: func(conn net.Conn) {
			defer conn.Close()
			// Simulate work
			buf := make([]byte, 1024)
			for {
				_, err := conn.Read(buf)
				if err != nil {
					close(closed)
					return
				}
			}
		},
	}

	cfg := Config{
		Address:         "localhost:0",
		ShutdownTimeout: 5 * time.Second,
	}

	server := New(cfg, handler)

	ctx, cancel := context.WithCancel(context.Background())

	// Start server
	errCh := make(chan error, 1)
	go func() {
		errCh <- server.Listen(ctx)
	}()

	time.Sleep(100 * time.Millisecond)

	// Connect a client
	conn, err := net.Dial("tcp", server.Addr().String())
	if err != nil {
		t.Fatalf("failed to connect: %v", err)
	}

	// Trigger shutdown
	cancel()

	// Close client connection
	conn.Close()

	// Wait for graceful shutdown
	select {
	case <-closed:
		// Connection was handled and closed gracefully
	case <-time.After(2 * time.Second):
		t.Fatal("connection did not close gracefully")
	}

	// Server should stop
	select {
	case err := <-errCh:
		if err != nil {
			t.Logf("shutdown completed with: %v", err)
		}
	case <-time.After(6 * time.Second):
		t.Fatal("server did not stop after shutdown timeout")
	}
}

func TestConnectionLimit(t *testing.T) {
	maxConns := 2
	handler := &mockHandler{
		onHandle: func(conn net.Conn) {
			defer conn.Close()
			// Keep connection open
			io.Copy(io.Discard, conn)
		},
	}

	cfg := Config{
		Address:         "localhost:0",
		MaxConnections:  maxConns,
		ShutdownTimeout: 1 * time.Second,
	}

	server := New(cfg, handler)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go server.Listen(ctx)
	time.Sleep(100 * time.Millisecond)

	// Create max connections
	conns := make([]net.Conn, maxConns)
	for i := 0; i < maxConns; i++ {
		conn, err := net.Dial("tcp", server.Addr().String())
		if err != nil {
			t.Fatalf("failed to connect %d: %v", i, err)
		}
		conns[i] = conn
	}

	// Wait for connections to be accepted
	time.Sleep(200 * time.Millisecond)

	// Try one more connection - should be rejected
	extraConn, err := net.DialTimeout("tcp", server.Addr().String(), 500*time.Millisecond)
	if err == nil {
		extraConn.Close()
		// Connection might get through briefly before being rejected
		// Wait and check if it stays open
		time.Sleep(100 * time.Millisecond)
	}

	// The extra connection should not be handled (or rejected quickly)
	// We can't easily test this without instrumenting the handler more

	// Clean up
	for _, conn := range conns {
		if conn != nil {
			conn.Close()
		}
	}
}

func TestConcurrentConnections(t *testing.T) {
	var mu sync.Mutex
	handled := 0
	handler := &mockHandler{
		onHandle: func(conn net.Conn) {
			defer conn.Close()
			mu.Lock()
			handled++
			mu.Unlock()
			// Do some work
			buf := make([]byte, 10)
			conn.Read(buf)
		},
	}

	cfg := Config{
		Address:         "localhost:0",
		ShutdownTimeout: 2 * time.Second,
	}

	server := New(cfg, handler)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go server.Listen(ctx)
	time.Sleep(100 * time.Millisecond)

	// Create many concurrent connections
	numConns := 50
	var wg sync.WaitGroup
	wg.Add(numConns)

	for i := 0; i < numConns; i++ {
		go func() {
			defer wg.Done()
			conn, err := net.Dial("tcp", server.Addr().String())
			if err != nil {
				return
			}
			conn.Write([]byte("test"))
			conn.Close()
		}()
	}

	wg.Wait()
	time.Sleep(500 * time.Millisecond)

	mu.Lock()
	count := handled
	mu.Unlock()

	if count != numConns {
		t.Errorf("expected %d connections handled, got %d", numConns, count)
	}
}

func TestTCPOptimizations(t *testing.T) {
	handler := &mockHandler{
		onHandle: func(conn net.Conn) {
			defer conn.Close()
			// Verify TCP options were set
			if tcpConn, ok := conn.(*net.TCPConn); ok {
				// We can't easily check if options were set, but we can verify it's a TCP conn
				_ = tcpConn
			}
		},
	}

	cfg := Config{
		Address:         "localhost:0",
		TCPKeepAlive:    15 * time.Second,
		DisableNoDelay:  false,
		ShutdownTimeout: 1 * time.Second,
	}

	server := New(cfg, handler)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go server.Listen(ctx)
	time.Sleep(100 * time.Millisecond)

	// Connect and test
	conn, err := net.Dial("tcp", server.Addr().String())
	if err != nil {
		t.Fatalf("failed to connect: %v", err)
	}
	conn.Close()

	time.Sleep(200 * time.Millisecond)

	if handler.getConnections() != 1 {
		t.Errorf("expected 1 connection, got %d", handler.getConnections())
	}
}
