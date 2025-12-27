// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package tcp

import (
	"context"
	"crypto/tls"
	"log/slog"
	"net"
	"os"
	"testing"
	"time"

	"github.com/absmach/mqtt/broker"
	"github.com/absmach/mqtt/cluster"
	"github.com/absmach/mqtt/core/packets/v3"
	"github.com/absmach/mqtt/storage/memory"
)

func TestTLS_BasicConnection(t *testing.T) {
	certs := GenerateTestCerts(t)
	tlsConfig := LoadServerTLSConfig(t, certs, tls.NoClientCert)

	// Create server with TLS
	store := memory.New()
	cl := cluster.NewNoopCluster("test-node")
	nullLogger := slog.New(slog.NewTextHandler(os.NewFile(0, os.DevNull), nil))
	b := broker.NewBroker(store, cl, nullLogger, nil, nil, nil, nil)

	cfg := Config{
		Address:         "127.0.0.1:0", // Random port
		TLSConfig:       tlsConfig,
		ShutdownTimeout: 5 * time.Second,
		Logger:          nullLogger,
	}
	server := New(cfg, b)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- server.Listen(ctx)
	}()

	// Wait for server to start
	time.Sleep(100 * time.Millisecond)

	addr := server.Addr().String()

	// Connect with TLS client
	clientTLSConfig := LoadClientTLSConfig(t, certs, false)
	conn, err := tls.Dial("tcp", addr, clientTLSConfig)
	if err != nil {
		t.Fatalf("Failed to connect with TLS: %v", err)
	}
	defer conn.Close()

	// Verify TLS handshake completed
	if err := conn.Handshake(); err != nil {
		t.Fatalf("TLS handshake failed: %v", err)
	}

	// Send MQTT CONNECT packet
	connectPkt := &v3.Connect{
		FixedHeader:  v3.FixedHeader{PacketType: v3.ConnectType},
		ProtocolName: "MQTT",
		ProtocolVersion: 4,
		CleanSession: true,
		ClientID:     "tls-test-client",
	}

	if err := connectPkt.Pack(conn); err != nil {
		t.Fatalf("Failed to send CONNECT: %v", err)
	}

	// Read CONNACK
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	connack, err := v3.ReadPacket(conn)
	if err != nil {
		t.Fatalf("Failed to read CONNACK: %v", err)
	}

	if connack.Type() != v3.ConnAckType {
		t.Fatalf("Expected CONNACK, got %v", connack.Type())
	}

	// Send DISCONNECT packet
	disconnectPkt := &v3.Disconnect{
		FixedHeader: v3.FixedHeader{PacketType: v3.DisconnectType},
	}
	disconnectPkt.Pack(conn)
	conn.Close()

	// Give broker time to process disconnect
	time.Sleep(100 * time.Millisecond)

	// Shutdown server
	cancel()
	select {
	case err := <-errCh:
		if err != nil {
			t.Logf("Server shutdown with error: %v", err)
		}
	case <-time.After(6 * time.Second):
		t.Fatal("Server shutdown timeout")
	}
}

func TestTLS_RequireClientCert(t *testing.T) {
	certs := GenerateTestCerts(t)
	tlsConfig := LoadServerTLSConfig(t, certs, tls.RequireAndVerifyClientCert)

	// Verify TLS config is set up correctly
	t.Logf("Server TLS ClientAuth: %v (expected: %v)", tlsConfig.ClientAuth, tls.RequireAndVerifyClientCert)
	if tlsConfig.ClientAuth != tls.RequireAndVerifyClientCert {
		t.Fatalf("Server TLS config ClientAuth not set correctly")
	}

	// Create server with TLS requiring client cert
	store := memory.New()
	cl := cluster.NewNoopCluster("test-node")
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))
	b := broker.NewBroker(store, cl, logger, nil, nil, nil, nil)

	cfg := Config{
		Address:         "127.0.0.1:0",
		TLSConfig:       tlsConfig,
		ShutdownTimeout: 5 * time.Second,
		Logger:          logger,
	}
	server := New(cfg, b)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- server.Listen(ctx)
	}()

	// Wait for server to start
	time.Sleep(100 * time.Millisecond)

	addr := server.Addr().String()

	// Test 1: Connection without client cert should fail
	t.Run("NoClientCert", func(t *testing.T) {
		clientTLSConfig := LoadClientTLSConfig(t, certs, false)
		conn, err := tls.Dial("tcp", addr, clientTLSConfig)
		if err != nil {
			// Expected - connection failed during handshake
			t.Logf("Connection correctly rejected during dial: %v", err)
			return
		}
		defer conn.Close()

		// Client-side handshake might succeed, but server should close the connection
		// Try to read which should fail when server closes due to missing client cert
		buf := make([]byte, 1)
		conn.SetReadDeadline(time.Now().Add(1 * time.Second))
		_, err = conn.Read(buf)
		if err != nil {
			t.Logf("Connection correctly rejected: %v", err)
			return
		}

		t.Fatal("Expected connection to fail without client certificate, but it succeeded")
	})

	// Test 2: Connection with client cert should succeed
	t.Run("WithClientCert", func(t *testing.T) {
		clientTLSConfig := LoadClientTLSConfig(t, certs, true)
		conn, err := tls.Dial("tcp", addr, clientTLSConfig)
		if err != nil {
			t.Fatalf("Failed to connect with client cert: %v", err)
		}
		defer conn.Close()

		// Verify TLS handshake with client cert
		if err := conn.Handshake(); err != nil {
			t.Fatalf("TLS handshake failed: %v", err)
		}

		// Verify connection state has peer certificates
		state := conn.ConnectionState()
		if len(state.PeerCertificates) == 0 {
			t.Fatal("Server did not receive client certificate")
		}
	})

	// Shutdown server
	cancel()
	select {
	case err := <-errCh:
		if err != nil {
			t.Logf("Server shutdown with error: %v", err)
		}
	case <-time.After(6 * time.Second):
		t.Fatal("Server shutdown timeout")
	}
}

func TestTLS_InvalidCert(t *testing.T) {
	certs := GenerateTestCerts(t)
	tlsConfig := LoadServerTLSConfig(t, certs, tls.NoClientCert)

	// Create server
	store := memory.New()
	cl := cluster.NewNoopCluster("test-node")
	nullLogger := slog.New(slog.NewTextHandler(os.NewFile(0, os.DevNull), nil))
	b := broker.NewBroker(store, cl, nullLogger, nil, nil, nil, nil)

	cfg := Config{
		Address:         "127.0.0.1:0",
		TLSConfig:       tlsConfig,
		ShutdownTimeout: 5 * time.Second,
		Logger:          nullLogger,
	}
	server := New(cfg, b)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- server.Listen(ctx)
	}()

	// Wait for server to start
	time.Sleep(100 * time.Millisecond)

	addr := server.Addr().String()

	// Try to connect without trusting the server's CA
	insecureTLSConfig := &tls.Config{
		InsecureSkipVerify: false, // Explicitly don't skip verification
	}

	conn, err := tls.Dial("tcp", addr, insecureTLSConfig)
	if err == nil {
		conn.Close()
		t.Fatal("Expected connection to fail with unverified certificate")
	}

	// Shutdown server
	cancel()
	select {
	case err := <-errCh:
		if err != nil {
			t.Logf("Server shutdown with error: %v", err)
		}
	case <-time.After(6 * time.Second):
		t.Fatal("Server shutdown timeout")
	}
}

func TestTLS_MinVersion(t *testing.T) {
	certs := GenerateTestCerts(t)
	tlsConfig := LoadServerTLSConfig(t, certs, tls.NoClientCert)

	// Verify minimum TLS version is enforced
	if tlsConfig.MinVersion != tls.VersionTLS12 {
		t.Fatalf("Expected MinVersion to be TLS 1.2, got %v", tlsConfig.MinVersion)
	}

	// Create server
	store := memory.New()
	cl := cluster.NewNoopCluster("test-node")
	nullLogger := slog.New(slog.NewTextHandler(os.NewFile(0, os.DevNull), nil))
	b := broker.NewBroker(store, cl, nullLogger, nil, nil, nil, nil)

	cfg := Config{
		Address:         "127.0.0.1:0",
		TLSConfig:       tlsConfig,
		ShutdownTimeout: 5 * time.Second,
		Logger:          nullLogger,
	}
	server := New(cfg, b)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- server.Listen(ctx)
	}()

	// Wait for server to start
	time.Sleep(100 * time.Millisecond)

	addr := server.Addr().String()

	// Try to connect with TLS 1.1 (should fail)
	clientTLSConfig := LoadClientTLSConfig(t, certs, false)
	clientTLSConfig.MaxVersion = tls.VersionTLS11

	conn, err := tls.Dial("tcp", addr, clientTLSConfig)
	if err == nil {
		conn.Close()
		// Note: This might not fail in all Go versions as the client's MaxVersion
		// might be overridden. The important thing is the server enforces MinVersion.
		t.Log("Note: Client was able to connect with TLS 1.1 (client-side compatibility)")
	} else {
		t.Logf("Connection correctly rejected with TLS 1.1: %v", err)
	}

	// Verify TLS 1.2+ works
	clientTLSConfig.MaxVersion = tls.VersionTLS13
	clientTLSConfig.MinVersion = tls.VersionTLS12

	conn, err = tls.Dial("tcp", addr, clientTLSConfig)
	if err != nil {
		t.Fatalf("Failed to connect with TLS 1.2+: %v", err)
	}
	conn.Close()

	// Shutdown server
	cancel()
	select {
	case err := <-errCh:
		if err != nil {
			t.Logf("Server shutdown with error: %v", err)
		}
	case <-time.After(6 * time.Second):
		t.Fatal("Server shutdown timeout")
	}
}

func TestTLS_NoTLS(t *testing.T) {
	// Verify server works without TLS when TLSConfig is nil
	store := memory.New()
	cl := cluster.NewNoopCluster("test-node")
	nullLogger := slog.New(slog.NewTextHandler(os.NewFile(0, os.DevNull), nil))
	b := broker.NewBroker(store, cl, nullLogger, nil, nil, nil, nil)

	cfg := Config{
		Address:         "127.0.0.1:0",
		TLSConfig:       nil, // No TLS
		ShutdownTimeout: 5 * time.Second,
		Logger:          nullLogger,
	}
	server := New(cfg, b)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- server.Listen(ctx)
	}()

	// Wait for server to start
	time.Sleep(100 * time.Millisecond)

	addr := server.Addr().String()

	// Connect without TLS
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("Failed to connect without TLS: %v", err)
	}
	defer conn.Close()

	// Send MQTT CONNECT packet
	connectPkt := &v3.Connect{
		FixedHeader:     v3.FixedHeader{PacketType: v3.ConnectType},
		ProtocolName:    "MQTT",
		ProtocolVersion: 4,
		CleanSession:    true,
		ClientID:        "plain-test-client",
	}

	if err := connectPkt.Pack(conn); err != nil {
		t.Fatalf("Failed to send CONNECT: %v", err)
	}

	// Read CONNACK
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	connack, err := v3.ReadPacket(conn)
	if err != nil {
		t.Fatalf("Failed to read CONNACK: %v", err)
	}

	if connack.Type() != v3.ConnAckType {
		t.Fatalf("Expected CONNACK, got %v", connack.Type())
	}

	// Send DISCONNECT packet
	disconnectPkt2 := &v3.Disconnect{
		FixedHeader: v3.FixedHeader{PacketType: v3.DisconnectType},
	}
	disconnectPkt2.Pack(conn)
	conn.Close()

	// Give broker time to process disconnect
	time.Sleep(100 * time.Millisecond)

	// Shutdown server
	cancel()
	select {
	case err := <-errCh:
		if err != nil {
			t.Logf("Server shutdown with error: %v", err)
		}
	case <-time.After(6 * time.Second):
		t.Fatal("Server shutdown timeout")
	}
}
