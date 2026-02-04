// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package tcp

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"log/slog"
	"net"
	"testing"
	"time"

	"github.com/absmach/fluxmq/config"
	"github.com/absmach/fluxmq/mqtt/broker"
	v3 "github.com/absmach/fluxmq/mqtt/packets/v3"
)

func newTestBroker(t *testing.T) *broker.Broker {
	t.Helper()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	b := broker.NewBroker(nil, nil, logger, nil, nil, nil, nil, config.SessionConfig{})
	t.Cleanup(func() { _ = b.Close() })
	return b
}

func runHandleConnection(s *Server, conn net.Conn) {
	s.wg.Add(1)
	go s.handleConnection(context.Background(), conn)
}

func waitForConnections(t *testing.T, s *Server) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("connection handler did not exit in time")
	}
}

func mqttHandshake(t *testing.T, conn net.Conn) {
	t.Helper()
	connectPkt := &v3.Connect{
		FixedHeader:     v3.FixedHeader{PacketType: v3.ConnectType},
		ProtocolName:    "MQTT",
		ProtocolVersion: 4,
		CleanSession:    true,
		ClientID:        "tls-test-client",
	}

	if err := connectPkt.Pack(conn); err != nil {
		t.Fatalf("failed to send CONNECT: %v", err)
	}

	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	connack, err := v3.ReadPacket(conn)
	if err != nil {
		t.Fatalf("failed to read CONNACK: %v", err)
	}
	if connack.Type() != v3.ConnAckType {
		t.Fatalf("expected CONNACK, got %v", connack.Type())
	}

	disconnectPkt := &v3.Disconnect{
		FixedHeader: v3.FixedHeader{PacketType: v3.DisconnectType},
	}
	_ = disconnectPkt.Pack(conn)
}

func tlsHandshakeWithTimeout(conn *tls.Conn, timeout time.Duration) error {
	errCh := make(chan error, 1)
	go func() {
		errCh <- conn.Handshake()
	}()
	select {
	case err := <-errCh:
		return err
	case <-time.After(timeout):
		return errors.New("handshake timeout")
	}
}

func TestTLS_BasicConnection(t *testing.T) {
	certs := GenerateTestCerts(t)
	serverTLS := LoadServerTLSConfig(t, certs, tls.NoClientCert)
	clientTLS := LoadClientTLSConfig(t, certs, false)
	clientTLS.ServerName = "localhost"

	b := newTestBroker(t)
	server := New(Config{TLSConfig: serverTLS}, b)

	serverConn, clientConn := net.Pipe()
	tlsServer := tls.Server(serverConn, serverTLS)
	tlsClient := tls.Client(clientConn, clientTLS)

	runHandleConnection(server, tlsServer)

	if err := tlsHandshakeWithTimeout(tlsClient, 2*time.Second); err != nil {
		t.Fatalf("TLS handshake failed: %v", err)
	}
	mqttHandshake(t, tlsClient)
	_ = tlsClient.Close()
	waitForConnections(t, server)
}

func TestTLS_RequireClientCert(t *testing.T) {
	certs := GenerateTestCerts(t)
	serverTLS := LoadServerTLSConfig(t, certs, tls.RequireAndVerifyClientCert)

	t.Run("NoClientCert", func(t *testing.T) {
		b := newTestBroker(t)
		server := New(Config{TLSConfig: serverTLS}, b)
		clientTLS := LoadClientTLSConfig(t, certs, false)
		clientTLS.ServerName = "localhost"
		serverConn, clientConn := net.Pipe()
		tlsServer := tls.Server(serverConn, serverTLS)
		tlsClient := tls.Client(clientConn, clientTLS)

		runHandleConnection(server, tlsServer)

		err := tlsHandshakeWithTimeout(tlsClient, 2*time.Second)
		if err == nil {
			connectPkt := &v3.Connect{
				FixedHeader:     v3.FixedHeader{PacketType: v3.ConnectType},
				ProtocolName:    "MQTT",
				ProtocolVersion: 4,
				CleanSession:    true,
				ClientID:        "no-cert-client",
			}
			err = connectPkt.Pack(tlsClient)
			if err == nil {
				tlsClient.SetReadDeadline(time.Now().Add(1 * time.Second))
				_, err = v3.ReadPacket(tlsClient)
			}
		}
		if err == nil {
			t.Fatal("expected connection to be rejected without client cert")
		}
		_ = tlsClient.Close()
		waitForConnections(t, server)
	})

	t.Run("WithClientCert", func(t *testing.T) {
		b := newTestBroker(t)
		server := New(Config{TLSConfig: serverTLS}, b)
		clientTLS := LoadClientTLSConfig(t, certs, true)
		clientTLS.ServerName = "localhost"
		serverConn, clientConn := net.Pipe()
		tlsServer := tls.Server(serverConn, serverTLS)
		tlsClient := tls.Client(clientConn, clientTLS)

		runHandleConnection(server, tlsServer)

		if err := tlsHandshakeWithTimeout(tlsClient, 2*time.Second); err != nil {
			t.Fatalf("TLS handshake failed: %v", err)
		}
		mqttHandshake(t, tlsClient)
		_ = tlsClient.Close()
		waitForConnections(t, server)
	})
}

func TestTLS_UntrustedServer(t *testing.T) {
	certs := GenerateTestCerts(t)
	serverTLS := LoadServerTLSConfig(t, certs, tls.NoClientCert)

	b := newTestBroker(t)
	server := New(Config{TLSConfig: serverTLS}, b)

	serverConn, clientConn := net.Pipe()
	tlsServer := tls.Server(serverConn, serverTLS)
	tlsClient := tls.Client(clientConn, &tls.Config{InsecureSkipVerify: false, ServerName: "localhost"})

	runHandleConnection(server, tlsServer)

	if err := tlsHandshakeWithTimeout(tlsClient, 2*time.Second); err == nil {
		t.Fatal("expected TLS handshake to fail for untrusted server cert")
	}
	_ = tlsClient.Close()
	waitForConnections(t, server)
}

func TestTLS_MinVersionConfig(t *testing.T) {
	certs := GenerateTestCerts(t)
	serverTLS := LoadServerTLSConfig(t, certs, tls.NoClientCert)
	if serverTLS.MinVersion != tls.VersionTLS12 {
		t.Fatalf("expected MinVersion to be TLS 1.2, got %v", serverTLS.MinVersion)
	}
}

func TestTLS_NoTLS(t *testing.T) {
	b := newTestBroker(t)
	server := New(Config{}, b)

	serverConn, clientConn := net.Pipe()
	runHandleConnection(server, serverConn)

	mqttHandshake(t, clientConn)
	_ = clientConn.Close()
	waitForConnections(t, server)
}
