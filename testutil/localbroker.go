// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package testutil

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"testing"
	"time"

	"github.com/absmach/fluxmq/cluster"
	"github.com/absmach/fluxmq/config"
	"github.com/absmach/fluxmq/mqtt/broker"
	"github.com/absmach/fluxmq/server/tcp"
	"github.com/absmach/fluxmq/server/websocket"
	"github.com/absmach/fluxmq/storage/memory"
)

// LocalBroker is a single-node MQTT broker served over TCP and WebSocket on
// loopback ports, for tests that talk to it the way real clients do.
type LocalBroker struct {
	// TCPAddr serves MQTT 3.1.1 and 5.0 over TCP, detecting the version.
	TCPAddr string
	// WSAddr serves MQTT over WebSocket at WSPath, detecting the version.
	WSAddr string
	WSPath string
	Broker *broker.Broker
}

// StartLocalBroker starts a broker configured as the shipped defaults
// configure one, adjusted by tune, and stops it when the test ends.
func StartLocalBroker(t testing.TB, tune ...func(*config.Config)) *LocalBroker {
	t.Helper()

	cfg := config.Default()
	for _, fn := range tune {
		fn(cfg)
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	b := broker.NewBroker(memory.New(), cluster.NewNoopCluster("local"),
		broker.WithLogger(logger),
		broker.WithSessionConfig(cfg.Session),
		broker.WithBrokerConfig(cfg.Broker),
	)

	lb := &LocalBroker{
		TCPAddr: freeLoopbackAddr(t),
		WSAddr:  freeLoopbackAddr(t),
		WSPath:  "/mqtt",
		Broker:  b,
	}

	tcpListener := cfg.Server.MQTT.TCP.V3
	tcpServer := tcp.New(tcp.Config{
		Address:          lb.TCPAddr,
		Logger:           logger,
		ReadTimeout:      tcpListener.ReadTimeout,
		WriteTimeout:     tcpListener.WriteTimeout,
		SendQueueSize:    cfg.Session.MaxSendQueueSize,
		DisconnectOnFull: cfg.Session.DisconnectOnFull,
		ShutdownTimeout:  time.Second,
	}, b)
	wsServer := websocket.New(websocket.Config{
		Address:         lb.WSAddr,
		Path:            lb.WSPath,
		ReadTimeout:     tcpListener.ReadTimeout,
		WriteTimeout:    tcpListener.WriteTimeout,
		ShutdownTimeout: time.Second,
	}, b, logger)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{}, 2)
	for _, listen := range []func(context.Context) error{tcpServer.Listen, wsServer.Listen} {
		go func() {
			defer func() { done <- struct{}{} }()
			if err := listen(ctx); err != nil && !errors.Is(err, context.Canceled) {
				t.Logf("local broker listener: %v", err)
			}
		}()
	}
	t.Cleanup(func() {
		cancel()
		<-done
		<-done
		_ = b.Close()
	})

	for _, addr := range []string{lb.TCPAddr, lb.WSAddr} {
		waitListening(t, addr)
	}
	return lb
}

func freeLoopbackAddr(t testing.TB) string {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve port: %v", err)
	}
	addr := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatalf("release port: %v", err)
	}
	return addr
}

func waitListening(t testing.TB, addr string) {
	t.Helper()

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", addr, 100*time.Millisecond)
		if err == nil {
			_ = conn.Close()
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("%s is not listening", addr)
}
