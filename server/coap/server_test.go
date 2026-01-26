// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package coap

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/absmach/fluxmq/broker"
	"github.com/absmach/fluxmq/cluster"
	"github.com/absmach/fluxmq/storage/memory"
	piondtls "github.com/pion/dtls/v3"
)

func TestNew(t *testing.T) {
	store := memory.New()
	cl := cluster.NewNoopCluster("test-node")
	stats := broker.NewStats()
	b := broker.NewBroker(store, cl, slog.Default(), stats, nil, nil, nil)
	defer b.Close()

	cfg := Config{
		Address:         ":5683",
		ShutdownTimeout: 5 * time.Second,
	}

	server := New(cfg, b, nil)

	if server == nil {
		t.Fatal("Expected non-nil server")
	}
	if server.config.Address != ":5683" {
		t.Errorf("Expected address :5683, got %s", server.config.Address)
	}
	if server.broker == nil {
		t.Error("Expected broker to be set")
	}
	if server.mux == nil {
		t.Error("Expected mux router to be set")
	}
}

func TestConfig_TLSConfig(t *testing.T) {
	cfg := Config{
		Address:         ":5684",
		ShutdownTimeout: 5 * time.Second,
		TLSConfig: &piondtls.Config{
			ClientAuth: piondtls.RequireAndVerifyClientCert,
		},
	}

	if cfg.TLSConfig == nil {
		t.Fatal("Expected TLSConfig to be set")
	}
	if cfg.TLSConfig.ClientAuth != piondtls.RequireAndVerifyClientCert {
		t.Errorf("Expected RequireAndVerifyClientCert, got %v", cfg.TLSConfig.ClientAuth)
	}
}

func TestServer_ListenUDP_ContextCancel(t *testing.T) {
	store := memory.New()
	cl := cluster.NewNoopCluster("test-node")
	stats := broker.NewStats()
	b := broker.NewBroker(store, cl, slog.Default(), stats, nil, nil, nil)
	defer b.Close()

	cfg := Config{
		Address:         ":15683", // Use non-standard port for testing
		ShutdownTimeout: 1 * time.Second,
	}

	server := New(cfg, b, slog.Default())

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan error, 1)
	go func() {
		done <- server.Listen(ctx)
	}()

	// Give server time to start
	time.Sleep(100 * time.Millisecond)

	// Cancel context to trigger shutdown
	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Errorf("Expected nil error on shutdown, got: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Error("Server did not shut down in time")
	}
}

func TestServer_ListenDTLS_InvalidConfig(t *testing.T) {
	store := memory.New()
	cl := cluster.NewNoopCluster("test-node")
	stats := broker.NewStats()
	b := broker.NewBroker(store, cl, slog.Default(), stats, nil, nil, nil)
	defer b.Close()

	cfg := Config{
		Address:         ":15684",
		ShutdownTimeout: 1 * time.Second,
		TLSConfig:       &piondtls.Config{},
	}

	server := New(cfg, b, slog.Default())

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	err := server.Listen(ctx)
	if err == nil {
		t.Error("Expected error for invalid DTLS configuration")
	}
}
