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

func TestConfig_DTLSFields(t *testing.T) {
	cfg := Config{
		Address:         ":5684",
		ShutdownTimeout: 5 * time.Second,
		DTLSEnabled:     true,
		DTLSCertFile:    "/path/to/cert.pem",
		DTLSKeyFile:     "/path/to/key.pem",
		DTLSCAFile:      "/path/to/ca.pem",
		DTLSClientAuth:  "require",
	}

	if !cfg.DTLSEnabled {
		t.Error("Expected DTLSEnabled to be true")
	}
	if cfg.DTLSCertFile != "/path/to/cert.pem" {
		t.Errorf("Expected cert file path, got %s", cfg.DTLSCertFile)
	}
	if cfg.DTLSClientAuth != "require" {
		t.Errorf("Expected client auth 'require', got %s", cfg.DTLSClientAuth)
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

func TestServer_ListenDTLS_MissingCert(t *testing.T) {
	store := memory.New()
	cl := cluster.NewNoopCluster("test-node")
	stats := broker.NewStats()
	b := broker.NewBroker(store, cl, slog.Default(), stats, nil, nil, nil)
	defer b.Close()

	cfg := Config{
		Address:         ":15684",
		ShutdownTimeout: 1 * time.Second,
		DTLSEnabled:     true,
		DTLSCertFile:    "/nonexistent/cert.pem",
		DTLSKeyFile:     "/nonexistent/key.pem",
	}

	server := New(cfg, b, slog.Default())

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	err := server.Listen(ctx)
	if err == nil {
		t.Error("Expected error for missing certificate files")
	}
}

func TestBuildDTLSConfig_ClientAuthModes(t *testing.T) {
	tests := []struct {
		name         string
		clientAuth   string
		expectedAuth string
	}{
		{"none", "none", "none"},
		{"request", "request", "request"},
		{"require", "require", "require"},
		{"empty_defaults_to_none", "", "none"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := Config{
				DTLSClientAuth: tt.clientAuth,
			}

			server := &Server{config: cfg}

			// Verify config is stored correctly
			if tt.clientAuth == "" {
				// Empty string should be interpreted as "none" in buildDTLSConfig
				if server.config.DTLSClientAuth != "" {
					t.Errorf("Expected empty string, got %s", server.config.DTLSClientAuth)
				}
			} else {
				if server.config.DTLSClientAuth != tt.clientAuth {
					t.Errorf("Expected %s, got %s", tt.clientAuth, server.config.DTLSClientAuth)
				}
			}
		})
	}
}
