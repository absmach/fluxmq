// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package broker

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/absmach/fluxmq/cluster"
	"github.com/absmach/fluxmq/mqtt/session"
	"github.com/absmach/fluxmq/storage/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBroker_Shutdown_NoSessions(t *testing.T) {
	store := memory.New()
	cl := cluster.NewNoopCluster("node-1")
	logger := slog.Default()
	stats := NewStats()

	broker := NewBroker(store, cl, WithLogger(logger), WithStats(stats))

	ctx := context.Background()
	err := broker.Shutdown(ctx, 5*time.Second)
	assert.NoError(t, err)
}

func TestBroker_Shutdown_ImmediateWithNoSessions(t *testing.T) {
	store := memory.New()
	cl := cluster.NewNoopCluster("node-1")
	logger := slog.Default()
	stats := NewStats()

	broker := NewBroker(store, cl, WithLogger(logger), WithStats(stats))

	ctx := context.Background()
	start := time.Now()
	err := broker.Shutdown(ctx, 5*time.Second)
	elapsed := time.Since(start)

	assert.NoError(t, err)
	// Should complete immediately (no sessions to drain)
	assert.Less(t, elapsed, 2*time.Second)
	assert.Equal(t, 0, broker.sessionsMap.Count())
}

func TestBroker_Shutdown_ContextCancelled(t *testing.T) {
	store := memory.New()
	cl := cluster.NewNoopCluster("node-1")
	logger := slog.Default()
	stats := NewStats()

	broker := NewBroker(store, cl, WithLogger(logger), WithStats(stats))

	ctx, cancel := context.WithCancel(context.Background())

	// Cancel context immediately
	cancel()

	start := time.Now()
	err := broker.Shutdown(ctx, 10*time.Second)
	elapsed := time.Since(start)

	assert.NoError(t, err)
	// Should cancel quickly
	assert.Less(t, elapsed, 1*time.Second)
}

func TestBroker_Close_Idempotent(t *testing.T) {
	store := memory.New()
	cl := cluster.NewNoopCluster("node-1")
	logger := slog.Default()
	stats := NewStats()

	broker := NewBroker(store, cl, WithLogger(logger), WithStats(stats))

	// First close should succeed
	err := broker.Close()
	require.NoError(t, err)

	// Second close should not panic
	err = broker.Close()
	assert.NoError(t, err)
}

func TestBroker_Shutdown_SetsFlag(t *testing.T) {
	store := memory.New()
	cl := cluster.NewNoopCluster("node-1")
	logger := slog.Default()
	stats := NewStats()

	broker := NewBroker(store, cl, WithLogger(logger), WithStats(stats))

	// Initially not shutting down
	assert.False(t, broker.shuttingDown.Load())

	// Start shutdown in background
	go func() {
		broker.Shutdown(context.Background(), 100*time.Millisecond) //nolint:errcheck // best-effort
	}()

	// Wait a bit for shutdown to start
	time.Sleep(50 * time.Millisecond)

	// Flag should be set
	shuttingDown := broker.shuttingDown.Load()

	assert.True(t, shuttingDown)
}

func TestBroker_Shutdown_DoesNotWaitForSessionsWithoutClients(t *testing.T) {
	broker := NewBroker(memory.New(), cluster.NewNoopCluster("node-1"), WithLogger(slog.Default()))

	// A persistent session whose client has gone stays in the session map
	// until it expires, long after any drain timeout.
	_, _, err := broker.CreateSession("offline", 4, session.Options{ExpiryInterval: 3600})
	require.NoError(t, err)
	require.Equal(t, 1, broker.sessionsMap.Count())

	start := time.Now()
	require.NoError(t, broker.Shutdown(context.Background(), 30*time.Second))
	assert.Less(t, time.Since(start), 3*time.Second)
}

func TestBroker_Shutdown_WaitsForConnectedClients(t *testing.T) {
	broker := NewBroker(memory.New(), cluster.NewNoopCluster("node-1"), WithLogger(slog.Default()))

	s, _, err := broker.CreateSession("online", 4, session.Options{ExpiryInterval: 3600})
	require.NoError(t, err)
	_, err = s.Connect(&captureConnection{})
	require.NoError(t, err)

	done := make(chan error, 1)
	start := time.Now()
	go func() { done <- broker.Shutdown(context.Background(), 30*time.Second) }()

	// Shutdown polls once a second; it must still be waiting after two polls.
	require.Never(t, func() bool { return len(done) > 0 }, 2500*time.Millisecond, 50*time.Millisecond)

	require.NoError(t, s.Disconnect(true, 0))
	select {
	case err := <-done:
		require.NoError(t, err)
		assert.Less(t, time.Since(start), 10*time.Second)
	case <-time.After(5 * time.Second):
		t.Fatal("Shutdown kept waiting after the last client disconnected")
	}
}
