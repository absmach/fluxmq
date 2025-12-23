// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package broker

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/absmach/mqtt/cluster"
	"github.com/absmach/mqtt/storage/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBroker_Shutdown_NoSessions(t *testing.T) {
	store := memory.New()
	cl := cluster.NewNoopCluster("node-1")
	logger := slog.Default()
	stats := NewStats()

	broker := NewBroker(store, cl, logger, stats, nil)

	ctx := context.Background()
	err := broker.Shutdown(ctx, 5*time.Second)
	assert.NoError(t, err)
}

func TestBroker_Shutdown_ImmediateWithNoSessions(t *testing.T) {
	store := memory.New()
	cl := cluster.NewNoopCluster("node-1")
	logger := slog.Default()
	stats := NewStats()

	broker := NewBroker(store, cl, logger, stats, nil)

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

	broker := NewBroker(store, cl, logger, stats, nil)

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

	broker := NewBroker(store, cl, logger, stats, nil)

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

	broker := NewBroker(store, cl, logger, stats, nil)

	// Initially not shutting down
	assert.False(t, broker.shuttingDown)

	// Start shutdown in background
	go func() {
		broker.Shutdown(context.Background(), 100*time.Millisecond)
	}()

	// Wait a bit for shutdown to start
	time.Sleep(50 * time.Millisecond)

	// Flag should be set
	broker.mu.RLock()
	shuttingDown := broker.shuttingDown
	broker.mu.RUnlock()

	assert.True(t, shuttingDown)
}
