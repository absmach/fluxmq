//go:build ignore

// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package archive

import (
	"context"
	"testing"
	"time"

	"github.com/absmach/fluxmq/queue"
	"github.com/absmach/fluxmq/queue/storage/memory"
	"github.com/absmach/fluxmq/queue/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRetentionPolicy(t *testing.T) {
	// Setup
	memStore := memory.New()
	manager, err := queue.NewManager(queue.Config{
		QueueStore:    memStore,
		MessageStore:  memStore,
		ConsumerStore: memStore,
		DeliverFn: func(ctx context.Context, clientID string, msg any) error {
			return nil
		},
	})
	require.NoError(t, err)

	ctx := context.Background()
	queueName := "$queue/retention-test"
	config := types.DefaultQueueConfig(queueName)
	config.MaxQueueDepth = 5                   // Small limit for testing
	config.MessageTTL = 100 * time.Millisecond // Short TTL

	err = manager.CreateQueue(ctx, config)
	require.NoError(t, err)

	t.Run("MaxQueueDepth Enforcement", func(t *testing.T) {
		// Fill queue to capacity
		for i := 0; i < 5; i++ {
			err := manager.Enqueue(ctx, queueName, []byte("payload"), nil)
			require.NoError(t, err, "failed to enqueue message %d", i)
		}

		// Verify queue is full
		stats, err := manager.GetStats(ctx, queueName)
		require.NoError(t, err)
		assert.Equal(t, int64(5), stats.TotalMessages)

		// Try to enqueue one more
		err = manager.Enqueue(ctx, queueName, []byte("overflow"), nil)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "queue is full")
	})
}

func TestMessageTTL(t *testing.T) {
	// Setup
	memStore := memory.New()
	manager, err := queue.NewManager(queue.Config{
		QueueStore:    memStore,
		MessageStore:  memStore,
		ConsumerStore: memStore,
		DeliverFn: func(ctx context.Context, clientID string, msg any) error {
			return nil
		},
	})
	require.NoError(t, err)

	ctx := context.Background()
	queueName := "$queue/ttl-test"
	config := types.DefaultQueueConfig(queueName)
	config.MessageTTL = 100 * time.Millisecond

	err = manager.CreateQueue(ctx, config)
	require.NoError(t, err)

	// Enqueue message
	err = manager.Enqueue(ctx, queueName, []byte("payload"), nil)
	require.NoError(t, err)

	// Retrieve message immediately (should exist)
	// We need to inspect underlying store to check ExpireAt
	// Or we can rely on BadgerDB behavior, but here we are using Memory store.
	// Memory store doesn't auto-expire, but we can check if ExpireAt was set correctly.

	// Access via internal store methods only exposed in package or via reflection/interface
	// But we can check via Dequeue if we modify memory store to respect TTL?
	// The implementation plan mainly relied on BadgerDB TTL.
	// However, we modified Manager.Enqueue to set ExpireAt.
	// We can verify ExpireAt is set.

	// For Memory store, we can use ListQueued to inspect the message
	// Note: manager.GetQueue is available but not needed since we query memStore directly

	// Since we use random partition if no key, might be in any partition.
	// But DefaultQueueConfig uses 10 partitions.
	// Let's find it.
	var found *types.Message
	for i := 0; i < 10; i++ {
		msgs, err := memStore.ListQueued(ctx, queueName, i, 1)
		require.NoError(t, err)

		if len(msgs) > 0 {
			found = msgs[0]
			break
		}
	}

	require.NotNil(t, found, "message should be found in storage")
	assert.False(t, found.ExpiresAt.IsZero(), "ExpireAt should be set")
	assert.WithinDuration(t, time.Now().Add(100*time.Millisecond), found.ExpiresAt, 50*time.Millisecond)
}
