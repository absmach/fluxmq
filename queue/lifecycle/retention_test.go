// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package lifecycle

import (
	"context"
	"fmt"
	"log/slog"
	"testing"
	"time"

	"github.com/absmach/fluxmq/queue/storage/memory"
	"github.com/absmach/fluxmq/queue/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRetentionManager_SizeBasedRetention_Messages(t *testing.T) {
	ctx := context.Background()
	store := memory.New()
	queueName := "$queue/test-retention"

	// Create queue
	config := types.DefaultQueueConfig(queueName)
	err := store.CreateQueue(ctx, config)
	require.NoError(t, err)

	// Configure retention: max 5 messages
	policy := types.RetentionPolicy{
		RetentionMessages: 5,
		SizeCheckEvery:    1, // Check on every enqueue for testing
	}

	rm := NewRetentionManager(queueName, policy, store, nil, slog.Default())

	// Enqueue 10 messages
	for i := 0; i < 10; i++ {
		msg := &types.Message{
			ID:        fmt.Sprintf("msg-%d", i),
			Topic:     queueName,
			Payload:   []byte("test message"),
			Sequence:  uint64(i),
			State:     types.StateQueued,
			CreatedAt: time.Now(),
		}
		err := store.Enqueue(ctx, queueName, msg)
		require.NoError(t, err)
	}

	// Verify we have 10 messages
	count, err := store.Count(ctx, queueName)
	require.NoError(t, err)
	assert.Equal(t, int64(10), count)

	// Trigger size-based retention
	deletedCount, bytesFreed, err := rm.CheckSizeRetention(ctx, 0)
	require.NoError(t, err)

	// Should delete 5 oldest messages (10 - 5 = 5 deleted)
	assert.Equal(t, int64(5), deletedCount)
	assert.Greater(t, bytesFreed, int64(0))

	// Verify we now have 5 messages
	count, err = store.Count(ctx, queueName)
	require.NoError(t, err)
	assert.Equal(t, int64(5), count)

	// Verify the oldest messages were deleted (sequences 0-4 should be gone)
	messages, err := store.ListOldestMessages(ctx, queueName, 0, 10)
	require.NoError(t, err)
	assert.Len(t, messages, 5)
	assert.Equal(t, uint64(5), messages[0].Sequence) // Oldest remaining is seq 5
}

func TestRetentionManager_SizeBasedRetention_Bytes(t *testing.T) {
	ctx := context.Background()
	store := memory.New()
	queueName := "$queue/test-retention-bytes"

	// Create queue
	config := types.DefaultQueueConfig(queueName)
	err := store.CreateQueue(ctx, config)
	require.NoError(t, err)

	// Configure retention: max 100 bytes
	policy := types.RetentionPolicy{
		RetentionBytes: 100,
		SizeCheckEvery: 1, // Check on every enqueue for testing
	}

	rm := NewRetentionManager(queueName, policy, store, nil, slog.Default())

	// Enqueue messages with 30-byte payloads (total: 150 bytes)
	for i := 0; i < 5; i++ {
		msg := &types.Message{
			ID:        fmt.Sprintf("msg-%d", i),
			Topic:     queueName,
			Payload:   make([]byte, 30), // 30 bytes each
			Sequence:  uint64(i),
			State:     types.StateQueued,
			CreatedAt: time.Now(),
		}
		err := store.Enqueue(ctx, queueName, msg)
		require.NoError(t, err)
	}

	// Verify we have 5 messages (150 bytes)
	count, err := store.Count(ctx, queueName)
	require.NoError(t, err)
	assert.Equal(t, int64(5), count)

	size, err := store.GetQueueSize(ctx, queueName)
	require.NoError(t, err)
	assert.Equal(t, int64(150), size)

	// Trigger size-based retention
	deletedCount, bytesFreed, err := rm.CheckSizeRetention(ctx, 0)
	require.NoError(t, err)

	// Should delete at least 2 messages to get under 100 bytes
	assert.GreaterOrEqual(t, deletedCount, int64(2))
	assert.Equal(t, bytesFreed, deletedCount*30)

	// Verify size is now under 100 bytes
	size, err = store.GetQueueSize(ctx, queueName)
	require.NoError(t, err)
	assert.LessOrEqual(t, size, int64(100))
}

func TestRetentionManager_TimeBasedRetention(t *testing.T) {
	ctx := context.Background()
	store := memory.New()
	queueName := "$queue/test-time-retention"

	// Create queue
	config := types.DefaultQueueConfig(queueName)
	err := store.CreateQueue(ctx, config)
	require.NoError(t, err)

	// Configure retention: 200ms time retention
	// This ensures new messages (created during test) won't become eligible for deletion
	// while old messages (400ms ago) will definitely be deleted
	policy := types.RetentionPolicy{
		RetentionTime:     200 * time.Millisecond,
		TimeCheckInterval: 50 * time.Millisecond,
	}

	rm := NewRetentionManager(queueName, policy, store, nil, slog.Default())

	// Enqueue old messages (created 400ms ago - well past retention time)
	oldTime := time.Now().Add(-400 * time.Millisecond)
	for i := 0; i < 3; i++ {
		msg := &types.Message{
			ID:        fmt.Sprintf("old-msg-%d", i),
			Topic:     queueName,
			Payload:   []byte("old message"),
			Sequence:  uint64(i),
			State:     types.StateQueued,
			CreatedAt: oldTime,
		}
		err := store.Enqueue(ctx, queueName, msg)
		require.NoError(t, err)
	}

	// Enqueue new messages (just created)
	for i := 3; i < 6; i++ {
		msg := &types.Message{
			ID:        fmt.Sprintf("new-msg-%d", i),
			Topic:     queueName,
			Payload:   []byte("new message"),
			Sequence:  uint64(i),
			State:     types.StateQueued,
			CreatedAt: time.Now(),
		}
		err := store.Enqueue(ctx, queueName, msg)
		require.NoError(t, err)
	}

	// Verify we have 6 messages
	count, err := store.Count(ctx, queueName)
	require.NoError(t, err)
	assert.Equal(t, int64(6), count)

	// Start retention manager in background
	ctxWithCancel, cancel := context.WithCancel(ctx)
	defer cancel()

	go rm.Start(ctxWithCancel, 0)

	// Wait for time-based cleanup to run (only wait 80ms so new messages don't become old)
	time.Sleep(150 * time.Millisecond)

	// Stop retention manager
	rm.Stop()

	// Verify old messages were deleted (should have 3 new messages left)
	count, err = store.Count(ctx, queueName)
	require.NoError(t, err)
	assert.Equal(t, int64(3), count)
}

func TestRetentionManager_NoRetentionConfigured(t *testing.T) {
	ctx := context.Background()
	store := memory.New()
	queueName := "$queue/test-no-retention"

	// Create queue
	config := types.DefaultQueueConfig(queueName)
	err := store.CreateQueue(ctx, config)
	require.NoError(t, err)

	// No retention policy configured
	policy := types.RetentionPolicy{}
	rm := NewRetentionManager(queueName, policy, store, nil, slog.Default())

	// Enqueue messages
	for i := 0; i < 10; i++ {
		msg := &types.Message{
			ID:        fmt.Sprintf("msg-%d", i),
			Topic:     queueName,
			Payload:   []byte("test message"),
			Sequence:  uint64(i),
			State:     types.StateQueued,
			CreatedAt: time.Now(),
		}
		err := store.Enqueue(ctx, queueName, msg)
		require.NoError(t, err)
	}

	// Trigger size retention (should do nothing)
	deletedCount, bytesFreed, err := rm.CheckSizeRetention(ctx, 0)
	require.NoError(t, err)
	assert.Equal(t, int64(0), deletedCount)
	assert.Equal(t, int64(0), bytesFreed)

	// Verify all messages still present
	count, err := store.Count(ctx, queueName)
	require.NoError(t, err)
	assert.Equal(t, int64(10), count)
}

func TestRetentionManager_SizeCheckOptimization(t *testing.T) {
	ctx := context.Background()
	store := memory.New()
	queueName := "$queue/test-check-optimization"

	// Create queue
	config := types.DefaultQueueConfig(queueName)
	err := store.CreateQueue(ctx, config)
	require.NoError(t, err)

	// Configure retention with specific check interval
	policy := types.RetentionPolicy{
		RetentionMessages: 5,
		SizeCheckEvery:    5, // Only check every 5 enqueues
	}

	rm := NewRetentionManager(queueName, policy, store, nil, slog.Default())

	// Enqueue 10 messages
	for i := 0; i < 10; i++ {
		msg := &types.Message{
			ID:        fmt.Sprintf("msg-%d", i),
			Topic:     queueName,
			Payload:   []byte("test"),
			Sequence:  uint64(i),
			State:     types.StateQueued,
			CreatedAt: time.Now(),
		}
		err := store.Enqueue(ctx, queueName, msg)
		require.NoError(t, err)
	}

	// Verify we have 10 messages before retention
	count, err := store.Count(ctx, queueName)
	require.NoError(t, err)
	assert.Equal(t, int64(10), count)

	// Manually trigger retention check - need to call 5 times to reach the threshold
	var totalDeleted int64
	for i := 0; i < 5; i++ {
		deletedCount, _, err := rm.CheckSizeRetention(ctx, 0)
		require.NoError(t, err)
		totalDeleted += deletedCount
	}

	// Should have deleted 5 messages to get to limit
	assert.Equal(t, int64(5), totalDeleted)

	// Verify we now have 5 messages
	count, err = store.Count(ctx, queueName)
	require.NoError(t, err)
	assert.Equal(t, int64(5), count)
}

func TestRetentionManager_BothSizeLimits(t *testing.T) {
	ctx := context.Background()
	store := memory.New()
	queueName := "$queue/test-both-limits"

	// Create queue
	config := types.DefaultQueueConfig(queueName)
	err := store.CreateQueue(ctx, config)
	require.NoError(t, err)

	// Configure both byte and message limits
	policy := types.RetentionPolicy{
		RetentionBytes:    100, // 100 bytes
		RetentionMessages: 10,  // 10 messages
		SizeCheckEvery:    1,
	}

	rm := NewRetentionManager(queueName, policy, store, nil, slog.Default())

	// Enqueue 5 messages with 30 bytes each (150 bytes total, but only 5 messages)
	for i := 0; i < 5; i++ {
		msg := &types.Message{
			ID:        fmt.Sprintf("msg-%d", i),
			Topic:     queueName,
			Payload:   make([]byte, 30),
			Sequence:  uint64(i),
			State:     types.StateQueued,
			CreatedAt: time.Now(),
		}
		err := store.Enqueue(ctx, queueName, msg)
		require.NoError(t, err)
	}

	// Trigger retention
	deletedCount, _, err := rm.CheckSizeRetention(ctx, 0)
	require.NoError(t, err)

	// Should delete messages to meet byte limit (more restrictive than message limit)
	assert.Greater(t, deletedCount, int64(0))

	// Verify byte limit is respected
	size, err := store.GetQueueSize(ctx, queueName)
	require.NoError(t, err)
	assert.LessOrEqual(t, size, int64(100))

	// Message count should also be under limit
	count, err := store.Count(ctx, queueName)
	require.NoError(t, err)
	assert.LessOrEqual(t, count, int64(10))
}

func TestRetentionManager_Compaction_Basic(t *testing.T) {
	ctx := context.Background()
	store := memory.New()
	queueName := "$queue/test-compaction"

	// Create queue
	config := types.DefaultQueueConfig(queueName)
	err := store.CreateQueue(ctx, config)
	require.NoError(t, err)

	// Configure compaction with a key
	policy := types.RetentionPolicy{
		CompactionEnabled:  true,
		CompactionKey:      "entity_id", // Compact by entity_id property
		CompactionLag:      0,           // No lag for testing
		CompactionInterval: 100 * time.Millisecond,
	}

	rm := NewRetentionManager(queueName, policy, store, nil, slog.Default())

	// Enqueue multiple messages for the same entity_id (should compact to 1)
	// Entity A: 3 messages (only latest should survive)
	for i := 0; i < 3; i++ {
		msg := &types.Message{
			ID:        fmt.Sprintf("entity-a-msg-%d", i),
			Topic:     queueName,
			Payload:   []byte(fmt.Sprintf("entity A update %d", i)),
			Sequence:  uint64(i),
			State:     types.StateQueued,
			CreatedAt: time.Now().Add(-time.Hour), // Old messages
			Properties: map[string]string{
				"entity_id": "entity-A",
			},
		}
		err := store.Enqueue(ctx, queueName, msg)
		require.NoError(t, err)
	}

	// Entity B: 2 messages
	for i := 3; i < 5; i++ {
		msg := &types.Message{
			ID:        fmt.Sprintf("entity-b-msg-%d", i),
			Topic:     queueName,
			Payload:   []byte(fmt.Sprintf("entity B update %d", i-3)),
			Sequence:  uint64(i),
			State:     types.StateQueued,
			CreatedAt: time.Now().Add(-time.Hour),
			Properties: map[string]string{
				"entity_id": "entity-B",
			},
		}
		err := store.Enqueue(ctx, queueName, msg)
		require.NoError(t, err)
	}

	// Verify we have 5 messages
	count, err := store.Count(ctx, queueName)
	require.NoError(t, err)
	assert.Equal(t, int64(5), count)

	// Run compaction
	stats, err := rm.RunCompaction(ctx, 0)
	require.NoError(t, err)

	// Should delete 3 messages (2 from entity-A, 1 from entity-B)
	assert.Equal(t, int64(3), stats.MessagesDeleted)

	// Verify we now have 2 messages (one per entity)
	count, err = store.Count(ctx, queueName)
	require.NoError(t, err)
	assert.Equal(t, int64(2), count)

	// Verify the latest messages survived (highest sequence per entity)
	messages, err := store.ListAllMessages(ctx, queueName, 0)
	require.NoError(t, err)
	assert.Len(t, messages, 2)

	// Should have entity-a-msg-2 (seq 2) and entity-b-msg-4 (seq 4)
	seqs := make(map[uint64]bool)
	for _, msg := range messages {
		seqs[msg.Sequence] = true
	}
	assert.True(t, seqs[2], "entity-A latest message should survive")
	assert.True(t, seqs[4], "entity-B latest message should survive")
}

func TestRetentionManager_Compaction_RespectLag(t *testing.T) {
	ctx := context.Background()
	store := memory.New()
	queueName := "$queue/test-compaction-lag"

	// Create queue
	config := types.DefaultQueueConfig(queueName)
	err := store.CreateQueue(ctx, config)
	require.NoError(t, err)

	// Configure compaction with 1-hour lag
	policy := types.RetentionPolicy{
		CompactionEnabled:  true,
		CompactionKey:      "entity_id",
		CompactionLag:      1 * time.Hour, // Messages within the last hour won't be compacted
		CompactionInterval: 100 * time.Millisecond,
	}

	rm := NewRetentionManager(queueName, policy, store, nil, slog.Default())

	// Enqueue old messages (should be compacted)
	for i := 0; i < 2; i++ {
		msg := &types.Message{
			ID:        fmt.Sprintf("old-msg-%d", i),
			Topic:     queueName,
			Payload:   []byte("old message"),
			Sequence:  uint64(i),
			State:     types.StateQueued,
			CreatedAt: time.Now().Add(-2 * time.Hour), // 2 hours ago
			Properties: map[string]string{
				"entity_id": "entity-X",
			},
		}
		err := store.Enqueue(ctx, queueName, msg)
		require.NoError(t, err)
	}

	// Enqueue recent messages (should NOT be compacted due to lag)
	for i := 2; i < 4; i++ {
		msg := &types.Message{
			ID:        fmt.Sprintf("new-msg-%d", i),
			Topic:     queueName,
			Payload:   []byte("new message"),
			Sequence:  uint64(i),
			State:     types.StateQueued,
			CreatedAt: time.Now().Add(-10 * time.Minute), // 10 minutes ago (within lag)
			Properties: map[string]string{
				"entity_id": "entity-X",
			},
		}
		err := store.Enqueue(ctx, queueName, msg)
		require.NoError(t, err)
	}

	// Verify we have 4 messages
	count, err := store.Count(ctx, queueName)
	require.NoError(t, err)
	assert.Equal(t, int64(4), count)

	// Run compaction
	stats, err := rm.RunCompaction(ctx, 0)
	require.NoError(t, err)

	// Should only delete 1 old message (keep latest old message, all new messages untouched)
	assert.Equal(t, int64(1), stats.MessagesDeleted)

	// Verify we have 3 messages (2 recent + 1 old latest)
	count, err = store.Count(ctx, queueName)
	require.NoError(t, err)
	assert.Equal(t, int64(3), count)
}

func TestRetentionManager_Compaction_NoKeyProperty(t *testing.T) {
	ctx := context.Background()
	store := memory.New()
	queueName := "$queue/test-compaction-no-key"

	// Create queue
	config := types.DefaultQueueConfig(queueName)
	err := store.CreateQueue(ctx, config)
	require.NoError(t, err)

	// Configure compaction with a key
	policy := types.RetentionPolicy{
		CompactionEnabled:  true,
		CompactionKey:      "entity_id",
		CompactionLag:      0,
		CompactionInterval: 100 * time.Millisecond,
	}

	rm := NewRetentionManager(queueName, policy, store, nil, slog.Default())

	// Enqueue messages WITHOUT the compaction key property
	for i := 0; i < 5; i++ {
		msg := &types.Message{
			ID:        fmt.Sprintf("msg-%d", i),
			Topic:     queueName,
			Payload:   []byte("message without entity_id"),
			Sequence:  uint64(i),
			State:     types.StateQueued,
			CreatedAt: time.Now().Add(-time.Hour),
			Properties: map[string]string{
				"other_key": "some_value", // No entity_id
			},
		}
		err := store.Enqueue(ctx, queueName, msg)
		require.NoError(t, err)
	}

	// Verify we have 5 messages
	count, err := store.Count(ctx, queueName)
	require.NoError(t, err)
	assert.Equal(t, int64(5), count)

	// Run compaction
	stats, err := rm.RunCompaction(ctx, 0)
	require.NoError(t, err)

	// No messages should be deleted (no compaction key)
	assert.Equal(t, int64(0), stats.MessagesDeleted)

	// All messages should still exist
	count, err = store.Count(ctx, queueName)
	require.NoError(t, err)
	assert.Equal(t, int64(5), count)
}

func TestRetentionManager_Compaction_NotConfigured(t *testing.T) {
	ctx := context.Background()
	store := memory.New()
	queueName := "$queue/test-no-compaction"

	// Create queue
	config := types.DefaultQueueConfig(queueName)
	err := store.CreateQueue(ctx, config)
	require.NoError(t, err)

	// No compaction configured (empty key)
	policy := types.RetentionPolicy{
		CompactionEnabled:  true,
		CompactionKey:      "", // No key = no compaction
		CompactionInterval: 100 * time.Millisecond,
	}

	rm := NewRetentionManager(queueName, policy, store, nil, slog.Default())

	// Enqueue messages
	for i := 0; i < 5; i++ {
		msg := &types.Message{
			ID:        fmt.Sprintf("msg-%d", i),
			Topic:     queueName,
			Payload:   []byte("message"),
			Sequence:  uint64(i),
			State:     types.StateQueued,
			CreatedAt: time.Now().Add(-time.Hour),
			Properties: map[string]string{
				"entity_id": "entity-A",
			},
		}
		err := store.Enqueue(ctx, queueName, msg)
		require.NoError(t, err)
	}

	// Run compaction
	stats, err := rm.RunCompaction(ctx, 0)
	require.NoError(t, err)

	// No messages should be deleted (no compaction key configured)
	assert.Equal(t, int64(0), stats.MessagesDeleted)
}
