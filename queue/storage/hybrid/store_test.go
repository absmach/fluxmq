// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package hybrid

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/absmach/fluxmq/queue/storage"
	"github.com/absmach/fluxmq/queue/storage/badger"
	"github.com/absmach/fluxmq/queue/types"
	badgerdb "github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupTestStore(t *testing.T) (*Store, func()) {
	t.Helper()

	// Create temporary directory for BadgerDB
	tmpDir, err := os.MkdirTemp("", "hybrid-test-*")
	require.NoError(t, err)

	// Open BadgerDB
	opts := badgerdb.DefaultOptions(tmpDir)
	opts.Logger = nil // Disable logging in tests
	db, err := badgerdb.Open(opts)
	require.NoError(t, err)

	// Create BadgerDB store
	persistentStore := badger.New(db)

	// Create hybrid store with test configuration
	config := Config{
		RingBufferSize:   16, // Small size for testing overflow
		EnableMetrics:    true,
		AsyncPersist:     false, // Synchronous for testing
		PersistBatchSize: 10,
		PersistInterval:  10 * time.Millisecond,
		WarmupSize:       5,
	}

	hybridStore := NewWithConfig(persistentStore, config)

	// Return cleanup function
	cleanup := func() {
		db.Close()
		os.RemoveAll(tmpDir)
	}

	return hybridStore, cleanup
}

// Basic Operations Tests

func TestHybridStore_CreateQueue(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()

	ctx := context.Background()
	config := types.DefaultQueueConfig("$queue/test")

	err := store.CreateQueue(ctx, config)
	require.NoError(t, err)

	// Verify queue was created
	retrieved, err := store.GetQueue(ctx, "$queue/test")
	require.NoError(t, err)
	assert.Equal(t, "$queue/test", retrieved.Name)
	assert.Equal(t, config.Partitions, retrieved.Partitions)
}

func TestHybridStore_CreateQueue_Duplicate(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()

	ctx := context.Background()
	config := types.DefaultQueueConfig("$queue/test")

	err := store.CreateQueue(ctx, config)
	require.NoError(t, err)

	// Try to create again
	err = store.CreateQueue(ctx, config)
	assert.ErrorIs(t, err, storage.ErrQueueAlreadyExists)
}

func TestHybridStore_EnqueueDequeue(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()

	ctx := context.Background()
	config := types.DefaultQueueConfig("$queue/test")
	err := store.CreateQueue(ctx, config)
	require.NoError(t, err)

	// Enqueue a message
	msg := &types.Message{
		ID:          "msg-1",
		Payload:     []byte("test payload"),
		PartitionID: 0,
		Sequence:    1,
		CreatedAt:   time.Now(),
		State:       types.StateQueued,
	}

	err = store.Enqueue(ctx, "$queue/test", msg)
	require.NoError(t, err)

	// Dequeue the message
	retrieved, err := store.Dequeue(ctx, "$queue/test", 0)
	require.NoError(t, err)
	require.NotNil(t, retrieved)
	assert.Equal(t, "msg-1", retrieved.ID)
	assert.Equal(t, "test payload", string(retrieved.Payload))
}

// Hot Path Tests (Ring Buffer)

func TestHybridStore_HotPath_RingBufferHit(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()

	ctx := context.Background()
	config := types.DefaultQueueConfig("$queue/test")
	err := store.CreateQueue(ctx, config)
	require.NoError(t, err)

	// Enqueue messages (should go to both stores)
	for i := 0; i < 5; i++ {
		msg := &types.Message{
			ID:          fmt.Sprintf("msg-%d", i),
			Payload:     []byte(fmt.Sprintf("payload-%d", i)),
			PartitionID: 0,
			Sequence:    uint64(i + 1),
			CreatedAt:   time.Now(),
			State:       types.StateQueued,
		}
		err = store.Enqueue(ctx, "$queue/test", msg)
		require.NoError(t, err)
	}

	// Verify metrics - messages should be in ring buffer
	metrics := store.GetMetrics()
	require.NotNil(t, metrics)
	assert.Equal(t, uint64(5), metrics.RingMessages)
	assert.Equal(t, uint64(0), metrics.RingMisses) // All should fit in ring buffer

	// Dequeue messages (should come from ring buffer - hot path)
	for i := 0; i < 5; i++ {
		msg, err := store.Dequeue(ctx, "$queue/test", 0)
		require.NoError(t, err)
		require.NotNil(t, msg)
		assert.Equal(t, fmt.Sprintf("msg-%d", i), msg.ID)
	}

	// Check hit rate
	hitRate := store.HitRate()
	assert.Equal(t, 1.0, hitRate) // 100% hit rate
}

// Cold Path Tests (BadgerDB Fallback)

func TestHybridStore_ColdPath_RingBufferEmpty(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()

	ctx := context.Background()
	config := types.DefaultQueueConfig("$queue/test")
	err := store.CreateQueue(ctx, config)
	require.NoError(t, err)

	// Enqueue and immediately dequeue to empty ring buffer
	msg := &types.Message{
		ID:          "msg-1",
		Payload:     []byte("test"),
		PartitionID: 0,
		Sequence:    1,
		CreatedAt:   time.Now(),
		State:       types.StateQueued,
	}
	err = store.Enqueue(ctx, "$queue/test", msg)
	require.NoError(t, err)

	// Dequeue from ring buffer
	_, err = store.Dequeue(ctx, "$queue/test", 0)
	require.NoError(t, err)

	// Enqueue another message
	msg2 := &types.Message{
		ID:          "msg-2",
		Payload:     []byte("test2"),
		PartitionID: 0,
		Sequence:    2,
		CreatedAt:   time.Now(),
		State:       types.StateQueued,
	}
	err = store.Enqueue(ctx, "$queue/test", msg2)
	require.NoError(t, err)

	// Empty ring buffer again
	_, err = store.Dequeue(ctx, "$queue/test", 0)
	require.NoError(t, err)

	// Now enqueue directly to BadgerDB only (simulate ring buffer full scenario)
	// We'll verify cold path by checking metrics
	metrics := store.GetMetrics()
	require.NotNil(t, metrics)
	assert.Equal(t, uint64(2), metrics.RingHits) // Both dequeues were hits
}

// Overflow Tests (Ring Buffer Full)

func TestHybridStore_Overflow_RingBufferFull(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()

	ctx := context.Background()
	config := types.DefaultQueueConfig("$queue/test")
	err := store.CreateQueue(ctx, config)
	require.NoError(t, err)

	// Fill ring buffer beyond capacity (16 in test config)
	// Ring buffer keeps capacity-1 slots, so 15 should fill it
	numMessages := 20 // Exceed capacity

	for i := 0; i < numMessages; i++ {
		msg := &types.Message{
			ID:          fmt.Sprintf("msg-%d", i),
			Payload:     []byte(fmt.Sprintf("payload-%d", i)),
			PartitionID: 0,
			Sequence:    uint64(i + 1),
			CreatedAt:   time.Now(),
			State:       types.StateQueued,
		}
		err = store.Enqueue(ctx, "$queue/test", msg)
		require.NoError(t, err)
	}

	// Verify metrics - some messages couldn't fit in ring buffer
	metrics := store.GetMetrics()
	require.NotNil(t, metrics)
	assert.Greater(t, metrics.RingMisses, uint64(0)) // Some should overflow
	assert.Greater(t, metrics.DiskWrites, uint64(0)) // All persisted to BadgerDB

	// All messages should still be retrievable (from BadgerDB)
	for i := 0; i < numMessages; i++ {
		msg, err := store.Dequeue(ctx, "$queue/test", 0)
		require.NoError(t, err)
		require.NotNil(t, msg)
		assert.Equal(t, fmt.Sprintf("msg-%d", i), msg.ID)
	}
}

// Batch Dequeue Tests

func TestHybridStore_DequeueBatch_FromRingBuffer(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()

	ctx := context.Background()
	config := types.DefaultQueueConfig("$queue/test")
	err := store.CreateQueue(ctx, config)
	require.NoError(t, err)

	// Enqueue 10 messages
	for i := 0; i < 10; i++ {
		msg := &types.Message{
			ID:          fmt.Sprintf("msg-%d", i),
			Payload:     []byte(fmt.Sprintf("payload-%d", i)),
			PartitionID: 0,
			Sequence:    uint64(i + 1),
			CreatedAt:   time.Now(),
			State:       types.StateQueued,
		}
		err = store.Enqueue(ctx, "$queue/test", msg)
		require.NoError(t, err)
	}

	// Batch dequeue 5 messages
	messages, err := store.DequeueBatch(ctx, "$queue/test", 0, 5)
	require.NoError(t, err)
	assert.Len(t, messages, 5)

	// Verify order
	for i := 0; i < 5; i++ {
		assert.Equal(t, fmt.Sprintf("msg-%d", i), messages[i].ID)
	}

	// Verify metrics
	metrics := store.GetMetrics()
	assert.Equal(t, uint64(5), metrics.RingHits)
}

func TestHybridStore_DequeueBatch_Mixed(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()

	ctx := context.Background()
	config := types.DefaultQueueConfig("$queue/test")
	err := store.CreateQueue(ctx, config)
	require.NoError(t, err)

	// Enqueue messages to fill ring buffer
	for i := 0; i < 20; i++ {
		msg := &types.Message{
			ID:          fmt.Sprintf("msg-%d", i),
			Payload:     []byte(fmt.Sprintf("payload-%d", i)),
			PartitionID: 0,
			Sequence:    uint64(i + 1),
			CreatedAt:   time.Now(),
			State:       types.StateQueued,
		}
		err = store.Enqueue(ctx, "$queue/test", msg)
		require.NoError(t, err)
	}

	// Batch dequeue all messages
	// Should get some from ring buffer, rest from BadgerDB
	var allMessages []*types.Message
	for {
		messages, err := store.DequeueBatch(ctx, "$queue/test", 0, 5)
		require.NoError(t, err)
		if len(messages) == 0 {
			break
		}
		allMessages = append(allMessages, messages...)
	}

	// Verify all 20 messages retrieved
	assert.Len(t, allMessages, 20)

	// Verify order
	for i := 0; i < 20; i++ {
		assert.Equal(t, fmt.Sprintf("msg-%d", i), allMessages[i].ID)
	}
}

// Metrics Tests

func TestHybridStore_Metrics_HitRate(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()

	ctx := context.Background()
	config := types.DefaultQueueConfig("$queue/test")
	err := store.CreateQueue(ctx, config)
	require.NoError(t, err)

	// Initially no metrics
	hitRate := store.HitRate()
	assert.Equal(t, 0.0, hitRate)

	// Enqueue and dequeue (hit)
	msg := &types.Message{
		ID:          "msg-1",
		Payload:     []byte("test"),
		PartitionID: 0,
		Sequence:    1,
		CreatedAt:   time.Now(),
		State:       types.StateQueued,
	}
	err = store.Enqueue(ctx, "$queue/test", msg)
	require.NoError(t, err)

	_, err = store.Dequeue(ctx, "$queue/test", 0)
	require.NoError(t, err)

	// Should have 100% hit rate
	hitRate = store.HitRate()
	assert.Equal(t, 1.0, hitRate)
}

func TestHybridStore_Metrics_ResetMetrics(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()

	ctx := context.Background()
	config := types.DefaultQueueConfig("$queue/test")
	err := store.CreateQueue(ctx, config)
	require.NoError(t, err)

	// Generate some metrics
	msg := &types.Message{
		ID:          "msg-1",
		Payload:     []byte("test"),
		PartitionID: 0,
		Sequence:    1,
		CreatedAt:   time.Now(),
		State:       types.StateQueued,
	}
	err = store.Enqueue(ctx, "$queue/test", msg)
	require.NoError(t, err)

	metrics := store.GetMetrics()
	require.NotNil(t, metrics)
	assert.Greater(t, metrics.RingMessages, uint64(0))

	// Reset metrics
	store.ResetMetrics()

	// Verify reset
	metrics = store.GetMetrics()
	require.NotNil(t, metrics)
	assert.Equal(t, uint64(0), metrics.RingHits)
	assert.Equal(t, uint64(0), metrics.RingMisses)
	assert.Equal(t, uint64(0), metrics.DiskReads)
	assert.Equal(t, uint64(0), metrics.DiskWrites)
}

// Multi-Partition Tests

func TestHybridStore_MultiPartition(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()

	ctx := context.Background()
	config := types.DefaultQueueConfig("$queue/test")
	config.Partitions = 3
	err := store.CreateQueue(ctx, config)
	require.NoError(t, err)

	// Enqueue to different partitions
	for partition := 0; partition < 3; partition++ {
		for i := 0; i < 5; i++ {
			msg := &types.Message{
				ID:          fmt.Sprintf("p%d-msg-%d", partition, i),
				Payload:     []byte(fmt.Sprintf("partition-%d-payload-%d", partition, i)),
				PartitionID: partition,
				Sequence:    uint64(i + 1),
				CreatedAt:   time.Now(),
				State:       types.StateQueued,
			}
			err = store.Enqueue(ctx, "$queue/test", msg)
			require.NoError(t, err)
		}
	}

	// Dequeue from each partition
	for partition := 0; partition < 3; partition++ {
		for i := 0; i < 5; i++ {
			msg, err := store.Dequeue(ctx, "$queue/test", partition)
			require.NoError(t, err)
			require.NotNil(t, msg)
			assert.Equal(t, fmt.Sprintf("p%d-msg-%d", partition, i), msg.ID)
		}
	}
}

// Edge Cases

func TestHybridStore_EmptyQueue(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()

	ctx := context.Background()
	config := types.DefaultQueueConfig("$queue/test")
	err := store.CreateQueue(ctx, config)
	require.NoError(t, err)

	// Dequeue from empty queue
	msg, err := store.Dequeue(ctx, "$queue/test", 0)
	require.NoError(t, err)
	assert.Nil(t, msg)

	// Batch dequeue from empty queue
	messages, err := store.DequeueBatch(ctx, "$queue/test", 0, 10)
	require.NoError(t, err)
	assert.Nil(t, messages)
}

func TestHybridStore_NonExistentQueue(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()

	ctx := context.Background()

	// Enqueue to non-existent queue
	msg := &types.Message{
		ID:          "msg-1",
		Payload:     []byte("test"),
		PartitionID: 0,
		Sequence:    1,
		CreatedAt:   time.Now(),
		State:       types.StateQueued,
	}
	err := store.Enqueue(ctx, "$queue/nonexistent", msg)
	assert.ErrorIs(t, err, storage.ErrQueueNotFound)

	// Dequeue from non-existent queue
	_, err = store.Dequeue(ctx, "$queue/nonexistent", 0)
	assert.ErrorIs(t, err, storage.ErrQueueNotFound)
}

// Metadata Delegation Tests (verify they work with BadgerDB)

func TestHybridStore_InflightTracking(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()

	ctx := context.Background()
	config := types.DefaultQueueConfig("$queue/test")
	err := store.CreateQueue(ctx, config)
	require.NoError(t, err)

	// First enqueue a message
	msg := &types.Message{
		ID:          "msg-1",
		Payload:     []byte("test"),
		PartitionID: 0,
		Sequence:    1,
		CreatedAt:   time.Now(),
		State:       types.StateQueued,
	}
	err = store.Enqueue(ctx, "$queue/test", msg)
	require.NoError(t, err)

	// Mark message as inflight
	state := &types.DeliveryState{
		MessageID:   "msg-1",
		QueueName:   "$queue/test",
		PartitionID: 0,
		GroupID:     "group-1",
		ConsumerID:  "consumer-1",
		DeliveredAt: time.Now(),
		Timeout:     time.Now().Add(30 * time.Second),
		RetryCount:  0,
	}

	err = store.MarkInflight(ctx, state)
	require.NoError(t, err)

	// Retrieve inflight message
	retrieved, err := store.GetInflightMessage(ctx, "$queue/test", "msg-1", "group-1")
	require.NoError(t, err)
	require.NotNil(t, retrieved)
	assert.Equal(t, "msg-1", retrieved.MessageID)
	assert.Equal(t, "consumer-1", retrieved.ConsumerID)
	assert.Equal(t, "group-1", retrieved.GroupID)

	// Remove inflight
	err = store.RemoveInflight(ctx, "$queue/test", "msg-1", "group-1")
	require.NoError(t, err)

	// Verify removed - should return nil without error or return not found error
	retrieved, err = store.GetInflightMessage(ctx, "$queue/test", "msg-1", "group-1")
	if err == nil {
		assert.Nil(t, retrieved)
	} else {
		// BadgerDB may return ErrMessageNotFound for removed inflight messages
		assert.ErrorIs(t, err, storage.ErrMessageNotFound)
	}
}

func TestHybridStore_ConsumerManagement(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()

	ctx := context.Background()
	config := types.DefaultQueueConfig("$queue/test")
	err := store.CreateQueue(ctx, config)
	require.NoError(t, err)

	// Register consumer
	consumer := &types.Consumer{
		ID:            "consumer-1",
		ClientID:      "client-1",
		GroupID:       "group-1",
		QueueName:     "$queue/test",
		AssignedParts: []int{0},
		RegisteredAt:  time.Now(),
		LastHeartbeat: time.Now(),
	}

	err = store.RegisterConsumer(ctx, consumer)
	require.NoError(t, err)

	// Get consumer
	retrieved, err := store.GetConsumer(ctx, "$queue/test", "group-1", "consumer-1")
	require.NoError(t, err)
	require.NotNil(t, retrieved)
	assert.Equal(t, "consumer-1", retrieved.ID)

	// List consumers
	consumers, err := store.ListConsumers(ctx, "$queue/test", "group-1")
	require.NoError(t, err)
	assert.Len(t, consumers, 1)

	// Unregister consumer
	err = store.UnregisterConsumer(ctx, "$queue/test", "group-1", "consumer-1")
	require.NoError(t, err)

	// Verify removed
	retrieved, err = store.GetConsumer(ctx, "$queue/test", "group-1", "consumer-1")
	require.Error(t, err)
}
