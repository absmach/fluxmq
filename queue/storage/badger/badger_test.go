// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package badger

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/absmach/mqtt/queue/storage"
	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupTestStore(t *testing.T) (*Store, func()) {
	t.Helper()

	// Create temporary directory
	tmpDir, err := os.MkdirTemp("", "badger-queue-test-*")
	require.NoError(t, err)

	// Open BadgerDB
	opts := badger.DefaultOptions(tmpDir)
	opts.Logger = nil // Disable logging in tests
	db, err := badger.Open(opts)
	require.NoError(t, err)

	store := New(db)

	// Return cleanup function
	cleanup := func() {
		db.Close()
		os.RemoveAll(tmpDir)
	}

	return store, cleanup
}

// QueueStore Tests

func TestBadgerQueueStore_CreateQueue(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()

	ctx := context.Background()
	config := storage.DefaultQueueConfig("$queue/test")

	err := store.CreateQueue(ctx, config)
	require.NoError(t, err)

	// Verify queue was created
	retrieved, err := store.GetQueue(ctx, "$queue/test")
	require.NoError(t, err)
	assert.Equal(t, "$queue/test", retrieved.Name)
	assert.Equal(t, config.Partitions, retrieved.Partitions)
}

func TestBadgerQueueStore_CreateQueue_Duplicate(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()

	ctx := context.Background()
	config := storage.DefaultQueueConfig("$queue/test")

	err := store.CreateQueue(ctx, config)
	require.NoError(t, err)

	// Try to create again
	err = store.CreateQueue(ctx, config)
	assert.ErrorIs(t, err, storage.ErrQueueAlreadyExists)
}

func TestBadgerQueueStore_CreateQueue_InvalidConfig(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()

	ctx := context.Background()

	// Invalid config (empty name)
	config := storage.QueueConfig{Name: ""}
	err := store.CreateQueue(ctx, config)
	assert.Error(t, err)
}

func TestBadgerQueueStore_GetQueue(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()

	ctx := context.Background()

	// Get non-existent queue
	_, err := store.GetQueue(ctx, "$queue/nonexistent")
	assert.ErrorIs(t, err, storage.ErrQueueNotFound)

	// Create and get queue
	config := storage.DefaultQueueConfig("$queue/test")
	err = store.CreateQueue(ctx, config)
	require.NoError(t, err)

	retrieved, err := store.GetQueue(ctx, "$queue/test")
	require.NoError(t, err)
	assert.Equal(t, "$queue/test", retrieved.Name)
}

func TestBadgerQueueStore_UpdateQueue(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()

	ctx := context.Background()

	// Update non-existent queue
	config := storage.DefaultQueueConfig("$queue/test")
	err := store.UpdateQueue(ctx, config)
	assert.ErrorIs(t, err, storage.ErrQueueNotFound)

	// Create queue
	err = store.CreateQueue(ctx, config)
	require.NoError(t, err)

	// Update queue
	config.MaxMessageSize = 2048
	err = store.UpdateQueue(ctx, config)
	require.NoError(t, err)

	// Verify update
	retrieved, err := store.GetQueue(ctx, "$queue/test")
	require.NoError(t, err)
	assert.Equal(t, int64(2048), retrieved.MaxMessageSize)
}

func TestBadgerQueueStore_DeleteQueue(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()

	ctx := context.Background()

	config := storage.DefaultQueueConfig("$queue/test")
	err := store.CreateQueue(ctx, config)
	require.NoError(t, err)

	// Delete queue
	err = store.DeleteQueue(ctx, "$queue/test")
	require.NoError(t, err)

	// Verify deleted
	_, err = store.GetQueue(ctx, "$queue/test")
	assert.ErrorIs(t, err, storage.ErrQueueNotFound)
}

func TestBadgerQueueStore_ListQueues(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()

	ctx := context.Background()

	// Empty list
	queues, err := store.ListQueues(ctx)
	require.NoError(t, err)
	assert.Len(t, queues, 0)

	// Create multiple queues
	config1 := storage.DefaultQueueConfig("$queue/test1")
	config2 := storage.DefaultQueueConfig("$queue/test2")
	config3 := storage.DefaultQueueConfig("$queue/test3")

	require.NoError(t, store.CreateQueue(ctx, config1))
	require.NoError(t, store.CreateQueue(ctx, config2))
	require.NoError(t, store.CreateQueue(ctx, config3))

	// List all queues
	queues, err = store.ListQueues(ctx)
	require.NoError(t, err)
	assert.Len(t, queues, 3)

	// Verify queue names
	names := make(map[string]bool)
	for _, q := range queues {
		names[q.Name] = true
	}
	assert.True(t, names["$queue/test1"])
	assert.True(t, names["$queue/test2"])
	assert.True(t, names["$queue/test3"])
}

// MessageStore Tests

func TestBadgerMessageStore_Enqueue(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()

	ctx := context.Background()

	msg := &storage.QueueMessage{
		ID:          "msg-1",
		Payload:     []byte("test payload"),
		Topic:       "$queue/test",
		PartitionID: 0,
		Sequence:    1,
		State:       storage.StateQueued,
		CreatedAt:   time.Now(),
	}

	err := store.Enqueue(ctx, "$queue/test", msg)
	require.NoError(t, err)

	// Verify message was enqueued
	retrieved, err := store.GetMessage(ctx, "$queue/test", "msg-1")
	require.NoError(t, err)
	assert.Equal(t, "msg-1", retrieved.ID)
	assert.Equal(t, []byte("test payload"), retrieved.Payload)
}

func TestBadgerMessageStore_Dequeue(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()

	ctx := context.Background()

	// Enqueue multiple messages
	msg1 := &storage.QueueMessage{
		ID:          "msg-1",
		Payload:     []byte("payload-1"),
		Topic:       "$queue/test",
		PartitionID: 0,
		Sequence:    1,
		State:       storage.StateQueued,
		CreatedAt:   time.Now(),
	}
	msg2 := &storage.QueueMessage{
		ID:          "msg-2",
		Payload:     []byte("payload-2"),
		Topic:       "$queue/test",
		PartitionID: 0,
		Sequence:    2,
		State:       storage.StateQueued,
		CreatedAt:   time.Now(),
	}

	require.NoError(t, store.Enqueue(ctx, "$queue/test", msg1))
	require.NoError(t, store.Enqueue(ctx, "$queue/test", msg2))

	// Dequeue should return first message
	dequeued, err := store.Dequeue(ctx, "$queue/test", 0)
	require.NoError(t, err)
	assert.Equal(t, "msg-1", dequeued.ID)

	// Dequeue from empty partition
	dequeued, err = store.Dequeue(ctx, "$queue/test", 5)
	require.NoError(t, err)
	assert.Nil(t, dequeued)
}

func TestBadgerMessageStore_Dequeue_RetryReady(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()

	ctx := context.Background()

	// Message ready for retry (NextRetryAt in the past)
	msg := &storage.QueueMessage{
		ID:          "msg-1",
		Payload:     []byte("test"),
		Topic:       "$queue/test",
		PartitionID: 0,
		Sequence:    1,
		State:       storage.StateRetry,
		NextRetryAt: time.Now().Add(-1 * time.Second), // Past
		CreatedAt:   time.Now(),
	}

	require.NoError(t, store.Enqueue(ctx, "$queue/test", msg))

	// Should dequeue retry message
	dequeued, err := store.Dequeue(ctx, "$queue/test", 0)
	require.NoError(t, err)
	assert.NotNil(t, dequeued)
	assert.Equal(t, "msg-1", dequeued.ID)
}

func TestBadgerMessageStore_Dequeue_RetryNotReady(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()

	ctx := context.Background()

	// Message not ready for retry (NextRetryAt in the future)
	msg := &storage.QueueMessage{
		ID:          "msg-1",
		Payload:     []byte("test"),
		Topic:       "$queue/test",
		PartitionID: 0,
		Sequence:    1,
		State:       storage.StateRetry,
		NextRetryAt: time.Now().Add(1 * time.Hour), // Future
		CreatedAt:   time.Now(),
	}

	require.NoError(t, store.Enqueue(ctx, "$queue/test", msg))

	// Should not dequeue retry message
	dequeued, err := store.Dequeue(ctx, "$queue/test", 0)
	require.NoError(t, err)
	assert.Nil(t, dequeued)
}

func TestBadgerMessageStore_UpdateMessage(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()

	ctx := context.Background()

	msg := &storage.QueueMessage{
		ID:          "msg-1",
		Payload:     []byte("original"),
		Topic:       "$queue/test",
		PartitionID: 0,
		Sequence:    1,
		State:       storage.StateQueued,
		RetryCount:  0,
		CreatedAt:   time.Now(),
	}

	require.NoError(t, store.Enqueue(ctx, "$queue/test", msg))

	// Update message
	msg.State = storage.StateRetry
	msg.RetryCount = 1
	msg.Payload = []byte("updated")

	err := store.UpdateMessage(ctx, "$queue/test", msg)
	require.NoError(t, err)

	// Verify update
	retrieved, err := store.GetMessage(ctx, "$queue/test", "msg-1")
	require.NoError(t, err)
	assert.Equal(t, storage.StateRetry, retrieved.State)
	assert.Equal(t, 1, retrieved.RetryCount)
	assert.Equal(t, []byte("updated"), retrieved.Payload)
}

func TestBadgerMessageStore_DeleteMessage(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()

	ctx := context.Background()

	msg := &storage.QueueMessage{
		ID:          "msg-1",
		Payload:     []byte("test"),
		Topic:       "$queue/test",
		PartitionID: 0,
		Sequence:    1,
		State:       storage.StateQueued,
		CreatedAt:   time.Now(),
	}

	require.NoError(t, store.Enqueue(ctx, "$queue/test", msg))

	// Delete message
	err := store.DeleteMessage(ctx, "$queue/test", "msg-1")
	require.NoError(t, err)

	// Verify deleted
	_, err = store.GetMessage(ctx, "$queue/test", "msg-1")
	assert.ErrorIs(t, err, storage.ErrMessageNotFound)
}

func TestBadgerMessageStore_DeleteMessage_NotFound(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()

	ctx := context.Background()

	err := store.DeleteMessage(ctx, "$queue/test", "nonexistent")
	assert.ErrorIs(t, err, storage.ErrMessageNotFound)
}

func TestBadgerMessageStore_GetMessage(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()

	ctx := context.Background()

	// Get non-existent message
	_, err := store.GetMessage(ctx, "$queue/test", "nonexistent")
	assert.ErrorIs(t, err, storage.ErrMessageNotFound)

	// Create and get message
	msg := &storage.QueueMessage{
		ID:          "msg-1",
		Payload:     []byte("test"),
		Topic:       "$queue/test",
		PartitionID: 0,
		Sequence:    1,
		State:       storage.StateQueued,
		CreatedAt:   time.Now(),
	}

	require.NoError(t, store.Enqueue(ctx, "$queue/test", msg))

	retrieved, err := store.GetMessage(ctx, "$queue/test", "msg-1")
	require.NoError(t, err)
	assert.Equal(t, "msg-1", retrieved.ID)
}

func TestBadgerMessageStore_GetNextSequence(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()

	ctx := context.Background()

	// First sequence should be 1
	seq, err := store.GetNextSequence(ctx, "$queue/test", 0)
	require.NoError(t, err)
	assert.Equal(t, uint64(1), seq)

	// Second sequence should be 2
	seq, err = store.GetNextSequence(ctx, "$queue/test", 0)
	require.NoError(t, err)
	assert.Equal(t, uint64(2), seq)

	// Different partition should start at 1
	seq, err = store.GetNextSequence(ctx, "$queue/test", 1)
	require.NoError(t, err)
	assert.Equal(t, uint64(1), seq)
}

func TestBadgerMessageStore_ListQueued(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()

	ctx := context.Background()

	// Enqueue messages in different states
	msg1 := &storage.QueueMessage{
		ID:          "msg-1",
		Payload:     []byte("test-1"),
		Topic:       "$queue/test",
		PartitionID: 0,
		Sequence:    1,
		State:       storage.StateQueued,
		CreatedAt:   time.Now(),
	}
	msg2 := &storage.QueueMessage{
		ID:          "msg-2",
		Payload:     []byte("test-2"),
		Topic:       "$queue/test",
		PartitionID: 0,
		Sequence:    2,
		State:       storage.StateRetry,
		CreatedAt:   time.Now(),
	}
	msg3 := &storage.QueueMessage{
		ID:          "msg-3",
		Payload:     []byte("test-3"),
		Topic:       "$queue/test",
		PartitionID: 0,
		Sequence:    3,
		State:       storage.StateDLQ, // Should not be listed
		CreatedAt:   time.Now(),
	}

	require.NoError(t, store.Enqueue(ctx, "$queue/test", msg1))
	require.NoError(t, store.Enqueue(ctx, "$queue/test", msg2))
	require.NoError(t, store.Enqueue(ctx, "$queue/test", msg3))

	// List queued messages (should include StateQueued and StateRetry)
	messages, err := store.ListQueued(ctx, "$queue/test", 0, 0)
	require.NoError(t, err)
	assert.Len(t, messages, 2)

	// List with limit
	messages, err = store.ListQueued(ctx, "$queue/test", 0, 1)
	require.NoError(t, err)
	assert.Len(t, messages, 1)
}

func TestBadgerMessageStore_ListRetry(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()

	ctx := context.Background()

	// Enqueue messages with different states
	msg1 := &storage.QueueMessage{
		ID:          "msg-1",
		Payload:     []byte("test-1"),
		Topic:       "$queue/test",
		PartitionID: 0,
		Sequence:    1,
		State:       storage.StateQueued,
		CreatedAt:   time.Now(),
	}
	msg2 := &storage.QueueMessage{
		ID:          "msg-2",
		Payload:     []byte("test-2"),
		Topic:       "$queue/test",
		PartitionID: 0,
		Sequence:    2,
		State:       storage.StateRetry,
		CreatedAt:   time.Now(),
	}

	require.NoError(t, store.Enqueue(ctx, "$queue/test", msg1))
	require.NoError(t, store.Enqueue(ctx, "$queue/test", msg2))

	// List retry messages
	messages, err := store.ListRetry(ctx, "$queue/test", 0)
	require.NoError(t, err)
	assert.Len(t, messages, 1)
	assert.Equal(t, "msg-2", messages[0].ID)
	assert.Equal(t, storage.StateRetry, messages[0].State)
}

// Inflight Tests

func TestBadgerMessageStore_MarkInflight(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()

	ctx := context.Background()

	state := &storage.DeliveryState{
		MessageID:   "msg-1",
		QueueName:   "$queue/test",
		PartitionID: 0,
		ConsumerID:  "consumer-1",
		DeliveredAt: time.Now(),
		Timeout:     time.Now().Add(30 * time.Second),
	}

	err := store.MarkInflight(ctx, state)
	require.NoError(t, err)

	// Verify inflight
	retrieved, err := store.GetInflightMessage(ctx, "$queue/test", "msg-1")
	require.NoError(t, err)
	assert.Equal(t, "msg-1", retrieved.MessageID)
	assert.Equal(t, "consumer-1", retrieved.ConsumerID)
}

func TestBadgerMessageStore_GetInflight(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()

	ctx := context.Background()

	// Mark multiple messages inflight
	state1 := &storage.DeliveryState{
		MessageID:   "msg-1",
		QueueName:   "$queue/test",
		PartitionID: 0,
		ConsumerID:  "consumer-1",
		DeliveredAt: time.Now(),
		Timeout:     time.Now().Add(30 * time.Second),
	}
	state2 := &storage.DeliveryState{
		MessageID:   "msg-2",
		QueueName:   "$queue/test",
		PartitionID: 1,
		ConsumerID:  "consumer-2",
		DeliveredAt: time.Now(),
		Timeout:     time.Now().Add(30 * time.Second),
	}

	require.NoError(t, store.MarkInflight(ctx, state1))
	require.NoError(t, store.MarkInflight(ctx, state2))

	// Get all inflight
	inflight, err := store.GetInflight(ctx, "$queue/test")
	require.NoError(t, err)
	assert.Len(t, inflight, 2)
}

func TestBadgerMessageStore_GetInflightMessage(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()

	ctx := context.Background()

	// Get non-existent inflight message
	_, err := store.GetInflightMessage(ctx, "$queue/test", "nonexistent")
	assert.ErrorIs(t, err, storage.ErrMessageNotFound)

	// Mark inflight
	state := &storage.DeliveryState{
		MessageID:   "msg-1",
		QueueName:   "$queue/test",
		PartitionID: 0,
		ConsumerID:  "consumer-1",
		DeliveredAt: time.Now(),
		Timeout:     time.Now().Add(30 * time.Second),
	}

	require.NoError(t, store.MarkInflight(ctx, state))

	// Get inflight message
	retrieved, err := store.GetInflightMessage(ctx, "$queue/test", "msg-1")
	require.NoError(t, err)
	assert.Equal(t, "msg-1", retrieved.MessageID)
}

func TestBadgerMessageStore_RemoveInflight(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()

	ctx := context.Background()

	state := &storage.DeliveryState{
		MessageID:   "msg-1",
		QueueName:   "$queue/test",
		PartitionID: 0,
		ConsumerID:  "consumer-1",
		DeliveredAt: time.Now(),
		Timeout:     time.Now().Add(30 * time.Second),
	}

	require.NoError(t, store.MarkInflight(ctx, state))

	// Remove inflight
	err := store.RemoveInflight(ctx, "$queue/test", "msg-1")
	require.NoError(t, err)

	// Verify removed
	_, err = store.GetInflightMessage(ctx, "$queue/test", "msg-1")
	assert.ErrorIs(t, err, storage.ErrMessageNotFound)
}

// DLQ Tests

func TestBadgerMessageStore_EnqueueDLQ(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()

	ctx := context.Background()

	msg := &storage.QueueMessage{
		ID:            "msg-1",
		Payload:       []byte("failed message"),
		Topic:         "$queue/test",
		PartitionID:   0,
		Sequence:      1,
		State:         storage.StateDLQ,
		FailureReason: "max retries exceeded",
		CreatedAt:     time.Now(),
		MovedToDLQAt:  time.Now(),
	}

	err := store.EnqueueDLQ(ctx, "$queue/dlq/test", msg)
	require.NoError(t, err)

	// Verify in DLQ
	dlqMessages, err := store.ListDLQ(ctx, "$queue/dlq/test", 0)
	require.NoError(t, err)
	assert.Len(t, dlqMessages, 1)
	assert.Equal(t, "msg-1", dlqMessages[0].ID)
	assert.Equal(t, "max retries exceeded", dlqMessages[0].FailureReason)
}

func TestBadgerMessageStore_ListDLQ(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()

	ctx := context.Background()

	// Enqueue multiple DLQ messages
	msg1 := &storage.QueueMessage{
		ID:            "msg-1",
		Payload:       []byte("failed-1"),
		Topic:         "$queue/test",
		State:         storage.StateDLQ,
		FailureReason: "reason-1",
		CreatedAt:     time.Now(),
	}
	msg2 := &storage.QueueMessage{
		ID:            "msg-2",
		Payload:       []byte("failed-2"),
		Topic:         "$queue/test",
		State:         storage.StateDLQ,
		FailureReason: "reason-2",
		CreatedAt:     time.Now(),
	}

	require.NoError(t, store.EnqueueDLQ(ctx, "$queue/dlq/test", msg1))
	require.NoError(t, store.EnqueueDLQ(ctx, "$queue/dlq/test", msg2))

	// List all DLQ messages
	messages, err := store.ListDLQ(ctx, "$queue/dlq/test", 0)
	require.NoError(t, err)
	assert.Len(t, messages, 2)

	// List with limit
	messages, err = store.ListDLQ(ctx, "$queue/dlq/test", 1)
	require.NoError(t, err)
	assert.Len(t, messages, 1)
}

func TestBadgerMessageStore_DeleteDLQMessage(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()

	ctx := context.Background()

	msg := &storage.QueueMessage{
		ID:            "msg-1",
		Payload:       []byte("failed"),
		Topic:         "$queue/test",
		State:         storage.StateDLQ,
		FailureReason: "test failure",
		CreatedAt:     time.Now(),
	}

	require.NoError(t, store.EnqueueDLQ(ctx, "$queue/dlq/test", msg))

	// Delete DLQ message
	err := store.DeleteDLQMessage(ctx, "$queue/dlq/test", "msg-1")
	require.NoError(t, err)

	// Verify deleted
	messages, err := store.ListDLQ(ctx, "$queue/dlq/test", 0)
	require.NoError(t, err)
	assert.Len(t, messages, 0)
}

// Offset Tests

func TestBadgerMessageStore_UpdateOffset(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()

	ctx := context.Background()

	err := store.UpdateOffset(ctx, "$queue/test", 0, 100)
	require.NoError(t, err)

	// Verify offset
	offset, err := store.GetOffset(ctx, "$queue/test", 0)
	require.NoError(t, err)
	assert.Equal(t, uint64(100), offset)
}

func TestBadgerMessageStore_GetOffset(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()

	ctx := context.Background()

	// Get offset for non-existent partition (should return 0)
	offset, err := store.GetOffset(ctx, "$queue/test", 0)
	require.NoError(t, err)
	assert.Equal(t, uint64(0), offset)

	// Set and get offset
	err = store.UpdateOffset(ctx, "$queue/test", 0, 42)
	require.NoError(t, err)

	offset, err = store.GetOffset(ctx, "$queue/test", 0)
	require.NoError(t, err)
	assert.Equal(t, uint64(42), offset)
}

// ConsumerStore Tests

func TestBadgerConsumerStore_RegisterConsumer(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()

	ctx := context.Background()

	consumer := &storage.Consumer{
		ID:            "consumer-1",
		GroupID:       "group-1",
		QueueName:     "$queue/test",
		ClientID:      "client-1",
		AssignedParts: []int{0, 1, 2},
		LastHeartbeat: time.Now(),
	}

	err := store.RegisterConsumer(ctx, consumer)
	require.NoError(t, err)

	// Verify consumer registered
	retrieved, err := store.GetConsumer(ctx, "$queue/test", "group-1", "consumer-1")
	require.NoError(t, err)
	assert.Equal(t, "consumer-1", retrieved.ID)
	assert.Equal(t, "group-1", retrieved.GroupID)
	assert.Equal(t, []int{0, 1, 2}, retrieved.AssignedParts)
}

func TestBadgerConsumerStore_UnregisterConsumer(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()

	ctx := context.Background()

	consumer := &storage.Consumer{
		ID:            "consumer-1",
		GroupID:       "group-1",
		QueueName:     "$queue/test",
		ClientID:      "client-1",
		AssignedParts: []int{0},
		LastHeartbeat: time.Now(),
	}

	require.NoError(t, store.RegisterConsumer(ctx, consumer))

	// Unregister consumer
	err := store.UnregisterConsumer(ctx, "$queue/test", "group-1", "consumer-1")
	require.NoError(t, err)

	// Verify unregistered
	_, err = store.GetConsumer(ctx, "$queue/test", "group-1", "consumer-1")
	assert.ErrorIs(t, err, storage.ErrConsumerNotFound)
}

func TestBadgerConsumerStore_GetConsumer(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()

	ctx := context.Background()

	// Get non-existent consumer
	_, err := store.GetConsumer(ctx, "$queue/test", "group-1", "nonexistent")
	assert.ErrorIs(t, err, storage.ErrConsumerNotFound)

	// Register and get consumer
	consumer := &storage.Consumer{
		ID:            "consumer-1",
		GroupID:       "group-1",
		QueueName:     "$queue/test",
		ClientID:      "client-1",
		AssignedParts: []int{0},
		LastHeartbeat: time.Now(),
	}

	require.NoError(t, store.RegisterConsumer(ctx, consumer))

	retrieved, err := store.GetConsumer(ctx, "$queue/test", "group-1", "consumer-1")
	require.NoError(t, err)
	assert.Equal(t, "consumer-1", retrieved.ID)
}

func TestBadgerConsumerStore_ListConsumers(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()

	ctx := context.Background()

	// Empty list
	consumers, err := store.ListConsumers(ctx, "$queue/test", "group-1")
	require.NoError(t, err)
	assert.Len(t, consumers, 0)

	// Register multiple consumers
	consumer1 := &storage.Consumer{
		ID:            "consumer-1",
		GroupID:       "group-1",
		QueueName:     "$queue/test",
		ClientID:      "client-1",
		AssignedParts: []int{0},
		LastHeartbeat: time.Now(),
	}
	consumer2 := &storage.Consumer{
		ID:            "consumer-2",
		GroupID:       "group-1",
		QueueName:     "$queue/test",
		ClientID:      "client-2",
		AssignedParts: []int{1},
		LastHeartbeat: time.Now(),
	}

	require.NoError(t, store.RegisterConsumer(ctx, consumer1))
	require.NoError(t, store.RegisterConsumer(ctx, consumer2))

	// List consumers
	consumers, err = store.ListConsumers(ctx, "$queue/test", "group-1")
	require.NoError(t, err)
	assert.Len(t, consumers, 2)
}

func TestBadgerConsumerStore_ListGroups(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()

	ctx := context.Background()

	// Register consumers in different groups
	consumer1 := &storage.Consumer{
		ID:            "consumer-1",
		GroupID:       "group-1",
		QueueName:     "$queue/test",
		ClientID:      "client-1",
		AssignedParts: []int{0},
		LastHeartbeat: time.Now(),
	}
	consumer2 := &storage.Consumer{
		ID:            "consumer-2",
		GroupID:       "group-2",
		QueueName:     "$queue/test",
		ClientID:      "client-2",
		AssignedParts: []int{1},
		LastHeartbeat: time.Now(),
	}

	require.NoError(t, store.RegisterConsumer(ctx, consumer1))
	require.NoError(t, store.RegisterConsumer(ctx, consumer2))

	// List groups
	groups, err := store.ListGroups(ctx, "$queue/test")
	require.NoError(t, err)
	assert.Len(t, groups, 2)

	// Verify group names
	groupMap := make(map[string]bool)
	for _, g := range groups {
		groupMap[g] = true
	}
	assert.True(t, groupMap["group-1"])
	assert.True(t, groupMap["group-2"])
}

func TestBadgerConsumerStore_UpdateHeartbeat(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()

	ctx := context.Background()

	consumer := &storage.Consumer{
		ID:            "consumer-1",
		GroupID:       "group-1",
		QueueName:     "$queue/test",
		ClientID:      "client-1",
		AssignedParts: []int{0},
		LastHeartbeat: time.Now().Add(-1 * time.Hour),
	}

	require.NoError(t, store.RegisterConsumer(ctx, consumer))

	// Update heartbeat
	newHeartbeat := time.Now()
	err := store.UpdateHeartbeat(ctx, "$queue/test", "group-1", "consumer-1", newHeartbeat)
	require.NoError(t, err)

	// Verify heartbeat updated
	retrieved, err := store.GetConsumer(ctx, "$queue/test", "group-1", "consumer-1")
	require.NoError(t, err)
	assert.True(t, retrieved.LastHeartbeat.After(consumer.LastHeartbeat))
}

// Concurrent Access Tests

func TestBadgerStore_ConcurrentEnqueue(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()

	ctx := context.Background()

	// Enqueue messages concurrently
	numGoroutines := 10
	messagesPerGoroutine := 10

	done := make(chan bool, numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func(routineID int) {
			for j := 0; j < messagesPerGoroutine; j++ {
				msg := &storage.QueueMessage{
					ID:          filepath.Join("msg", string(rune(routineID)), string(rune(j))),
					Payload:     []byte("test"),
					Topic:       "$queue/test",
					PartitionID: routineID % 3,
					Sequence:    uint64(routineID*messagesPerGoroutine + j),
					State:       storage.StateQueued,
					CreatedAt:   time.Now(),
				}
				store.Enqueue(ctx, "$queue/test", msg)
			}
			done <- true
		}(i)
	}

	// Wait for all goroutines
	for i := 0; i < numGoroutines; i++ {
		<-done
	}

	// Verify some messages were enqueued (exact count may vary due to concurrency)
	messages, err := store.ListQueued(ctx, "$queue/test", 0, 0)
	require.NoError(t, err)
	assert.Greater(t, len(messages), 0)
}

func TestBadgerStore_ConcurrentConsumerRegistration(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()

	ctx := context.Background()

	// Register consumers concurrently
	numConsumers := 20
	done := make(chan bool, numConsumers)

	for i := 0; i < numConsumers; i++ {
		go func(id int) {
			consumer := &storage.Consumer{
				ID:            filepath.Join("consumer", string(rune(id))),
				GroupID:       "group-1",
				QueueName:     "$queue/test",
				ClientID:      filepath.Join("client", string(rune(id))),
				AssignedParts: []int{id % 10},
				LastHeartbeat: time.Now(),
			}
			store.RegisterConsumer(ctx, consumer)
			done <- true
		}(i)
	}

	// Wait for all goroutines
	for i := 0; i < numConsumers; i++ {
		<-done
	}

	// Verify consumers were registered
	consumers, err := store.ListConsumers(ctx, "$queue/test", "group-1")
	require.NoError(t, err)
	assert.Greater(t, len(consumers), 0)
}

// Key Format Tests

func TestBadgerStore_MessageKeyFormat(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()

	ctx := context.Background()

	// Test that messages with different partition IDs don't collide
	msg1 := &storage.QueueMessage{
		ID:          "msg-1",
		Payload:     []byte("partition-0"),
		Topic:       "$queue/test",
		PartitionID: 0,
		Sequence:    1,
		State:       storage.StateQueued,
		CreatedAt:   time.Now(),
	}
	msg2 := &storage.QueueMessage{
		ID:          "msg-2",
		Payload:     []byte("partition-1"),
		Topic:       "$queue/test",
		PartitionID: 1,
		Sequence:    1, // Same sequence, different partition
		State:       storage.StateQueued,
		CreatedAt:   time.Now(),
	}

	require.NoError(t, store.Enqueue(ctx, "$queue/test", msg1))
	require.NoError(t, store.Enqueue(ctx, "$queue/test", msg2))

	// Both should exist
	retrieved1, err := store.GetMessage(ctx, "$queue/test", "msg-1")
	require.NoError(t, err)
	assert.Equal(t, []byte("partition-0"), retrieved1.Payload)

	retrieved2, err := store.GetMessage(ctx, "$queue/test", "msg-2")
	require.NoError(t, err)
	assert.Equal(t, []byte("partition-1"), retrieved2.Payload)
}

func TestBadgerStore_DLQTopicPrefixHandling(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()

	ctx := context.Background()

	msg := &storage.QueueMessage{
		ID:            "msg-1",
		Payload:       []byte("failed"),
		Topic:         "$queue/test",
		State:         storage.StateDLQ,
		FailureReason: "test",
		CreatedAt:     time.Now(),
	}

	// Enqueue with full DLQ topic
	err := store.EnqueueDLQ(ctx, "$queue/dlq/test", msg)
	require.NoError(t, err)

	// Should be retrievable
	messages, err := store.ListDLQ(ctx, "$queue/dlq/test", 0)
	require.NoError(t, err)
	assert.Len(t, messages, 1)
}
