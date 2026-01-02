// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package memory

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/absmach/mqtt/queue/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// QueueStore Tests

func TestMemoryQueueStore_CreateQueue(t *testing.T) {
	store := New()
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

func TestMemoryQueueStore_CreateQueue_Duplicate(t *testing.T) {
	store := New()
	ctx := context.Background()

	config := storage.DefaultQueueConfig("$queue/test")
	err := store.CreateQueue(ctx, config)
	require.NoError(t, err)

	// Try to create again
	err = store.CreateQueue(ctx, config)
	assert.ErrorIs(t, err, storage.ErrQueueAlreadyExists)
}

func TestMemoryQueueStore_CreateQueue_InvalidConfig(t *testing.T) {
	store := New()
	ctx := context.Background()

	// Invalid config (empty name)
	config := storage.QueueConfig{Name: ""}
	err := store.CreateQueue(ctx, config)
	assert.Error(t, err)
}

func TestMemoryQueueStore_GetQueue(t *testing.T) {
	store := New()
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

func TestMemoryQueueStore_UpdateQueue(t *testing.T) {
	store := New()
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

func TestMemoryQueueStore_DeleteQueue(t *testing.T) {
	store := New()
	ctx := context.Background()

	// Delete non-existent queue
	err := store.DeleteQueue(ctx, "$queue/nonexistent")
	assert.ErrorIs(t, err, storage.ErrQueueNotFound)

	config := storage.DefaultQueueConfig("$queue/test")
	err = store.CreateQueue(ctx, config)
	require.NoError(t, err)

	// Delete queue
	err = store.DeleteQueue(ctx, "$queue/test")
	require.NoError(t, err)

	// Verify deleted
	_, err = store.GetQueue(ctx, "$queue/test")
	assert.ErrorIs(t, err, storage.ErrQueueNotFound)
}

func TestMemoryQueueStore_ListQueues(t *testing.T) {
	store := New()
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

func TestMemoryMessageStore_Enqueue(t *testing.T) {
	store := New()
	ctx := context.Background()

	// Create queue first
	config := storage.DefaultQueueConfig("$queue/test")
	require.NoError(t, store.CreateQueue(ctx, config))

	msg := &storage.Message{
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

func TestMemoryMessageStore_Enqueue_InvalidQueue(t *testing.T) {
	store := New()
	ctx := context.Background()

	msg := &storage.Message{
		ID:          "msg-1",
		Payload:     []byte("test"),
		Topic:       "$queue/nonexistent",
		PartitionID: 0,
		Sequence:    1,
		State:       storage.StateQueued,
		CreatedAt:   time.Now(),
	}

	err := store.Enqueue(ctx, "$queue/nonexistent", msg)
	assert.ErrorIs(t, err, storage.ErrQueueNotFound)
}

func TestMemoryMessageStore_Dequeue(t *testing.T) {
	store := New()
	ctx := context.Background()

	// Create queue
	config := storage.DefaultQueueConfig("$queue/test")
	require.NoError(t, store.CreateQueue(ctx, config))

	// Enqueue multiple messages
	msg1 := &storage.Message{
		ID:          "msg-1",
		Payload:     []byte("payload-1"),
		Topic:       "$queue/test",
		PartitionID: 0,
		Sequence:    1,
		State:       storage.StateQueued,
		CreatedAt:   time.Now(),
	}
	msg2 := &storage.Message{
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

func TestMemoryMessageStore_Dequeue_RetryReady(t *testing.T) {
	store := New()
	ctx := context.Background()

	config := storage.DefaultQueueConfig("$queue/test")
	require.NoError(t, store.CreateQueue(ctx, config))

	// Message ready for retry (NextRetryAt in the past)
	msg := &storage.Message{
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

func TestMemoryMessageStore_Dequeue_RetryNotReady(t *testing.T) {
	store := New()
	ctx := context.Background()

	config := storage.DefaultQueueConfig("$queue/test")
	require.NoError(t, store.CreateQueue(ctx, config))

	// Message not ready for retry (NextRetryAt in the future)
	msg := &storage.Message{
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

func TestMemoryMessageStore_UpdateMessage(t *testing.T) {
	store := New()
	ctx := context.Background()

	config := storage.DefaultQueueConfig("$queue/test")
	require.NoError(t, store.CreateQueue(ctx, config))

	msg := &storage.Message{
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

func TestMemoryMessageStore_DeleteMessage(t *testing.T) {
	store := New()
	ctx := context.Background()

	config := storage.DefaultQueueConfig("$queue/test")
	require.NoError(t, store.CreateQueue(ctx, config))

	msg := &storage.Message{
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

func TestMemoryMessageStore_DeleteMessage_NotFound(t *testing.T) {
	store := New()
	ctx := context.Background()

	config := storage.DefaultQueueConfig("$queue/test")
	require.NoError(t, store.CreateQueue(ctx, config))

	err := store.DeleteMessage(ctx, "$queue/test", "nonexistent")
	assert.ErrorIs(t, err, storage.ErrMessageNotFound)
}

func TestMemoryMessageStore_GetMessage(t *testing.T) {
	store := New()
	ctx := context.Background()

	config := storage.DefaultQueueConfig("$queue/test")
	require.NoError(t, store.CreateQueue(ctx, config))

	// Get non-existent message
	_, err := store.GetMessage(ctx, "$queue/test", "nonexistent")
	assert.ErrorIs(t, err, storage.ErrMessageNotFound)

	// Create and get message
	msg := &storage.Message{
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

func TestMemoryMessageStore_GetNextSequence(t *testing.T) {
	store := New()
	ctx := context.Background()

	config := storage.DefaultQueueConfig("$queue/test")
	require.NoError(t, store.CreateQueue(ctx, config))

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

func TestMemoryMessageStore_ListQueued(t *testing.T) {
	store := New()
	ctx := context.Background()

	config := storage.DefaultQueueConfig("$queue/test")
	require.NoError(t, store.CreateQueue(ctx, config))

	// Enqueue messages in different states
	msg1 := &storage.Message{
		ID:          "msg-1",
		Payload:     []byte("test-1"),
		Topic:       "$queue/test",
		PartitionID: 0,
		Sequence:    1,
		State:       storage.StateQueued,
		CreatedAt:   time.Now(),
	}
	msg2 := &storage.Message{
		ID:          "msg-2",
		Payload:     []byte("test-2"),
		Topic:       "$queue/test",
		PartitionID: 0,
		Sequence:    2,
		State:       storage.StateRetry,
		CreatedAt:   time.Now(),
	}
	msg3 := &storage.Message{
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

func TestMemoryMessageStore_ListRetry(t *testing.T) {
	store := New()
	ctx := context.Background()

	config := storage.DefaultQueueConfig("$queue/test")
	require.NoError(t, store.CreateQueue(ctx, config))

	// Enqueue messages with different states
	msg1 := &storage.Message{
		ID:          "msg-1",
		Payload:     []byte("test-1"),
		Topic:       "$queue/test",
		PartitionID: 0,
		Sequence:    1,
		State:       storage.StateQueued,
		CreatedAt:   time.Now(),
	}
	msg2 := &storage.Message{
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

func TestMemoryMessageStore_MarkInflight(t *testing.T) {
	store := New()
	ctx := context.Background()

	config := storage.DefaultQueueConfig("$queue/test")
	require.NoError(t, store.CreateQueue(ctx, config))

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

func TestMemoryMessageStore_GetInflight(t *testing.T) {
	store := New()
	ctx := context.Background()

	config := storage.DefaultQueueConfig("$queue/test")
	require.NoError(t, store.CreateQueue(ctx, config))

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

func TestMemoryMessageStore_GetInflightMessage(t *testing.T) {
	store := New()
	ctx := context.Background()

	config := storage.DefaultQueueConfig("$queue/test")
	require.NoError(t, store.CreateQueue(ctx, config))

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

func TestMemoryMessageStore_RemoveInflight(t *testing.T) {
	store := New()
	ctx := context.Background()

	config := storage.DefaultQueueConfig("$queue/test")
	require.NoError(t, store.CreateQueue(ctx, config))

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

func TestMemoryMessageStore_EnqueueDLQ(t *testing.T) {
	store := New()
	ctx := context.Background()

	msg := &storage.Message{
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

func TestMemoryMessageStore_ListDLQ(t *testing.T) {
	store := New()
	ctx := context.Background()

	// Enqueue multiple DLQ messages
	msg1 := &storage.Message{
		ID:            "msg-1",
		Payload:       []byte("failed-1"),
		Topic:         "$queue/test",
		State:         storage.StateDLQ,
		FailureReason: "reason-1",
		CreatedAt:     time.Now(),
	}
	msg2 := &storage.Message{
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

func TestMemoryMessageStore_DeleteDLQMessage(t *testing.T) {
	store := New()
	ctx := context.Background()

	msg := &storage.Message{
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

func TestMemoryMessageStore_UpdateOffset(t *testing.T) {
	store := New()
	ctx := context.Background()

	config := storage.DefaultQueueConfig("$queue/test")
	require.NoError(t, store.CreateQueue(ctx, config))

	err := store.UpdateOffset(ctx, "$queue/test", 0, 100)
	require.NoError(t, err)

	// Verify offset
	offset, err := store.GetOffset(ctx, "$queue/test", 0)
	require.NoError(t, err)
	assert.Equal(t, uint64(100), offset)
}

func TestMemoryMessageStore_GetOffset(t *testing.T) {
	store := New()
	ctx := context.Background()

	config := storage.DefaultQueueConfig("$queue/test")
	require.NoError(t, store.CreateQueue(ctx, config))

	// Get offset for partition (should return 0 initially)
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

func TestMemoryConsumerStore_RegisterConsumer(t *testing.T) {
	store := New()
	ctx := context.Background()

	config := storage.DefaultQueueConfig("$queue/test")
	require.NoError(t, store.CreateQueue(ctx, config))

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

func TestMemoryConsumerStore_RegisterConsumer_NoQueue(t *testing.T) {
	store := New()
	ctx := context.Background()

	consumer := &storage.Consumer{
		ID:            "consumer-1",
		GroupID:       "group-1",
		QueueName:     "$queue/nonexistent",
		ClientID:      "client-1",
		AssignedParts: []int{0},
		LastHeartbeat: time.Now(),
	}

	// Should create consumer even without existing queue
	err := store.RegisterConsumer(ctx, consumer)
	require.NoError(t, err)
}

func TestMemoryConsumerStore_UnregisterConsumer(t *testing.T) {
	store := New()
	ctx := context.Background()

	config := storage.DefaultQueueConfig("$queue/test")
	require.NoError(t, store.CreateQueue(ctx, config))

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

func TestMemoryConsumerStore_GetConsumer(t *testing.T) {
	store := New()
	ctx := context.Background()

	config := storage.DefaultQueueConfig("$queue/test")
	require.NoError(t, store.CreateQueue(ctx, config))

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

func TestMemoryConsumerStore_ListConsumers(t *testing.T) {
	store := New()
	ctx := context.Background()

	config := storage.DefaultQueueConfig("$queue/test")
	require.NoError(t, store.CreateQueue(ctx, config))

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

func TestMemoryConsumerStore_ListGroups(t *testing.T) {
	store := New()
	ctx := context.Background()

	config := storage.DefaultQueueConfig("$queue/test")
	require.NoError(t, store.CreateQueue(ctx, config))

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

func TestMemoryConsumerStore_UpdateHeartbeat(t *testing.T) {
	store := New()
	ctx := context.Background()

	config := storage.DefaultQueueConfig("$queue/test")
	require.NoError(t, store.CreateQueue(ctx, config))

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

func TestMemoryStore_ConcurrentEnqueue(t *testing.T) {
	store := New()
	ctx := context.Background()

	config := storage.DefaultQueueConfig("$queue/test")
	require.NoError(t, store.CreateQueue(ctx, config))

	// Enqueue messages concurrently
	numGoroutines := 10
	messagesPerGoroutine := 10
	var wg sync.WaitGroup

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(routineID int) {
			defer wg.Done()
			for j := 0; j < messagesPerGoroutine; j++ {
				seq, _ := store.GetNextSequence(ctx, "$queue/test", routineID%3)
				msg := &storage.Message{
					ID:          string(rune(routineID*1000 + j)),
					Payload:     []byte("test"),
					Topic:       "$queue/test",
					PartitionID: routineID % 3,
					Sequence:    seq,
					State:       storage.StateQueued,
					CreatedAt:   time.Now(),
				}
				store.Enqueue(ctx, "$queue/test", msg)
			}
		}(i)
	}

	wg.Wait()

	// Verify messages were enqueued
	messages, err := store.ListQueued(ctx, "$queue/test", 0, 0)
	require.NoError(t, err)
	assert.Greater(t, len(messages), 0)
}

func TestMemoryStore_ConcurrentConsumerRegistration(t *testing.T) {
	store := New()
	ctx := context.Background()

	config := storage.DefaultQueueConfig("$queue/test")
	require.NoError(t, store.CreateQueue(ctx, config))

	// Register consumers concurrently
	numConsumers := 20
	var wg sync.WaitGroup

	for i := 0; i < numConsumers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			consumer := &storage.Consumer{
				ID:            string(rune(id)),
				GroupID:       "group-1",
				QueueName:     "$queue/test",
				ClientID:      string(rune(id)),
				AssignedParts: []int{id % 10},
				LastHeartbeat: time.Now(),
			}
			store.RegisterConsumer(ctx, consumer)
		}(i)
	}

	wg.Wait()

	// Verify consumers were registered
	consumers, err := store.ListConsumers(ctx, "$queue/test", "group-1")
	require.NoError(t, err)
	assert.Greater(t, len(consumers), 0)
}

// Error Handling Tests

func TestMemoryStore_ErrorHandling_NonExistentQueue(t *testing.T) {
	store := New()
	ctx := context.Background()

	// Various operations on non-existent queue
	_, err := store.GetQueue(ctx, "$queue/nonexistent")
	assert.ErrorIs(t, err, storage.ErrQueueNotFound)

	err = store.DeleteQueue(ctx, "$queue/nonexistent")
	assert.ErrorIs(t, err, storage.ErrQueueNotFound)

	msg := &storage.Message{
		ID:          "msg-1",
		Payload:     []byte("test"),
		Topic:       "$queue/nonexistent",
		PartitionID: 0,
		Sequence:    1,
		State:       storage.StateQueued,
		CreatedAt:   time.Now(),
	}
	err = store.Enqueue(ctx, "$queue/nonexistent", msg)
	assert.ErrorIs(t, err, storage.ErrQueueNotFound)

	_, err = store.Dequeue(ctx, "$queue/nonexistent", 0)
	assert.ErrorIs(t, err, storage.ErrQueueNotFound)

	_, err = store.GetMessage(ctx, "$queue/nonexistent", "msg-1")
	assert.ErrorIs(t, err, storage.ErrQueueNotFound)
}
