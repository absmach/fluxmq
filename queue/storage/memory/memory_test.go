// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package memory

import (
	"context"
	"testing"
	"time"

	"github.com/absmach/mqtt/queue/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestQueueStore(t *testing.T) {
	store := New()
	ctx := context.Background()

	t.Run("CreateQueue", func(t *testing.T) {
		config := storage.DefaultQueueConfig("$queue/test")
		err := store.CreateQueue(ctx, config)
		require.NoError(t, err)

		// Verify queue was created
		retrieved, err := store.GetQueue(ctx, "$queue/test")
		require.NoError(t, err)
		assert.Equal(t, config.Name, retrieved.Name)
		assert.Equal(t, config.Partitions, retrieved.Partitions)
	})

	t.Run("DuplicateQueueError", func(t *testing.T) {
		config := storage.DefaultQueueConfig("$queue/duplicate")
		err := store.CreateQueue(ctx, config)
		require.NoError(t, err)

		// Try to create again
		err = store.CreateQueue(ctx, config)
		assert.ErrorIs(t, err, storage.ErrQueueAlreadyExists)
	})

	t.Run("ListQueues", func(t *testing.T) {
		queues, err := store.ListQueues(ctx)
		require.NoError(t, err)
		assert.GreaterOrEqual(t, len(queues), 2) // At least test and duplicate queues
	})
}

func TestMessageStore(t *testing.T) {
	store := New()
	ctx := context.Background()

	// Create a queue first
	config := storage.DefaultQueueConfig("$queue/messages")
	err := store.CreateQueue(ctx, config)
	require.NoError(t, err)

	t.Run("EnqueueAndDequeue", func(t *testing.T) {
		msg := &storage.QueueMessage{
			ID:          "msg-1",
			Payload:     []byte("test payload"),
			Topic:       "$queue/messages",
			PartitionID: 0,
			Sequence:    1,
			State:       storage.StateQueued,
			CreatedAt:   time.Now(),
			Properties:  map[string]string{"key": "value"},
		}

		// Enqueue
		err := store.Enqueue(ctx, "$queue/messages", msg)
		require.NoError(t, err)

		// Dequeue
		dequeued, err := store.Dequeue(ctx, "$queue/messages", 0)
		require.NoError(t, err)
		require.NotNil(t, dequeued)
		assert.Equal(t, msg.ID, dequeued.ID)
		assert.Equal(t, msg.Payload, dequeued.Payload)
		assert.Equal(t, msg.Properties["key"], "value")
	})

	t.Run("GetNextSequence", func(t *testing.T) {
		seq1, err := store.GetNextSequence(ctx, "$queue/messages", 0)
		require.NoError(t, err)
		assert.Equal(t, uint64(1), seq1)

		seq2, err := store.GetNextSequence(ctx, "$queue/messages", 0)
		require.NoError(t, err)
		assert.Equal(t, uint64(2), seq2)
	})

	t.Run("InflightTracking", func(t *testing.T) {
		state := &storage.DeliveryState{
			MessageID:   "msg-2",
			QueueName:   "$queue/messages",
			PartitionID: 0,
			ConsumerID:  "consumer-1",
			DeliveredAt: time.Now(),
			Timeout:     time.Now().Add(30 * time.Second),
			RetryCount:  0,
		}

		// Mark inflight
		err := store.MarkInflight(ctx, state)
		require.NoError(t, err)

		// Get inflight
		inflight, err := store.GetInflight(ctx, "$queue/messages")
		require.NoError(t, err)
		assert.Len(t, inflight, 1)
		assert.Equal(t, "msg-2", inflight[0].MessageID)

		// Remove inflight
		err = store.RemoveInflight(ctx, "$queue/messages", "msg-2")
		require.NoError(t, err)

		// Verify removed
		inflight, err = store.GetInflight(ctx, "$queue/messages")
		require.NoError(t, err)
		assert.Len(t, inflight, 0)
	})
}

func TestConsumerStore(t *testing.T) {
	store := New()
	ctx := context.Background()

	// Create a queue first
	config := storage.DefaultQueueConfig("$queue/consumers")
	err := store.CreateQueue(ctx, config)
	require.NoError(t, err)

	t.Run("RegisterAndListConsumers", func(t *testing.T) {
		consumer := &storage.Consumer{
			ID:            "consumer-1",
			ClientID:      "client-1",
			GroupID:       "group-1",
			QueueName:     "$queue/consumers",
			RegisteredAt:  time.Now(),
			LastHeartbeat: time.Now(),
		}

		// Register
		err := store.RegisterConsumer(ctx, consumer)
		require.NoError(t, err)

		// List consumers
		consumers, err := store.ListConsumers(ctx, "$queue/consumers", "group-1")
		require.NoError(t, err)
		assert.Len(t, consumers, 1)
		assert.Equal(t, "consumer-1", consumers[0].ID)
	})

	t.Run("UpdateHeartbeat", func(t *testing.T) {
		newTime := time.Now().Add(5 * time.Second)
		err := store.UpdateHeartbeat(ctx, "$queue/consumers", "group-1", "consumer-1", newTime)
		require.NoError(t, err)

		// Verify
		consumer, err := store.GetConsumer(ctx, "$queue/consumers", "group-1", "consumer-1")
		require.NoError(t, err)
		assert.WithinDuration(t, newTime, consumer.LastHeartbeat, time.Second)
	})

	t.Run("UnregisterConsumer", func(t *testing.T) {
		err := store.UnregisterConsumer(ctx, "$queue/consumers", "group-1", "consumer-1")
		require.NoError(t, err)

		// Verify removed
		consumers, err := store.ListConsumers(ctx, "$queue/consumers", "group-1")
		require.NoError(t, err)
		assert.Len(t, consumers, 0)
	})
}
