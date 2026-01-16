// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package queue

import (
	"context"
	"fmt"
	"testing"
	"time"

	queueStorage "github.com/absmach/fluxmq/queue/storage"
	"github.com/absmach/fluxmq/queue/storage/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRetryManager_CalculateBackoff(t *testing.T) {
	store := memory.New()
	dlqManager := NewDLQManager(store, &NoOpAlertHandler{})
	rm := NewRetryManager(store, dlqManager)

	policy := queueStorage.RetryPolicy{
		InitialBackoff:    5 * time.Second,
		MaxBackoff:        5 * time.Minute,
		BackoffMultiplier: 2.0,
	}

	tests := []struct {
		name       string
		retryCount int
		expected   time.Duration
	}{
		{"First retry", 1, 5 * time.Second},
		{"Second retry", 2, 10 * time.Second},
		{"Third retry", 3, 20 * time.Second},
		{"Fourth retry", 4, 40 * time.Second},
		{"Fifth retry", 5, 80 * time.Second},
		{"Sixth retry", 6, 160 * time.Second},
		{"Seventh retry (capped)", 7, 5 * time.Minute}, // Capped at max
		{"Eighth retry (capped)", 8, 5 * time.Minute},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			backoff := rm.calculateBackoff(tt.retryCount, policy)
			assert.Equal(t, tt.expected, backoff)
		})
	}
}

func TestRetryManager_InflightTimeout(t *testing.T) {
	ctx := context.Background()
	store := memory.New()
	dlqManager := NewDLQManager(store, &NoOpAlertHandler{})
	rm := NewRetryManager(store, dlqManager)
	rm.checkInterval = 100 * time.Millisecond

	// Create queue
	config := queueStorage.DefaultQueueConfig("$queue/test")
	config.RetryPolicy.InitialBackoff = 1 * time.Second
	config.DeliveryTimeout = 200 * time.Millisecond // Short timeout for testing

	err := store.CreateQueue(ctx, config)
	require.NoError(t, err)

	queue := NewQueue(config, store, store)
	rm.RegisterQueue(queue)

	// Enqueue a message
	msg := &queueStorage.Message{
		ID:          "msg-1",
		Payload:     []byte("test"),
		Topic:       "$queue/test",
		PartitionID: 0,
		Sequence:    1,
		State:       queueStorage.StateQueued,
		CreatedAt:   time.Now(),
	}
	err = store.Enqueue(ctx, queue.Name(), msg)
	require.NoError(t, err)

	// Mark as inflight with short timeout
	deliveryState := &queueStorage.DeliveryState{
		MessageID:   msg.ID,
		QueueName:   queue.Name(),
		PartitionID: 0,
		ConsumerID:  "consumer-1",
		DeliveredAt: time.Now(),
		Timeout:     time.Now().Add(200 * time.Millisecond),
		RetryCount:  0,
	}
	err = store.MarkInflight(ctx, deliveryState)
	require.NoError(t, err)

	// Wait for timeout
	time.Sleep(300 * time.Millisecond)

	// Check inflight timeouts
	rm.checkInflightTimeouts(ctx)

	// Message should be in retry state
	retrieved, err := store.GetMessage(ctx, queue.Name(), msg.ID)
	require.NoError(t, err)
	assert.Equal(t, queueStorage.StateRetry, retrieved.State)
	assert.Equal(t, 1, retrieved.RetryCount)
	assert.True(t, retrieved.NextRetryAt.After(time.Now()))

	// Should be removed from inflight
	inflight, err := store.GetInflight(ctx, queue.Name())
	require.NoError(t, err)
	assert.Len(t, inflight, 0)
}

func TestRetryManager_MaxRetriesExceeded(t *testing.T) {
	ctx := context.Background()
	store := memory.New()
	dlqManager := NewDLQManager(store, &NoOpAlertHandler{})
	rm := NewRetryManager(store, dlqManager)

	// Create queue with low max retries
	config := queueStorage.DefaultQueueConfig("$queue/test")
	config.RetryPolicy.MaxRetries = 3
	config.DLQConfig.Enabled = true

	err := store.CreateQueue(ctx, config)
	require.NoError(t, err)

	queue := NewQueue(config, store, store)
	rm.RegisterQueue(queue)

	// Create message that has already been retried max times
	msg := &queueStorage.Message{
		ID:          "msg-1",
		Payload:     []byte("test"),
		Topic:       "$queue/test",
		PartitionID: 0,
		Sequence:    1,
		State:       queueStorage.StateDelivered,
		RetryCount:  3, // Already at max
		CreatedAt:   time.Now().Add(-1 * time.Hour),
	}
	err = store.Enqueue(ctx, queue.Name(), msg)
	require.NoError(t, err)

	// Mark as inflight
	deliveryState := &queueStorage.DeliveryState{
		MessageID:   msg.ID,
		QueueName:   queue.Name(),
		PartitionID: 0,
		ConsumerID:  "consumer-1",
		DeliveredAt: time.Now(),
		Timeout:     time.Now().Add(-1 * time.Second), // Already timed out
		RetryCount:  3,
	}
	err = store.MarkInflight(ctx, deliveryState)
	require.NoError(t, err)

	// Process timeout
	rm.handleInflightTimeout(ctx, queue, deliveryState)

	// Message should be moved to DLQ
	dlqTopic := "$queue/dlq/test"
	dlqMessages, err := store.ListDLQ(ctx, dlqTopic, 0)
	require.NoError(t, err)
	assert.Len(t, dlqMessages, 1)
	assert.Equal(t, "msg-1", dlqMessages[0].ID)
	assert.Equal(t, queueStorage.StateDLQ, dlqMessages[0].State)
	assert.Equal(t, "max retries exceeded", dlqMessages[0].FailureReason)

	// Should be removed from original queue
	_, err = store.GetMessage(ctx, queue.Name(), msg.ID)
	assert.Error(t, err)
}

func TestRetryManager_TotalTimeoutExceeded(t *testing.T) {
	ctx := context.Background()
	store := memory.New()
	dlqManager := NewDLQManager(store, &NoOpAlertHandler{})
	rm := NewRetryManager(store, dlqManager)

	// Create queue with short total timeout
	config := queueStorage.DefaultQueueConfig("$queue/test")
	config.RetryPolicy.MaxRetries = 10
	config.RetryPolicy.TotalTimeout = 1 * time.Hour
	config.DLQConfig.Enabled = true

	err := store.CreateQueue(ctx, config)
	require.NoError(t, err)

	queue := NewQueue(config, store, store)
	rm.RegisterQueue(queue)

	// Create message created 2 hours ago (exceeds total timeout)
	msg := &queueStorage.Message{
		ID:          "msg-1",
		Payload:     []byte("test"),
		Topic:       "$queue/test",
		PartitionID: 0,
		Sequence:    1,
		State:       queueStorage.StateDelivered,
		RetryCount:  2,
		CreatedAt:   time.Now().Add(-2 * time.Hour),
	}
	err = store.Enqueue(ctx, queue.Name(), msg)
	require.NoError(t, err)

	// Process retry
	rm.processRetry(ctx, queue, msg)

	// Message should be moved to DLQ due to total timeout
	dlqTopic := "$queue/dlq/test"
	dlqMessages, err := store.ListDLQ(ctx, dlqTopic, 0)
	require.NoError(t, err)
	assert.Len(t, dlqMessages, 1)
	assert.Equal(t, "total timeout exceeded", dlqMessages[0].FailureReason)
}

func TestRetryManager_RetryScheduling(t *testing.T) {
	ctx := context.Background()
	store := memory.New()
	dlqManager := NewDLQManager(store, &NoOpAlertHandler{})
	rm := NewRetryManager(store, dlqManager)
	rm.checkInterval = 100 * time.Millisecond

	// Create queue
	config := queueStorage.DefaultQueueConfig("$queue/test")
	config.RetryPolicy.InitialBackoff = 200 * time.Millisecond

	err := store.CreateQueue(ctx, config)
	require.NoError(t, err)

	queue := NewQueue(config, store, store)
	rm.RegisterQueue(queue)

	// Create message in retry state scheduled for near future
	msg := &queueStorage.Message{
		ID:          "msg-1",
		Payload:     []byte("test"),
		Topic:       "$queue/test",
		PartitionID: 0,
		Sequence:    1,
		State:       queueStorage.StateRetry,
		RetryCount:  1,
		CreatedAt:   time.Now(),
		NextRetryAt: time.Now().Add(150 * time.Millisecond),
	}
	err = store.Enqueue(ctx, queue.Name(), msg)
	require.NoError(t, err)

	// Check retry schedule too early
	rm.checkRetrySchedule(ctx)

	// Message should still be in retry state (not ready yet)
	retrieved, err := store.GetMessage(ctx, queue.Name(), msg.ID)
	require.NoError(t, err)
	assert.Equal(t, queueStorage.StateRetry, retrieved.State)
	assert.Equal(t, 1, retrieved.RetryCount) // Still 1

	// Wait for retry time
	time.Sleep(200 * time.Millisecond)

	// Check retry schedule again
	rm.checkRetrySchedule(ctx)

	// Message should now be processed for retry (retry count incremented)
	retrieved, err = store.GetMessage(ctx, queue.Name(), msg.ID)
	require.NoError(t, err)
	assert.Equal(t, 2, retrieved.RetryCount) // Incremented to 2
}

func TestRetryManager_GetStats(t *testing.T) {
	ctx := context.Background()
	store := memory.New()
	dlqManager := NewDLQManager(store, &NoOpAlertHandler{})
	rm := NewRetryManager(store, dlqManager)

	// Create queue
	config := queueStorage.DefaultQueueConfig("$queue/test")
	err := store.CreateQueue(ctx, config)
	require.NoError(t, err)

	queue := NewQueue(config, store, store)
	rm.RegisterQueue(queue)

	// Add some inflight messages
	for i := 0; i < 3; i++ {
		deliveryState := &queueStorage.DeliveryState{
			MessageID:   fmt.Sprintf("msg-%d", i),
			QueueName:   queue.Name(),
			PartitionID: 0,
			ConsumerID:  "consumer-1",
			DeliveredAt: time.Now(),
			Timeout:     time.Now().Add(30 * time.Second),
		}
		err = store.MarkInflight(ctx, deliveryState)
		require.NoError(t, err)
	}

	// Add some retry messages
	for i := 0; i < 2; i++ {
		msg := &queueStorage.Message{
			ID:          fmt.Sprintf("retry-msg-%d", i),
			Payload:     []byte("test"),
			Topic:       "$queue/test",
			PartitionID: 0,
			Sequence:    uint64(i + 10),
			State:       queueStorage.StateRetry,
			RetryCount:  1,
			CreatedAt:   time.Now(),
			NextRetryAt: time.Now().Add(5 * time.Second),
		}
		err = store.Enqueue(ctx, queue.Name(), msg)
		require.NoError(t, err)
	}

	// Get stats
	stats, err := rm.GetStats(ctx, queue.Name())
	require.NoError(t, err)
	assert.Equal(t, queue.Name(), stats.QueueName)
	assert.Equal(t, int64(3), stats.InflightCount)
	assert.Equal(t, int64(2), stats.RetryingCount)
}

func TestRetryManager_RegisterUnregisterQueue(t *testing.T) {
	store := memory.New()
	dlqManager := NewDLQManager(store, &NoOpAlertHandler{})
	rm := NewRetryManager(store, dlqManager)

	config := queueStorage.DefaultQueueConfig("$queue/test")
	queue := NewQueue(config, store, store)

	// Register queue
	rm.RegisterQueue(queue)
	rm.mu.RLock()
	_, exists := rm.queues[queue.Name()]
	rm.mu.RUnlock()
	assert.True(t, exists)

	// Unregister queue
	rm.UnregisterQueue(queue.Name())
	rm.mu.RLock()
	_, exists = rm.queues[queue.Name()]
	rm.mu.RUnlock()
	assert.False(t, exists)
}
