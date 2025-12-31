// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package queue

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	queueStorage "github.com/absmach/mqtt/queue/storage"
	"github.com/absmach/mqtt/queue/storage/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDLQManager_MoveToDLQ(t *testing.T) {
	ctx := context.Background()
	store := memory.New()
	dlqManager := NewDLQManager(store, &NoOpAlertHandler{})

	// Create queue
	config := queueStorage.DefaultQueueConfig("$queue/test")
	config.DLQConfig.Enabled = true
	config.DLQConfig.Topic = "$queue/dlq/test"

	err := store.CreateQueue(ctx, config)
	require.NoError(t, err)

	queue := NewQueue(config, store, store)

	// Create message
	msg := &queueStorage.QueueMessage{
		ID:          "msg-1",
		Payload:     []byte("test payload"),
		Topic:       "$queue/test",
		PartitionID: 0,
		Sequence:    1,
		State:       queueStorage.StateDelivered,
		RetryCount:  5,
		CreatedAt:   time.Now().Add(-1 * time.Hour),
	}
	err = store.Enqueue(ctx, queue.Name(), msg)
	require.NoError(t, err)

	// Move to DLQ
	err = dlqManager.MoveToDLQ(ctx, queue, msg, "max retries exceeded")
	require.NoError(t, err)

	// Verify message is in DLQ
	dlqMessages, err := store.ListDLQ(ctx, config.DLQConfig.Topic, 0)
	require.NoError(t, err)
	assert.Len(t, dlqMessages, 1)
	assert.Equal(t, "msg-1", dlqMessages[0].ID)
	assert.Equal(t, queueStorage.StateDLQ, dlqMessages[0].State)
	assert.Equal(t, "max retries exceeded", dlqMessages[0].FailureReason)
	assert.False(t, dlqMessages[0].MovedToDLQAt.IsZero())

	// Verify message removed from original queue
	_, err = store.GetMessage(ctx, queue.Name(), msg.ID)
	assert.Error(t, err)
}

func TestDLQManager_MoveToDLQ_DLQDisabled(t *testing.T) {
	ctx := context.Background()
	store := memory.New()
	dlqManager := NewDLQManager(store, &NoOpAlertHandler{})

	// Create queue with DLQ disabled
	config := queueStorage.DefaultQueueConfig("$queue/test")
	config.DLQConfig.Enabled = false

	err := store.CreateQueue(ctx, config)
	require.NoError(t, err)

	queue := NewQueue(config, store, store)

	// Create message
	msg := &queueStorage.QueueMessage{
		ID:          "msg-1",
		Payload:     []byte("test payload"),
		Topic:       "$queue/test",
		PartitionID: 0,
		Sequence:    1,
		State:       queueStorage.StateDelivered,
		RetryCount:  5,
		CreatedAt:   time.Now(),
	}
	err = store.Enqueue(ctx, queue.Name(), msg)
	require.NoError(t, err)

	// Move to DLQ (should just delete)
	err = dlqManager.MoveToDLQ(ctx, queue, msg, "test reason")
	require.NoError(t, err)

	// Verify message removed from original queue
	_, err = store.GetMessage(ctx, queue.Name(), msg.ID)
	assert.Error(t, err)

	// Verify no DLQ messages
	dlqMessages, err := store.ListDLQ(ctx, "$queue/dlq/test", 0)
	require.NoError(t, err)
	assert.Len(t, dlqMessages, 0)
}

func TestDLQManager_RetryFromDLQ(t *testing.T) {
	ctx := context.Background()
	store := memory.New()
	dlqManager := NewDLQManager(store, &NoOpAlertHandler{})

	// Create queue
	config := queueStorage.DefaultQueueConfig("$queue/test")
	config.DLQConfig.Enabled = true
	config.DLQConfig.Topic = "$queue/dlq/test"

	err := store.CreateQueue(ctx, config)
	require.NoError(t, err)

	queue := NewQueue(config, store, store)

	// Create message and move to DLQ
	msg := &queueStorage.QueueMessage{
		ID:            "msg-1",
		Payload:       []byte("test payload"),
		Topic:         "$queue/test",
		PartitionID:   0,
		Sequence:      1,
		State:         queueStorage.StateDLQ,
		RetryCount:    10,
		FailureReason: "max retries exceeded",
		CreatedAt:     time.Now().Add(-2 * time.Hour),
		MovedToDLQAt:  time.Now().Add(-1 * time.Hour),
	}
	err = store.EnqueueDLQ(ctx, config.DLQConfig.Topic, msg)
	require.NoError(t, err)

	// Retry from DLQ
	err = dlqManager.RetryFromDLQ(ctx, queue.Name(), msg.ID)
	require.NoError(t, err)

	// Verify message back in original queue with reset state
	retrieved, err := store.GetMessage(ctx, queue.Name(), msg.ID)
	require.NoError(t, err)
	assert.Equal(t, queueStorage.StateQueued, retrieved.State)
	assert.Equal(t, 0, retrieved.RetryCount)
	assert.True(t, retrieved.NextRetryAt.IsZero())
	assert.Equal(t, "", retrieved.FailureReason)

	// Verify removed from DLQ
	dlqMessages, err := store.ListDLQ(ctx, config.DLQConfig.Topic, 0)
	require.NoError(t, err)
	assert.Len(t, dlqMessages, 0)
}

func TestDLQManager_RetryFromDLQ_NotFound(t *testing.T) {
	ctx := context.Background()
	store := memory.New()
	dlqManager := NewDLQManager(store, &NoOpAlertHandler{})

	// Create queue
	config := queueStorage.DefaultQueueConfig("$queue/test")
	err := store.CreateQueue(ctx, config)
	require.NoError(t, err)

	// Try to retry non-existent message
	err = dlqManager.RetryFromDLQ(ctx, "$queue/test", "non-existent")
	assert.ErrorIs(t, err, queueStorage.ErrMessageNotFound)
}

func TestDLQManager_PurgeDLQ(t *testing.T) {
	ctx := context.Background()
	store := memory.New()
	dlqManager := NewDLQManager(store, &NoOpAlertHandler{})

	// Create queue
	config := queueStorage.DefaultQueueConfig("$queue/test")
	config.DLQConfig.Topic = "$queue/dlq/test"
	err := store.CreateQueue(ctx, config)
	require.NoError(t, err)

	// Add multiple messages to DLQ
	for i := 0; i < 5; i++ {
		msg := &queueStorage.QueueMessage{
			ID:           fmt.Sprintf("msg-%d", i),
			Payload:      []byte("test"),
			Topic:        "$queue/test",
			State:        queueStorage.StateDLQ,
			MovedToDLQAt: time.Now(),
		}
		err = store.EnqueueDLQ(ctx, config.DLQConfig.Topic, msg)
		require.NoError(t, err)
	}

	// Verify 5 messages in DLQ
	dlqMessages, err := store.ListDLQ(ctx, config.DLQConfig.Topic, 0)
	require.NoError(t, err)
	assert.Len(t, dlqMessages, 5)

	// Purge DLQ
	count, err := dlqManager.PurgeDLQ(ctx, "$queue/test")
	require.NoError(t, err)
	assert.Equal(t, 5, count)

	// Verify DLQ is empty
	dlqMessages, err = store.ListDLQ(ctx, config.DLQConfig.Topic, 0)
	require.NoError(t, err)
	assert.Len(t, dlqMessages, 0)
}

func TestDLQManager_GetDLQStats(t *testing.T) {
	ctx := context.Background()
	store := memory.New()
	dlqManager := NewDLQManager(store, &NoOpAlertHandler{})

	// Create queue
	config := queueStorage.DefaultQueueConfig("$queue/test")
	config.DLQConfig.Topic = "$queue/dlq/test"
	err := store.CreateQueue(ctx, config)
	require.NoError(t, err)

	// Add messages with different failure reasons
	reasons := []string{"max retries", "max retries", "timeout", "timeout", "timeout"}
	for i, reason := range reasons {
		msg := &queueStorage.QueueMessage{
			ID:            fmt.Sprintf("msg-%d", i),
			Payload:       []byte("test"),
			Topic:         "$queue/test",
			State:         queueStorage.StateDLQ,
			FailureReason: reason,
			MovedToDLQAt:  time.Now(),
		}
		err = store.EnqueueDLQ(ctx, config.DLQConfig.Topic, msg)
		require.NoError(t, err)
	}

	// Get stats
	stats, err := dlqManager.GetDLQStats(ctx, "$queue/test")
	require.NoError(t, err)
	assert.Equal(t, "$queue/test", stats.QueueName)
	assert.Equal(t, "$queue/dlq/test", stats.DLQTopic)
	assert.Equal(t, int64(5), stats.TotalMessages)
	assert.Equal(t, int64(2), stats.ByReason["max retries"])
	assert.Equal(t, int64(3), stats.ByReason["timeout"])
}

func TestHTTPAlertHandler_Send(t *testing.T) {
	// Create test server to receive webhook
	var receivedAlert *DLQAlert
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var alert DLQAlert
		err := json.NewDecoder(r.Body).Decode(&alert)
		require.NoError(t, err)
		receivedAlert = &alert
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	handler := NewHTTPAlertHandler(5 * time.Second)

	alert := &DLQAlert{
		QueueName:     "$queue/test",
		DLQTopic:      "$queue/dlq/test",
		MessageID:     "msg-1",
		FailureReason: "max retries exceeded",
		RetryCount:    10,
		FirstAttempt:  time.Now().Add(-2 * time.Hour),
		MovedToDLQAt:  time.Now(),
		TotalDuration: 2 * time.Hour,
	}

	err := handler.Send(server.URL, alert)
	require.NoError(t, err)

	// Verify alert was received
	require.NotNil(t, receivedAlert)
	assert.Equal(t, alert.QueueName, receivedAlert.QueueName)
	assert.Equal(t, alert.MessageID, receivedAlert.MessageID)
	assert.Equal(t, alert.FailureReason, receivedAlert.FailureReason)
}

func TestHTTPAlertHandler_Send_Error(t *testing.T) {
	// Create test server that returns error
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	handler := NewHTTPAlertHandler(5 * time.Second)

	alert := &DLQAlert{
		QueueName: "$queue/test",
		MessageID: "msg-1",
	}

	err := handler.Send(server.URL, alert)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "webhook returned error status")
}

func TestNoOpAlertHandler(t *testing.T) {
	handler := &NoOpAlertHandler{}

	alert := &DLQAlert{
		QueueName: "$queue/test",
		MessageID: "msg-1",
	}

	// Should not error
	err := handler.Send("http://nowhere.test", alert)
	assert.NoError(t, err)
}
