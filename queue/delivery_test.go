// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package queue

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/absmach/mqtt/queue/delivery"
	queueStorage "github.com/absmach/mqtt/queue/storage"
	"github.com/absmach/mqtt/queue/storage/memory"
	brokerStorage "github.com/absmach/mqtt/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// MockBrokerWithError extends MockBroker to simulate delivery errors
type MockBrokerWithError struct {
	deliveries    map[string][]interface{}
	deliveryError error
	mu            sync.Mutex
}

func NewMockBrokerWithError() *MockBrokerWithError {
	return &MockBrokerWithError{
		deliveries: make(map[string][]interface{}),
	}
}

func (m *MockBrokerWithError) DeliverToSession(ctx context.Context, clientID string, msg interface{}) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.deliveryError != nil {
		return m.deliveryError
	}

	m.deliveries[clientID] = append(m.deliveries[clientID], msg)
	return nil
}

func (m *MockBrokerWithError) SetDeliveryError(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.deliveryError = err
}

func (m *MockBrokerWithError) GetDeliveries(clientID string) []interface{} {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.deliveries[clientID]
}

func (m *MockBrokerWithError) ClearDeliveries() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.deliveries = make(map[string][]interface{})
}

func TestNewDeliveryWorker(t *testing.T) {
	store := memory.New()
	config := queueStorage.DefaultQueueConfig("$queue/test")
	queue := NewQueue(config, store, store)
	broker := NewMockBrokerWithError()

	worker := delivery.NewWorker(queue, store, broker.DeliverToSession, nil, "local", delivery.ProxyMode, nil, nil)

	assert.NotNil(t, worker)
	assert.NotNil(t, worker)
}

func TestDeliveryWorker_StartStop(t *testing.T) {
	store := memory.New()
	config := queueStorage.DefaultQueueConfig("$queue/test")
	queue := NewQueue(config, store, store)
	broker := NewMockBrokerWithError()

	worker := delivery.NewWorker(queue, store, broker.DeliverToSession, nil, "local", delivery.ProxyMode, nil, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start worker in background
	done := make(chan struct{})
	go func() {
		worker.Start(ctx)
		close(done)
	}()

	// Give it a moment to start
	time.Sleep(50 * time.Millisecond)

	// Stop worker
	worker.Stop()

	// Wait for worker to finish
	select {
	case <-done:
		// Success
	case <-time.After(1 * time.Second):
		t.Fatal("Worker did not stop in time")
	}
}

func TestDeliveryWorker_StartStopViaContext(t *testing.T) {
	store := memory.New()
	config := queueStorage.DefaultQueueConfig("$queue/test")
	queue := NewQueue(config, store, store)
	broker := NewMockBrokerWithError()

	worker := delivery.NewWorker(queue, store, broker.DeliverToSession, nil, "local", delivery.ProxyMode, nil, nil)

	ctx, cancel := context.WithCancel(context.Background())

	// Start worker in background
	done := make(chan struct{})
	go func() {
		worker.Start(ctx)
		close(done)
	}()

	// Give it a moment to start
	time.Sleep(50 * time.Millisecond)

	// Stop via context cancellation
	cancel()

	// Wait for worker to finish
	select {
	case <-done:
		// Success
	case <-time.After(1 * time.Second):
		t.Fatal("Worker did not stop in time")
	}
}

func TestDeliveryWorker_DeliverMessages(t *testing.T) {
	ctx := context.Background()
	store := memory.New()
	config := queueStorage.DefaultQueueConfig("$queue/test")
	config.Partitions = 2
	err := store.CreateQueue(ctx, config)
	require.NoError(t, err)

	queue := NewQueue(config, store, store)
	broker := NewMockBrokerWithError()

	// Add consumer
	err = queue.AddConsumer(ctx, "group1", "consumer1", "client1", "")
	require.NoError(t, err)

	// Enqueue message with partition key
	props := map[string]string{"partition-key": "test-key"}
	partitionID := queue.GetPartitionForMessage("test-key")
	msg := &queueStorage.Message{
		ID:          "msg-1",
		Topic:       "$queue/test",
		Payload:     []byte("test payload"),
		Properties:  props,
		PartitionID: partitionID,
		Sequence:    1,
		State:       queueStorage.StateQueued,
		CreatedAt:   time.Now(),
	}
	err = store.Enqueue(ctx, "$queue/test", msg)
	require.NoError(t, err)

	// Create and run delivery worker
	worker := delivery.NewWorker(queue, store, broker.DeliverToSession, nil, "local", delivery.ProxyMode, nil, nil)
	worker.DeliverMessages(ctx)

	// Verify message was delivered
	deliveries := broker.GetDeliveries("client1")
	assert.Len(t, deliveries, 1)

	// Verify message state changed to delivered
	retrieved, err := store.GetMessage(ctx, "$queue/test", "msg-1")
	require.NoError(t, err)
	assert.Equal(t, queueStorage.StateDelivered, retrieved.State)

	// Verify marked as inflight
	inflight, err := store.GetInflight(ctx, "$queue/test")
	require.NoError(t, err)
	assert.Len(t, inflight, 1)
}

func TestDeliveryWorker_DeliverMessages_MultiplePartitions(t *testing.T) {
	ctx := context.Background()
	store := memory.New()
	config := queueStorage.DefaultQueueConfig("$queue/test")
	config.Partitions = 3
	err := store.CreateQueue(ctx, config)
	require.NoError(t, err)

	queue := NewQueue(config, store, store)
	broker := NewMockBrokerWithError()

	// Add consumer
	err = queue.AddConsumer(ctx, "group1", "consumer1", "client1", "")
	require.NoError(t, err)

	// Enqueue messages to different partitions
	for i := 0; i < 3; i++ {
		msg := &queueStorage.Message{
			ID:          "msg-" + string(rune('0'+i)),
			Topic:       "$queue/test",
			Payload:     []byte("test payload " + string(rune('0'+i))),
			PartitionID: i,
			Sequence:    uint64(i + 1),
			State:       queueStorage.StateQueued,
			CreatedAt:   time.Now(),
		}
		err = store.Enqueue(ctx, "$queue/test", msg)
		require.NoError(t, err)
	}

	// Create and run delivery worker
	worker := delivery.NewWorker(queue, store, broker.DeliverToSession, nil, "local", delivery.ProxyMode, nil, nil)
	worker.DeliverMessages(ctx)

	// Verify all messages were delivered
	deliveries := broker.GetDeliveries("client1")
	assert.Len(t, deliveries, 3)
}

func TestDeliveryWorker_DeliverMessages_MultipleGroups(t *testing.T) {
	ctx := context.Background()
	store := memory.New()
	config := queueStorage.DefaultQueueConfig("$queue/test")
	config.Partitions = 2
	err := store.CreateQueue(ctx, config)
	require.NoError(t, err)

	queue := NewQueue(config, store, store)
	broker := NewMockBrokerWithError()

	// Add consumers to different groups
	err = queue.AddConsumer(ctx, "group1", "consumer1", "client1", "")
	require.NoError(t, err)
	err = queue.AddConsumer(ctx, "group2", "consumer2", "client2", "")
	require.NoError(t, err)

	// Enqueue two messages with same partition key to ensure they go to same partition
	props := map[string]string{"partition-key": "test-key"}
	partitionID := queue.GetPartitionForMessage("test-key")

	for i := 0; i < 2; i++ {
		msg := &queueStorage.Message{
			ID:          "msg-" + string(rune('1'+i)),
			Topic:       "$queue/test",
			Payload:     []byte("test payload " + string(rune('1'+i))),
			Properties:  props,
			PartitionID: partitionID,
			Sequence:    uint64(i + 1),
			State:       queueStorage.StateQueued,
			CreatedAt:   time.Now(),
		}
		err = store.Enqueue(ctx, "$queue/test", msg)
		require.NoError(t, err)
	}

	// Create and run delivery worker
	worker := delivery.NewWorker(queue, store, broker.DeliverToSession, nil, "local", delivery.ProxyMode, nil, nil)
	worker.DeliverMessages(ctx)

	// Verify messages were delivered (each group gets one message)
	deliveries1 := broker.GetDeliveries("client1")
	deliveries2 := broker.GetDeliveries("client2")

	// Both groups should receive a message
	assert.Len(t, deliveries1, 1)
	assert.Len(t, deliveries2, 1)
}

func TestDeliveryWorker_DeliverMessages_UnassignedPartition(t *testing.T) {
	ctx := context.Background()
	store := memory.New()
	config := queueStorage.DefaultQueueConfig("$queue/test")
	config.Partitions = 2
	err := store.CreateQueue(ctx, config)
	require.NoError(t, err)

	queue := NewQueue(config, store, store)
	broker := NewMockBrokerWithError()

	// Enqueue message but don't assign any consumer
	msg := &queueStorage.Message{
		ID:          "msg-1",
		Topic:       "$queue/test",
		Payload:     []byte("test payload"),
		PartitionID: 0,
		Sequence:    1,
		State:       queueStorage.StateQueued,
		CreatedAt:   time.Now(),
	}
	err = store.Enqueue(ctx, "$queue/test", msg)
	require.NoError(t, err)

	// Create and run delivery worker
	worker := delivery.NewWorker(queue, store, broker.DeliverToSession, nil, "local", delivery.ProxyMode, nil, nil)
	worker.DeliverMessages(ctx)

	// Verify message was NOT delivered (no consumers)
	deliveries := broker.GetDeliveries("client1")
	assert.Len(t, deliveries, 0)

	// Message should still be queued
	retrieved, err := store.GetMessage(ctx, "$queue/test", "msg-1")
	require.NoError(t, err)
	assert.Equal(t, queueStorage.StateQueued, retrieved.State)
}

func TestDeliveryWorker_DeliverNext_BrokerDeliveryError(t *testing.T) {
	ctx := context.Background()
	store := memory.New()
	config := queueStorage.DefaultQueueConfig("$queue/test")
	config.Partitions = 1
	err := store.CreateQueue(ctx, config)
	require.NoError(t, err)

	queue := NewQueue(config, store, store)
	broker := NewMockBrokerWithError()

	// Add consumer
	err = queue.AddConsumer(ctx, "group1", "consumer1", "client1", "")
	require.NoError(t, err)

	// Enqueue message
	msg := &queueStorage.Message{
		ID:          "msg-1",
		Topic:       "$queue/test",
		Payload:     []byte("test payload"),
		PartitionID: 0,
		Sequence:    1,
		State:       queueStorage.StateQueued,
		CreatedAt:   time.Now(),
	}
	err = store.Enqueue(ctx, "$queue/test", msg)
	require.NoError(t, err)

	// Set broker to fail delivery
	broker.SetDeliveryError(errors.New("broker delivery failed"))

	// Create and run delivery worker
	worker := delivery.NewWorker(queue, store, broker.DeliverToSession, nil, "local", delivery.ProxyMode, nil, nil)
	worker.DeliverMessages(ctx)

	// Message should still be marked as inflight despite broker error
	inflight, err := store.GetInflight(ctx, "$queue/test")
	require.NoError(t, err)
	assert.Len(t, inflight, 1)

	// Message state should be delivered (state updated before broker delivery)
	retrieved, err := store.GetMessage(ctx, "$queue/test", "msg-1")
	require.NoError(t, err)
	assert.Equal(t, queueStorage.StateDelivered, retrieved.State)
}

func TestDeliveryWorker_DeliverNext_NoMessages(t *testing.T) {
	ctx := context.Background()
	store := memory.New()
	config := queueStorage.DefaultQueueConfig("$queue/test")
	config.Partitions = 1
	err := store.CreateQueue(ctx, config)
	require.NoError(t, err)

	queue := NewQueue(config, store, store)
	broker := NewMockBrokerWithError()

	// Add consumer but no messages
	err = queue.AddConsumer(ctx, "group1", "consumer1", "client1", "")
	require.NoError(t, err)

	// Create and run delivery worker
	worker := delivery.NewWorker(queue, store, broker.DeliverToSession, nil, "local", delivery.ProxyMode, nil, nil)
	worker.DeliverMessages(ctx)

	// No deliveries should occur
	deliveries := broker.GetDeliveries("client1")
	assert.Len(t, deliveries, 0)
}

func TestDeliveryWorker_OrderingEnforcement(t *testing.T) {
	ctx := context.Background()
	store := memory.New()
	config := queueStorage.DefaultQueueConfig("$queue/test")
	config.Partitions = 1
	config.Ordering = queueStorage.OrderingStrict
	config.BatchSize = 1 // Use small batch size to test single-message delivery
	err := store.CreateQueue(ctx, config)
	require.NoError(t, err)

	queue := NewQueue(config, store, store)
	broker := NewMockBrokerWithError()

	// Add consumer
	err = queue.AddConsumer(ctx, "group1", "consumer1", "client1", "")
	require.NoError(t, err)

	// Enqueue messages in order
	for i := 0; i < 3; i++ {
		msg := &queueStorage.Message{
			ID:          "msg-" + string(rune('0'+i)),
			Topic:       "$queue/test",
			Payload:     []byte("test payload " + string(rune('0'+i))),
			PartitionID: 0,
			Sequence:    uint64(i + 1),
			State:       queueStorage.StateQueued,
			CreatedAt:   time.Now(),
		}
		err = store.Enqueue(ctx, "$queue/test", msg)
		require.NoError(t, err)
	}

	// Create and run delivery worker
	worker := delivery.NewWorker(queue, store, broker.DeliverToSession, nil, "local", delivery.ProxyMode, nil, nil)

	// Deliver first message
	worker.DeliverMessages(ctx)
	deliveries := broker.GetDeliveries("client1")
	assert.Len(t, deliveries, 1)

	// Deliver second message - strict ordering ensures they're delivered in sequence order
	worker.DeliverMessages(ctx)
	deliveries = broker.GetDeliveries("client1")
	assert.Len(t, deliveries, 2) // Both messages delivered in order

	// Verify messages were delivered in correct sequence order
	msg1 := deliveries[0].(*brokerStorage.Message)
	msg2 := deliveries[1].(*brokerStorage.Message)
	assert.Equal(t, []byte("test payload 0"), msg1.GetPayload())
	assert.Equal(t, []byte("test payload 1"), msg2.GetPayload())
}

func TestDeliveryWorker_IntegrationWithRetry(t *testing.T) {
	ctx := context.Background()
	store := memory.New()
	config := queueStorage.DefaultQueueConfig("$queue/test")
	config.Partitions = 1
	err := store.CreateQueue(ctx, config)
	require.NoError(t, err)

	queue := NewQueue(config, store, store)
	broker := NewMockBrokerWithError()

	// Add consumer
	err = queue.AddConsumer(ctx, "group1", "consumer1", "client1", "")
	require.NoError(t, err)

	// Enqueue message
	msg := &queueStorage.Message{
		ID:          "msg-1",
		Topic:       "$queue/test",
		Payload:     []byte("test payload"),
		PartitionID: 0,
		Sequence:    1,
		State:       queueStorage.StateQueued,
		RetryCount:  2,
		CreatedAt:   time.Now(),
	}
	err = store.Enqueue(ctx, "$queue/test", msg)
	require.NoError(t, err)

	// Create and run delivery worker
	worker := delivery.NewWorker(queue, store, broker.DeliverToSession, nil, "local", delivery.ProxyMode, nil, nil)
	worker.DeliverMessages(ctx)

	// Verify message was delivered
	deliveries := broker.GetDeliveries("client1")
	assert.Len(t, deliveries, 1)

	// Verify retry count was preserved in delivery state
	inflight, err := store.GetInflight(ctx, "$queue/test")
	require.NoError(t, err)
	assert.Len(t, inflight, 1)
	assert.Equal(t, 2, inflight[0].RetryCount)
}

func TestToStorageMessage(t *testing.T) {
	msg := &queueStorage.Message{
		ID:      "msg-1",
		Topic:   "$queue/test",
		Payload: []byte("test payload"),
		Properties: map[string]string{
			"key1": "value1",
			"key2": "value2",
		},
		State:     queueStorage.StateQueued,
		CreatedAt: time.Now(),
	}

	storageMsg := delivery.ToStorageMessage(msg, "$queue/test")

	assert.NotNil(t, storageMsg)
	brokerMsg, ok := storageMsg.(*brokerStorage.Message)
	require.True(t, ok)

	assert.Equal(t, "$queue/test", brokerMsg.Topic)
	assert.Equal(t, byte(1), brokerMsg.QoS)
	assert.False(t, brokerMsg.Retain)
	assert.Equal(t, "value1", brokerMsg.Properties["key1"])
	assert.Equal(t, "value2", brokerMsg.Properties["key2"])
	assert.Equal(t, []byte("test payload"), brokerMsg.GetPayload())
}

func TestDeliveryWorker_ConcurrentDelivery(t *testing.T) {
	ctx := context.Background()
	store := memory.New()
	config := queueStorage.DefaultQueueConfig("$queue/test")
	config.Partitions = 4
	err := store.CreateQueue(ctx, config)
	require.NoError(t, err)

	queue := NewQueue(config, store, store)
	broker := NewMockBrokerWithError()

	// Add multiple consumers to same group
	err = queue.AddConsumer(ctx, "group1", "consumer1", "client1", "")
	require.NoError(t, err)
	err = queue.AddConsumer(ctx, "group1", "consumer2", "client2", "")
	require.NoError(t, err)

	// Enqueue messages to different partitions
	for i := 0; i < 10; i++ {
		partitionID := i % 4
		msg := &queueStorage.Message{
			ID:          "msg-" + string(rune('0'+i)),
			Topic:       "$queue/test",
			Payload:     []byte("test payload " + string(rune('0'+i))),
			PartitionID: partitionID,
			Sequence:    uint64(i + 1),
			State:       queueStorage.StateQueued,
			CreatedAt:   time.Now(),
		}
		err = store.Enqueue(ctx, "$queue/test", msg)
		require.NoError(t, err)
	}

	// Create and run delivery worker
	worker := delivery.NewWorker(queue, store, broker.DeliverToSession, nil, "local", delivery.ProxyMode, nil, nil)

	// Run multiple delivery cycles
	for i := 0; i < 5; i++ {
		worker.DeliverMessages(ctx)
	}

	// Verify messages were distributed across consumers
	deliveries1 := broker.GetDeliveries("client1")
	deliveries2 := broker.GetDeliveries("client2")

	totalDeliveries := len(deliveries1) + len(deliveries2)
	assert.Equal(t, 10, totalDeliveries)

	// Both consumers should have received messages
	assert.Greater(t, len(deliveries1), 0)
	assert.Greater(t, len(deliveries2), 0)
}
