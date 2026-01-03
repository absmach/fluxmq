// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package queue

import (
	"context"
	"sync"
	"testing"
	"time"

	queueStorage "github.com/absmach/mqtt/queue/storage"
	"github.com/absmach/mqtt/queue/storage/memory"
	brokerStorage "github.com/absmach/mqtt/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// MockBroker implements BrokerInterface for testing
type MockBroker struct {
	mu         sync.Mutex
	deliveries map[string][]interface{}
}

func NewMockBroker() *MockBroker {
	return &MockBroker{
		deliveries: make(map[string][]interface{}),
	}
}

func (m *MockBroker) DeliverToSession(ctx context.Context, clientID string, msg interface{}) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// For storage.Message with RefCountedBuffer, copy payload to prevent
	// buffer pool exhaustion while keeping tests working
	if storageMsg, ok := msg.(*brokerStorage.Message); ok && storageMsg.PayloadBuf != nil {
		// Copy payload bytes before releasing buffer
		payloadCopy := make([]byte, len(storageMsg.PayloadBuf.Bytes()))
		copy(payloadCopy, storageMsg.PayloadBuf.Bytes())

		// Release the buffer to return it to pool
		storageMsg.ReleasePayload()

		// Set legacy Payload field with the copy for test access
		storageMsg.Payload = payloadCopy
	}

	m.deliveries[clientID] = append(m.deliveries[clientID], msg)
	return nil
}

func (m *MockBroker) GetDeliveries(clientID string) []interface{} {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.deliveries[clientID]
}

func TestNewManager(t *testing.T) {
	store := memory.New()

	cfg := Config{
		QueueStore:    store,
		MessageStore:  store,
		ConsumerStore: store,
		DeliverFn:     NewMockBroker().DeliverToSession,
	}

	mgr, err := NewManager(cfg)
	require.NoError(t, err)
	assert.NotNil(t, mgr)
	assert.NotNil(t, mgr.retryManager)
	assert.NotNil(t, mgr.dlqManager)
}

func TestNewManager_NilStores(t *testing.T) {
	cfg := Config{
		QueueStore:    nil,
		MessageStore:  nil,
		ConsumerStore: nil,
	}

	mgr, err := NewManager(cfg)
	assert.Error(t, err)
	assert.Nil(t, mgr)
	assert.Contains(t, err.Error(), "queue stores cannot be nil")
}

func TestManager_CreateQueue(t *testing.T) {
	ctx := context.Background()
	store := memory.New()

	cfg := Config{
		QueueStore:    store,
		MessageStore:  store,
		ConsumerStore: store,
	}

	mgr, err := NewManager(cfg)
	require.NoError(t, err)

	config := queueStorage.DefaultQueueConfig("$queue/test")
	err = mgr.CreateQueue(ctx, config)
	require.NoError(t, err)

	// Verify queue exists
	queue, err := mgr.GetQueue("$queue/test")
	require.NoError(t, err)
	assert.NotNil(t, queue)
	assert.Equal(t, "$queue/test", queue.Name())
}

func TestManager_CreateQueue_Duplicate(t *testing.T) {
	ctx := context.Background()
	store := memory.New()

	cfg := Config{
		QueueStore:    store,
		MessageStore:  store,
		ConsumerStore: store,
	}

	mgr, err := NewManager(cfg)
	require.NoError(t, err)

	config := queueStorage.DefaultQueueConfig("$queue/test")
	err = mgr.CreateQueue(ctx, config)
	require.NoError(t, err)

	// Try to create again
	err = mgr.CreateQueue(ctx, config)
	assert.ErrorIs(t, err, queueStorage.ErrQueueAlreadyExists)
}

func TestManager_CreateQueue_InvalidConfig(t *testing.T) {
	ctx := context.Background()
	store := memory.New()

	cfg := Config{
		QueueStore:    store,
		MessageStore:  store,
		ConsumerStore: store,
	}

	mgr, err := NewManager(cfg)
	require.NoError(t, err)

	// Invalid config (empty name)
	config := queueStorage.QueueConfig{
		Name: "",
	}
	err = mgr.CreateQueue(ctx, config)
	assert.Error(t, err)
}

func TestManager_GetQueue(t *testing.T) {
	ctx := context.Background()
	store := memory.New()

	cfg := Config{
		QueueStore:    store,
		MessageStore:  store,
		ConsumerStore: store,
	}

	mgr, err := NewManager(cfg)
	require.NoError(t, err)

	// Non-existent queue
	queue, err := mgr.GetQueue("$queue/nonexistent")
	assert.ErrorIs(t, err, queueStorage.ErrQueueNotFound)
	assert.Nil(t, queue)

	// Create and get queue
	config := queueStorage.DefaultQueueConfig("$queue/test")
	err = mgr.CreateQueue(ctx, config)
	require.NoError(t, err)

	queue, err = mgr.GetQueue("$queue/test")
	require.NoError(t, err)
	assert.NotNil(t, queue)
}

func TestManager_GetOrCreateQueue(t *testing.T) {
	ctx := context.Background()
	store := memory.New()

	cfg := Config{
		QueueStore:    store,
		MessageStore:  store,
		ConsumerStore: store,
	}

	mgr, err := NewManager(cfg)
	require.NoError(t, err)

	// Get or create non-existent queue
	queue, err := mgr.GetOrCreateQueue(ctx, "$queue/autocreate")
	require.NoError(t, err)
	assert.NotNil(t, queue)
	assert.Equal(t, "$queue/autocreate", queue.Name())

	// Get existing queue
	queue2, err := mgr.GetOrCreateQueue(ctx, "$queue/autocreate")
	require.NoError(t, err)
	assert.Equal(t, queue.Name(), queue2.Name())
}

func TestManager_Enqueue(t *testing.T) {
	ctx := context.Background()
	store := memory.New()

	cfg := Config{
		QueueStore:    store,
		MessageStore:  store,
		ConsumerStore: store,
	}

	mgr, err := NewManager(cfg)
	require.NoError(t, err)

	err = mgr.Enqueue(ctx, "$queue/test", []byte("test payload"), map[string]string{"key": "value"})
	require.NoError(t, err)

	// Verify message was enqueued
	queue, err := mgr.GetQueue("$queue/test")
	require.NoError(t, err)

	// Check all partitions since message could be in any partition
	config := queue.Config()
	found := false
	for i := 0; i < config.Partitions; i++ {
		messages, err := store.ListQueued(ctx, "$queue/test", i, 0)
		require.NoError(t, err)

		for _, msg := range messages {
			if string(msg.Payload) == "test payload" {
				found = true
				assert.Equal(t, "$queue/test", msg.Topic)
				assert.Equal(t, "value", msg.Properties["key"])
				assert.NotEmpty(t, msg.ID)
				break
			}
		}
		if found {
			break
		}
	}
	assert.True(t, found, "Message should be found in queue")
}

func TestManager_Enqueue_WithPartitionKey(t *testing.T) {
	ctx := context.Background()
	store := memory.New()

	cfg := Config{
		QueueStore:    store,
		MessageStore:  store,
		ConsumerStore: store,
	}

	mgr, err := NewManager(cfg)
	require.NoError(t, err)

	properties := map[string]string{
		"partition-key": "user-123",
	}

	err = mgr.Enqueue(ctx, "$queue/test", []byte("test"), properties)
	require.NoError(t, err)

	// Verify partition key was used
	queue, err := mgr.GetQueue("$queue/test")
	require.NoError(t, err)

	expectedPartition := queue.GetPartitionForMessage("user-123")

	messages, err := store.ListQueued(ctx, "$queue/test", expectedPartition, 0)
	require.NoError(t, err)
	assert.GreaterOrEqual(t, len(messages), 1)
}

func TestManager_Subscribe(t *testing.T) {
	ctx := context.Background()
	store := memory.New()

	cfg := Config{
		QueueStore:    store,
		MessageStore:  store,
		ConsumerStore: store,
	}

	mgr, err := NewManager(cfg)
	require.NoError(t, err)

	err = mgr.Subscribe(ctx, "$queue/test", "client-1", "group-1", "")
	require.NoError(t, err)

	// Verify consumer was added
	queue, err := mgr.GetQueue("$queue/test")
	require.NoError(t, err)

	group, exists := queue.ConsumerGroups().GetGroup("group-1")
	assert.True(t, exists)
	assert.Equal(t, 1, group.Size())
}

func TestManager_Subscribe_DefaultGroup(t *testing.T) {
	ctx := context.Background()
	store := memory.New()

	cfg := Config{
		QueueStore:    store,
		MessageStore:  store,
		ConsumerStore: store,
	}

	mgr, err := NewManager(cfg)
	require.NoError(t, err)

	// Subscribe without specifying group (uses client ID prefix)
	err = mgr.Subscribe(ctx, "$queue/test", "web-client-1", "", "")
	require.NoError(t, err)

	queue, err := mgr.GetQueue("$queue/test")
	require.NoError(t, err)

	// Group should be "web" (prefix before first dash)
	_, exists := queue.ConsumerGroups().GetGroup("web")
	assert.True(t, exists)
}

func TestManager_Unsubscribe(t *testing.T) {
	ctx := context.Background()
	store := memory.New()

	cfg := Config{
		QueueStore:    store,
		MessageStore:  store,
		ConsumerStore: store,
	}

	mgr, err := NewManager(cfg)
	require.NoError(t, err)

	err = mgr.Subscribe(ctx, "$queue/test", "client-1", "group-1", "")
	require.NoError(t, err)

	err = mgr.Unsubscribe(ctx, "$queue/test", "client-1", "group-1")
	require.NoError(t, err)

	// Verify consumer was removed
	queue, err := mgr.GetQueue("$queue/test")
	require.NoError(t, err)

	group, exists := queue.ConsumerGroups().GetGroup("group-1")
	if exists {
		assert.Equal(t, 0, group.Size())
	}
}

func TestManager_Ack(t *testing.T) {
	ctx := context.Background()
	store := memory.New()

	cfg := Config{
		QueueStore:    store,
		MessageStore:  store,
		ConsumerStore: store,
	}

	mgr, err := NewManager(cfg)
	require.NoError(t, err)

	// Enqueue message with partition key to ensure it goes to partition 0
	props := map[string]string{"partition-key": "test-key"}
	err = mgr.Enqueue(ctx, "$queue/test", []byte("test"), props)
	require.NoError(t, err)

	// Get message ID
	queue, _ := mgr.GetQueue("$queue/test")
	partitionID := queue.GetPartitionForMessage("test-key")
	messages, err := store.ListQueued(ctx, "$queue/test", partitionID, 1)
	require.NoError(t, err)
	require.Len(t, messages, 1)

	msg := messages[0]

	// Mark as inflight
	deliveryState := &queueStorage.DeliveryState{
		MessageID:   msg.ID,
		QueueName:   queue.Name(),
		PartitionID: partitionID,
		ConsumerID:  "consumer-1",
		DeliveredAt: time.Now(),
		Timeout:     time.Now().Add(30 * time.Second),
	}
	err = store.MarkInflight(ctx, deliveryState)
	require.NoError(t, err)

	// Ack the message
	err = mgr.Ack(ctx, "$queue/test", msg.ID)
	require.NoError(t, err)

	// Verify message was deleted
	_, err = store.GetMessage(ctx, queue.Name(), msg.ID)
	assert.Error(t, err)

	// Verify removed from inflight
	inflight, err := store.GetInflight(ctx, queue.Name())
	require.NoError(t, err)
	assert.Len(t, inflight, 0)
}

func TestManager_Nack(t *testing.T) {
	ctx := context.Background()
	store := memory.New()

	cfg := Config{
		QueueStore:    store,
		MessageStore:  store,
		ConsumerStore: store,
	}

	mgr, err := NewManager(cfg)
	require.NoError(t, err)

	// Enqueue message with partition key
	props := map[string]string{"partition-key": "test-key"}
	err = mgr.Enqueue(ctx, "$queue/test", []byte("test"), props)
	require.NoError(t, err)

	queue, _ := mgr.GetQueue("$queue/test")
	partitionID := queue.GetPartitionForMessage("test-key")
	messages, err := store.ListQueued(ctx, "$queue/test", partitionID, 1)
	require.NoError(t, err)
	require.Len(t, messages, 1)
	msg := messages[0]

	// Mark as inflight
	deliveryState := &queueStorage.DeliveryState{
		MessageID:   msg.ID,
		QueueName:   queue.Name(),
		PartitionID: partitionID,
		ConsumerID:  "consumer-1",
		DeliveredAt: time.Now(),
		Timeout:     time.Now().Add(30 * time.Second),
	}
	err = store.MarkInflight(ctx, deliveryState)
	require.NoError(t, err)

	// Nack the message
	err = mgr.Nack(ctx, "$queue/test", msg.ID)
	require.NoError(t, err)

	// Verify message state changed to retry
	retrieved, err := store.GetMessage(ctx, queue.Name(), msg.ID)
	require.NoError(t, err)
	assert.Equal(t, queueStorage.StateRetry, retrieved.State)
	assert.Equal(t, 1, retrieved.RetryCount)
}

func TestManager_Reject(t *testing.T) {
	ctx := context.Background()
	store := memory.New()

	cfg := Config{
		QueueStore:    store,
		MessageStore:  store,
		ConsumerStore: store,
	}

	mgr, err := NewManager(cfg)
	require.NoError(t, err)

	// Create queue with DLQ enabled
	config := queueStorage.DefaultQueueConfig("$queue/test")
	config.DLQConfig.Enabled = true
	err = mgr.CreateQueue(ctx, config)
	require.NoError(t, err)

	// Enqueue message with partition key
	props := map[string]string{"partition-key": "test-key"}
	err = mgr.Enqueue(ctx, "$queue/test", []byte("test"), props)
	require.NoError(t, err)

	queue, _ := mgr.GetQueue("$queue/test")
	partitionID := queue.GetPartitionForMessage("test-key")
	messages, err := store.ListQueued(ctx, "$queue/test", partitionID, 1)
	require.NoError(t, err)
	require.Len(t, messages, 1)
	msg := messages[0]

	// Mark as inflight
	deliveryState := &queueStorage.DeliveryState{
		MessageID:   msg.ID,
		QueueName:   queue.Name(),
		PartitionID: partitionID,
		ConsumerID:  "consumer-1",
		DeliveredAt: time.Now(),
		Timeout:     time.Now().Add(30 * time.Second),
	}
	err = store.MarkInflight(ctx, deliveryState)
	require.NoError(t, err)

	// Reject the message
	err = mgr.Reject(ctx, "$queue/test", msg.ID, "processing failed")
	require.NoError(t, err)

	// Verify message moved to DLQ
	dlqConfig := queue.Config().DLQConfig
	dlqMessages, err := store.ListDLQ(ctx, dlqConfig.Topic, 0)
	require.NoError(t, err)
	assert.Len(t, dlqMessages, 1)
	assert.Equal(t, msg.ID, dlqMessages[0].ID)
	assert.Equal(t, "processing failed", dlqMessages[0].FailureReason)

	// Verify removed from original queue
	_, err = store.GetMessage(ctx, queue.Name(), msg.ID)
	assert.Error(t, err)
}

func TestManager_GetStats(t *testing.T) {
	ctx := context.Background()
	store := memory.New()

	cfg := Config{
		QueueStore:    store,
		MessageStore:  store,
		ConsumerStore: store,
	}

	mgr, err := NewManager(cfg)
	require.NoError(t, err)

	config := queueStorage.DefaultQueueConfig("$queue/test")
	err = mgr.CreateQueue(ctx, config)
	require.NoError(t, err)

	// Enqueue some messages
	for i := 0; i < 5; i++ {
		err = mgr.Enqueue(ctx, "$queue/test", []byte("test"), nil)
		require.NoError(t, err)
	}

	// Subscribe consumer
	err = mgr.Subscribe(ctx, "$queue/test", "client-1", "group-1", "")
	require.NoError(t, err)

	// Get stats
	stats, err := mgr.GetStats(ctx, "$queue/test")
	require.NoError(t, err)
	assert.Equal(t, "$queue/test", stats.Name)
	assert.Equal(t, 10, stats.Partitions) // Default config
	assert.Equal(t, 1, stats.ActiveConsumers)
	assert.GreaterOrEqual(t, stats.TotalMessages, int64(5))
}
