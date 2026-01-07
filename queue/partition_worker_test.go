// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package queue

import (
	"context"
	"testing"

	queueStorage "github.com/absmach/mqtt/queue/storage"
	"github.com/absmach/mqtt/queue/storage/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPartitionWorker_RouteQueueMessage_ProxyMode(t *testing.T) {
	ctx := context.Background()
	store := memory.New()
	queueName := "$queue/test"

	// Create queue
	config := queueStorage.DefaultQueueConfig(queueName)
	queue := NewQueue(config, store, store)

	// Create mock cluster
	mockCluster := NewMockCluster("node-1")

	// Create partition worker in proxy mode
	worker := NewPartitionWorker(
		queueName,
		0,
		queue,
		store,
		NewMockBroker().DeliverToSession,
		100,
		mockCluster,
		"node-1",
		ProxyMode,
		nil,
	)

	// Create queue in store first
	err := store.CreateQueue(ctx, config)
	require.NoError(t, err)

	// Create a consumer on a remote node
	consumer := &queueStorage.Consumer{
		ID:            "consumer-1",
		ClientID:      "client-1",
		GroupID:       "group-1",
		QueueName:     queueName,
		ProxyNodeID:   "node-2", // Consumer is on node-2
		AssignedParts: []int{0},
	}

	// Create and store a message
	msg := &queueStorage.Message{
		ID:          "msg-1",
		Topic:       queueName,
		Payload:     []byte("test payload"),
		Properties:  map[string]string{"key": "value"},
		State:       queueStorage.StateQueued,
		PartitionID: 0,
		Sequence:    1,
	}
	err = store.Enqueue(ctx, queueName, msg)
	require.NoError(t, err)

	// Deliver message
	err = worker.deliverMessage(ctx, msg, consumer, config)
	require.NoError(t, err)

	// Verify RouteQueueMessage was called
	mockCluster.mu.Lock()
	calls := mockCluster.routeQueueMsgCalls
	mockCluster.mu.Unlock()

	require.Len(t, calls, 1)
	assert.Equal(t, "node-2", calls[0].NodeID)
	assert.Equal(t, "client-1", calls[0].ClientID)
	assert.Equal(t, queueName, calls[0].QueueName)
	assert.Equal(t, "msg-1", calls[0].MessageID)
	assert.Equal(t, []byte("test payload"), calls[0].Payload)
	assert.Equal(t, "value", calls[0].Properties["key"])
	assert.Equal(t, int64(1), calls[0].Sequence)
	assert.Equal(t, 0, calls[0].PartitionID)
}

func TestPartitionWorker_LocalDelivery_ProxyMode(t *testing.T) {
	ctx := context.Background()
	store := memory.New()
	queueName := "$queue/test"
	mockBroker := NewMockBroker()

	// Create queue
	config := queueStorage.DefaultQueueConfig(queueName)
	queue := NewQueue(config, store, store)

	// Create mock cluster
	mockCluster := NewMockCluster("node-1")

	// Create partition worker in proxy mode
	worker := NewPartitionWorker(
		queueName,
		0,
		queue,
		store,
		mockBroker.DeliverToSession,
		100,
		mockCluster,
		"node-1",
		ProxyMode,
		nil,
	)

	// Create queue in store first
	err := store.CreateQueue(ctx, config)
	require.NoError(t, err)

	// Create a local consumer (ProxyNodeID = local node)
	consumer := &queueStorage.Consumer{
		ID:            "consumer-1",
		ClientID:      "client-1",
		GroupID:       "group-1",
		QueueName:     queueName,
		ProxyNodeID:   "node-1", // Consumer is on same node
		AssignedParts: []int{0},
	}

	// Create and store a message
	msg := &queueStorage.Message{
		ID:          "msg-1",
		Topic:       queueName,
		Payload:     []byte("test payload"),
		Properties:  map[string]string{"key": "value"},
		State:       queueStorage.StateQueued,
		PartitionID: 0,
		Sequence:    1,
	}
	err = store.Enqueue(ctx, queueName, msg)
	require.NoError(t, err)

	// Deliver message
	err = worker.deliverMessage(ctx, msg, consumer, config)
	require.NoError(t, err)

	// Verify RouteQueueMessage was NOT called
	mockCluster.mu.Lock()
	calls := mockCluster.routeQueueMsgCalls
	mockCluster.mu.Unlock()
	assert.Len(t, calls, 0)

	// Verify local delivery happened
	deliveries := mockBroker.GetDeliveries("client-1")
	require.Len(t, deliveries, 1)
}

func TestPartitionWorker_DirectMode_LocalDelivery(t *testing.T) {
	ctx := context.Background()
	store := memory.New()
	queueName := "$queue/test"
	mockBroker := NewMockBroker()

	// Create queue
	config := queueStorage.DefaultQueueConfig(queueName)
	queue := NewQueue(config, store, store)

	// Create mock cluster
	mockCluster := NewMockCluster("node-1")

	// Create partition worker in DIRECT mode
	worker := NewPartitionWorker(
		queueName,
		0,
		queue,
		store,
		mockBroker.DeliverToSession,
		100,
		mockCluster,
		"node-1",
		DirectMode, // Direct mode - no routing
		nil,
	)

	// Create queue in store first
	err := store.CreateQueue(ctx, config)
	require.NoError(t, err)

	// Create a consumer that would be remote in proxy mode
	consumer := &queueStorage.Consumer{
		ID:            "consumer-1",
		ClientID:      "client-1",
		GroupID:       "group-1",
		QueueName:     queueName,
		ProxyNodeID:   "node-2", // Would be remote in proxy mode
		AssignedParts: []int{0},
	}

	// Create and store a message
	msg := &queueStorage.Message{
		ID:          "msg-1",
		Topic:       queueName,
		Payload:     []byte("test payload"),
		Properties:  map[string]string{"key": "value"},
		State:       queueStorage.StateQueued,
		PartitionID: 0,
		Sequence:    1,
	}
	err = store.Enqueue(ctx, queueName, msg)
	require.NoError(t, err)

	// Deliver message
	err = worker.deliverMessage(ctx, msg, consumer, config)
	require.NoError(t, err)

	// Verify RouteQueueMessage was NOT called (direct mode)
	mockCluster.mu.Lock()
	calls := mockCluster.routeQueueMsgCalls
	mockCluster.mu.Unlock()
	assert.Len(t, calls, 0)

	// Verify local delivery happened
	deliveries := mockBroker.GetDeliveries("client-1")
	require.Len(t, deliveries, 1)
}

func TestPartitionWorker_NoCluster(t *testing.T) {
	ctx := context.Background()
	store := memory.New()
	queueName := "$queue/test"
	mockBroker := NewMockBroker()

	// Create queue
	config := queueStorage.DefaultQueueConfig(queueName)
	queue := NewQueue(config, store, store)

	// Create partition worker WITHOUT cluster
	worker := NewPartitionWorker(
		queueName,
		0,
		queue,
		store,
		mockBroker.DeliverToSession,
		100,
		nil, // No cluster
		"node-1",
		ProxyMode,
		nil,
	)

	// Create queue in store first
	err := store.CreateQueue(ctx, config)
	require.NoError(t, err)

	// Create a consumer
	consumer := &queueStorage.Consumer{
		ID:            "consumer-1",
		ClientID:      "client-1",
		GroupID:       "group-1",
		QueueName:     queueName,
		ProxyNodeID:   "",
		AssignedParts: []int{0},
	}

	// Create and store a message
	msg := &queueStorage.Message{
		ID:          "msg-1",
		Topic:       queueName,
		Payload:     []byte("test payload"),
		Properties:  map[string]string{},
		State:       queueStorage.StateQueued,
		PartitionID: 0,
		Sequence:    1,
	}
	err = store.Enqueue(ctx, queueName, msg)
	require.NoError(t, err)

	// Deliver message - should work locally
	err = worker.deliverMessage(ctx, msg, consumer, config)
	require.NoError(t, err)

	// Verify local delivery happened
	deliveries := mockBroker.GetDeliveries("client-1")
	require.Len(t, deliveries, 1)
}

func TestPartitionWorker_RemoteConsumer_NoCluster_Error(t *testing.T) {
	ctx := context.Background()
	store := memory.New()
	queueName := "$queue/test"
	mockBroker := NewMockBroker()

	// Create queue
	config := queueStorage.DefaultQueueConfig(queueName)
	queue := NewQueue(config, store, store)

	// Create partition worker WITHOUT cluster but in proxy mode
	worker := NewPartitionWorker(
		queueName,
		0,
		queue,
		store,
		mockBroker.DeliverToSession,
		100,
		nil, // No cluster
		"node-1",
		ProxyMode,
		nil,
	)

	// Create queue in store first
	err := store.CreateQueue(ctx, config)
	require.NoError(t, err)

	// Create a remote consumer
	consumer := &queueStorage.Consumer{
		ID:            "consumer-1",
		ClientID:      "client-1",
		GroupID:       "group-1",
		QueueName:     queueName,
		ProxyNodeID:   "node-2", // Remote consumer
		AssignedParts: []int{0},
	}

	// Create and store a message
	msg := &queueStorage.Message{
		ID:          "msg-1",
		Topic:       queueName,
		Payload:     []byte("test payload"),
		Properties:  map[string]string{},
		State:       queueStorage.StateQueued,
		PartitionID: 0,
		Sequence:    1,
	}
	err = store.Enqueue(ctx, queueName, msg)
	require.NoError(t, err)

	// Deliver message - should fail because no cluster
	err = worker.deliverMessage(ctx, msg, consumer, config)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "cluster not configured")
}
