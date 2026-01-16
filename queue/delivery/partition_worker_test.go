// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package delivery

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/absmach/fluxmq/cluster"
	"github.com/absmach/fluxmq/cluster/grpc"
	"github.com/absmach/fluxmq/queue/consumer"
	queueStorage "github.com/absmach/fluxmq/queue/storage"
	"github.com/absmach/fluxmq/queue/storage/memory"
	brokerStorage "github.com/absmach/fluxmq/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// MockQueue implements QueueSource interface
type MockQueue struct {
	config           queueStorage.QueueConfig
	consumerGroups   *consumer.GroupManager
	orderingEnforcer OrderingEnforcer
}

func (m *MockQueue) Name() string {
	return m.config.Name
}

func (m *MockQueue) Config() queueStorage.QueueConfig {
	return m.config
}

func (m *MockQueue) OrderingEnforcer() OrderingEnforcer {
	return m.orderingEnforcer
}

func (m *MockQueue) ConsumerGroups() *consumer.GroupManager {
	return m.consumerGroups
}

// MockOrderingEnforcer implements OrderingEnforcer interface
type MockOrderingEnforcer struct{}

func (m *MockOrderingEnforcer) CanDeliver(msg *queueStorage.Message, groupID string) (bool, error) {
	return true, nil
}

func (m *MockOrderingEnforcer) MarkDelivered(msg *queueStorage.Message, groupID string) {}

// MockBroker is a simple mock for broker delivery
type MockBroker struct {
	deliveries map[string][]any
	mu         sync.Mutex
}

func NewMockBroker() *MockBroker {
	return &MockBroker{
		deliveries: make(map[string][]any),
	}
}

func (m *MockBroker) DeliverToSession(ctx context.Context, clientID string, msg any) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.deliveries[clientID] = append(m.deliveries[clientID], msg)
	return nil
}

func (m *MockBroker) GetDeliveries(clientID string) []any {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.deliveries[clientID]
}

// MockCluster implements cluster.Cluster interface
type MockCluster struct {
	localNodeID        string
	routeQueueMsgCalls []RouteQueueMsgCall
	mu                 sync.Mutex
}

type RouteQueueMsgCall struct {
	NodeID      string
	ClientID    string
	QueueName   string
	MessageID   string
	Payload     []byte
	Properties  map[string]string
	Sequence    int64
	PartitionID int
}

func NewMockCluster(nodeID string) *MockCluster {
	return &MockCluster{
		localNodeID: nodeID,
	}
}

// Implement required methods of cluster.Cluster interface (stubbed)
func (c *MockCluster) NodeID() string                        { return c.localNodeID }
func (c *MockCluster) IsLeader() bool                        { return true }
func (c *MockCluster) WaitForLeader(context.Context) error   { return nil }
func (c *MockCluster) Start() error                          { return nil }
func (c *MockCluster) Stop() error                           { return nil }
func (c *MockCluster) Nodes() []cluster.NodeInfo             { return nil }
func (c *MockCluster) Retained() brokerStorage.RetainedStore { return nil }
func (c *MockCluster) Wills() brokerStorage.WillStore        { return nil }
func (c *MockCluster) RoutePublish(ctx context.Context, topic string, payload []byte, qos byte, retain bool, properties map[string]string) error {
	return nil
}

func (c *MockCluster) TakeoverSession(ctx context.Context, clientID, fromNode, toNode string) (*grpc.SessionState, error) {
	return nil, nil
}
func (c *MockCluster) ID() string                { return c.localNodeID }
func (c *MockCluster) Notify(any) error          { return nil }
func (c *MockCluster) Join(string, string) error { return nil }
func (c *MockCluster) Leave(string) error        { return nil }
func (c *MockCluster) WatchSessionOwner(context.Context, string) <-chan cluster.OwnershipChange {
	return nil
}

func (c *MockCluster) WatchPartitionOwnership(context.Context, string) <-chan cluster.PartitionOwnershipChange {
	return nil
}
func (c *MockCluster) ReleaseSession(context.Context, string) error             { return nil }
func (c *MockCluster) ReleasePartition(context.Context, string, int) error      { return nil }
func (c *MockCluster) RemoveSubscription(context.Context, string, string) error { return nil }
func (c *MockCluster) GetPartitionOwner(ctx context.Context, queueName string, partitionID int) (string, bool, error) {
	return "", false, nil
}

func (c *MockCluster) AcquirePartition(ctx context.Context, queueName string, partitionID int, owner string) error {
	return nil
}

func (c *MockCluster) GetSessionOwner(ctx context.Context, clientID string) (string, bool, error) {
	return "", false, nil
}

func (c *MockCluster) GetSubscribersForTopic(ctx context.Context, topic string) ([]*brokerStorage.Subscription, error) {
	return nil, nil
}

func (c *MockCluster) GetSubscriptionsForClient(ctx context.Context, clientID string) ([]*brokerStorage.Subscription, error) {
	return nil, nil
}

func (c *MockCluster) EnqueueRemote(ctx context.Context, nodeID, queue string, payload []byte, properties map[string]string) (string, error) {
	return "", nil
}

func (c *MockCluster) AcquireSession(ctx context.Context, clientID, nodeID string) error {
	return nil
}

func (c *MockCluster) AddSubscription(ctx context.Context, clientID, topic string, qos byte, opts brokerStorage.SubscribeOptions) error {
	return nil
}

func (c *MockCluster) RouteQueueMessage(ctx context.Context, nodeID, clientID, queueName, messageID string, payload []byte, properties map[string]string, sequence int64, partitionID int) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.routeQueueMsgCalls = append(c.routeQueueMsgCalls, RouteQueueMsgCall{
		NodeID:      nodeID,
		ClientID:    clientID,
		QueueName:   queueName,
		MessageID:   messageID,
		Payload:     payload,
		Properties:  properties,
		Sequence:    sequence,
		PartitionID: partitionID,
	})
	return nil
}

// Helper to create consumer partitions
type mockPartition struct {
	id int
}

func (p *mockPartition) ID() int            { return p.id }
func (p *mockPartition) AssignTo(string)    {}
func (p *mockPartition) AssignedTo() string { return "" }
func (p *mockPartition) IsAssigned() bool   { return false }
func (p *mockPartition) Unassign()          {}

func toConsumerPartitions(count int) []consumer.Partition {
	parts := make([]consumer.Partition, count)
	for i := 0; i < count; i++ {
		parts[i] = &mockPartition{id: i}
	}
	return parts
}

func TestPartitionWorker_RouteQueueMessage_ProxyMode(t *testing.T) {
	ctx := context.Background()
	store := memory.New()
	queueName := "$queue/test"

	// Create consumer store and mock partitions
	consumerStore := memory.New()
	parts := toConsumerPartitions(1)
	consumerMgr := consumer.NewGroupManager(queueName, consumerStore, time.Second, parts)

	config := queueStorage.DefaultQueueConfig(queueName)
	mockQueue := &MockQueue{
		config:           config,
		consumerGroups:   consumerMgr,
		orderingEnforcer: &MockOrderingEnforcer{},
	}

	mockCluster := NewMockCluster("node-1")

	worker := NewPartitionWorker(
		queueName,
		0,
		mockQueue,
		store,
		NewMockBroker().DeliverToSession,
		100,
		mockCluster,
		"node-1",
		ProxyMode,
		nil,
		nil,
	)

	// Create queue in store first
	err := store.CreateQueue(ctx, config)
	require.NoError(t, err)

	// Create a consumer on a remote node
	// We need to register it in the consumer manager so it can be found
	err = consumerMgr.AddConsumer(ctx, "group-1", "consumer-1", "client-1", "node-2")
	require.NoError(t, err)

	// Assign partition to consumer
	consumerMgr.Rebalance("group-1", parts)

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

	// Get the consumer object from storage (as expected by deliverMessage)
	group, _ := consumerMgr.GetGroup("group-1")
	c, _ := group.GetConsumer("consumer-1")

	// Deliver message
	err = worker.deliverMessage(ctx, msg, c, config)
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

	// Create consumer manager
	consumerStore := memory.New()
	parts := toConsumerPartitions(1)
	consumerMgr := consumer.NewGroupManager(queueName, consumerStore, time.Second, parts)

	config := queueStorage.DefaultQueueConfig(queueName)
	mockQueue := &MockQueue{
		config:           config,
		consumerGroups:   consumerMgr,
		orderingEnforcer: &MockOrderingEnforcer{},
	}

	mockCluster := NewMockCluster("node-1")
	worker := NewPartitionWorker(
		queueName,
		0,
		mockQueue,
		store,
		mockBroker.DeliverToSession,
		100,
		mockCluster,
		"node-1",
		ProxyMode,
		nil,
		nil,
	)

	// Create queue in store first
	err := store.CreateQueue(ctx, config)
	require.NoError(t, err)

	// Create a local consumer
	err = consumerMgr.AddConsumer(ctx, "group-1", "consumer-1", "client-1", "node-1")
	require.NoError(t, err)
	consumerMgr.Rebalance("group-1", parts)

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

	group, _ := consumerMgr.GetGroup("group-1")
	c, _ := group.GetConsumer("consumer-1")

	// Deliver message
	err = worker.deliverMessage(ctx, msg, c, config)
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

	consumerStore := memory.New()
	parts := toConsumerPartitions(1)
	consumerMgr := consumer.NewGroupManager(queueName, consumerStore, time.Second, parts)

	config := queueStorage.DefaultQueueConfig(queueName)
	mockQueue := &MockQueue{
		config:           config,
		consumerGroups:   consumerMgr,
		orderingEnforcer: &MockOrderingEnforcer{},
	}

	mockCluster := NewMockCluster("node-1")
	worker := NewPartitionWorker(
		queueName,
		0,
		mockQueue,
		store,
		mockBroker.DeliverToSession,
		100,
		mockCluster,
		"node-1",
		DirectMode, // Direct mode - no routing
		nil,
		nil,
	)

	err := store.CreateQueue(ctx, config)
	require.NoError(t, err)

	// Create a consumer that would be remote in proxy mode
	err = consumerMgr.AddConsumer(ctx, "group-1", "consumer-1", "client-1", "node-2")
	require.NoError(t, err)
	consumerMgr.Rebalance("group-1", parts)

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

	group, _ := consumerMgr.GetGroup("group-1")
	c, _ := group.GetConsumer("consumer-1")

	// Deliver message
	err = worker.deliverMessage(ctx, msg, c, config)
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
