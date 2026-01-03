// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package queue

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/absmach/mqtt/cluster"
	"github.com/absmach/mqtt/cluster/grpc"
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

// MockCluster implements cluster.Cluster for testing
type MockCluster struct {
	nodeID               string
	nodes                []string // List of node IDs in the cluster
	enqueueRemoteCalls   []EnqueueRemoteCall
	routeQueueMsgCalls   []RouteQueueMessageCall
	partitionOwners      map[string]map[int]string // queueName -> partitionID -> nodeID
	enqueueRemoteError   error
	routeQueueMsgError   error
	mu                   sync.Mutex
}

type EnqueueRemoteCall struct {
	NodeID     string
	QueueName  string
	Payload    []byte
	Properties map[string]string
}

type RouteQueueMessageCall struct {
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
		nodeID:          nodeID,
		nodes:           []string{nodeID},
		partitionOwners: make(map[string]map[int]string),
	}
}

func (m *MockCluster) AddNode(nodeID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.nodes = append(m.nodes, nodeID)
}

func (m *MockCluster) EnqueueRemote(ctx context.Context, nodeID, queueName string, payload []byte, properties map[string]string) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.enqueueRemoteError != nil {
		return "", m.enqueueRemoteError
	}

	m.enqueueRemoteCalls = append(m.enqueueRemoteCalls, EnqueueRemoteCall{
		NodeID:     nodeID,
		QueueName:  queueName,
		Payload:    payload,
		Properties: properties,
	})

	return "mock-message-id", nil
}

func (m *MockCluster) RouteQueueMessage(ctx context.Context, nodeID, clientID, queueName, messageID string, payload []byte, properties map[string]string, sequence int64, partitionID int) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.routeQueueMsgError != nil {
		return m.routeQueueMsgError
	}

	m.routeQueueMsgCalls = append(m.routeQueueMsgCalls, RouteQueueMessageCall{
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

func (m *MockCluster) GetPartitionOwner(ctx context.Context, queueName string, partitionID int) (string, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if owners, ok := m.partitionOwners[queueName]; ok {
		if nodeID, exists := owners[partitionID]; exists {
			return nodeID, true, nil
		}
	}
	return m.nodeID, true, nil
}

func (m *MockCluster) SetPartitionOwner(queueName string, partitionID int, nodeID string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, ok := m.partitionOwners[queueName]; !ok {
		m.partitionOwners[queueName] = make(map[int]string)
	}
	m.partitionOwners[queueName][partitionID] = nodeID
}

// Stub methods for cluster.Cluster interface
func (m *MockCluster) AcquireSession(ctx context.Context, clientID, nodeID string) error { return nil }
func (m *MockCluster) ReleaseSession(ctx context.Context, clientID string) error { return nil }
func (m *MockCluster) GetSessionOwner(ctx context.Context, clientID string) (string, bool, error) {
	return m.nodeID, true, nil
}
func (m *MockCluster) WatchSessionOwner(ctx context.Context, clientID string) <-chan cluster.OwnershipChange {
	ch := make(chan cluster.OwnershipChange)
	close(ch)
	return ch
}
func (m *MockCluster) AcquirePartition(ctx context.Context, queueName string, partitionID int, nodeID string) error {
	m.SetPartitionOwner(queueName, partitionID, nodeID)
	return nil
}
func (m *MockCluster) ReleasePartition(ctx context.Context, queueName string, partitionID int) error { return nil }
func (m *MockCluster) WatchPartitionOwnership(ctx context.Context, queueName string) <-chan cluster.PartitionOwnershipChange {
	ch := make(chan cluster.PartitionOwnershipChange)
	close(ch)
	return ch
}
func (m *MockCluster) AddSubscription(ctx context.Context, clientID, filter string, qos byte, opts brokerStorage.SubscribeOptions) error {
	return nil
}
func (m *MockCluster) RemoveSubscription(ctx context.Context, clientID, filter string) error { return nil }
func (m *MockCluster) GetSubscriptionsForClient(ctx context.Context, clientID string) ([]*brokerStorage.Subscription, error) {
	return nil, nil
}
func (m *MockCluster) GetSubscribersForTopic(ctx context.Context, topic string) ([]*brokerStorage.Subscription, error) {
	return nil, nil
}
func (m *MockCluster) Retained() brokerStorage.RetainedStore { return nil }
func (m *MockCluster) Wills() brokerStorage.WillStore { return nil }
func (m *MockCluster) RoutePublish(ctx context.Context, topic string, payload []byte, qos byte, retain bool, properties map[string]string) error {
	return nil
}
func (m *MockCluster) TakeoverSession(ctx context.Context, clientID, fromNode, toNode string) (*grpc.SessionState, error) {
	return nil, nil
}
func (m *MockCluster) IsLeader() bool { return true }
func (m *MockCluster) WaitForLeader(ctx context.Context) error { return nil }
func (m *MockCluster) Start() error { return nil }
func (m *MockCluster) Stop() error { return nil }
func (m *MockCluster) NodeID() string { return m.nodeID }
func (m *MockCluster) Nodes() []cluster.NodeInfo {
	m.mu.Lock()
	defer m.mu.Unlock()

	nodes := make([]cluster.NodeInfo, len(m.nodes))
	for i, nodeID := range m.nodes {
		nodes[i] = cluster.NodeInfo{
			ID:      nodeID,
			Address: "localhost",
			Healthy: true,
			Leader:  nodeID == m.nodeID,
		}
	}
	return nodes
}

func TestManager_EnqueueRemote(t *testing.T) {
	ctx := context.Background()
	store := memory.New()
	mockCluster := NewMockCluster("node-1")
	mockCluster.AddNode("node-2") // Add node-2 to the cluster

	cfg := Config{
		QueueStore:    store,
		MessageStore:  store,
		ConsumerStore: store,
		Cluster:       mockCluster,
		LocalNodeID:   "node-1",
	}

	mgr, err := NewManager(cfg)
	require.NoError(t, err)

	queueName := "$queue/test"

	// Create queue
	config := queueStorage.DefaultQueueConfig(queueName)
	err = mgr.CreateQueue(ctx, config)
	require.NoError(t, err)

	// Find a partition key that maps to partition 1 (which will be owned by node-2)
	// With HashPartitionAssigner and 2 nodes: partition 1 % 2 = 1 = nodes[1] = node-2
	queue, _ := mgr.GetQueue(queueName)
	partitionKey := ""
	for i := 0; i < 1000; i++ {
		testKey := string(rune('a' + i))
		if queue.GetPartitionForMessage(testKey) == 1 {
			partitionKey = testKey
			break
		}
	}
	require.NotEmpty(t, partitionKey, "Should find a partition key for partition 1")

	properties := map[string]string{"partition-key": partitionKey}
	err = mgr.Enqueue(ctx, queueName, []byte("test payload"), properties)
	require.NoError(t, err)

	// Verify EnqueueRemote was called
	mockCluster.mu.Lock()
	calls := mockCluster.enqueueRemoteCalls
	mockCluster.mu.Unlock()

	require.Len(t, calls, 1)
	assert.Equal(t, "node-2", calls[0].NodeID)
	assert.Equal(t, queueName, calls[0].QueueName)
	assert.Equal(t, []byte("test payload"), calls[0].Payload)
	assert.Equal(t, partitionKey, calls[0].Properties["partition-key"])
}

func TestManager_EnqueueRemote_NoCluster(t *testing.T) {
	ctx := context.Background()
	store := memory.New()

	cfg := Config{
		QueueStore:    store,
		MessageStore:  store,
		ConsumerStore: store,
		Cluster:       nil,
		LocalNodeID:   "node-1",
	}

	mgr, err := NewManager(cfg)
	require.NoError(t, err)

	queueName := "$queue/test"
	config := queueStorage.DefaultQueueConfig(queueName)
	err = mgr.CreateQueue(ctx, config)
	require.NoError(t, err)

	// Enqueue should succeed locally even without cluster
	err = mgr.Enqueue(ctx, queueName, []byte("test"), nil)
	require.NoError(t, err)
}
