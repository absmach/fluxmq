// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package queue

import (
	"context"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/absmach/fluxmq/cluster"
	clusterv1 "github.com/absmach/fluxmq/pkg/proto/cluster/v1"
	"github.com/absmach/fluxmq/queue/storage"
	"github.com/absmach/fluxmq/queue/types"
	brokerStorage "github.com/absmach/fluxmq/storage"
	brokerstorage "github.com/absmach/fluxmq/storage"
)

// MockBroker implements BrokerInterface for testing.
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

// mockLogStore implements storage.LogStore for testing.
type mockLogStore struct {
	mu       sync.RWMutex
	queues   map[string]*types.QueueConfig
	messages map[string]map[int][]*types.Message // queueName -> partitionID -> messages
	heads    map[string]map[int]uint64           // queueName -> partitionID -> head
	tails    map[string]map[int]uint64           // queueName -> partitionID -> tail
}

func newMockLogStore() *mockLogStore {
	return &mockLogStore{
		queues:   make(map[string]*types.QueueConfig),
		messages: make(map[string]map[int][]*types.Message),
		heads:    make(map[string]map[int]uint64),
		tails:    make(map[string]map[int]uint64),
	}
}

func (s *mockLogStore) CreateQueue(ctx context.Context, config types.QueueConfig) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.queues[config.Name]; exists {
		return storage.ErrQueueAlreadyExists
	}

	s.queues[config.Name] = &config
	s.messages[config.Name] = make(map[int][]*types.Message)
	s.heads[config.Name] = make(map[int]uint64)
	s.tails[config.Name] = make(map[int]uint64)

	for i := 0; i < config.Partitions; i++ {
		s.messages[config.Name][i] = make([]*types.Message, 0)
		s.heads[config.Name][i] = 0
		s.tails[config.Name][i] = 0
	}

	return nil
}

func (s *mockLogStore) GetQueue(ctx context.Context, queueName string) (*types.QueueConfig, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	config, exists := s.queues[queueName]
	if !exists {
		return nil, storage.ErrQueueNotFound
	}
	return config, nil
}

func (s *mockLogStore) DeleteQueue(ctx context.Context, queueName string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.queues, queueName)
	delete(s.messages, queueName)
	delete(s.heads, queueName)
	delete(s.tails, queueName)
	return nil
}

func (s *mockLogStore) ListQueues(ctx context.Context) ([]types.QueueConfig, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var configs []types.QueueConfig
	for _, config := range s.queues {
		configs = append(configs, *config)
	}
	return configs, nil
}

func (s *mockLogStore) Append(ctx context.Context, queueName string, partitionID int, msg *types.Message) (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.queues[queueName]; !exists {
		return 0, storage.ErrQueueNotFound
	}

	offset := s.tails[queueName][partitionID]
	msg.Sequence = offset
	msg.PartitionID = partitionID

	s.messages[queueName][partitionID] = append(s.messages[queueName][partitionID], msg)
	s.tails[queueName][partitionID] = offset + 1

	return offset, nil
}

func (s *mockLogStore) AppendBatch(ctx context.Context, queueName string, partitionID int, msgs []*types.Message) (uint64, error) {
	if len(msgs) == 0 {
		return 0, nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.queues[queueName]; !exists {
		return 0, storage.ErrQueueNotFound
	}

	firstOffset := s.tails[queueName][partitionID]

	for _, msg := range msgs {
		offset := s.tails[queueName][partitionID]
		msg.Sequence = offset
		msg.PartitionID = partitionID
		s.messages[queueName][partitionID] = append(s.messages[queueName][partitionID], msg)
		s.tails[queueName][partitionID]++
	}

	return firstOffset, nil
}

func (s *mockLogStore) Read(ctx context.Context, queueName string, partitionID int, offset uint64) (*types.Message, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if _, exists := s.queues[queueName]; !exists {
		return nil, storage.ErrQueueNotFound
	}

	head := s.heads[queueName][partitionID]
	tail := s.tails[queueName][partitionID]

	if offset < head || offset >= tail {
		return nil, storage.ErrOffsetOutOfRange
	}

	// Find message by offset
	for _, msg := range s.messages[queueName][partitionID] {
		if msg.Sequence == offset {
			return msg, nil
		}
	}

	return nil, storage.ErrOffsetOutOfRange
}

func (s *mockLogStore) ReadBatch(ctx context.Context, queueName string, partitionID int, startOffset uint64, limit int) ([]*types.Message, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if _, exists := s.queues[queueName]; !exists {
		return nil, storage.ErrQueueNotFound
	}

	var result []*types.Message
	for _, msg := range s.messages[queueName][partitionID] {
		if msg.Sequence >= startOffset && len(result) < limit {
			result = append(result, msg)
		}
	}

	return result, nil
}

func (s *mockLogStore) Head(ctx context.Context, queueName string, partitionID int) (uint64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if _, exists := s.queues[queueName]; !exists {
		return 0, storage.ErrQueueNotFound
	}

	return s.heads[queueName][partitionID], nil
}

func (s *mockLogStore) Tail(ctx context.Context, queueName string, partitionID int) (uint64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if _, exists := s.queues[queueName]; !exists {
		return 0, storage.ErrQueueNotFound
	}

	return s.tails[queueName][partitionID], nil
}

func (s *mockLogStore) Truncate(ctx context.Context, queueName string, partitionID int, minOffset uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.queues[queueName]; !exists {
		return storage.ErrQueueNotFound
	}

	s.heads[queueName][partitionID] = minOffset
	return nil
}

func (s *mockLogStore) Count(ctx context.Context, queueName string, partitionID int) (uint64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if _, exists := s.queues[queueName]; !exists {
		return 0, storage.ErrQueueNotFound
	}

	return s.tails[queueName][partitionID] - s.heads[queueName][partitionID], nil
}

func (s *mockLogStore) TotalCount(ctx context.Context, queueName string) (uint64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	config, exists := s.queues[queueName]
	if !exists {
		return 0, storage.ErrQueueNotFound
	}

	var total uint64
	for i := 0; i < config.Partitions; i++ {
		total += s.tails[queueName][i] - s.heads[queueName][i]
	}

	return total, nil
}

// mockGroupStore implements storage.ConsumerGroupStore for testing.
type mockGroupStore struct {
	mu     sync.RWMutex
	groups map[string]map[string]*types.ConsumerGroupState // queueName -> groupID -> state
}

func newMockGroupStore() *mockGroupStore {
	return &mockGroupStore{
		groups: make(map[string]map[string]*types.ConsumerGroupState),
	}
}

func (s *mockGroupStore) CreateConsumerGroup(ctx context.Context, group *types.ConsumerGroupState) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.groups[group.QueueName] == nil {
		s.groups[group.QueueName] = make(map[string]*types.ConsumerGroupState)
	}

	if _, exists := s.groups[group.QueueName][group.ID]; exists {
		return storage.ErrConsumerGroupExists
	}

	s.groups[group.QueueName][group.ID] = group
	return nil
}

func (s *mockGroupStore) GetConsumerGroup(ctx context.Context, queueName, groupID string) (*types.ConsumerGroupState, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.groups[queueName] == nil {
		return nil, storage.ErrConsumerNotFound
	}

	group, exists := s.groups[queueName][groupID]
	if !exists {
		return nil, storage.ErrConsumerNotFound
	}

	return group, nil
}

func (s *mockGroupStore) UpdateConsumerGroup(ctx context.Context, group *types.ConsumerGroupState) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.groups[group.QueueName] == nil {
		return storage.ErrConsumerNotFound
	}

	s.groups[group.QueueName][group.ID] = group
	return nil
}

func (s *mockGroupStore) DeleteConsumerGroup(ctx context.Context, queueName, groupID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.groups[queueName] != nil {
		delete(s.groups[queueName], groupID)
	}
	return nil
}

func (s *mockGroupStore) ListConsumerGroups(ctx context.Context, queueName string) ([]*types.ConsumerGroupState, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var groups []*types.ConsumerGroupState
	if s.groups[queueName] != nil {
		for _, group := range s.groups[queueName] {
			groups = append(groups, group)
		}
	}
	return groups, nil
}

func (s *mockGroupStore) AddPendingEntry(ctx context.Context, queueName, groupID string, entry *types.PendingEntry) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.groups[queueName] == nil || s.groups[queueName][groupID] == nil {
		return storage.ErrConsumerNotFound
	}

	group := s.groups[queueName][groupID]
	group.AddPending(entry.ConsumerID, entry)
	return nil
}

func (s *mockGroupStore) RemovePendingEntry(ctx context.Context, queueName, groupID, consumerID string, partitionID int, offset uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.groups[queueName] == nil || s.groups[queueName][groupID] == nil {
		return storage.ErrConsumerNotFound
	}

	group := s.groups[queueName][groupID]
	if !group.RemovePending(consumerID, partitionID, offset) {
		return storage.ErrPendingEntryNotFound
	}
	return nil
}

func (s *mockGroupStore) GetPendingEntries(ctx context.Context, queueName, groupID, consumerID string) ([]*types.PendingEntry, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.groups[queueName] == nil || s.groups[queueName][groupID] == nil {
		return nil, storage.ErrConsumerNotFound
	}

	group := s.groups[queueName][groupID]
	entries, ok := group.PEL[consumerID]
	if !ok {
		return []*types.PendingEntry{}, nil
	}

	result := make([]*types.PendingEntry, len(entries))
	copy(result, entries)
	return result, nil
}

func (s *mockGroupStore) GetAllPendingEntries(ctx context.Context, queueName, groupID string) ([]*types.PendingEntry, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.groups[queueName] == nil || s.groups[queueName][groupID] == nil {
		return nil, storage.ErrConsumerNotFound
	}

	group := s.groups[queueName][groupID]
	var result []*types.PendingEntry
	for _, entries := range group.PEL {
		result = append(result, entries...)
	}
	return result, nil
}

func (s *mockGroupStore) TransferPendingEntry(ctx context.Context, queueName, groupID string, partitionID int, offset uint64, fromConsumer, toConsumer string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.groups[queueName] == nil || s.groups[queueName][groupID] == nil {
		return storage.ErrConsumerNotFound
	}

	group := s.groups[queueName][groupID]
	if !group.TransferPending(partitionID, offset, fromConsumer, toConsumer) {
		return storage.ErrPendingEntryNotFound
	}
	return nil
}

func (s *mockGroupStore) UpdateCursor(ctx context.Context, queueName, groupID string, partitionID int, cursor uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.groups[queueName] == nil || s.groups[queueName][groupID] == nil {
		return storage.ErrConsumerNotFound
	}

	group := s.groups[queueName][groupID]
	pc := group.GetCursor(partitionID)
	pc.Cursor = cursor
	return nil
}

func (s *mockGroupStore) UpdateCommitted(ctx context.Context, queueName, groupID string, partitionID int, committed uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.groups[queueName] == nil || s.groups[queueName][groupID] == nil {
		return storage.ErrConsumerNotFound
	}

	group := s.groups[queueName][groupID]
	pc := group.GetCursor(partitionID)
	pc.Committed = committed
	return nil
}

func (s *mockGroupStore) RegisterConsumer(ctx context.Context, queueName, groupID string, consumer *types.ConsumerInfo) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.groups[queueName] == nil || s.groups[queueName][groupID] == nil {
		return storage.ErrConsumerNotFound
	}

	group := s.groups[queueName][groupID]
	group.Consumers[consumer.ID] = consumer
	return nil
}

func (s *mockGroupStore) UnregisterConsumer(ctx context.Context, queueName, groupID, consumerID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.groups[queueName] == nil || s.groups[queueName][groupID] == nil {
		return storage.ErrConsumerNotFound
	}

	group := s.groups[queueName][groupID]
	delete(group.Consumers, consumerID)
	delete(group.PEL, consumerID)
	return nil
}

func (s *mockGroupStore) ListConsumers(ctx context.Context, queueName, groupID string) ([]*types.ConsumerInfo, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.groups[queueName] == nil || s.groups[queueName][groupID] == nil {
		return nil, storage.ErrConsumerNotFound
	}

	group := s.groups[queueName][groupID]
	var result []*types.ConsumerInfo
	for _, c := range group.Consumers {
		result = append(result, c)
	}
	return result, nil
}

func TestWildcardQueueSubscription(t *testing.T) {
	logStore := newMockLogStore()
	groupStore := newMockGroupStore()

	deliveredMsgs := make(chan *brokerstorage.Message, 10)

	deliverFn := func(ctx context.Context, clientID string, msg any) error {
		if brokerMsg, ok := msg.(*brokerstorage.Message); ok {
			t.Logf("Delivered message to %s: topic=%s", clientID, brokerMsg.Topic)
			deliveredMsgs <- brokerMsg
		} else {
			t.Errorf("Wrong message type: %T", msg)
		}
		return nil
	}

	config := DefaultConfig()
	config.DeliveryInterval = 50 * time.Millisecond // Faster for testing
	logger := slog.Default()

	manager := NewManager(logStore, groupStore, deliverFn, config, logger, nil)

	ctx := context.Background()

	// Start the manager
	if err := manager.Start(ctx); err != nil {
		t.Fatalf("Failed to start manager: %v", err)
	}
	defer manager.Stop()

	// Subscribe to wildcard pattern
	clientID := "test-client-1"
	filter := "$queue/topic/#"

	t.Logf("Subscribing client %s to %s", clientID, filter)
	if err := manager.Subscribe(ctx, filter, clientID, "", ""); err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}

	// Check that queue was created
	queues, _ := logStore.ListQueues(ctx)
	t.Logf("Queues after subscribe: %v", len(queues))
	for _, q := range queues {
		t.Logf("  Queue: %s (partitions=%d)", q.Name, q.Partitions)
	}

	// Check that group was created
	groups, _ := groupStore.ListConsumerGroups(ctx, "$queue/topic")
	t.Logf("Groups after subscribe: %v", len(groups))
	for _, g := range groups {
		t.Logf("  Group: %s (pattern=%s, consumers=%d)", g.ID, g.Pattern, len(g.Consumers))
		for cid, ci := range g.Consumers {
			t.Logf("    Consumer: %s (clientID=%s)", cid, ci.ClientID)
		}
	}

	// Publish a message
	publishTopic := "$queue/topic/test"
	payload := []byte("hello world")

	t.Logf("Publishing message to %s", publishTopic)
	if err := manager.Enqueue(ctx, publishTopic, payload, nil); err != nil {
		t.Fatalf("Enqueue failed: %v", err)
	}

	// Check message was appended (check all partitions)
	for p := 0; p < 10; p++ {
		tail, _ := logStore.Tail(ctx, "$queue/topic", p)
		if tail > 0 {
			t.Logf("Partition %d tail: %d", p, tail)
		}
	}

	// Wait for delivery
	t.Log("Waiting for message delivery...")
	select {
	case msg := <-deliveredMsgs:
		t.Logf("Received message: topic=%s payload=%s", msg.Topic, string(msg.GetPayload()))
		if msg.Topic != publishTopic {
			t.Errorf("Expected topic %s, got %s", publishTopic, msg.Topic)
		}
		if string(msg.GetPayload()) != string(payload) {
			t.Errorf("Expected payload %s, got %s", payload, msg.GetPayload())
		}
	case <-time.After(2 * time.Second):
		// Debug: check state
		groups, _ = groupStore.ListConsumerGroups(ctx, "$queue/topic")
		for _, g := range groups {
			t.Logf("Group state: %s cursor=%v", g.ID, g.Cursors)
		}
		t.Fatal("Timeout waiting for message delivery")
	}
}

func TestExactQueueSubscription(t *testing.T) {
	logStore := newMockLogStore()
	groupStore := newMockGroupStore()

	deliveredMsgs := make(chan *brokerstorage.Message, 10)

	deliverFn := func(ctx context.Context, clientID string, msg any) error {
		if brokerMsg, ok := msg.(*brokerstorage.Message); ok {
			deliveredMsgs <- brokerMsg
		}
		return nil
	}

	config := DefaultConfig()
	config.DeliveryInterval = 50 * time.Millisecond
	logger := slog.Default()

	manager := NewManager(logStore, groupStore, deliverFn, config, logger, nil)

	ctx := context.Background()

	if err := manager.Start(ctx); err != nil {
		t.Fatalf("Failed to start manager: %v", err)
	}
	defer manager.Stop()

	// Subscribe to exact topic (no wildcards)
	clientID := "test-client-1"
	filter := "$queue/tasks"

	if err := manager.Subscribe(ctx, filter, clientID, "", ""); err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}

	// Publish to exact topic
	if err := manager.Enqueue(ctx, "$queue/tasks", []byte("task1"), nil); err != nil {
		t.Fatalf("Enqueue failed: %v", err)
	}

	select {
	case msg := <-deliveredMsgs:
		if string(msg.GetPayload()) != "task1" {
			t.Errorf("Expected payload 'task1', got %s", msg.GetPayload())
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Timeout waiting for message delivery")
	}
}

func TestMultiLevelWildcard(t *testing.T) {
	logStore := newMockLogStore()
	groupStore := newMockGroupStore()

	deliveredMsgs := make(chan *brokerstorage.Message, 10)

	deliverFn := func(ctx context.Context, clientID string, msg any) error {
		if brokerMsg, ok := msg.(*brokerstorage.Message); ok {
			deliveredMsgs <- brokerMsg
		}
		return nil
	}

	config := DefaultConfig()
	config.DeliveryInterval = 50 * time.Millisecond
	logger := slog.Default()

	manager := NewManager(logStore, groupStore, deliverFn, config, logger, nil)

	ctx := context.Background()

	if err := manager.Start(ctx); err != nil {
		t.Fatalf("Failed to start manager: %v", err)
	}
	defer manager.Stop()

	// Subscribe to multi-level wildcard
	if err := manager.Subscribe(ctx, "$queue/images/#", "client1", "", ""); err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}

	// Publish to nested topics
	topics := []string{
		"$queue/images/png",
		"$queue/images/jpg",
		"$queue/images/photos/vacation",
	}

	for _, topic := range topics {
		if err := manager.Enqueue(ctx, topic, []byte(topic), nil); err != nil {
			t.Fatalf("Enqueue to %s failed: %v", topic, err)
		}
	}

	// Should receive all 3 messages
	received := 0
	timeout := time.After(3 * time.Second)

	for received < 3 {
		select {
		case msg := <-deliveredMsgs:
			t.Logf("Received: %s", msg.Topic)
			received++
		case <-timeout:
			t.Fatalf("Timeout: only received %d/3 messages", received)
		}
	}
}

func TestSingleLevelWildcard(t *testing.T) {
	logStore := newMockLogStore()
	groupStore := newMockGroupStore()

	deliveredMsgs := make(chan *brokerstorage.Message, 10)

	deliverFn := func(ctx context.Context, clientID string, msg any) error {
		if brokerMsg, ok := msg.(*brokerstorage.Message); ok {
			deliveredMsgs <- brokerMsg
		}
		return nil
	}

	config := DefaultConfig()
	config.DeliveryInterval = 50 * time.Millisecond
	logger := slog.Default()

	manager := NewManager(logStore, groupStore, deliverFn, config, logger, nil)

	ctx := context.Background()

	if err := manager.Start(ctx); err != nil {
		t.Fatalf("Failed to start manager: %v", err)
	}
	defer manager.Stop()

	// Subscribe to single-level wildcard
	if err := manager.Subscribe(ctx, "$queue/sensors/+/temperature", "client1", "", ""); err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}

	// These should match
	matching := []string{
		"$queue/sensors/room1/temperature",
		"$queue/sensors/room2/temperature",
	}

	// These should NOT match
	nonMatching := []string{
		"$queue/sensors/room1/humidity",             // wrong suffix
		"$queue/sensors/building/room1/temperature", // too many levels
	}

	for _, topic := range matching {
		if err := manager.Enqueue(ctx, topic, []byte("match"), nil); err != nil {
			t.Fatalf("Enqueue to %s failed: %v", topic, err)
		}
	}

	for _, topic := range nonMatching {
		if err := manager.Enqueue(ctx, topic, []byte("nomatch"), nil); err != nil {
			t.Fatalf("Enqueue to %s failed: %v", topic, err)
		}
	}

	// Should only receive matching messages
	received := 0
	timeout := time.After(2 * time.Second)

loop:
	for {
		select {
		case msg := <-deliveredMsgs:
			if string(msg.GetPayload()) == "nomatch" {
				t.Errorf("Received non-matching message: %s", msg.Topic)
			}
			received++
			if received >= 2 {
				break loop
			}
		case <-timeout:
			break loop
		}
	}

	if received != 2 {
		t.Errorf("Expected 2 messages, got %d", received)
	}
}

// mockCluster implements cluster.Cluster for testing cross-node routing.
type mockCluster struct {
	nodeID           string
	routedMessages   []routedMessage
	routedMessagesMu sync.Mutex
	partitionOwners  map[string]map[int]string // queueName -> partitionID -> nodeID
}

type routedMessage struct {
	nodeID      string
	clientID    string
	queueName   string
	messageID   string
	payload     []byte
	properties  map[string]string
	sequence    int64
	partitionID int
}

func newMockCluster(nodeID string) *mockCluster {
	return &mockCluster{
		nodeID:          nodeID,
		routedMessages:  make([]routedMessage, 0),
		partitionOwners: make(map[string]map[int]string),
	}
}

func (c *mockCluster) NodeID() string { return c.nodeID }

func (c *mockCluster) RouteQueueMessage(ctx context.Context, nodeID, clientID, queueName, messageID string, payload []byte, properties map[string]string, sequence int64, partitionID int) error {
	c.routedMessagesMu.Lock()
	defer c.routedMessagesMu.Unlock()
	c.routedMessages = append(c.routedMessages, routedMessage{
		nodeID:      nodeID,
		clientID:    clientID,
		queueName:   queueName,
		messageID:   messageID,
		payload:     payload,
		properties:  properties,
		sequence:    sequence,
		partitionID: partitionID,
	})
	return nil
}

func (c *mockCluster) GetRoutedMessages() []routedMessage {
	c.routedMessagesMu.Lock()
	defer c.routedMessagesMu.Unlock()
	result := make([]routedMessage, len(c.routedMessages))
	copy(result, c.routedMessages)
	return result
}

// Implement remaining cluster.Cluster interface methods as no-ops
func (c *mockCluster) Start() error                            { return nil }
func (c *mockCluster) Stop() error                             { return nil }
func (c *mockCluster) IsLeader() bool                          { return true }
func (c *mockCluster) WaitForLeader(ctx context.Context) error { return nil }
func (c *mockCluster) Nodes() []cluster.NodeInfo               { return nil }
func (c *mockCluster) AcquireSession(ctx context.Context, clientID, nodeID string) error {
	return nil
}
func (c *mockCluster) ReleaseSession(ctx context.Context, clientID string) error { return nil }
func (c *mockCluster) GetSessionOwner(ctx context.Context, clientID string) (string, bool, error) {
	return "", false, nil
}

func (c *mockCluster) WatchSessionOwner(ctx context.Context, clientID string) <-chan cluster.OwnershipChange {
	return nil
}

func (c *mockCluster) AcquirePartition(ctx context.Context, queueName string, partitionID int, nodeID string) error {
	return nil
}

func (c *mockCluster) ReleasePartition(ctx context.Context, queueName string, partitionID int) error {
	return nil
}

func (c *mockCluster) GetPartitionOwner(ctx context.Context, queueName string, partitionID int) (string, bool, error) {
	if owners, ok := c.partitionOwners[queueName]; ok {
		if owner, ok := owners[partitionID]; ok {
			return owner, true, nil
		}
	}
	return "", false, nil
}

func (c *mockCluster) WatchPartitionOwnership(ctx context.Context, queueName string) <-chan cluster.PartitionOwnershipChange {
	return nil
}

func (c *mockCluster) AddSubscription(ctx context.Context, clientID, filter string, qos byte, opts brokerstorage.SubscribeOptions) error {
	return nil
}

func (c *mockCluster) RemoveSubscription(ctx context.Context, clientID, filter string) error {
	return nil
}

func (c *mockCluster) GetSubscriptionsForClient(ctx context.Context, clientID string) ([]*brokerstorage.Subscription, error) {
	return nil, nil
}

func (c *mockCluster) GetSubscribersForTopic(ctx context.Context, topic string) ([]*brokerstorage.Subscription, error) {
	return nil, nil
}
func (c *mockCluster) Retained() brokerstorage.RetainedStore { return nil }
func (c *mockCluster) Wills() brokerstorage.WillStore        { return nil }
func (c *mockCluster) RoutePublish(ctx context.Context, topic string, payload []byte, qos byte, retain bool, properties map[string]string) error {
	return nil
}

func (c *mockCluster) TakeoverSession(ctx context.Context, clientID, fromNode, toNode string) (*clusterv1.SessionState, error) {
	return nil, nil
}

func (c *mockCluster) EnqueueRemote(ctx context.Context, nodeID, queueName string, payload []byte, properties map[string]string) (string, error) {
	return "", nil
}

// TestCrossNodeMessageRouting tests that messages are routed to remote nodes
// when the consumer is connected to a different node.
func TestCrossNodeMessageRouting(t *testing.T) {
	logStore := newMockLogStore()
	groupStore := newMockGroupStore()

	localNodeID := "node-1"
	remoteNodeID := "node-2"
	mockCl := newMockCluster(localNodeID)

	// Track local deliveries
	var localDeliveries []string
	var localMu sync.Mutex
	deliverFn := func(ctx context.Context, clientID string, msg any) error {
		localMu.Lock()
		localDeliveries = append(localDeliveries, clientID)
		localMu.Unlock()
		return nil
	}

	config := DefaultConfig()
	config.DeliveryInterval = 50 * time.Millisecond
	logger := slog.Default()

	manager := NewManager(logStore, groupStore, deliverFn, config, logger, mockCl)

	ctx := context.Background()

	if err := manager.Start(ctx); err != nil {
		t.Fatalf("Failed to start manager: %v", err)
	}
	defer manager.Stop()

	// Subscribe a local consumer (on node-1)
	localClientID := "local-client"
	if err := manager.Subscribe(ctx, "$queue/test/#", localClientID, "", localNodeID); err != nil {
		t.Fatalf("Subscribe local client failed: %v", err)
	}

	// Subscribe a remote consumer (on node-2)
	remoteClientID := "remote-client"
	if err := manager.Subscribe(ctx, "$queue/test/#", remoteClientID, "", remoteNodeID); err != nil {
		t.Fatalf("Subscribe remote client failed: %v", err)
	}

	// Publish a message
	if err := manager.Enqueue(ctx, "$queue/test/msg", []byte("hello"), nil); err != nil {
		t.Fatalf("Enqueue failed: %v", err)
	}

	// Wait for delivery
	time.Sleep(200 * time.Millisecond)

	// Check that local client received via deliverFn
	localMu.Lock()
	localCount := len(localDeliveries)
	localMu.Unlock()

	// Check that remote client was routed via cluster
	routedMsgs := mockCl.GetRoutedMessages()

	t.Logf("Local deliveries: %d, Routed messages: %d", localCount, len(routedMsgs))

	// Verify remote routing happened
	if len(routedMsgs) == 0 {
		t.Error("Expected at least one message to be routed to remote node")
	} else {
		for _, rm := range routedMsgs {
			t.Logf("Routed message: nodeID=%s, clientID=%s, queue=%s", rm.nodeID, rm.clientID, rm.queueName)
			if rm.nodeID != remoteNodeID {
				t.Errorf("Expected routed message to node %s, got %s", remoteNodeID, rm.nodeID)
			}
			if rm.clientID != remoteClientID {
				t.Errorf("Expected routed message to client %s, got %s", remoteClientID, rm.clientID)
			}
		}
	}
}

// TestDeliverQueueMessage tests the DeliverQueueMessage method used for
// receiving routed messages from other nodes.
func TestDeliverQueueMessage(t *testing.T) {
	logStore := newMockLogStore()
	groupStore := newMockGroupStore()

	var deliveredMsg *brokerstorage.Message
	var deliveredClientID string
	var mu sync.Mutex

	deliverFn := func(ctx context.Context, clientID string, msg any) error {
		mu.Lock()
		defer mu.Unlock()
		deliveredClientID = clientID
		if m, ok := msg.(*brokerstorage.Message); ok {
			deliveredMsg = m
		}
		return nil
	}

	config := DefaultConfig()
	logger := slog.Default()

	manager := NewManager(logStore, groupStore, deliverFn, config, logger, nil)

	ctx := context.Background()

	// Simulate a routed message from another node
	msg := map[string]interface{}{
		"id":          "msg-123",
		"queueName":   "$queue/test",
		"payload":     []byte("routed payload"),
		"properties":  map[string]string{"custom": "prop"},
		"sequence":    int64(42),
		"partitionId": int32(0),
	}

	err := manager.DeliverQueueMessage(ctx, "target-client", msg)
	if err != nil {
		t.Fatalf("DeliverQueueMessage failed: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()

	if deliveredClientID != "target-client" {
		t.Errorf("Expected clientID 'target-client', got '%s'", deliveredClientID)
	}

	if deliveredMsg == nil {
		t.Fatal("Expected delivered message, got nil")
	}

	if deliveredMsg.Topic != "$queue/test" {
		t.Errorf("Expected topic '$queue/test', got '%s'", deliveredMsg.Topic)
	}

	if string(deliveredMsg.GetPayload()) != "routed payload" {
		t.Errorf("Expected payload 'routed payload', got '%s'", string(deliveredMsg.GetPayload()))
	}

	if deliveredMsg.Properties["message-id"] != "msg-123" {
		t.Errorf("Expected message-id 'msg-123', got '%s'", deliveredMsg.Properties["message-id"])
	}
}

// TestEnqueueLocal tests the EnqueueLocal method used for receiving
// enqueue requests from other nodes.
func TestEnqueueLocal(t *testing.T) {
	logStore := newMockLogStore()
	groupStore := newMockGroupStore()

	deliverFn := func(ctx context.Context, clientID string, msg any) error {
		return nil
	}

	config := DefaultConfig()
	logger := slog.Default()

	manager := NewManager(logStore, groupStore, deliverFn, config, logger, nil)

	ctx := context.Background()

	if err := manager.Start(ctx); err != nil {
		t.Fatalf("Failed to start manager: %v", err)
	}
	defer manager.Stop()

	// EnqueueLocal should create the queue and enqueue the message
	msgID, err := manager.EnqueueLocal(ctx, "$queue/remote", []byte("remote payload"), map[string]string{"key": "value"})
	if err != nil {
		t.Fatalf("EnqueueLocal failed: %v", err)
	}

	if msgID == "" {
		t.Error("Expected non-empty message ID")
	}

	// Verify the queue was created
	queues, err := logStore.ListQueues(ctx)
	if err != nil {
		t.Fatalf("ListQueues failed: %v", err)
	}

	found := false
	for _, q := range queues {
		if q.Name == "$queue/remote" {
			found = true
			break
		}
	}

	if !found {
		t.Error("Expected queue '$queue/remote' to be created")
	}
}
