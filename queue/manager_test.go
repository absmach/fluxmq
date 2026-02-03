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
	memlog "github.com/absmach/fluxmq/queue/storage/memory/log"
	"github.com/absmach/fluxmq/queue/types"
	brokerstorage "github.com/absmach/fluxmq/storage"
)

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

func (s *mockGroupStore) RemovePendingEntry(ctx context.Context, queueName, groupID, consumerID string, offset uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.groups[queueName] == nil || s.groups[queueName][groupID] == nil {
		return storage.ErrConsumerNotFound
	}

	group := s.groups[queueName][groupID]
	if !group.RemovePending(consumerID, offset) {
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

func (s *mockGroupStore) TransferPendingEntry(ctx context.Context, queueName, groupID string, offset uint64, fromConsumer, toConsumer string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.groups[queueName] == nil || s.groups[queueName][groupID] == nil {
		return storage.ErrConsumerNotFound
	}

	group := s.groups[queueName][groupID]
	if !group.TransferPending(offset, fromConsumer, toConsumer) {
		return storage.ErrPendingEntryNotFound
	}
	return nil
}

func (s *mockGroupStore) UpdateCursor(ctx context.Context, queueName, groupID string, cursor uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.groups[queueName] == nil || s.groups[queueName][groupID] == nil {
		return storage.ErrConsumerNotFound
	}

	group := s.groups[queueName][groupID]
	pc := group.GetCursor()
	pc.Cursor = cursor
	return nil
}

func (s *mockGroupStore) UpdateCommitted(ctx context.Context, queueName, groupID string, committed uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.groups[queueName] == nil || s.groups[queueName][groupID] == nil {
		return storage.ErrConsumerNotFound
	}

	group := s.groups[queueName][groupID]
	pc := group.GetCursor()
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
	logStore := memlog.New()
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
	config.DeliveryInterval = 50 * time.Millisecond
	logger := slog.Default()

	manager := NewManager(logStore, groupStore, deliverFn, config, logger, nil)

	ctx := context.Background()

	if err := manager.Start(ctx); err != nil {
		t.Fatalf("Failed to start manager: %v", err)
	}
	defer manager.Stop()

	clientID := "test-client-1"
	queueName := "topic"
	pattern := "#"

	t.Logf("Subscribing client %s to queue %s with pattern %s", clientID, queueName, pattern)
	if err := manager.Subscribe(ctx, queueName, pattern, clientID, "", ""); err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}

	queues, _ := logStore.ListQueues(ctx)
	t.Logf("Queues after subscribe: %v", len(queues))
	for _, q := range queues {
		t.Logf("  Queue: %s", q.Name)
	}

	groups, _ := groupStore.ListConsumerGroups(ctx, queueName)
	t.Logf("Groups after subscribe: %v", len(groups))
	for _, g := range groups {
		t.Logf("  Group: %s (pattern=%s, consumers=%d)", g.ID, g.Pattern, len(g.Consumers))
		for cid, ci := range g.Consumers {
			t.Logf("    Consumer: %s (clientID=%s)", cid, ci.ClientID)
		}
	}

	publishTopic := "$queue/topic/test"
	payload := []byte("hello world")

	t.Logf("Publishing message to %s", publishTopic)
	if err := manager.Enqueue(ctx, publishTopic, payload, nil); err != nil {
		t.Fatalf("Enqueue failed: %v", err)
	}

	tail, _ := logStore.Tail(ctx, queueName)
	t.Logf("Tail after publish: %d", tail)

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
		groups, _ = groupStore.ListConsumerGroups(ctx, queueName)
		for _, g := range groups {
			t.Logf("Group state: %s cursor=%v", g.ID, g.Cursor)
		}
		t.Fatal("Timeout waiting for message delivery")
	}
}

func TestExactQueueSubscription(t *testing.T) {
	logStore := memlog.New()
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

	clientID := "test-client-1"
	queueName := "tasks"

	if err := manager.Subscribe(ctx, queueName, "", clientID, "", ""); err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}

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
	logStore := memlog.New()
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

	if err := manager.Subscribe(ctx, "images", "#", "client1", "", ""); err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}

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
	logStore := memlog.New()
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

	if err := manager.Subscribe(ctx, "sensors", "+/temperature", "client1", "", ""); err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}

	matching := []string{
		"$queue/sensors/room1/temperature",
		"$queue/sensors/room2/temperature",
	}

	nonMatching := []string{
		"$queue/sensors/room1/humidity",
		"$queue/sensors/building/room1/temperature",
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

func TestQueueNameWildcardSingleLevel(t *testing.T) {
	logStore := memlog.New()
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

	if err := manager.Subscribe(ctx, "+", "temperature", "client1", "", ""); err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}

	matching := []string{
		"$queue/sensors/temperature",
		"$queue/metrics/temperature",
	}

	nonMatching := []string{
		"$queue/sensors/humidity",
		"$queue/sensors/room/temperature",
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
}

type routedMessage struct {
	nodeID    string
	clientID  string
	queueName string
	messageID string
	payload   []byte
	sequence  int64
}

func newMockCluster(nodeID string) *mockCluster {
	return &mockCluster{
		nodeID:         nodeID,
		routedMessages: make([]routedMessage, 0),
	}
}

func (c *mockCluster) NodeID() string { return c.nodeID }

func (c *mockCluster) RouteQueueMessage(ctx context.Context, nodeID, clientID, queueName, messageID string, payload []byte, properties map[string]string, sequence int64) error {
	c.routedMessagesMu.Lock()
	defer c.routedMessagesMu.Unlock()
	c.routedMessages = append(c.routedMessages, routedMessage{
		nodeID:    nodeID,
		clientID:  clientID,
		queueName: queueName,
		messageID: messageID,
		payload:   payload,
		sequence:  sequence,
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

func (c *mockCluster) RegisterQueueConsumer(ctx context.Context, info *cluster.QueueConsumerInfo) error {
	return nil
}

func (c *mockCluster) UnregisterQueueConsumer(ctx context.Context, queueName, groupID, consumerID string) error {
	return nil
}

func (c *mockCluster) ListQueueConsumers(ctx context.Context, queueName string) ([]*cluster.QueueConsumerInfo, error) {
	return nil, nil
}

func (c *mockCluster) ListQueueConsumersByGroup(ctx context.Context, queueName, groupID string) ([]*cluster.QueueConsumerInfo, error) {
	return nil, nil
}

func (c *mockCluster) ListAllQueueConsumers(ctx context.Context) ([]*cluster.QueueConsumerInfo, error) {
	return nil, nil
}

func (c *mockCluster) ForwardQueuePublish(ctx context.Context, nodeID, topic string, payload []byte, properties map[string]string) error {
	return nil
}

func TestCrossNodeMessageRouting(t *testing.T) {
	logStore := memlog.New()
	groupStore := newMockGroupStore()

	localNodeID := "node-1"
	remoteNodeID := "node-2"
	mockCl := newMockCluster(localNodeID)

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

	localClientID := "local-client"
	if err := manager.Subscribe(ctx, "test", "#", localClientID, "", localNodeID); err != nil {
		t.Fatalf("Subscribe local client failed: %v", err)
	}

	remoteClientID := "remote-client"
	if err := manager.Subscribe(ctx, "test", "#", remoteClientID, "", remoteNodeID); err != nil {
		t.Fatalf("Subscribe remote client failed: %v", err)
	}

	if err := manager.Enqueue(ctx, "$queue/test/msg", []byte("hello"), nil); err != nil {
		t.Fatalf("Enqueue failed: %v", err)
	}

	time.Sleep(200 * time.Millisecond)

	localMu.Lock()
	localCount := len(localDeliveries)
	localMu.Unlock()

	routedMsgs := mockCl.GetRoutedMessages()

	t.Logf("Local deliveries: %d, Routed messages: %d", localCount, len(routedMsgs))

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

func TestDeliverQueueMessage(t *testing.T) {
	logStore := memlog.New()
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

	msg := map[string]interface{}{
		"id":         "msg-123",
		"queueName":  "$queue/test",
		"payload":    []byte("routed payload"),
		"properties": map[string]string{"custom": "prop"},
		"sequence":   int64(42),
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

func TestGetOrCreateQueue_CreatesEphemeral(t *testing.T) {
	logStore := memlog.New()
	groupStore := newMockGroupStore()

	deliverFn := func(ctx context.Context, clientID string, msg any) error { return nil }
	config := DefaultConfig()
	logger := slog.Default()

	manager := NewManager(logStore, groupStore, deliverFn, config, logger, nil)
	ctx := context.Background()

	if err := manager.Start(ctx); err != nil {
		t.Fatalf("Failed to start manager: %v", err)
	}
	defer manager.Stop()

	cfg, err := manager.GetOrCreateQueue(ctx, "ephemeral-test", "$queue/ephemeral-test/#")
	if err != nil {
		t.Fatalf("GetOrCreateQueue failed: %v", err)
	}

	if cfg.Durable {
		t.Error("Expected auto-created queue to be ephemeral (Durable=false)")
	}
	if cfg.ExpiresAfter != 5*time.Minute {
		t.Errorf("Expected ExpiresAfter=5m, got %v", cfg.ExpiresAfter)
	}
}

func TestEphemeralQueue_DisconnectAndCleanup(t *testing.T) {
	logStore := memlog.New()
	groupStore := newMockGroupStore()

	deliverFn := func(ctx context.Context, clientID string, msg any) error { return nil }
	config := DefaultConfig()
	config.DeliveryInterval = 50 * time.Millisecond
	logger := slog.Default()

	manager := NewManager(logStore, groupStore, deliverFn, config, logger, nil)
	ctx := context.Background()

	if err := manager.Start(ctx); err != nil {
		t.Fatalf("Failed to start manager: %v", err)
	}
	defer manager.Stop()

	// Subscribe creates an ephemeral queue
	if err := manager.Subscribe(ctx, "eph-queue", "#", "client1", "", ""); err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}

	// Verify queue exists and disconnect time is zero
	cfg, err := logStore.GetQueue(ctx, "eph-queue")
	if err != nil {
		t.Fatalf("GetQueue failed: %v", err)
	}
	if !cfg.LastConsumerDisconnect.IsZero() {
		t.Error("Expected LastConsumerDisconnect to be zero while consumer is active")
	}

	// Unsubscribe - should set disconnect time
	if err := manager.Unsubscribe(ctx, "eph-queue", "#", "client1", ""); err != nil {
		t.Fatalf("Unsubscribe failed: %v", err)
	}

	cfg, err = logStore.GetQueue(ctx, "eph-queue")
	if err != nil {
		t.Fatalf("GetQueue failed: %v", err)
	}
	if cfg.LastConsumerDisconnect.IsZero() {
		t.Error("Expected LastConsumerDisconnect to be set after last consumer leaves")
	}

	// Re-subscribe should clear disconnect time
	if err := manager.Subscribe(ctx, "eph-queue", "#", "client2", "", ""); err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}

	cfg, err = logStore.GetQueue(ctx, "eph-queue")
	if err != nil {
		t.Fatalf("GetQueue failed: %v", err)
	}
	if !cfg.LastConsumerDisconnect.IsZero() {
		t.Error("Expected LastConsumerDisconnect to be cleared after new consumer subscribes")
	}
}

func TestCleanupEphemeralQueues(t *testing.T) {
	logStore := memlog.New()
	groupStore := newMockGroupStore()

	deliverFn := func(ctx context.Context, clientID string, msg any) error { return nil }
	config := DefaultConfig()
	logger := slog.Default()

	manager := NewManager(logStore, groupStore, deliverFn, config, logger, nil)
	ctx := context.Background()

	if err := manager.Start(ctx); err != nil {
		t.Fatalf("Failed to start manager: %v", err)
	}
	defer manager.Stop()

	// Create an ephemeral queue with a very short expiry that already expired
	ephCfg := types.DefaultEphemeralQueueConfig("expired-queue", "$queue/expired/#")
	ephCfg.ExpiresAfter = 1 * time.Millisecond
	ephCfg.LastConsumerDisconnect = time.Now().Add(-1 * time.Second) // expired
	if err := logStore.CreateQueue(ctx, ephCfg); err != nil {
		t.Fatalf("CreateQueue failed: %v", err)
	}

	// Create a durable queue
	durCfg := types.DefaultQueueConfig("durable-queue", "$queue/durable/#")
	if err := logStore.CreateQueue(ctx, durCfg); err != nil {
		t.Fatalf("CreateQueue failed: %v", err)
	}

	// Create an ephemeral queue with active consumers (zero disconnect time)
	activeCfg := types.DefaultEphemeralQueueConfig("active-queue", "$queue/active/#")
	if err := logStore.CreateQueue(ctx, activeCfg); err != nil {
		t.Fatalf("CreateQueue failed: %v", err)
	}

	// Run cleanup
	manager.cleanupEphemeralQueues()

	// Expired ephemeral queue should be deleted
	if _, err := logStore.GetQueue(ctx, "expired-queue"); err != storage.ErrQueueNotFound {
		t.Error("Expected expired ephemeral queue to be deleted")
	}

	// Durable queue should still exist
	if _, err := logStore.GetQueue(ctx, "durable-queue"); err != nil {
		t.Error("Expected durable queue to still exist")
	}

	// Active ephemeral queue should still exist (zero disconnect time)
	if _, err := logStore.GetQueue(ctx, "active-queue"); err != nil {
		t.Error("Expected active ephemeral queue to still exist")
	}
}

func TestEnqueueLocal(t *testing.T) {
	logStore := memlog.New()
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

	msgID, err := manager.EnqueueLocal(ctx, "$queue/remote", []byte("remote payload"), map[string]string{"key": "value"})
	if err != nil {
		t.Fatalf("EnqueueLocal failed: %v", err)
	}

	if msgID == "" {
		t.Error("Expected non-empty message ID")
	}

	// The message should be routed to the mqtt queue (topic pattern $queue/#)
	tail, err := logStore.Tail(ctx, "mqtt")
	if err != nil {
		t.Fatalf("Tail failed: %v", err)
	}

	if tail == 0 {
		t.Error("Expected message to be stored in mqtt queue")
	}
}
