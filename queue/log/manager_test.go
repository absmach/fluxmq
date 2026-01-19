// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package log

import (
	"context"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/absmach/fluxmq/queue/storage"
	"github.com/absmach/fluxmq/queue/types"
	brokerstorage "github.com/absmach/fluxmq/storage"
)

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
		"$queue/sensors/room1/humidity",               // wrong suffix
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
