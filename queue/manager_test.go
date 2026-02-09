// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package queue

import (
	"context"
	"io"
	"log/slog"
	"reflect"
	"sync"
	"testing"
	"time"
	"unsafe"

	"github.com/absmach/fluxmq/cluster"
	clusterv1 "github.com/absmach/fluxmq/pkg/proto/cluster/v1"
	queueraft "github.com/absmach/fluxmq/queue/raft"
	"github.com/absmach/fluxmq/queue/storage"
	memlog "github.com/absmach/fluxmq/queue/storage/memory/log"
	"github.com/absmach/fluxmq/queue/types"
	brokerstorage "github.com/absmach/fluxmq/storage"
	hraft "github.com/hashicorp/raft"
)

// mockGroupStore implements storage.ConsumerGroupStore for testing.
type mockGroupStore struct {
	mu     sync.RWMutex
	groups map[string]map[string]*types.ConsumerGroup // queueName -> groupID -> state
}

func newMockGroupStore() *mockGroupStore {
	return &mockGroupStore{
		groups: make(map[string]map[string]*types.ConsumerGroup),
	}
}

func (s *mockGroupStore) CreateConsumerGroup(ctx context.Context, group *types.ConsumerGroup) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.groups[group.QueueName] == nil {
		s.groups[group.QueueName] = make(map[string]*types.ConsumerGroup)
	}

	if _, exists := s.groups[group.QueueName][group.ID]; exists {
		return storage.ErrConsumerGroupExists
	}

	s.groups[group.QueueName][group.ID] = group
	return nil
}

func (s *mockGroupStore) GetConsumerGroup(ctx context.Context, queueName, groupID string) (*types.ConsumerGroup, error) {
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

func (s *mockGroupStore) UpdateConsumerGroup(ctx context.Context, group *types.ConsumerGroup) error {
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

func (s *mockGroupStore) ListConsumerGroups(ctx context.Context, queueName string) ([]*types.ConsumerGroup, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var groups []*types.ConsumerGroup
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
	group.SetConsumer(consumer.ID, consumer)
	return nil
}

func (s *mockGroupStore) UnregisterConsumer(ctx context.Context, queueName, groupID, consumerID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.groups[queueName] == nil || s.groups[queueName][groupID] == nil {
		return storage.ErrConsumerNotFound
	}

	group := s.groups[queueName][groupID]
	group.DeleteConsumer(consumerID)
	group.DeleteConsumerPEL(consumerID)
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
	group.ForEachConsumer(func(_ string, c *types.ConsumerInfo) bool {
		result = append(result, c)
		return true
	})
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
		t.Logf("  Group: %s (pattern=%s, consumers=%d)", g.ID, g.Pattern, g.ConsumerCount())
		g.ForEachConsumer(func(cid string, ci *types.ConsumerInfo) bool {
			t.Logf("    Consumer: %s (clientID=%s)", cid, ci.ClientID)
			return true
		})
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

func TestStreamGroupDeliversWithoutPEL(t *testing.T) {
	logStore := memlog.New()
	groupStore := newMockGroupStore()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	delivered := make(chan *brokerstorage.Message, 1)

	deliverFn := func(ctx context.Context, clientID string, msg any) error {
		if m, ok := msg.(*brokerstorage.Message); ok {
			delivered <- m
		}
		return nil
	}

	cfg := DefaultConfig()
	cfg.DeliveryBatchSize = 10
	mgr := NewManager(logStore, groupStore, deliverFn, cfg, logger, nil)

	queueCfg := types.DefaultQueueConfig("events", "$queue/events/#")
	queueCfg.Type = types.QueueTypeStream
	if err := mgr.CreateQueue(context.Background(), queueCfg); err != nil {
		t.Fatalf("CreateQueue failed: %v", err)
	}

	cursor := &types.CursorOption{Position: types.CursorEarliest, Mode: types.GroupModeStream}
	if err := mgr.SubscribeWithCursor(context.Background(), "events", "", "client-1", "streamer", "", cursor); err != nil {
		t.Fatalf("SubscribeWithCursor failed: %v", err)
	}

	if err := mgr.Publish(context.Background(), types.PublishRequest{
		Topic:      "$queue/events/test",
		Payload:    []byte("hello"),
		Properties: nil,
	}); err != nil {
		t.Fatalf("Publish failed: %v", err)
	}

	mgr.deliverMessages()

	select {
	case msg := <-delivered:
		if got := msg.Properties[types.PropStreamOffset]; got != "0" {
			t.Fatalf("expected stream offset 0, got %q", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for delivery")
	}

	group, err := groupStore.GetConsumerGroup(context.Background(), "events", "streamer")
	if err != nil {
		t.Fatalf("GetConsumerGroup failed: %v", err)
	}
	if count := group.PendingCount(); count != 0 {
		t.Fatalf("expected no pending entries, got %d", count)
	}
	if cursor := group.GetCursor().Cursor; cursor != 1 {
		t.Fatalf("expected cursor 1, got %d", cursor)
	}
}

func TestStreamAckAdvancesCursor(t *testing.T) {
	logStore := memlog.New()
	groupStore := newMockGroupStore()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	mgr := NewManager(logStore, groupStore, nil, DefaultConfig(), logger, nil)

	queueCfg := types.DefaultQueueConfig("events", "$queue/events/#")
	queueCfg.Type = types.QueueTypeStream
	if err := mgr.CreateQueue(context.Background(), queueCfg); err != nil {
		t.Fatalf("CreateQueue failed: %v", err)
	}

	cursor := &types.CursorOption{Position: types.CursorEarliest, Mode: types.GroupModeStream}
	if err := mgr.SubscribeWithCursor(context.Background(), "events", "", "client-1", "streamer", "", cursor); err != nil {
		t.Fatalf("SubscribeWithCursor failed: %v", err)
	}

	if err := mgr.Ack(context.Background(), "events", "events:0", "streamer"); err != nil {
		t.Fatalf("Ack failed: %v", err)
	}

	group, err := groupStore.GetConsumerGroup(context.Background(), "events", "streamer")
	if err != nil {
		t.Fatalf("GetConsumerGroup failed: %v", err)
	}
	if cursor := group.GetCursor().Cursor; cursor != 1 {
		t.Fatalf("expected cursor 1, got %d", cursor)
	}
}

func TestStreamRejectAdvancesCursor(t *testing.T) {
	logStore := memlog.New()
	groupStore := newMockGroupStore()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	mgr := NewManager(logStore, groupStore, nil, DefaultConfig(), logger, nil)

	queueCfg := types.DefaultQueueConfig("events", "$queue/events/#")
	queueCfg.Type = types.QueueTypeStream
	if err := mgr.CreateQueue(context.Background(), queueCfg); err != nil {
		t.Fatalf("CreateQueue failed: %v", err)
	}

	cursor := &types.CursorOption{Position: types.CursorEarliest, Mode: types.GroupModeStream}
	if err := mgr.SubscribeWithCursor(context.Background(), "events", "", "client-1", "streamer", "", cursor); err != nil {
		t.Fatalf("SubscribeWithCursor failed: %v", err)
	}

	if err := mgr.Reject(context.Background(), "events", "events:0", "streamer", "bad message"); err != nil {
		t.Fatalf("Reject failed: %v", err)
	}

	group, err := groupStore.GetConsumerGroup(context.Background(), "events", "streamer")
	if err != nil {
		t.Fatalf("GetConsumerGroup failed: %v", err)
	}
	if c := group.GetCursor().Cursor; c != 1 {
		t.Fatalf("expected cursor 1 after reject, got %d", c)
	}
	if c := group.GetCursor().Committed; c != 1 {
		t.Fatalf("expected committed 1 after reject, got %d", c)
	}
}

func TestRetentionOffsetMessages(t *testing.T) {
	logStore := memlog.New()
	groupStore := newMockGroupStore()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	mgr := NewManager(logStore, groupStore, nil, DefaultConfig(), logger, nil)

	queueCfg := types.DefaultQueueConfig("events", "$queue/events/#")
	if err := mgr.CreateQueue(context.Background(), queueCfg); err != nil {
		t.Fatalf("CreateQueue failed: %v", err)
	}

	for i := 0; i < 3; i++ {
		if err := mgr.Publish(context.Background(), types.PublishRequest{
			Topic:      "$queue/events/test",
			Payload:    []byte("msg"),
			Properties: nil,
		}); err != nil {
			t.Fatalf("Publish failed: %v", err)
		}
	}

	queueCfg.Retention.RetentionMessages = 1
	queueCfg.Name = "events"
	offset, ok := mgr.computeRetentionOffset(context.Background(), &queueCfg)
	if !ok {
		t.Fatal("expected retention offset")
	}
	if offset != 2 {
		t.Fatalf("expected retention offset 2, got %d", offset)
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
	forwardCalls     []forwardPublishCall
	forwardCallsMu   sync.Mutex
	queueConsumers   []*cluster.QueueConsumerInfo
	queueConsumersMu sync.RWMutex
	registered       []*cluster.QueueConsumerInfo
	registeredMu     sync.Mutex
}

type routedMessage struct {
	nodeID    string
	clientID  string
	queueName string
	message   *cluster.QueueMessage
}

type forwardPublishCall struct {
	nodeID          string
	topic           string
	payload         []byte
	properties      map[string]string
	forwardToLeader bool
}

func newMockCluster(nodeID string) *mockCluster {
	return &mockCluster{
		nodeID:         nodeID,
		routedMessages: make([]routedMessage, 0),
		forwardCalls:   make([]forwardPublishCall, 0),
	}
}

func (c *mockCluster) NodeID() string { return c.nodeID }

func (c *mockCluster) RouteQueueMessage(ctx context.Context, nodeID, clientID, queueName string, msg *cluster.QueueMessage) error {
	var msgCopy *cluster.QueueMessage
	if msg != nil {
		userProps := make(map[string]string, len(msg.UserProperties))
		for k, v := range msg.UserProperties {
			userProps[k] = v
		}
		payloadCopy := make([]byte, len(msg.Payload))
		copy(payloadCopy, msg.Payload)
		copied := *msg
		copied.UserProperties = userProps
		copied.Payload = payloadCopy
		msgCopy = &copied
	}
	c.routedMessagesMu.Lock()
	defer c.routedMessagesMu.Unlock()
	c.routedMessages = append(c.routedMessages, routedMessage{
		nodeID:    nodeID,
		clientID:  clientID,
		queueName: queueName,
		message:   msgCopy,
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

func (c *mockCluster) GetForwardCalls() []forwardPublishCall {
	c.forwardCallsMu.Lock()
	defer c.forwardCallsMu.Unlock()
	result := make([]forwardPublishCall, len(c.forwardCalls))
	copy(result, c.forwardCalls)
	return result
}

func (c *mockCluster) SetQueueConsumers(consumers []*cluster.QueueConsumerInfo) {
	c.queueConsumersMu.Lock()
	defer c.queueConsumersMu.Unlock()
	c.queueConsumers = consumers
}

func (c *mockCluster) GetRegisteredQueueConsumers() []*cluster.QueueConsumerInfo {
	c.registeredMu.Lock()
	defer c.registeredMu.Unlock()

	out := make([]*cluster.QueueConsumerInfo, len(c.registered))
	copy(out, c.registered)
	return out
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
	c.registeredMu.Lock()
	defer c.registeredMu.Unlock()
	c.registered = append(c.registered, info)
	return nil
}

func (c *mockCluster) UnregisterQueueConsumer(ctx context.Context, queueName, groupID, consumerID string) error {
	return nil
}

func (c *mockCluster) ListQueueConsumers(ctx context.Context, queueName string) ([]*cluster.QueueConsumerInfo, error) {
	c.queueConsumersMu.RLock()
	defer c.queueConsumersMu.RUnlock()

	if c.queueConsumers == nil {
		return nil, nil
	}

	consumers := make([]*cluster.QueueConsumerInfo, 0, len(c.queueConsumers))
	for _, consumer := range c.queueConsumers {
		if consumer != nil && consumer.QueueName == queueName {
			consumers = append(consumers, consumer)
		}
	}

	return consumers, nil
}

func (c *mockCluster) ListQueueConsumersByGroup(ctx context.Context, queueName, groupID string) ([]*cluster.QueueConsumerInfo, error) {
	return nil, nil
}

func (c *mockCluster) ListAllQueueConsumers(ctx context.Context) ([]*cluster.QueueConsumerInfo, error) {
	c.queueConsumersMu.RLock()
	defer c.queueConsumersMu.RUnlock()

	if c.queueConsumers == nil {
		return nil, nil
	}
	consumers := make([]*cluster.QueueConsumerInfo, len(c.queueConsumers))
	copy(consumers, c.queueConsumers)
	return consumers, nil
}

func (c *mockCluster) ForwardQueuePublish(ctx context.Context, nodeID, topic string, payload []byte, properties map[string]string, forwardToLeader bool) error {
	c.forwardCallsMu.Lock()
	defer c.forwardCallsMu.Unlock()
	c.forwardCalls = append(c.forwardCalls, forwardPublishCall{
		nodeID:          nodeID,
		topic:           topic,
		payload:         payload,
		properties:      properties,
		forwardToLeader: forwardToLeader,
	})
	return nil
}

func setUnexportedField(t *testing.T, target any, fieldName string, value any) {
	t.Helper()

	v := reflect.ValueOf(target)
	if v.Kind() != reflect.Ptr || v.IsNil() {
		t.Fatalf("target must be a non-nil pointer")
	}

	elem := v.Elem()
	field := elem.FieldByName(fieldName)
	if !field.IsValid() {
		t.Fatalf("field %q not found on %T", fieldName, target)
	}

	val := reflect.ValueOf(value)
	if !val.IsValid() {
		t.Fatalf("value for %q is invalid", fieldName)
	}

	if !val.Type().AssignableTo(field.Type()) {
		t.Fatalf("cannot assign %s to %s for %q", val.Type(), field.Type(), fieldName)
	}

	reflect.NewAt(field.Type(), unsafe.Pointer(field.UnsafeAddr())).Elem().Set(val)
}

func newTestRaftManager(t *testing.T, leaderID string) *queueraft.Manager {
	t.Helper()

	rm := &queueraft.Manager{}
	setUnexportedField(t, rm, "config", queueraft.ManagerConfig{Enabled: true})

	fakeRaft := &hraft.Raft{}
	setUnexportedField(t, fakeRaft, "leaderID", hraft.ServerID(leaderID))
	setUnexportedField(t, rm, "raft", fakeRaft)

	return rm
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
			if rm.message == nil {
				t.Error("Expected routed message payload to be set")
				continue
			}
			if rm.message.MessageID == "" {
				t.Error("Expected routed message-id to be set")
			}
			if rm.message.GroupID == "" {
				t.Error("Expected routed message to include group-id")
			}
		}
	}
}

func TestSubscribeDefaultsProxyNodeIDFromCluster(t *testing.T) {
	logStore := memlog.New()
	groupStore := newMockGroupStore()
	mockCl := newMockCluster("node-1")

	manager := NewManager(
		logStore,
		groupStore,
		func(ctx context.Context, clientID string, msg any) error { return nil },
		DefaultConfig(),
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		mockCl,
	)

	if err := manager.Subscribe(context.Background(), "demo-orders", "#", "amqp091-conn-1", "demo-workers", ""); err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}

	registered := mockCl.GetRegisteredQueueConsumers()
	if len(registered) != 1 {
		t.Fatalf("expected 1 registered consumer, got %d", len(registered))
	}
	if registered[0].ProxyNodeID != "node-1" {
		t.Fatalf("expected proxy node id node-1, got %q", registered[0].ProxyNodeID)
	}
}

func TestRemoteRoutingIncludesAckMetadata(t *testing.T) {
	logStore := memlog.New()
	groupStore := newMockGroupStore()
	mockCl := newMockCluster("node-1")

	manager := NewManager(
		logStore,
		groupStore,
		func(ctx context.Context, clientID string, msg any) error { return nil },
		DefaultConfig(),
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		mockCl,
	)

	ctx := context.Background()
	if err := manager.Subscribe(ctx, "tasks", "", "remote-client", "workers", "node-2"); err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}

	if err := manager.Enqueue(ctx, "$queue/tasks/new", []byte("job"), map[string]string{"custom": "value"}); err != nil {
		t.Fatalf("Enqueue failed: %v", err)
	}

	manager.deliverMessages()

	routed := mockCl.GetRoutedMessages()
	if len(routed) != 1 {
		t.Fatalf("expected 1 routed message, got %d", len(routed))
	}

	msg := routed[0]
	if msg.message == nil {
		t.Fatal("expected routed queue message payload")
	}
	if msg.message.MessageID != "tasks:0" {
		t.Fatalf("expected message-id tasks:0, got %q", msg.message.MessageID)
	}
	if got := msg.message.GroupID; got != "workers" {
		t.Fatalf("expected group-id workers, got %q", got)
	}
	if got := msg.message.QueueName; got != "tasks" {
		t.Fatalf("expected queue tasks, got %q", got)
	}
	if got := msg.message.Sequence; got != 0 {
		t.Fatalf("expected sequence 0, got %d", got)
	}
	if got := msg.message.UserProperties["custom"]; got != "value" {
		t.Fatalf("expected user property custom=value, got %q", got)
	}
}

func TestRemoteStreamBacklogDeliveredByFallbackSweep(t *testing.T) {
	logStore := memlog.New()
	groupStore := newMockGroupStore()
	mockCl := newMockCluster("node-1")

	cfg := DefaultConfig()
	cfg.DeliveryInterval = 5 * time.Second

	manager := NewManager(
		logStore,
		groupStore,
		func(ctx context.Context, clientID string, msg any) error { return nil },
		cfg,
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		mockCl,
	)

	if err := manager.Start(context.Background()); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer manager.Stop()

	ctx := context.Background()
	queueCfg := types.DefaultQueueConfig("events", "$queue/events/#")
	queueCfg.Type = types.QueueTypeStream
	if err := manager.CreateQueue(ctx, queueCfg); err != nil && err != storage.ErrQueueAlreadyExists {
		t.Fatalf("CreateQueue failed: %v", err)
	}

	if err := manager.Enqueue(ctx, "$queue/events/user.action", []byte("event-1"), nil); err != nil {
		t.Fatalf("Enqueue failed: %v", err)
	}
	time.Sleep(200 * time.Millisecond)

	// Simulate remote stream consumer registration that happens after publish.
	mockCl.SetQueueConsumers([]*cluster.QueueConsumerInfo{
		{
			QueueName:    "events",
			GroupID:      "demo-readers@#",
			ConsumerID:   "remote-consumer-1",
			ClientID:     "amqp091-conn-remote",
			Pattern:      "#",
			Mode:         string(types.GroupModeStream),
			ProxyNodeID:  "node-2",
			RegisteredAt: time.Now(),
		},
	})

	deadline := time.After(3 * time.Second)
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()

	for {
		routed := mockCl.GetRoutedMessages()
		if len(routed) > 0 {
			if routed[0].message == nil {
				t.Fatal("expected routed stream message payload")
			}
			if got := routed[0].message.StreamOffset; got != 0 {
				t.Fatalf("expected stream offset=0, got %d", got)
			}
			return
		}

		select {
		case <-deadline:
			t.Fatal("expected fallback sweep to deliver backlog to remote stream consumer")
		case <-ticker.C:
		}
	}
}

func TestSubscribeWithCursorDefaultsProxyNodeIDFromCluster(t *testing.T) {
	logStore := memlog.New()
	groupStore := newMockGroupStore()
	mockCl := newMockCluster("node-1")

	manager := NewManager(
		logStore,
		groupStore,
		func(ctx context.Context, clientID string, msg any) error { return nil },
		DefaultConfig(),
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		mockCl,
	)

	cursor := &types.CursorOption{
		Position: types.CursorEarliest,
		Mode:     types.GroupModeStream,
	}
	if err := manager.SubscribeWithCursor(context.Background(), "demo-events", "#", "amqp091-conn-1", "demo-readers", "", cursor); err != nil {
		t.Fatalf("SubscribeWithCursor failed: %v", err)
	}

	registered := mockCl.GetRegisteredQueueConsumers()
	if len(registered) != 1 {
		t.Fatalf("expected 1 registered consumer, got %d", len(registered))
	}
	if registered[0].ProxyNodeID != "node-1" {
		t.Fatalf("expected proxy node id node-1, got %q", registered[0].ProxyNodeID)
	}
}

func TestPublishForwardPolicySkipsRemoteForwarding(t *testing.T) {
	logStore := memlog.New()
	groupStore := newMockGroupStore()
	logger := slog.Default()

	mockCl := newMockCluster("node-1")
	mockCl.SetQueueConsumers([]*cluster.QueueConsumerInfo{
		{
			QueueName:   "test",
			ProxyNodeID: "node-2",
		},
	})

	config := DefaultConfig()
	config.WritePolicy = WritePolicyForward
	config.DistributionMode = DistributionForward

	manager := NewManager(logStore, groupStore, func(ctx context.Context, clientID string, msg any) error {
		return nil
	}, config, logger, mockCl)

	manager.SetRaftManager(newTestRaftManager(t, "node-2"))

	ctx := context.Background()
	err := manager.Publish(ctx, types.PublishRequest{
		Topic:   "$queue/test/msg",
		Payload: []byte("hello"),
	})
	if err != nil {
		t.Fatalf("Publish returned error: %v", err)
	}

	calls := mockCl.GetForwardCalls()
	if len(calls) != 1 {
		t.Fatalf("expected 1 forward call, got %d", len(calls))
	}

	if !calls[0].forwardToLeader {
		t.Fatalf("expected forward-to-leader call, got forwardToLeader=%v", calls[0].forwardToLeader)
	}

	if calls[0].nodeID != "node-2" {
		t.Fatalf("expected leader nodeID node-2, got %s", calls[0].nodeID)
	}
}

func TestPublishReplicateModeForwardsUnknownQueues(t *testing.T) {
	logStore := memlog.New()
	groupStore := newMockGroupStore()
	logger := slog.Default()

	mockCl := newMockCluster("node-1")
	mockCl.SetQueueConsumers([]*cluster.QueueConsumerInfo{
		{
			QueueName:   "+",
			ProxyNodeID: "node-2",
		},
	})

	config := DefaultConfig()
	config.DistributionMode = DistributionReplicate

	manager := NewManager(logStore, groupStore, func(ctx context.Context, clientID string, msg any) error {
		return nil
	}, config, logger, mockCl)

	ctx := context.Background()
	err := manager.Publish(ctx, types.PublishRequest{
		Topic:   "$queue/test/tpc/msg",
		Payload: []byte("hello"),
	})
	if err != nil {
		t.Fatalf("Publish returned error: %v", err)
	}

	calls := mockCl.GetForwardCalls()
	if len(calls) != 1 {
		t.Fatalf("expected 1 forward call, got %d", len(calls))
	}

	if calls[0].forwardToLeader {
		t.Fatalf("expected forward-to-remote call, got forwardToLeader=%v", calls[0].forwardToLeader)
	}

	if calls[0].nodeID != "node-2" {
		t.Fatalf("expected remote nodeID node-2, got %s", calls[0].nodeID)
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

	msg := &cluster.QueueMessage{
		MessageID:      "msg-123",
		QueueName:      "test",
		GroupID:        "workers",
		Payload:        []byte("routed payload"),
		Sequence:       42,
		UserProperties: map[string]string{"custom": "prop"},
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

	if deliveredMsg.Properties[types.PropMessageID] != "msg-123" {
		t.Errorf("Expected message-id 'msg-123', got '%s'", deliveredMsg.Properties[types.PropMessageID])
	}
	if deliveredMsg.Properties[types.PropQueueName] != "test" {
		t.Errorf("Expected queue 'test', got '%s'", deliveredMsg.Properties[types.PropQueueName])
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

func TestAutoQueueFromTopic(t *testing.T) {
	tests := []struct {
		name      string
		topic     string
		queueName string
		pattern   string
	}{
		{
			name:      "queue root topic",
			topic:     "$queue/demo-events",
			queueName: "demo-events",
			pattern:   "$queue/demo-events/#",
		},
		{
			name:      "queue nested topic",
			topic:     "$queue/demo-events/eu/images",
			queueName: "demo-events",
			pattern:   "$queue/demo-events/#",
		},
		{
			name:      "regular topic",
			topic:     "sensors/temp",
			queueName: "sensors/temp",
			pattern:   "sensors/temp",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			gotQueue, gotPattern := autoQueueFromTopic(tc.topic)
			if gotQueue != tc.queueName {
				t.Fatalf("expected queue name %q, got %q", tc.queueName, gotQueue)
			}
			if gotPattern != tc.pattern {
				t.Fatalf("expected pattern %q, got %q", tc.pattern, gotPattern)
			}
		})
	}
}

func TestPublishAutoCreateQueueFromQueueTopic(t *testing.T) {
	logStore := memlog.New()
	groupStore := newMockGroupStore()

	manager := NewManager(
		logStore,
		groupStore,
		func(ctx context.Context, clientID string, msg any) error { return nil },
		DefaultConfig(),
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		nil,
	)

	ctx := context.Background()
	topic := "$queue/demo-events"

	if err := manager.Publish(ctx, types.PublishRequest{
		Topic:   topic,
		Payload: []byte("hello"),
	}); err != nil {
		t.Fatalf("Publish failed: %v", err)
	}

	if _, err := logStore.GetQueue(ctx, "demo-events"); err != nil {
		t.Fatalf("expected queue demo-events to exist: %v", err)
	}

	if _, err := logStore.GetQueue(ctx, topic); err != storage.ErrQueueNotFound {
		t.Fatalf("expected queue %q to not exist, got err=%v", topic, err)
	}

	msg, err := logStore.Read(ctx, "demo-events", 0)
	if err != nil {
		t.Fatalf("failed to read message from auto-created queue: %v", err)
	}
	if msg.Topic != topic {
		t.Fatalf("expected stored topic %q, got %q", topic, msg.Topic)
	}
	if string(msg.GetPayload()) != "hello" {
		t.Fatalf("expected payload hello, got %q", string(msg.GetPayload()))
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

func TestSubscriptionTrackingReferenceCounts(t *testing.T) {
	manager := NewManager(
		memlog.New(),
		newMockGroupStore(),
		func(context.Context, string, any) error { return nil },
		DefaultConfig(),
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		nil,
	)

	manager.trackSubscription("client-1", "orders", "workers@#")
	manager.trackSubscription("client-1", "orders", "workers@#")

	targets := manager.getSubscriptionTargets("client-1")
	if len(targets) != 1 {
		t.Fatalf("expected 1 tracked target after duplicate subscriptions, got %d", len(targets))
	}

	manager.untrackSubscription("client-1", "orders", "workers@#")
	targets = manager.getSubscriptionTargets("client-1")
	if len(targets) != 1 {
		t.Fatalf("expected tracked target to remain after first untrack, got %d", len(targets))
	}

	manager.untrackSubscription("client-1", "orders", "workers@#")
	targets = manager.getSubscriptionTargets("client-1")
	if len(targets) != 0 {
		t.Fatalf("expected no tracked targets after reference count reaches zero, got %d", len(targets))
	}
}

func TestSubscriptionTrackingPrunesStaleEntries(t *testing.T) {
	cfg := DefaultConfig()
	cfg.ConsumerTimeout = 20 * time.Millisecond

	manager := NewManager(
		memlog.New(),
		newMockGroupStore(),
		func(context.Context, string, any) error { return nil },
		cfg,
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		nil,
	)

	manager.trackSubscription("client-1", "orders", "workers@#")

	key := manager.subscriptionRefKey("orders", "workers@#")
	manager.subscriptionsMu.Lock()
	manager.subscriptions["client-1"][key].lastSeen = time.Now().Add(-time.Minute)
	manager.subscriptionsMu.Unlock()

	manager.pruneStaleSubscriptions()

	targets := manager.getSubscriptionTargets("client-1")
	if len(targets) != 0 {
		t.Fatalf("expected stale tracked target to be pruned, got %d entries", len(targets))
	}
}

func TestUpdateHeartbeatRemovesStaleTrackedTargets(t *testing.T) {
	manager := NewManager(
		memlog.New(),
		newMockGroupStore(),
		func(context.Context, string, any) error { return nil },
		DefaultConfig(),
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		nil,
	)

	manager.trackSubscription("client-1", "orders", "workers@#")

	if err := manager.UpdateHeartbeat(context.Background(), "client-1"); err != nil {
		t.Fatalf("UpdateHeartbeat failed: %v", err)
	}

	targets := manager.getSubscriptionTargets("client-1")
	if len(targets) != 0 {
		t.Fatalf("expected stale tracked target to be removed after heartbeat update, got %d entries", len(targets))
	}
}
