// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package queue

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/absmach/fluxmq/cluster"
	"github.com/absmach/fluxmq/logstorage"
	"github.com/absmach/fluxmq/message"
	clusterv1 "github.com/absmach/fluxmq/pkg/proto/cluster/v1"
	"github.com/absmach/fluxmq/queue/consumer"
	queueraft "github.com/absmach/fluxmq/queue/raft"
	"github.com/absmach/fluxmq/queue/storage"
	memlog "github.com/absmach/fluxmq/queue/storage/memory/log"
	"github.com/absmach/fluxmq/queue/types"
	brokerstorage "github.com/absmach/fluxmq/storage"
	"github.com/stretchr/testify/require"
)

var (
	errTestAppend = errors.New("append failed")
	errTestSync   = errors.New("sync failed")
)

const (
	node1                = "node-1"
	testAuditQueueName   = "audit.events"
	testAuditQueueTopic  = "$queue/audit.events"
	testAuditQueueFilter = "$queue/audit.events/#"
	testCapturedTopic    = "m/domain/c/channel/tst"
	testCaptureQueue     = "messages"
	testCapturePublisher = "mqtt-publisher"
	testReplicatedQueue  = "replicated"
)

type targetCheckingDeliverer struct {
	targets map[string]bool
}

type recordingDurableQueueStore struct {
	storage.QueueStore
	mu         sync.Mutex
	appendErr  error
	syncErr    error
	operations []string
}

// queueStoreWithoutDurableSync deliberately narrows the wrapped store to the
// QueueStore contract so its atomic durable-append method is not exposed to the
// manager.
type queueStoreWithoutDurableSync struct {
	storage.QueueStore
}

type getErrorQueueStore struct {
	storage.QueueStore
	getErr error
}

func (s *getErrorQueueStore) GetQueue(ctx context.Context, queueName string) (*types.QueueConfig, error) {
	if s.getErr != nil {
		return nil, s.getErr
	}
	return s.QueueStore.GetQueue(ctx, queueName)
}

func (s *recordingDurableQueueStore) Append(ctx context.Context, queueName string, msg *message.Envelope) (uint64, error) {
	s.mu.Lock()
	s.operations = append(s.operations, "append:"+queueName)
	err := s.appendErr
	s.mu.Unlock()
	if err != nil {
		return 0, err
	}
	return s.QueueStore.Append(ctx, queueName, msg)
}

func (s *recordingDurableQueueStore) AppendAndSync(ctx context.Context, queueName string, msg *message.Envelope) (uint64, error) {
	s.mu.Lock()
	s.operations = append(s.operations, "append:"+queueName)
	appendErr := s.appendErr
	syncErr := s.syncErr
	s.mu.Unlock()
	if appendErr != nil {
		return 0, appendErr
	}

	if syncErr != nil {
		offset, err := s.QueueStore.Append(ctx, queueName, msg)
		if err != nil {
			return offset, err
		}
		s.mu.Lock()
		s.operations = append(s.operations, "sync:"+queueName)
		s.mu.Unlock()
		return offset, syncErr
	}

	durableStore, ok := s.QueueStore.(storage.DurableQueueStore)
	if !ok {
		return 0, ErrDurableSyncUnsupported
	}
	offset, err := durableStore.AppendAndSync(ctx, queueName, msg)
	s.mu.Lock()
	s.operations = append(s.operations, "sync:"+queueName)
	s.mu.Unlock()
	return offset, err
}

// SupportsDurableSync reports true so the manager treats this test double as a
// crash-durable store; the wrapped in-memory store never is.
func (s *recordingDurableQueueStore) SupportsDurableSync() bool { return true }

func (s *recordingDurableQueueStore) Operations() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.operations...)
}

func (d *targetCheckingDeliverer) Deliver(context.Context, string, *message.Envelope) error {
	return nil
}

func (d *targetCheckingDeliverer) HasDeliveryTarget(clientID string) bool {
	return d.targets[clientID]
}

func protectedAuditQueueConfig() types.QueueConfig {
	config := types.DefaultQueueConfig(testAuditQueueName, testAuditQueueFilter)
	config.Type = types.QueueTypeStream
	config.Reserved = true
	config.Retention.RetentionTime = 30 * 24 * time.Hour
	config.Retention.RetentionBytes = 10 * 1024 * 1024 * 1024
	config.MessageTTL = 30 * 24 * time.Hour
	return config
}

func managerConfigWithProtectedQueue(contract types.QueueConfig) Config {
	config := DefaultConfig()
	config.ProtectedQueueContracts = []types.QueueConfig{contract}
	return config
}

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

func (s *mockGroupStore) RequeuePendingEntry(ctx context.Context, queueName, groupID, consumerID string, offset uint64, attemptedAt time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.groups[queueName] == nil || s.groups[queueName][groupID] == nil {
		return storage.ErrConsumerNotFound
	}
	entry, owner := s.groups[queueName][groupID].FindPending(offset)
	if entry == nil {
		return storage.ErrPendingEntryNotFound
	}
	if owner != consumerID {
		return storage.ErrConsumerNotFound
	}
	entry.ClaimedAt = attemptedAt
	entry.DeliveryCount++
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

	deliveredMsgs := make(chan *message.Envelope, 10)

	deliveryTarget := DeliveryTargetFunc(func(ctx context.Context, clientID string, msg *message.Envelope) error {
		t.Logf("Delivered message to %s: topic=%s", clientID, msg.Topic)
		deliveredMsgs <- msg
		return nil
	})

	config := DefaultConfig()
	config.DeliveryInterval = 50 * time.Millisecond
	logger := slog.Default()

	manager := NewManager(logStore, groupStore, deliveryTarget, config, logger, nil)

	ctx := context.Background()

	if err := manager.Start(ctx); err != nil {
		t.Fatalf("Failed to start manager: %v", err)
	}
	defer manager.Stop() //nolint:errcheck // test cleanup

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
	if err := manager.Publish(ctx, types.PublishRequest{Topic: publishTopic, Payload: payload}); err != nil {
		t.Fatalf("Publish failed: %v", err)
	}

	tail, _ := logStore.Tail(ctx, queueName)
	t.Logf("Tail after publish: %d", tail)

	t.Log("Waiting for message delivery...")
	select {
	case msg := <-deliveredMsgs:
		t.Logf("Received message: topic=%s payload=%s", msg.Topic, string(msg.PayloadBytes()))
		if msg.Topic != publishTopic {
			t.Errorf("Expected topic %s, got %s", publishTopic, msg.Topic)
		}
		if string(msg.PayloadBytes()) != string(payload) {
			t.Errorf("Expected payload %s, got %s", payload, msg.PayloadBytes())
		}
	case <-time.After(2 * time.Second):
		groups, _ = groupStore.ListConsumerGroups(ctx, queueName)
		for _, g := range groups {
			t.Logf("Group state: %s cursor=%v", g.ID, g.Cursor)
		}
		t.Fatal("Timeout waiting for message delivery")
	}
}

func TestPublishToDurableStreamRejectsMissingAndClassicQueue(t *testing.T) {
	ctx := context.Background()
	expected := protectedAuditQueueConfig()
	newManager := func() (*Manager, *recordingDurableQueueStore) {
		store := &recordingDurableQueueStore{QueueStore: memlog.New()}
		return NewManager(store, newMockGroupStore(), nil, managerConfigWithProtectedQueue(expected), slog.New(slog.NewTextHandler(io.Discard, nil)), nil), store
	}

	t.Run("missing", func(t *testing.T) {
		manager, store := newManager()
		err := manager.PublishToDurableStream(ctx, testAuditQueueName, types.PublishRequest{
			Topic:   testAuditQueueTopic,
			Payload: []byte("{}"),
		})
		if !errors.Is(err, storage.ErrQueueNotFound) {
			t.Fatalf("error = %v, want queue not found", err)
		}
		if operations := store.Operations(); len(operations) != 0 {
			t.Fatalf("operations = %v, want none", operations)
		}
	})

	t.Run("classic", func(t *testing.T) {
		manager, store := newManager()
		queueConfig := types.DefaultQueueConfig(testAuditQueueName, testAuditQueueFilter)
		queueConfig.Reserved = true
		if err := store.QueueStore.CreateQueue(ctx, queueConfig); err != nil {
			t.Fatalf("create classic queue: %v", err)
		}
		err := manager.PublishToDurableStream(ctx, testAuditQueueName, types.PublishRequest{
			Topic:   testAuditQueueTopic,
			Payload: []byte("{}"),
		})
		if !errors.Is(err, ErrProtectedQueueContractDrift) {
			t.Fatalf("error = %v, want ErrProtectedQueueContractDrift", err)
		}
		if operations := store.Operations(); len(operations) != 0 {
			t.Fatalf("operations = %v, want none", operations)
		}
	})
}

// volatileDurableQueueStore implements the durable-append method without real
// crash durability, the shape an in-memory store has.
type volatileDurableQueueStore struct {
	storage.QueueStore
	appendAndSyncCalls int
}

func (s *volatileDurableQueueStore) AppendAndSync(ctx context.Context, queueName string, msg *message.Envelope) (uint64, error) {
	s.appendAndSyncCalls++
	return s.QueueStore.Append(ctx, queueName, msg)
}

func (s *volatileDurableQueueStore) SupportsDurableSync() bool { return false }

func TestProtectedQueuesRejectStoreWithoutRealDurableSync(t *testing.T) {
	ctx := context.Background()
	expected := protectedAuditQueueConfig()
	store := &volatileDurableQueueStore{QueueStore: memlog.New()}
	if err := store.CreateQueue(ctx, expected); err != nil {
		t.Fatalf("create protected queue: %v", err)
	}
	manager := NewManager(store, newMockGroupStore(), nil, managerConfigWithProtectedQueue(expected), slog.New(slog.NewTextHandler(io.Discard, nil)), nil)

	err := manager.PublishToDurableStream(ctx, testAuditQueueName, types.PublishRequest{
		Topic:   testAuditQueueTopic,
		Payload: []byte("{}"),
	})
	if !errors.Is(err, ErrDurableSyncUnsupported) {
		t.Fatalf("error = %v, want ErrDurableSyncUnsupported", err)
	}
	if store.appendAndSyncCalls != 0 {
		t.Fatalf("appendAndSync calls = %d, want 0; a publisher must never be acknowledged on a volatile store", store.appendAndSyncCalls)
	}
	if err := manager.ValidateProtectedQueueContracts(ctx); !errors.Is(err, ErrDurableSyncUnsupported) {
		t.Fatalf("ValidateProtectedQueueContracts() error = %v, want ErrDurableSyncUnsupported", err)
	}
}

// blockingDurableQueueStore holds AppendAndSync open until the test releases
// it, standing in for a slow fsync.
type blockingDurableQueueStore struct {
	storage.QueueStore
	started chan struct{}
	release chan struct{}
}

func (s *blockingDurableQueueStore) AppendAndSync(ctx context.Context, queueName string, msg *message.Envelope) (uint64, error) {
	close(s.started)
	<-s.release
	return s.QueueStore.Append(ctx, queueName, msg)
}

func (s *blockingDurableQueueStore) SupportsDurableSync() bool { return true }

func TestPublishToDurableStreamDoesNotBlockContractReload(t *testing.T) {
	ctx := context.Background()
	expected := protectedAuditQueueConfig()
	store := &blockingDurableQueueStore{
		QueueStore: memlog.New(),
		started:    make(chan struct{}),
		release:    make(chan struct{}),
	}
	if err := store.CreateQueue(ctx, expected); err != nil {
		t.Fatalf("create protected queue: %v", err)
	}
	manager := NewManager(store, newMockGroupStore(), nil, managerConfigWithProtectedQueue(expected), slog.New(slog.NewTextHandler(io.Discard, nil)), nil)

	publishErr := make(chan error, 1)
	go func() {
		publishErr <- manager.PublishToDurableStream(ctx, testAuditQueueName, types.PublishRequest{
			Topic:   testAuditQueueTopic,
			Payload: []byte("{}"),
		})
	}()

	select {
	case <-store.started:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for the durable append to start")
	}

	reloaded := make(chan error, 1)
	go func() {
		reloaded <- manager.ReplaceProtectedQueueContracts(ctx, []types.QueueConfig{expected})
	}()

	select {
	case err := <-reloaded:
		if err != nil {
			t.Fatalf("ReplaceProtectedQueueContracts() error = %v", err)
		}
	case <-time.After(5 * time.Second):
		close(store.release)
		t.Fatal("contract reload blocked behind an in-flight durable append")
	}

	close(store.release)
	if err := <-publishErr; err != nil {
		t.Fatalf("PublishToDurableStream() error = %v", err)
	}
}

func TestPublishToDurableStreamRejectsUnsafeConfigurationBeforeAppend(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name          string
		configure     func(expected, persisted *types.QueueConfig)
		withoutSyncer bool
		wantErr       error
	}{
		{
			name: "non-reserved stream",
			configure: func(_, persisted *types.QueueConfig) {
				persisted.Reserved = false
			},
			wantErr: ErrProtectedQueueContractDrift,
		},
		{
			name: "non-durable stream",
			configure: func(_, persisted *types.QueueConfig) {
				persisted.Durable = false
			},
			wantErr: ErrProtectedQueueContractDrift,
		},
		{
			name: "replicated stream",
			configure: func(_, persisted *types.QueueConfig) {
				persisted.Replication.Enabled = true
			},
			wantErr: ErrProtectedQueueContractDrift,
		},
		{
			name: "message exceeds queue maximum",
			configure: func(expected, persisted *types.QueueConfig) {
				expected.MaxMessageSize = 1
				persisted.MaxMessageSize = 1
			},
			wantErr: ErrQueueMessageTooLarge,
		},
		{
			name:          "store without durable sync",
			withoutSyncer: true,
			wantErr:       ErrDurableSyncUnsupported,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			base := memlog.New()
			expected := protectedAuditQueueConfig()
			persisted := expected
			if tc.configure != nil {
				tc.configure(&expected, &persisted)
			}
			if err := base.CreateQueue(ctx, expected); err != nil {
				t.Fatalf("create stream: %v", err)
			}
			if err := base.UpdateQueue(ctx, persisted); err != nil {
				t.Fatalf("inject stored queue configuration: %v", err)
			}

			store := &recordingDurableQueueStore{QueueStore: base}
			var managerStore storage.QueueStore = store
			if tc.withoutSyncer {
				managerStore = &queueStoreWithoutDurableSync{QueueStore: base}
			}
			manager := NewManager(managerStore, newMockGroupStore(), nil, managerConfigWithProtectedQueue(expected), slog.New(slog.NewTextHandler(io.Discard, nil)), nil)

			err := manager.PublishToDurableStream(ctx, testAuditQueueName, types.PublishRequest{
				Topic:   testAuditQueueTopic,
				Payload: []byte("{}"),
			})
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("error = %v, want %v", err, tc.wantErr)
			}
			if operations := store.Operations(); len(operations) != 0 {
				t.Fatalf("operations = %v, want none", operations)
			}
			tail, tailErr := base.Tail(ctx, testAuditQueueName)
			if tailErr != nil || tail != 0 {
				t.Fatalf("tail = %d, error = %v, want empty queue", tail, tailErr)
			}
		})
	}
}

func TestProtectedQueueRejectsContractMutationsAndDelete(t *testing.T) {
	ctx := context.Background()
	expected := protectedAuditQueueConfig()
	tests := []struct {
		name   string
		mutate func(*types.QueueConfig)
	}{
		{name: "topics", mutate: func(config *types.QueueConfig) { config.Topics = []string{"#"} }},
		{name: "type", mutate: func(config *types.QueueConfig) { config.Type = types.QueueTypeClassic }},
		{name: "durable", mutate: func(config *types.QueueConfig) { config.Durable = false }},
		{name: "reserved", mutate: func(config *types.QueueConfig) { config.Reserved = false }},
		{name: "replication", mutate: func(config *types.QueueConfig) { config.Replication.Enabled = true }},
		{name: "retention age", mutate: func(config *types.QueueConfig) { config.Retention.RetentionTime-- }},
		{name: "retention bytes", mutate: func(config *types.QueueConfig) { config.Retention.RetentionBytes-- }},
		{name: "retention messages", mutate: func(config *types.QueueConfig) { config.Retention.RetentionMessages++ }},
		{name: "maximum message size", mutate: func(config *types.QueueConfig) { config.MaxMessageSize-- }},
		{name: "message TTL", mutate: func(config *types.QueueConfig) { config.MessageTTL-- }},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			store := memlog.New()
			manager := NewManager(store, newMockGroupStore(), nil, managerConfigWithProtectedQueue(expected), nil, nil)
			if err := manager.CreateQueue(ctx, expected); err != nil {
				t.Fatalf("create protected queue: %v", err)
			}
			updated := expected
			tc.mutate(&updated)
			if err := manager.UpdateQueue(ctx, updated); !errors.Is(err, ErrProtectedQueueMutation) {
				t.Fatalf("UpdateQueue() error = %v, want ErrProtectedQueueMutation", err)
			}
			persisted, err := store.GetQueue(ctx, expected.Name)
			if err != nil {
				t.Fatalf("GetQueue() error = %v", err)
			}
			if err := ValidateProtectedQueueContract(expected, *persisted); err != nil {
				t.Fatalf("rejected update changed persisted contract: %v", err)
			}
		})
	}

	t.Run("delete", func(t *testing.T) {
		store := memlog.New()
		manager := NewManager(store, newMockGroupStore(), nil, managerConfigWithProtectedQueue(expected), nil, nil)
		if err := manager.CreateQueue(ctx, expected); err != nil {
			t.Fatalf("create protected queue: %v", err)
		}
		if err := manager.DeleteQueue(ctx, expected.Name); !errors.Is(err, ErrProtectedQueueMutation) {
			t.Fatalf("DeleteQueue() error = %v, want ErrProtectedQueueMutation", err)
		}
		if _, err := store.GetQueue(ctx, expected.Name); err != nil {
			t.Fatalf("protected queue was deleted: %v", err)
		}
	})

	t.Run("MaxDepth is not protected", func(t *testing.T) {
		store := memlog.New()
		manager := NewManager(store, newMockGroupStore(), nil, managerConfigWithProtectedQueue(expected), nil, nil)
		if err := manager.CreateQueue(ctx, expected); err != nil {
			t.Fatalf("create protected queue: %v", err)
		}
		updated := expected
		updated.MaxDepth++
		if err := manager.UpdateQueue(ctx, updated); err != nil {
			t.Fatalf("MaxDepth-only update rejected: %v", err)
		}
	})

	t.Run("other reserved queue remains mutable", func(t *testing.T) {
		store := memlog.New()
		manager := NewManager(store, newMockGroupStore(), nil, managerConfigWithProtectedQueue(expected), nil, nil)
		other := types.DefaultQueueConfig("other-reserved", "other/#")
		other.Reserved = true
		if err := manager.CreateQueue(ctx, other); err != nil {
			t.Fatalf("create other reserved queue: %v", err)
		}
		other.MessageTTL++
		other.Reserved = false
		if err := manager.UpdateQueue(ctx, other); err != nil {
			t.Fatalf("update other reserved queue: %v", err)
		}
		if err := manager.DeleteQueue(ctx, other.Name); err != nil {
			t.Fatalf("delete other reserved queue: %v", err)
		}
	})
}

func TestNarrowProtectedQueueContractsDoesNotReadStorage(t *testing.T) {
	ctx := context.Background()
	base := memlog.New()
	audit := protectedAuditQueueConfig()
	security := protectedAuditQueueConfig()
	security.Name = "audit-security"
	security.Topics = []string{"$queue/audit-security/#"}
	for _, contract := range []types.QueueConfig{audit, security} {
		if err := base.CreateQueue(ctx, contract); err != nil {
			t.Fatalf("create queue %q: %v", contract.Name, err)
		}
	}
	store := &getErrorQueueStore{QueueStore: base}
	config := DefaultConfig()
	config.ProtectedQueueContracts = []types.QueueConfig{audit, security}
	manager := NewManager(store, newMockGroupStore(), nil, config, nil, nil)

	store.getErr = errors.New("queue store unavailable")
	if err := manager.NarrowProtectedQueueContracts([]types.QueueConfig{security}); err != nil {
		t.Fatalf("NarrowProtectedQueueContracts() performed storage I/O: %v", err)
	}
	contracts := manager.ProtectedQueueContracts()
	if len(contracts) != 1 || contracts[0].Name != security.Name {
		t.Fatalf("protected contracts = %+v, want only %q", contracts, security.Name)
	}
}

func TestPublishToDurableStreamRejectsPersistedContractDrift(t *testing.T) {
	ctx := context.Background()
	expected := protectedAuditQueueConfig()
	tests := []struct {
		name   string
		mutate func(*types.QueueConfig)
	}{
		{name: "topics", mutate: func(config *types.QueueConfig) { config.Topics = []string{"#"} }},
		{name: "retention age", mutate: func(config *types.QueueConfig) { config.Retention.RetentionTime-- }},
		{name: "retention bytes", mutate: func(config *types.QueueConfig) { config.Retention.RetentionBytes-- }},
		{name: "retention messages", mutate: func(config *types.QueueConfig) { config.Retention.RetentionMessages++ }},
		{name: "maximum message size", mutate: func(config *types.QueueConfig) { config.MaxMessageSize++ }},
		{name: "message TTL", mutate: func(config *types.QueueConfig) { config.MessageTTL-- }},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			base := memlog.New()
			if err := base.CreateQueue(ctx, expected); err != nil {
				t.Fatalf("create queue: %v", err)
			}
			drifted := expected
			tc.mutate(&drifted)
			if err := base.UpdateQueue(ctx, drifted); err != nil {
				t.Fatalf("inject persisted drift: %v", err)
			}
			store := &recordingDurableQueueStore{QueueStore: base}
			manager := NewManager(store, newMockGroupStore(), nil, managerConfigWithProtectedQueue(expected), nil, nil)
			err := manager.PublishToDurableStream(ctx, expected.Name, types.PublishRequest{Payload: []byte("{}")})
			if !errors.Is(err, ErrProtectedQueueContractDrift) {
				t.Fatalf("PublishToDurableStream() error = %v, want ErrProtectedQueueContractDrift", err)
			}
			if operations := store.Operations(); len(operations) != 0 {
				t.Fatalf("operations = %v, want no append", operations)
			}
		})
	}

	t.Run("MaxDepth drift is ignored", func(t *testing.T) {
		base := memlog.New()
		drifted := expected
		drifted.MaxDepth++
		if err := base.CreateQueue(ctx, drifted); err != nil {
			t.Fatalf("create queue: %v", err)
		}
		store := &recordingDurableQueueStore{QueueStore: base}
		manager := NewManager(store, newMockGroupStore(), nil, managerConfigWithProtectedQueue(expected), nil, nil)
		if err := manager.PublishToDurableStream(ctx, expected.Name, types.PublishRequest{Payload: []byte("{}")}); err != nil {
			t.Fatalf("PublishToDurableStream() rejected MaxDepth drift: %v", err)
		}
	})
}

func TestPublishToDurableStreamTargetsOneQueueAndSyncsBeforeSuccess(t *testing.T) {
	ctx := context.Background()
	base := memlog.New()
	store := &recordingDurableQueueStore{QueueStore: base}
	audit := protectedAuditQueueConfig()
	manager := NewManager(store, newMockGroupStore(), nil, managerConfigWithProtectedQueue(audit), slog.New(slog.NewTextHandler(io.Discard, nil)), nil)
	overlap := types.DefaultQueueConfig("overlap", "$queue/#")
	overlap.Type = types.QueueTypeStream
	for _, queueConfig := range []types.QueueConfig{audit, overlap} {
		if err := manager.CreateQueue(ctx, queueConfig); err != nil {
			t.Fatalf("create queue %q: %v", queueConfig.Name, err)
		}
	}

	if err := manager.PublishToDurableStream(ctx, testAuditQueueName, types.PublishRequest{
		Topic:   testAuditQueueTopic,
		Payload: []byte("audit-event"),
	}); err != nil {
		t.Fatalf("durable stream publish: %v", err)
	}
	if operations := store.Operations(); fmt.Sprint(operations) != "[append:audit.events sync:audit.events]" {
		t.Fatalf("operations = %v, want append then sync for audit.events", operations)
	}
	auditTail, err := base.Tail(ctx, testAuditQueueName)
	if err != nil || auditTail != 1 {
		t.Fatalf("audit.events tail = %d, error = %v, want 1", auditTail, err)
	}
	overlapTail, err := base.Tail(ctx, "overlap")
	if err != nil || overlapTail != 0 {
		t.Fatalf("overlap tail = %d, error = %v, want 0", overlapTail, err)
	}
}

// TestPublishToDurableStreamAcknowledgesOnlyPersistedRecords is the
// end-to-end form of the ACK contract: when the publish call returns success,
// the record must be recoverable from storage alone, with none of the
// publishing process's in-memory state. It uses the real file-backed store
// rather than a double, because the point under test is what reached disk.
func TestPublishToDurableStreamAcknowledgesOnlyPersistedRecords(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	queueConfig := protectedAuditQueueConfig()

	store, err := logstorage.NewAdapter(dir, logstorage.DefaultAdapterConfig())
	if err != nil {
		t.Fatalf("open queue store: %v", err)
	}
	manager := NewManager(store, newMockGroupStore(), nil, managerConfigWithProtectedQueue(queueConfig), slog.New(slog.NewTextHandler(io.Discard, nil)), nil)
	if err := manager.CreateQueue(ctx, queueConfig); err != nil {
		t.Fatalf("create protected stream: %v", err)
	}

	payload := []byte(`{"id":"durability-1"}`)
	if err := manager.PublishToDurableStream(ctx, testAuditQueueName, types.PublishRequest{
		Topic:   testAuditQueueTopic,
		Payload: payload,
	}); err != nil {
		t.Fatalf("PublishToDurableStream() error = %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close queue store: %v", err)
	}

	reopened, err := logstorage.NewAdapter(dir, logstorage.DefaultAdapterConfig())
	if err != nil {
		t.Fatalf("reopen queue store: %v", err)
	}
	t.Cleanup(func() { reopened.Close() })

	count, err := reopened.Count(ctx, testAuditQueueName)
	if err != nil {
		t.Fatalf("Count() error = %v", err)
	}
	if count != 1 {
		t.Fatalf("persisted records = %d, want 1", count)
	}
	msg, err := reopened.Read(ctx, testAuditQueueName, 0)
	if err != nil {
		t.Fatalf("Read() error = %v", err)
	}
	if string(msg.PayloadBytes()) != string(payload) {
		t.Fatalf("persisted payload = %q, want %q", msg.PayloadBytes(), payload)
	}
}

func TestPublishToDurableStreamPropagatesAppendAndSyncFailures(t *testing.T) {
	ctx := context.Background()
	tests := []struct {
		name           string
		appendErr      error
		syncErr        error
		wantErr        error
		wantOperations string
	}{
		{name: "append", appendErr: errTestAppend, wantErr: errTestAppend, wantOperations: "[append:audit.events]"},
		{name: "sync", syncErr: errTestSync, wantErr: errTestSync, wantOperations: "[append:audit.events sync:audit.events]"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			base := memlog.New()
			store := &recordingDurableQueueStore{QueueStore: base, appendErr: tc.appendErr, syncErr: tc.syncErr}
			queueConfig := protectedAuditQueueConfig()
			manager := NewManager(store, newMockGroupStore(), nil, managerConfigWithProtectedQueue(queueConfig), slog.New(slog.NewTextHandler(io.Discard, nil)), nil)
			if err := manager.CreateQueue(ctx, queueConfig); err != nil {
				t.Fatalf("create stream: %v", err)
			}

			err := manager.PublishToDurableStream(ctx, testAuditQueueName, types.PublishRequest{
				Topic:   testAuditQueueTopic,
				Payload: []byte("{}"),
			})
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("error = %v, want %v", err, tc.wantErr)
			}
			if operations := fmt.Sprint(store.Operations()); operations != tc.wantOperations {
				t.Fatalf("operations = %s, want %s", operations, tc.wantOperations)
			}
		})
	}
}

func TestPublishPropagatesQueueAppendFailure(t *testing.T) {
	ctx := context.Background()
	base := memlog.New()
	store := &recordingDurableQueueStore{QueueStore: base, appendErr: errTestAppend}
	manager := NewManager(store, newMockGroupStore(), nil, DefaultConfig(), slog.New(slog.NewTextHandler(io.Discard, nil)), nil)
	queueConfig := types.DefaultQueueConfig("events", "events/#")
	if err := manager.CreateQueue(ctx, queueConfig); err != nil {
		t.Fatalf("create queue: %v", err)
	}

	err := manager.Publish(ctx, types.PublishRequest{Topic: "events/created", Payload: []byte("event")})
	if !errors.Is(err, errTestAppend) {
		t.Fatalf("error = %v, want append failure", err)
	}
}

func TestStreamGroupDeliversWithoutPEL(t *testing.T) {
	logStore := memlog.New()
	groupStore := newMockGroupStore()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	delivered := make(chan *message.Envelope, 1)

	deliveryTarget := DeliveryTargetFunc(func(ctx context.Context, clientID string, msg *message.Envelope) error {
		delivered <- msg
		return nil
	})

	cfg := DefaultConfig()
	cfg.DeliveryBatchSize = 10
	mgr := NewManager(logStore, groupStore, deliveryTarget, cfg, logger, nil)

	queueCfg := types.DefaultQueueConfig(testQueueEvents, "$queue/events/#")
	queueCfg.Type = types.QueueTypeStream
	if err := mgr.CreateQueue(context.Background(), queueCfg); err != nil {
		t.Fatalf("CreateQueue failed: %v", err)
	}

	cursor := &types.CursorOption{Position: types.CursorEarliest, Mode: types.GroupModeStream}
	if err := mgr.SubscribeWithCursor(context.Background(), testQueueEvents, "", testClientOneID, "streamer", "", cursor); err != nil {
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
		if msg.Broker.Queue.Stream == nil || msg.Broker.Queue.Stream.Offset != 0 {
			t.Fatalf("expected stream offset 0, got %#v", msg.Broker.Queue.Stream)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for delivery")
	}

	group, err := groupStore.GetConsumerGroup(context.Background(), testQueueEvents, "streamer")
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

func TestPublishCarriesTypedSourceMetadata(t *testing.T) {
	logStore := memlog.New()
	groupStore := newMockGroupStore()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	delivered := make(chan *message.Envelope, 1)

	deliveryTarget := DeliveryTargetFunc(func(ctx context.Context, clientID string, msg *message.Envelope) error {
		delivered <- msg
		return nil
	})

	mgr := NewManager(logStore, groupStore, deliveryTarget, DefaultConfig(), logger, nil)
	ctx := context.Background()

	if err := mgr.CreateQueue(ctx, types.DefaultQueueConfig("orders", "$queue/orders/#")); err != nil {
		t.Fatalf("CreateQueue failed: %v", err)
	}
	if err := mgr.Subscribe(ctx, "orders", "", testClientOneID, testGroupWorkers, ""); err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}

	if err := mgr.Publish(ctx, types.PublishRequest{
		Source:  message.SourceMetadata{ClientID: "mqtt-pub-1", Protocol: message.ProtocolMQTT},
		Topic:   "$queue/orders/process",
		Payload: []byte("hello"),
	}); err != nil {
		t.Fatalf("Publish failed: %v", err)
	}

	mgr.deliverMessages()

	select {
	case msg := <-delivered:
		if got := msg.Broker.Source.ClientID; got != "mqtt-pub-1" {
			t.Fatalf("expected client id %q, got %q", "mqtt-pub-1", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for delivery")
	}
}

func TestPublishToMatchingQueuesCapturesOnlyExistingQueues(t *testing.T) {
	logStore := memlog.New()
	mgr := NewManager(logStore, newMockGroupStore(), nil, DefaultConfig(), nil, nil)
	ctx := context.Background()

	if err := mgr.CreateQueue(ctx, types.DefaultQueueConfig(testCaptureQueue, "m/#")); err != nil {
		t.Fatalf("CreateQueue messages failed: %v", err)
	}
	if err := mgr.CreateQueue(ctx, types.DefaultQueueConfig("other", "other/#")); err != nil {
		t.Fatalf("CreateQueue other failed: %v", err)
	}

	payload := []byte("original")
	properties := map[string]string{"source": "device"}
	flushCapture(t, mgr, func() {
		if err := mgr.PublishToMatchingQueues(ctx, types.PublishRequest{
			Source:     message.SourceMetadata{ClientID: testCapturePublisher, Protocol: message.ProtocolMQTT},
			Topic:      testCapturedTopic,
			Payload:    payload,
			Properties: properties,
		}); err != nil {
			t.Fatalf("PublishToMatchingQueues failed: %v", err)
		}
	})

	payload[0] = 'X'
	properties["source"] = "mutated"

	messageCount, err := logStore.Count(ctx, testCaptureQueue)
	if err != nil {
		t.Fatalf("Count messages failed: %v", err)
	}
	if messageCount != 1 {
		t.Fatalf("expected one captured message, got %d", messageCount)
	}
	otherCount, err := logStore.Count(ctx, "other")
	if err != nil {
		t.Fatalf("Count other failed: %v", err)
	}
	if otherCount != 0 {
		t.Fatalf("expected no message in unrelated queue, got %d", otherCount)
	}

	stored, err := logStore.Read(ctx, testCaptureQueue, 0)
	if err != nil {
		t.Fatalf("Read captured message failed: %v", err)
	}
	if got := string(stored.PayloadBytes()); got != "original" {
		t.Fatalf("captured payload = %q, want original", got)
	}
	if got := stored.User.Properties["source"]; got != "device" {
		t.Fatalf("captured source = %q, want device", got)
	}
	if got := stored.Broker.Source.ClientID; got != testCapturePublisher {
		t.Fatalf("captured client ID = %q, want mqtt-publisher", got)
	}

	if err := mgr.PublishToMatchingQueues(ctx, types.PublishRequest{
		Topic:   "unmatched/topic",
		Payload: []byte("ignored"),
	}); err != nil {
		t.Fatalf("unmatched PublishToMatchingQueues failed: %v", err)
	}
	if _, err := logStore.GetQueue(ctx, "unmatched/topic"); err != storage.ErrQueueNotFound {
		t.Fatalf("unmatched publish created a queue: %v", err)
	}
}

func TestPublishToMatchingQueuesRoutesCapturedStreamToRemoteConsumerOnce(t *testing.T) {
	logStore := memlog.New()
	groupStore := newMockGroupStore()
	mockCl := newMockCluster("node-1")
	mockCl.SetQueueConsumers([]*cluster.QueueConsumerInfo{
		{
			QueueName:   testCaptureQueue,
			GroupID:     "rules-engine@#",
			ConsumerID:  "amqp091:remote@1",
			ClientID:    "amqp091:remote",
			Pattern:     "#",
			Mode:        string(types.GroupModeStream),
			ProxyNodeID: testNode2,
		},
	})

	mgr := NewManager(logStore, groupStore, nil, DefaultConfig(), nil, mockCl)
	ctx := context.Background()
	config := types.DefaultQueueConfig(testCaptureQueue, "m/#")
	config.Type = types.QueueTypeStream
	if err := mgr.CreateQueue(ctx, config); err != nil {
		t.Fatalf("CreateQueue messages failed: %v", err)
	}

	flushCapture(t, mgr, func() {
		if err := mgr.PublishToMatchingQueues(ctx, types.PublishRequest{
			Topic:   testCapturedTopic,
			Payload: []byte("payload"),
		}); err != nil {
			t.Fatalf("PublishToMatchingQueues failed: %v", err)
		}
	})
	mgr.deliverMessages()

	routed := mockCl.GetRoutedMessages()
	if len(routed) != 1 {
		t.Fatalf("expected one remote delivery, got %d", len(routed))
	}
	if routed[0].nodeID != testNode2 {
		t.Fatalf("remote delivery node = %q, want %q", routed[0].nodeID, testNode2)
	}
	if routed[0].queueName != testCaptureQueue {
		t.Fatalf("remote delivery queue = %q, want messages", routed[0].queueName)
	}
	if got := routed[0].message.Topic; got != "$queue/messages/"+testCapturedTopic {
		t.Fatalf("remote delivery topic = %q, want canonical queue address", got)
	}
	if calls := mockCl.GetForwardCalls(); len(calls) != 0 {
		t.Fatalf("ordinary topic capture was also forwarded for remote append: %d calls", len(calls))
	}
}

func TestPublishToExistingQueueDoesNotForwardSecondAppend(t *testing.T) {
	logStore := memlog.New()
	groupStore := newMockGroupStore()
	mockCl := newMockCluster("node-1")
	mockCl.SetQueueConsumers([]*cluster.QueueConsumerInfo{
		{
			QueueName:   "writers",
			GroupID:     "timescale-writer@#",
			ConsumerID:  "amqp091:remote@1",
			ClientID:    "amqp091:remote",
			Pattern:     "#",
			Mode:        string(types.GroupModeStream),
			ProxyNodeID: testNode2,
		},
	})

	mgr := NewManager(logStore, groupStore, nil, DefaultConfig(), nil, mockCl)
	ctx := context.Background()
	config := types.DefaultQueueConfig("writers", "$queue/writers/#")
	config.Type = types.QueueTypeStream
	if err := mgr.CreateQueue(ctx, config); err != nil {
		t.Fatalf("CreateQueue writers failed: %v", err)
	}

	if err := mgr.Publish(ctx, types.PublishRequest{
		Topic:   "$queue/writers/domain/c/channel/tst",
		Payload: []byte("payload"),
	}); err != nil {
		t.Fatalf("Publish failed: %v", err)
	}
	mgr.deliverMessages()

	if calls := mockCl.GetForwardCalls(); len(calls) != 0 {
		t.Fatalf("known queue publish was forwarded for a second append: %d calls", len(calls))
	}
	if routed := mockCl.GetRoutedMessages(); len(routed) != 1 {
		t.Fatalf("expected one routed queue delivery, got %d", len(routed))
	}
}

func TestStartRejectsReplicateDistributionWithoutRaft(t *testing.T) {
	config := DefaultConfig()
	config.DistributionMode = DistributionReplicate
	mgr := NewManager(
		memlog.New(),
		newMockGroupStore(),
		nil,
		config,
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		newMockCluster("node-1"),
	)

	err := mgr.Start(context.Background())
	if !errors.Is(err, ErrReplicationUnavailable) {
		t.Fatalf("Start error = %v, want ErrReplicationUnavailable", err)
	}
	if mgr.distributionMode != DistributionReplicate || mgr.delivery.distributionMode != DistributionReplicate {
		t.Fatal("failed startup mutated distribution mode")
	}
}

func TestSubscribeExistingDoesNotCreateMissingQueue(t *testing.T) {
	logStore := memlog.New()
	mgr := NewManager(logStore, newMockGroupStore(), nil, DefaultConfig(), nil, nil)

	err := mgr.SubscribeExisting(context.Background(), "missing", "", testClientOneID, testGroupWorkers, "")
	if !errors.Is(err, storage.ErrQueueNotFound) {
		t.Fatalf("SubscribeExisting() error = %v, want %v", err, storage.ErrQueueNotFound)
	}
	if _, err := logStore.GetQueue(context.Background(), "missing"); !errors.Is(err, storage.ErrQueueNotFound) {
		t.Fatalf("missing queue was created: GetQueue() error = %v", err)
	}
}

func TestSubscribeExistingWithCursorDoesNotChangeQueueType(t *testing.T) {
	logStore := memlog.New()
	mgr := NewManager(logStore, newMockGroupStore(), nil, DefaultConfig(), nil, nil)
	queueCfg := types.DefaultQueueConfig(testQueueEvents, "$queue/events/#")
	if err := mgr.CreateQueue(context.Background(), queueCfg); err != nil {
		t.Fatalf("CreateQueue() error = %v", err)
	}

	cursor := &types.CursorOption{Position: types.CursorEarliest, Mode: types.GroupModeStream}
	err := mgr.SubscribeExistingWithCursor(context.Background(), testQueueEvents, "", testClientOneID, "streamer", "", cursor)
	if !errors.Is(err, ErrQueueNotStream) {
		t.Fatalf("SubscribeExistingWithCursor() error = %v, want %v", err, ErrQueueNotStream)
	}
	stored, err := mgr.GetQueue(context.Background(), testQueueEvents)
	if err != nil {
		t.Fatalf("GetQueue() error = %v", err)
	}
	if stored.Type != types.QueueTypeClassic {
		t.Fatalf("queue type = %q, want %q", stored.Type, types.QueueTypeClassic)
	}
}

func TestStreamAckManualCommitPreservesCommittedOffset(t *testing.T) {
	logStore := memlog.New()
	groupStore := newMockGroupStore()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	mgr := NewManager(logStore, groupStore, nil, DefaultConfig(), logger, nil)

	queueCfg := types.DefaultQueueConfig(testQueueEvents, "$queue/events/#")
	queueCfg.Type = types.QueueTypeStream
	if err := mgr.CreateQueue(context.Background(), queueCfg); err != nil {
		t.Fatalf("CreateQueue failed: %v", err)
	}

	group := types.NewConsumerGroupState(testQueueEvents, "streamer", "")
	group.Mode = types.GroupModeStream
	group.AutoCommit = false
	group.Cursor.Cursor = 1
	group.Cursor.Committed = 0
	if err := groupStore.CreateConsumerGroup(context.Background(), group); err != nil {
		t.Fatalf("CreateConsumerGroup failed: %v", err)
	}

	if err := mgr.Ack(context.Background(), testQueueEvents, "events:0", "streamer"); err != nil {
		t.Fatalf("Ack failed: %v", err)
	}

	stored, err := groupStore.GetConsumerGroup(context.Background(), testQueueEvents, "streamer")
	if err != nil {
		t.Fatalf("GetConsumerGroup failed: %v", err)
	}
	if cursor := stored.GetCursor().Cursor; cursor != 1 {
		t.Fatalf("expected cursor 1, got %d", cursor)
	}
	if committed := stored.GetCursor().Committed; committed != 0 {
		t.Fatalf("expected committed offset 0, got %d", committed)
	}
}

func TestStreamRejectAdvancesCursor(t *testing.T) {
	logStore := memlog.New()
	groupStore := newMockGroupStore()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	mgr := NewManager(logStore, groupStore, nil, DefaultConfig(), logger, nil)

	queueCfg := types.DefaultQueueConfig(testQueueEvents, "$queue/events/#")
	queueCfg.Type = types.QueueTypeStream
	if err := mgr.CreateQueue(context.Background(), queueCfg); err != nil {
		t.Fatalf("CreateQueue failed: %v", err)
	}
	if _, err := logStore.Append(context.Background(), testQueueEvents,
		newQueueEnvelope("stream-reject", "$queue/events/bad", []byte("bad"))); err != nil {
		t.Fatalf("Append failed: %v", err)
	}

	cursor := &types.CursorOption{Position: types.CursorEarliest, Mode: types.GroupModeStream}
	if err := mgr.SubscribeWithCursor(context.Background(), testQueueEvents, "", testClientOneID, "streamer", "", cursor); err != nil {
		t.Fatalf("SubscribeWithCursor failed: %v", err)
	}

	if err := mgr.Reject(context.Background(), testQueueEvents, "events:0", "streamer", "bad message"); err != nil {
		t.Fatalf("Reject failed: %v", err)
	}

	group, err := groupStore.GetConsumerGroup(context.Background(), testQueueEvents, "streamer")
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

func TestClassicRejectMovesToDLQBeforeRemovingPendingEntry(t *testing.T) {
	ctx := context.Background()
	logStore := memlog.New()
	groupStore := newMockGroupStore()
	mgr := NewManager(logStore, groupStore, nil, DefaultConfig(), slog.New(slog.NewTextHandler(io.Discard, nil)), nil)
	require.NoError(t, mgr.CreateQueue(ctx, types.DefaultQueueConfig("tasks", "$queue/tasks/#")))
	group := types.NewConsumerGroupState("tasks", testGroupWorkers, "")
	group.SetConsumer(testConsumer, &types.ConsumerInfo{ID: testConsumer, ClientID: testConsumer})
	require.NoError(t, groupStore.CreateConsumerGroup(ctx, group))
	_, err := logStore.Append(ctx, "tasks", newQueueEnvelope(testPoison, testQueueTasksJob, []byte("bad")))
	require.NoError(t, err)
	_, err = mgr.consumerManager.Claim(ctx, "tasks", testGroupWorkers, testConsumer, nil)
	require.NoError(t, err)

	require.NoError(t, mgr.Reject(ctx, "tasks", "tasks:0", testGroupWorkers, "invalid payload"))
	entries, err := groupStore.GetPendingEntries(ctx, "tasks", testGroupWorkers, testConsumer)
	require.NoError(t, err)
	require.Empty(t, entries)
	dlqMsg, err := logStore.Read(ctx, "$dlq/tasks", 0)
	require.NoError(t, err)
	require.Equal(t, "invalid payload", dlqMsg.Broker.Transfer.FailureReason)
	require.NotEmpty(t, dlqMsg.Broker.Transfer.ID)
}

func TestClassicRejectKeepsPendingWhenDLQDisabled(t *testing.T) {
	ctx := context.Background()
	logStore := memlog.New()
	groupStore := newMockGroupStore()
	mgr := NewManager(logStore, groupStore, nil, DefaultConfig(), slog.New(slog.NewTextHandler(io.Discard, nil)), nil)
	cfg := types.DefaultQueueConfig("tasks", "$queue/tasks/#")
	cfg.DLQConfig.Enabled = false
	require.NoError(t, mgr.CreateQueue(ctx, cfg))
	group := types.NewConsumerGroupState("tasks", testGroupWorkers, "")
	group.SetConsumer(testConsumer, &types.ConsumerInfo{ID: testConsumer, ClientID: testConsumer})
	require.NoError(t, groupStore.CreateConsumerGroup(ctx, group))
	_, err := logStore.Append(ctx, "tasks", newQueueEnvelope(testPoison, testQueueTasksJob, nil))
	require.NoError(t, err)
	_, err = mgr.consumerManager.Claim(ctx, "tasks", testGroupWorkers, testConsumer, nil)
	require.NoError(t, err)

	require.ErrorIs(t, mgr.Reject(ctx, "tasks", "tasks:0", testGroupWorkers, "invalid payload"), ErrDLQDisabled)
	entries, err := groupStore.GetPendingEntries(ctx, "tasks", testGroupWorkers, testConsumer)
	require.NoError(t, err)
	require.Len(t, entries, 1)
}

func TestRetentionOffsetMessages(t *testing.T) {
	logStore := memlog.New()
	groupStore := newMockGroupStore()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	mgr := NewManager(logStore, groupStore, nil, DefaultConfig(), logger, nil)

	queueCfg := types.DefaultQueueConfig(testQueueEvents, "$queue/events/#")
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
	queueCfg.Name = testQueueEvents
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

	deliveredMsgs := make(chan *message.Envelope, 10)

	deliveryTarget := DeliveryTargetFunc(func(ctx context.Context, clientID string, msg *message.Envelope) error {
		deliveredMsgs <- msg
		return nil
	})

	config := DefaultConfig()
	config.DeliveryInterval = 50 * time.Millisecond
	logger := slog.Default()

	manager := NewManager(logStore, groupStore, deliveryTarget, config, logger, nil)

	ctx := context.Background()

	if err := manager.Start(ctx); err != nil {
		t.Fatalf("Failed to start manager: %v", err)
	}
	defer manager.Stop() //nolint:errcheck // test cleanup

	clientID := "test-client-1"
	queueName := "tasks" //nolint:goconst // test value

	if err := manager.Subscribe(ctx, queueName, "", clientID, "", ""); err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}

	if err := manager.Publish(ctx, types.PublishRequest{Topic: "$queue/tasks", Payload: []byte("task1")}); err != nil {
		t.Fatalf("Enqueue failed: %v", err)
	}

	select {
	case msg := <-deliveredMsgs:
		if string(msg.PayloadBytes()) != "task1" {
			t.Errorf("Expected payload 'task1', got %s", msg.PayloadBytes())
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Timeout waiting for message delivery")
	}
}

func TestMultiLevelWildcard(t *testing.T) {
	logStore := memlog.New()
	groupStore := newMockGroupStore()

	deliveredMsgs := make(chan *message.Envelope, 10)

	deliveryTarget := DeliveryTargetFunc(func(ctx context.Context, clientID string, msg *message.Envelope) error {
		deliveredMsgs <- msg
		return nil
	})

	config := DefaultConfig()
	config.DeliveryInterval = 50 * time.Millisecond
	logger := slog.Default()

	manager := NewManager(logStore, groupStore, deliveryTarget, config, logger, nil)

	ctx := context.Background()

	if err := manager.Start(ctx); err != nil {
		t.Fatalf("Failed to start manager: %v", err)
	}
	defer manager.Stop() //nolint:errcheck // test cleanup

	if err := manager.Subscribe(ctx, "images", "#", "client1", "", ""); err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}

	topics := []string{
		"$queue/images/png",
		"$queue/images/jpg",
		"$queue/images/photos/vacation",
	}

	for _, topic := range topics {
		if err := manager.Publish(ctx, types.PublishRequest{Topic: topic, Payload: []byte(topic)}); err != nil {
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

	deliveredMsgs := make(chan *message.Envelope, 10)

	deliveryTarget := DeliveryTargetFunc(func(ctx context.Context, clientID string, msg *message.Envelope) error {
		deliveredMsgs <- msg
		return nil
	})

	config := DefaultConfig()
	config.DeliveryInterval = 50 * time.Millisecond
	logger := slog.Default()

	manager := NewManager(logStore, groupStore, deliveryTarget, config, logger, nil)

	ctx := context.Background()

	if err := manager.Start(ctx); err != nil {
		t.Fatalf("Failed to start manager: %v", err)
	}
	defer manager.Stop() //nolint:errcheck // test cleanup

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
		if err := manager.Publish(ctx, types.PublishRequest{Topic: topic, Payload: []byte("match")}); err != nil {
			t.Fatalf("Enqueue to %s failed: %v", topic, err)
		}
	}

	for _, topic := range nonMatching {
		if err := manager.Publish(ctx, types.PublishRequest{Topic: topic, Payload: []byte("nomatch")}); err != nil {
			t.Fatalf("Enqueue to %s failed: %v", topic, err)
		}
	}

	received := 0
	timeout := time.After(2 * time.Second)

loop:
	for {
		select {
		case msg := <-deliveredMsgs:
			if string(msg.PayloadBytes()) == "nomatch" {
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

	deliveredMsgs := make(chan *message.Envelope, 10)

	deliveryTarget := DeliveryTargetFunc(func(ctx context.Context, clientID string, msg *message.Envelope) error {
		deliveredMsgs <- msg
		return nil
	})

	config := DefaultConfig()
	config.DeliveryInterval = 50 * time.Millisecond
	logger := slog.Default()

	manager := NewManager(logStore, groupStore, deliveryTarget, config, logger, nil)

	ctx := context.Background()

	if err := manager.Start(ctx); err != nil {
		t.Fatalf("Failed to start manager: %v", err)
	}
	defer manager.Stop() //nolint:errcheck // test cleanup

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
		if err := manager.Publish(ctx, types.PublishRequest{Topic: topic, Payload: []byte("match")}); err != nil {
			t.Fatalf("Enqueue to %s failed: %v", topic, err)
		}
	}

	for _, topic := range nonMatching {
		if err := manager.Publish(ctx, types.PublishRequest{Topic: topic, Payload: []byte("nomatch")}); err != nil {
			t.Fatalf("Enqueue to %s failed: %v", topic, err)
		}
	}

	received := 0
	timeout := time.After(2 * time.Second)

loop:
	for {
		select {
		case msg := <-deliveredMsgs:
			if string(msg.PayloadBytes()) == "nomatch" {
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
	message   *message.Envelope
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

func (c *mockCluster) RouteQueueMessage(ctx context.Context, nodeID, clientID string, msg *message.Envelope) error {
	var msgCopy *message.Envelope
	if msg != nil {
		msgCopy = msg.Clone()
	}
	c.routedMessagesMu.Lock()
	defer c.routedMessagesMu.Unlock()
	c.routedMessages = append(c.routedMessages, routedMessage{
		nodeID:    nodeID,
		clientID:  clientID,
		queueName: msg.Broker.Queue.Name,
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
func (c *mockCluster) IsLeader(_ context.Context) bool         { return true }
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

func (c *mockCluster) RemoveAllSubscriptions(ctx context.Context, clientID string) error {
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

func (c *mockCluster) ForwardGroupOp(ctx context.Context, nodeID, queueName string, opData []byte) error {
	return nil
}

type mockQueueCoordinator struct {
	enabled           bool
	replicatedByQueue map[string]bool
	leaderByQueue     map[string]bool
	leaderAddrByQueue map[string]string
	leaderIDByQueue   map[string]string

	appendCalls []string
	createCalls []string
	cursorCalls []string
	commitCalls []string
}

func (m *mockQueueCoordinator) Stop() error { return nil }
func (m *mockQueueCoordinator) IsEnabled() bool {
	return m.enabled
}

func (m *mockQueueCoordinator) IsQueueReplicated(queueName string) bool {
	if m.replicatedByQueue == nil {
		return false
	}
	return m.replicatedByQueue[queueName]
}

func (m *mockQueueCoordinator) IsLeaderForQueue(queueName string) bool {
	if m.leaderByQueue == nil {
		return false
	}
	if leader, ok := m.leaderByQueue[queueName]; ok {
		return leader
	}
	return false
}

func (m *mockQueueCoordinator) LeaderForQueue(queueName string) string {
	if m.leaderAddrByQueue == nil {
		return ""
	}
	return m.leaderAddrByQueue[queueName]
}

func (m *mockQueueCoordinator) LeaderIDForQueue(queueName string) string {
	if m.leaderIDByQueue == nil {
		return ""
	}
	return m.leaderIDByQueue[queueName]
}

func (m *mockQueueCoordinator) ApplyCreateQueue(_ context.Context, _ types.QueueConfig) error {
	return nil
}

func (m *mockQueueCoordinator) ApplyUpdateQueue(_ context.Context, _ types.QueueConfig) error {
	return nil
}
func (m *mockQueueCoordinator) ApplyDeleteQueue(_ context.Context, _ string) error { return nil }
func (m *mockQueueCoordinator) ApplyAppendWithOptions(_ context.Context, queueName string, _ *message.Envelope, _ queueraft.ApplyOptions) (uint64, error) {
	m.appendCalls = append(m.appendCalls, queueName)
	return 1, nil
}

func (m *mockQueueCoordinator) ApplyTruncate(_ context.Context, _ string, _ uint64) error {
	return nil
}

func (m *mockQueueCoordinator) ApplyCreateGroup(_ context.Context, queueName string, group *types.ConsumerGroup) error {
	m.createCalls = append(m.createCalls, queueName+"/"+group.ID)
	return nil
}

func (m *mockQueueCoordinator) ApplyUpdateGroup(_ context.Context, _ string, _ *types.ConsumerGroup) error {
	return nil
}

func (m *mockQueueCoordinator) ApplyDeleteGroup(_ context.Context, _ string, _ string) error {
	return nil
}

func (m *mockQueueCoordinator) ApplyUpdateCursor(_ context.Context, queueName, groupID string, cursor uint64) error {
	m.cursorCalls = append(m.cursorCalls, fmt.Sprintf("%s/%s/%d", queueName, groupID, cursor))
	return nil
}

func (m *mockQueueCoordinator) ApplyUpdateCommitted(_ context.Context, queueName, groupID string, committed uint64) error {
	m.commitCalls = append(m.commitCalls, fmt.Sprintf("%s/%s/%d", queueName, groupID, committed))
	return nil
}

func (m *mockQueueCoordinator) ApplyAddPending(_ context.Context, _ string, _ string, _ *types.PendingEntry) error {
	return nil
}

func (m *mockQueueCoordinator) ApplyRemovePending(_ context.Context, _ string, _ string, _ string, _ uint64) error {
	return nil
}

func (m *mockQueueCoordinator) ApplyTransferPending(_ context.Context, _ string, _ string, _ uint64, _ string, _ string) error {
	return nil
}

func (m *mockQueueCoordinator) ApplyRegisterConsumer(_ context.Context, _ string, _ string, _ *types.ConsumerInfo) error {
	return nil
}

func (m *mockQueueCoordinator) ApplyUnregisterConsumer(_ context.Context, _ string, _ string, _ string) error {
	return nil
}

func (m *mockQueueCoordinator) EnsureQueue(_ context.Context, _ types.QueueConfig) error { return nil }

func (m *mockQueueCoordinator) UpdateQueue(_ context.Context, _ types.QueueConfig) error { return nil }

func (m *mockQueueCoordinator) DeleteQueue(_ context.Context, _ string) error { return nil }

func TestCrossNodeMessageRouting(t *testing.T) {
	logStore := memlog.New()
	groupStore := newMockGroupStore()

	localNodeID := "node-1"   //nolint:goconst // test value
	remoteNodeID := testNode2 //nolint:goconst // test value
	mockCl := newMockCluster(localNodeID)

	var localDeliveries []string
	var localMu sync.Mutex
	deliveryTarget := DeliveryTargetFunc(func(ctx context.Context, clientID string, msg *message.Envelope) error {
		localMu.Lock()
		localDeliveries = append(localDeliveries, clientID)
		localMu.Unlock()
		return nil
	})

	config := DefaultConfig()
	config.DeliveryInterval = 50 * time.Millisecond
	logger := slog.Default()

	manager := NewManager(logStore, groupStore, deliveryTarget, config, logger, mockCl)

	ctx := context.Background()

	if err := manager.Start(ctx); err != nil {
		t.Fatalf("Failed to start manager: %v", err)
	}
	defer manager.Stop() //nolint:errcheck // test cleanup

	localClientID := "local-client"
	if err := manager.Subscribe(ctx, testQueueTest, "#", localClientID, "", localNodeID); err != nil {
		t.Fatalf("Subscribe local client failed: %v", err)
	}

	remoteClientID := testRemoteClientID
	if err := manager.Subscribe(ctx, testQueueTest, "#", remoteClientID, "", remoteNodeID); err != nil {
		t.Fatalf("Subscribe remote client failed: %v", err)
	}

	if err := manager.Publish(ctx, types.PublishRequest{Topic: "$queue/test/msg", Payload: []byte("hello")}); err != nil {
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
			if rm.message.Broker.Queue.MessageID == "" {
				t.Error("Expected routed message-id to be set")
			}
			if rm.message.Broker.Queue.GroupID == "" {
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
		DeliveryTargetFunc(func(ctx context.Context, clientID string, msg *message.Envelope) error { return nil }),
		DefaultConfig(),
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		mockCl,
	)

	if err := manager.Subscribe(context.Background(), "demo-orders", "#", "amqp091:conn-1", "demo-workers", ""); err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}

	registered := mockCl.GetRegisteredQueueConsumers()
	if len(registered) != 1 {
		t.Fatalf("expected 1 registered consumer, got %d", len(registered))
	}
	if registered[0].ProxyNodeID != node1 {
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
		DeliveryTargetFunc(func(ctx context.Context, clientID string, msg *message.Envelope) error { return nil }),
		DefaultConfig(),
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		mockCl,
	)

	ctx := context.Background()
	if err := manager.Subscribe(ctx, "tasks", "", testRemoteClientID, testGroupWorkers, testNode2); err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}

	if err := manager.Publish(ctx, types.PublishRequest{Topic: testQueueTasksNew, Payload: []byte("job"), Properties: map[string]string{"custom": testCustomValue}}); err != nil {
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
	if msg.message.Broker.Queue.MessageID != "tasks:0" {
		t.Fatalf("expected message-id tasks:0, got %q", msg.message.Broker.Queue.MessageID)
	}
	if got := msg.message.Broker.Queue.GroupID; got != testGroupWorkers { //nolint:goconst // test value
		t.Fatalf("expected group-id workers, got %q", got)
	}
	if got := msg.message.Broker.Queue.Name; got != "tasks" {
		t.Fatalf("expected queue tasks, got %q", got)
	}
	if got := msg.message.Broker.Queue.Offset; got != 0 {
		t.Fatalf("expected sequence 0, got %d", got)
	}
	if got := msg.message.User.Properties["custom"]; got != testCustomValue {
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
		DeliveryTargetFunc(func(ctx context.Context, clientID string, msg *message.Envelope) error { return nil }),
		cfg,
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		mockCl,
	)

	if err := manager.Start(context.Background()); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer manager.Stop() //nolint:errcheck // test cleanup

	ctx := context.Background()
	queueCfg := types.DefaultQueueConfig(testQueueEvents, "$queue/events/#")
	queueCfg.Type = types.QueueTypeStream
	if err := manager.CreateQueue(ctx, queueCfg); err != nil && err != storage.ErrQueueAlreadyExists {
		t.Fatalf("CreateQueue failed: %v", err)
	}

	if err := manager.Publish(ctx, types.PublishRequest{Topic: "$queue/events/user.action", Payload: []byte("event-1")}); err != nil {
		t.Fatalf("Enqueue failed: %v", err)
	}
	time.Sleep(200 * time.Millisecond)

	// Simulate remote stream consumer registration that happens after publish.
	mockCl.SetQueueConsumers([]*cluster.QueueConsumerInfo{
		{
			QueueName:    testQueueEvents,
			GroupID:      "demo-readers@#",
			ConsumerID:   "remote-consumer-1",
			ClientID:     "amqp091:conn-remote",
			Pattern:      "#",
			Mode:         string(types.GroupModeStream),
			ProxyNodeID:  testNode2,
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
			if stream := routed[0].message.Broker.Queue.Stream; stream == nil || stream.Offset != 0 {
				t.Fatalf("expected stream offset=0, got %#v", stream)
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
		DeliveryTargetFunc(func(ctx context.Context, clientID string, msg *message.Envelope) error { return nil }),
		DefaultConfig(),
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		mockCl,
	)

	cursor := &types.CursorOption{
		Position: types.CursorEarliest,
		Mode:     types.GroupModeStream,
	}
	if err := manager.SubscribeWithCursor(context.Background(), "demo-events", "#", "amqp091:conn-1", "demo-readers", "", cursor); err != nil {
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

func TestSubscribeWithCursorStreamDefaultResumesStoredCursor(t *testing.T) {
	logStore := memlog.New()
	groupStore := newMockGroupStore()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	manager := NewManager(
		logStore,
		groupStore,
		DeliveryTargetFunc(func(ctx context.Context, clientID string, msg *message.Envelope) error { return nil }),
		DefaultConfig(),
		logger,
		nil,
	)

	queueCfg := types.DefaultQueueConfig(testQueueEvents, "$queue/events/#")
	queueCfg.Type = types.QueueTypeStream
	if err := manager.CreateQueue(context.Background(), queueCfg); err != nil {
		t.Fatalf("CreateQueue failed: %v", err)
	}

	group := types.NewConsumerGroupState(testQueueEvents, "streamers", "")
	group.Mode = types.GroupModeStream
	group.Cursor.Cursor = 7
	group.Cursor.Committed = 7
	if err := groupStore.CreateConsumerGroup(context.Background(), group); err != nil {
		t.Fatalf("CreateConsumerGroup failed: %v", err)
	}

	cursor := &types.CursorOption{
		Position: types.CursorDefault,
		Mode:     types.GroupModeStream,
	}
	if err := manager.SubscribeWithCursor(context.Background(), testQueueEvents, "", testClientOneID, "streamers", "", cursor); err != nil {
		t.Fatalf("SubscribeWithCursor failed: %v", err)
	}

	stored, err := groupStore.GetConsumerGroup(context.Background(), testQueueEvents, "streamers")
	if err != nil {
		t.Fatalf("GetConsumerGroup failed: %v", err)
	}
	if cursor := stored.GetCursor().Cursor; cursor != 7 {
		t.Fatalf("expected cursor 7 to be preserved, got %d", cursor)
	}
	if committed := stored.GetCursor().Committed; committed != 7 {
		t.Fatalf("expected committed 7 to be preserved, got %d", committed)
	}
}

func TestPublishForwardPolicySkipsRemoteForwarding(t *testing.T) {
	logStore := memlog.New()
	groupStore := newMockGroupStore()
	logger := slog.Default()

	mockCl := newMockCluster("node-1")
	mockCl.SetQueueConsumers([]*cluster.QueueConsumerInfo{
		{
			QueueName:   testQueueTest,
			ProxyNodeID: testNode2,
		},
	})

	config := DefaultConfig()
	config.WritePolicy = WritePolicyForward
	config.DistributionMode = DistributionForward

	manager := NewManager(logStore, groupStore, DeliveryTargetFunc(func(ctx context.Context, clientID string, msg *message.Envelope) error {
		return nil
	}), config, logger, mockCl)

	manager.SetRaftCoordinator(&mockQueueCoordinator{
		enabled:           true,
		replicatedByQueue: map[string]bool{testQueueTest: true},
		leaderByQueue:     map[string]bool{testQueueTest: false},
		leaderIDByQueue:   map[string]string{testQueueTest: testNode2},
		leaderAddrByQueue: map[string]string{testQueueTest: "127.0.0.1:7200"},
	})

	ctx := context.Background()
	replicated := types.DefaultQueueConfig(testQueueTest, "$queue/test/#")
	replicated.Replication.Enabled = true
	if err := manager.CreateQueue(ctx, replicated); err != nil && err != storage.ErrQueueAlreadyExists {
		t.Fatalf("CreateQueue failed: %v", err)
	}

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

	if calls[0].nodeID != testNode2 {
		t.Fatalf("expected leader nodeID node-2, got %s", calls[0].nodeID)
	}
}

func TestPublishForwardPolicyUsesQueueCoordinatorLeader(t *testing.T) {
	logStore := memlog.New()
	groupStore := newMockGroupStore()
	logger := slog.Default()

	mockCl := newMockCluster("node-1")

	config := DefaultConfig()
	config.WritePolicy = WritePolicyForward
	config.DistributionMode = DistributionForward

	manager := NewManager(logStore, groupStore, DeliveryTargetFunc(func(ctx context.Context, clientID string, msg *message.Envelope) error {
		return nil
	}), config, logger, mockCl)

	coordinator := &mockQueueCoordinator{
		enabled:           true,
		replicatedByQueue: map[string]bool{testQueueHotEvents: true},
		leaderByQueue:     map[string]bool{testQueueHotEvents: false},
		leaderIDByQueue:   map[string]string{testQueueHotEvents: testNode2},
		leaderAddrByQueue: map[string]string{testQueueHotEvents: "127.0.0.1:8200"},
	}
	manager.SetRaftCoordinator(coordinator)

	ctx := context.Background()
	replicated := types.DefaultQueueConfig(testQueueHotEvents, "$queue/hot-events/#")
	replicated.Replication.Enabled = true
	replicated.Replication.Group = "hot"
	if err := manager.CreateQueue(ctx, replicated); err != nil && err != storage.ErrQueueAlreadyExists {
		t.Fatalf("CreateQueue failed: %v", err)
	}

	err := manager.Publish(ctx, types.PublishRequest{
		Topic:   "$queue/hot-events/msg",
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
	if calls[0].nodeID != testNode2 {
		t.Fatalf("expected leader nodeID node-2, got %s", calls[0].nodeID)
	}
}

func TestSubscribeWithCursorReplicatedQueueRoutesStateThroughCoordinator(t *testing.T) {
	logStore := memlog.New()
	groupStore := newMockGroupStore()
	managerConfig := DefaultConfig()
	managerConfig.WritePolicy = WritePolicyReject
	manager := NewManager(
		logStore,
		groupStore,
		DeliveryTargetFunc(func(ctx context.Context, clientID string, msg *message.Envelope) error { return nil }),
		managerConfig,
		slog.Default(),
		nil,
	)

	coordinator := &mockQueueCoordinator{
		enabled:           true,
		replicatedByQueue: map[string]bool{"orders": true},
		leaderByQueue:     map[string]bool{"orders": true},
	}
	manager.SetRaftCoordinator(coordinator)

	ctx := context.Background()
	replicated := types.DefaultQueueConfig("orders", "$queue/orders/#")
	replicated.Replication.Enabled = true
	if err := manager.CreateQueue(ctx, replicated); err != nil {
		t.Fatalf("CreateQueue failed: %v", err)
	}

	cursor := &types.CursorOption{
		Position: types.CursorEarliest,
		Mode:     types.GroupModeStream,
	}
	if err := manager.SubscribeWithCursor(ctx, "orders", "#", testClientOneID, "", "", cursor); err != nil {
		t.Fatalf("SubscribeWithCursor failed: %v", err)
	}

	if len(coordinator.createCalls) == 0 {
		t.Fatalf("expected replicated CreateGroup call")
	}
	if len(coordinator.cursorCalls) == 0 {
		t.Fatalf("expected replicated UpdateCursor call")
	}
}

func TestPublishForwardPolicySplitsByLeaderAndMarksTargets(t *testing.T) {
	logStore := memlog.New()
	groupStore := newMockGroupStore()
	logger := slog.Default()

	mockCl := newMockCluster("node-1")
	config := DefaultConfig()
	config.WritePolicy = WritePolicyForward
	config.DistributionMode = DistributionForward

	manager := NewManager(logStore, groupStore, DeliveryTargetFunc(func(ctx context.Context, clientID string, msg *message.Envelope) error {
		return nil
	}), config, logger, mockCl)

	coordinator := &mockQueueCoordinator{
		enabled:           true,
		replicatedByQueue: map[string]bool{"q1": true, "q2": true},
		leaderByQueue:     map[string]bool{"q1": false, "q2": false},
		leaderIDByQueue:   map[string]string{"q1": testNode2, "q2": "node-3"},
		leaderAddrByQueue: map[string]string{"q1": "127.0.0.1:8200", "q2": "127.0.0.1:8300"},
	}
	manager.SetRaftCoordinator(coordinator)

	ctx := context.Background()
	q1 := types.DefaultQueueConfig("q1", "shared/#")
	q1.Replication.Enabled = true
	q2 := types.DefaultQueueConfig("q2", "shared/#")
	q2.Replication.Enabled = true
	if err := manager.CreateQueue(ctx, q1); err != nil {
		t.Fatalf("CreateQueue q1 failed: %v", err)
	}
	if err := manager.CreateQueue(ctx, q2); err != nil {
		t.Fatalf("CreateQueue q2 failed: %v", err)
	}

	if err := manager.Publish(ctx, types.PublishRequest{
		Topic:   "shared/topic",
		Payload: []byte("hello"),
	}); err != nil {
		t.Fatalf("Publish failed: %v", err)
	}

	calls := mockCl.GetForwardCalls()
	if len(calls) != 2 {
		t.Fatalf("expected 2 forward calls, got %d", len(calls))
	}

	seenTarget := map[string]bool{}
	for _, call := range calls {
		if !call.forwardToLeader {
			t.Fatalf("expected forward-to-leader call")
		}
		target := call.properties[message.PropertyForwardTargetQueues]
		if target == "" {
			t.Fatalf("expected target queues metadata")
		}
		seenTarget[target] = true
	}

	if !seenTarget["q1"] || !seenTarget["q2"] {
		t.Fatalf("expected forwarded target sets for q1 and q2, got %#v", seenTarget)
	}
}

func TestPublishForcedTargets(t *testing.T) {
	logStore := memlog.New()
	groupStore := newMockGroupStore()
	manager := NewManager(
		logStore,
		groupStore,
		DeliveryTargetFunc(func(ctx context.Context, clientID string, msg *message.Envelope) error { return nil }),
		DefaultConfig(),
		slog.Default(),
		nil,
	)

	ctx := context.Background()
	if err := manager.CreateQueue(ctx, types.DefaultQueueConfig("q1", "shared/#")); err != nil {
		t.Fatalf("CreateQueue q1 failed: %v", err)
	}
	if err := manager.CreateQueue(ctx, types.DefaultQueueConfig("q2", "shared/#")); err != nil {
		t.Fatalf("CreateQueue q2 failed: %v", err)
	}

	if err := manager.Publish(ctx, types.PublishRequest{
		Topic:               "shared/topic",
		Payload:             []byte("hello"),
		ForwardTargetQueues: []string{"q1"},
	}); err != nil {
		t.Fatalf("Publish failed: %v", err)
	}

	q1Count, _ := logStore.Count(ctx, "q1")
	q2Count, _ := logStore.Count(ctx, "q2")
	if q1Count != 1 || q2Count != 0 {
		t.Fatalf("unexpected forced target routing counts: q1=%d q2=%d", q1Count, q2Count)
	}

	msg, err := logStore.Read(ctx, "q1", 0)
	if err != nil {
		t.Fatalf("Read q1 message failed: %v", err)
	}
	if _, ok := msg.User.Properties[message.PropertyForwardTargetQueues]; ok {
		t.Fatalf("forwarding metadata must not be persisted in message properties")
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
			ProxyNodeID: testNode2,
		},
	})

	config := DefaultConfig()
	config.DistributionMode = DistributionReplicate

	manager := NewManager(logStore, groupStore, DeliveryTargetFunc(func(ctx context.Context, clientID string, msg *message.Envelope) error {
		return nil
	}), config, logger, mockCl)

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

	if calls[0].nodeID != testNode2 {
		t.Fatalf("expected remote nodeID node-2, got %s", calls[0].nodeID)
	}
}

func TestDeliverQueueMessage(t *testing.T) {
	logStore := memlog.New()
	groupStore := newMockGroupStore()

	var deliveredMsg *message.Envelope
	var deliveredClientID string
	var mu sync.Mutex

	deliveryTarget := DeliveryTargetFunc(func(ctx context.Context, clientID string, msg *message.Envelope) error {
		mu.Lock()
		defer mu.Unlock()
		deliveredClientID = clientID
		deliveredMsg = msg
		return nil
	})

	config := DefaultConfig()
	logger := slog.Default()

	manager := NewManager(logStore, groupStore, deliveryTarget, config, logger, nil)

	ctx := context.Background()

	msg := message.New("$queue/test", []byte("routed payload"))
	msg.User.Properties = map[string]string{"custom": "prop"}
	msg.Broker.Queue = message.QueueMetadata{
		MessageID: "msg-123",
		Name:      testQueueTest,
		GroupID:   testGroupWorkers,
		Offset:    42,
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

	if string(deliveredMsg.PayloadBytes()) != "routed payload" {
		t.Errorf("Expected payload 'routed payload', got '%s'", string(deliveredMsg.PayloadBytes()))
	}

	if deliveredMsg.Broker.Queue.MessageID != "msg-123" {
		t.Errorf("Expected message-id 'msg-123', got '%s'", deliveredMsg.Broker.Queue.MessageID)
	}
	if deliveredMsg.Broker.Queue.Name != testQueueTest {
		t.Errorf("Expected queue 'test', got '%s'", deliveredMsg.Broker.Queue.Name)
	}
	if deliveredMsg.Broker.Source.Topic != "" {
		t.Errorf("Expected an empty broker-owned source topic, got %q", deliveredMsg.Broker.Source.Topic)
	}
}

func TestDeliverQueueMessagePreservesCanonicalTopic(t *testing.T) {
	var deliveredMsg *message.Envelope
	manager := NewManager(
		memlog.New(),
		newMockGroupStore(),
		DeliveryTargetFunc(func(_ context.Context, _ string, msg *message.Envelope) error {
			deliveredMsg = msg
			return nil
		}),
		DefaultConfig(),
		nil,
		nil,
	)

	envelope := message.New("$queue/"+testCapturedTopic, []byte("payload"))
	envelope.Broker.Queue = message.QueueMetadata{MessageID: "m:1", Name: "m", GroupID: "rules-engine", Offset: 1}
	err := manager.DeliverQueueMessage(context.Background(), "target-client", envelope)
	if err != nil {
		t.Fatalf("DeliverQueueMessage failed: %v", err)
	}
	if deliveredMsg == nil {
		t.Fatal("expected delivered message")
	}
	if want := "$queue/" + testCapturedTopic; deliveredMsg.Topic != want {
		t.Fatalf("delivered topic = %q, want %q", deliveredMsg.Topic, want)
	}
}

func TestGetOrCreateQueue_CreatesEphemeral(t *testing.T) {
	logStore := memlog.New()
	groupStore := newMockGroupStore()

	deliveryTarget := DeliveryTargetFunc(func(ctx context.Context, clientID string, msg *message.Envelope) error { return nil })
	config := DefaultConfig()
	logger := slog.Default()

	manager := NewManager(logStore, groupStore, deliveryTarget, config, logger, nil)
	ctx := context.Background()

	if err := manager.Start(ctx); err != nil {
		t.Fatalf("Failed to start manager: %v", err)
	}
	defer manager.Stop() //nolint:errcheck // test cleanup

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
			topic:     testTopicSensorsTemp,
			queueName: testTopicSensorsTemp,
			pattern:   testTopicSensorsTemp,
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
		DeliveryTargetFunc(func(ctx context.Context, clientID string, msg *message.Envelope) error { return nil }),
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
	if string(msg.PayloadBytes()) != "hello" {
		t.Fatalf("expected payload hello, got %q", string(msg.PayloadBytes()))
	}
}

func TestEphemeralQueue_DisconnectAndCleanup(t *testing.T) {
	logStore := memlog.New()
	groupStore := newMockGroupStore()

	deliveryTarget := DeliveryTargetFunc(func(ctx context.Context, clientID string, msg *message.Envelope) error { return nil })
	config := DefaultConfig()
	config.DeliveryInterval = 50 * time.Millisecond
	logger := slog.Default()

	manager := NewManager(logStore, groupStore, deliveryTarget, config, logger, nil)
	ctx := context.Background()

	if err := manager.Start(ctx); err != nil {
		t.Fatalf("Failed to start manager: %v", err)
	}
	defer manager.Stop() //nolint:errcheck // test cleanup

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

func TestDeliveryStaleConsumerStartsEphemeralExpiry(t *testing.T) {
	logStore := memlog.New()
	groupStore := newMockGroupStore()

	var gotQueue, gotGroup string
	var gotConsumerIDs []string

	config := DefaultConfig()
	config.OnConsumerRemoved = func(queueName, groupID string, consumerIDs []string) {
		gotQueue = queueName
		gotGroup = groupID
		gotConsumerIDs = consumerIDs
	}

	manager := NewManager(
		logStore,
		groupStore,
		&targetCheckingDeliverer{targets: map[string]bool{testDeadClientID: false}},
		config,
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		nil,
	)
	ctx := context.Background()

	queueCfg := types.DefaultEphemeralQueueConfig("ephemeral-events", "$queue/ephemeral-events/#")
	if err := manager.CreateQueue(ctx, queueCfg); err != nil {
		t.Fatalf("CreateQueue failed: %v", err)
	}

	group := types.NewConsumerGroupState("ephemeral-events", "readers", "")
	group.SetConsumer(testDeadClientID, &types.ConsumerInfo{ID: testDeadClientID, ClientID: testDeadClientID})
	if err := groupStore.CreateConsumerGroup(ctx, group); err != nil {
		t.Fatalf("CreateConsumerGroup failed: %v", err)
	}

	if manager.deliverQueue(ctx, "ephemeral-events") {
		t.Fatal("expected no delivery for missing target")
	}

	cfg, err := logStore.GetQueue(ctx, "ephemeral-events")
	if err != nil {
		t.Fatalf("GetQueue failed: %v", err)
	}
	if cfg.LastConsumerDisconnect.IsZero() {
		t.Fatal("expected LastConsumerDisconnect to be set after delivery removed last stale consumer")
	}
	if gotQueue != "ephemeral-events" {
		t.Fatalf("expected callback queue ephemeral-events, got %q", gotQueue)
	}
	if gotGroup != "readers" {
		t.Fatalf("expected callback group readers, got %q", gotGroup)
	}
	if len(gotConsumerIDs) != 1 || gotConsumerIDs[0] != testDeadClientID {
		t.Fatalf("expected callback consumers [dead-client], got %v", gotConsumerIDs)
	}
}

func TestCleanupStaleConsumersStartsEphemeralExpiry(t *testing.T) {
	logStore := memlog.New()
	groupStore := newMockGroupStore()

	config := DefaultConfig()
	config.ConsumerTimeout = time.Millisecond
	manager := NewManager(
		logStore,
		groupStore,
		DeliveryTargetFunc(func(context.Context, string, *message.Envelope) error { return nil }),
		config,
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		nil,
	)
	ctx := context.Background()

	queueCfg := types.DefaultEphemeralQueueConfig("ephemeral-events", "$queue/ephemeral-events/#")
	if err := manager.CreateQueue(ctx, queueCfg); err != nil {
		t.Fatalf("CreateQueue failed: %v", err)
	}

	group := types.NewConsumerGroupState("ephemeral-events", "readers", "")
	if err := groupStore.CreateConsumerGroup(ctx, group); err != nil {
		t.Fatalf("CreateConsumerGroup failed: %v", err)
	}

	staleTime := time.Now().Add(-time.Hour)
	info := &types.ConsumerInfo{
		ID:            testDeadClientID,
		ClientID:      testDeadClientID,
		RegisteredAt:  staleTime,
		LastHeartbeat: staleTime,
	}
	if err := groupStore.RegisterConsumer(ctx, "ephemeral-events", "readers", info); err != nil {
		t.Fatalf("RegisterConsumer failed: %v", err)
	}

	manager.cleanupStaleConsumers()

	cfg, err := logStore.GetQueue(ctx, "ephemeral-events")
	if err != nil {
		t.Fatalf("GetQueue failed: %v", err)
	}
	if cfg.LastConsumerDisconnect.IsZero() {
		t.Fatal("expected LastConsumerDisconnect to be set after heartbeat cleanup removed last stale consumer")
	}
}

func TestCleanupEphemeralQueues(t *testing.T) {
	logStore := memlog.New()
	groupStore := newMockGroupStore()

	deliveryTarget := DeliveryTargetFunc(func(ctx context.Context, clientID string, msg *message.Envelope) error { return nil })
	config := DefaultConfig()
	logger := slog.Default()

	manager := NewManager(logStore, groupStore, deliveryTarget, config, logger, nil)
	ctx := context.Background()

	if err := manager.Start(ctx); err != nil {
		t.Fatalf("Failed to start manager: %v", err)
	}
	defer manager.Stop() //nolint:errcheck // test cleanup

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
	manager.cleanupEphemeralQueues(ctx)

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

	deliveryTarget := DeliveryTargetFunc(func(ctx context.Context, clientID string, msg *message.Envelope) error {
		return nil
	})

	config := DefaultConfig()
	logger := slog.Default()

	manager := NewManager(logStore, groupStore, deliveryTarget, config, logger, nil)

	ctx := context.Background()

	if err := manager.Start(ctx); err != nil {
		t.Fatalf("Failed to start manager: %v", err)
	}
	defer manager.Stop() //nolint:errcheck // test cleanup

	msgID, err := manager.EnqueueLocal(ctx, "$queue/remote", []byte("remote payload"), map[string]string{
		"key":                    testCustomValue,
		message.PropertyClientID: "mqtt:remote-client",
		message.PropertyProtocol: string(message.ProtocolMQTT),
		message.PropertyTraceID:  "trace-remote",
	})
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

	stored, err := logStore.Read(ctx, "mqtt", 0)
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}
	defer message.Release(stored)
	if stored.Broker.Source.ClientID != "mqtt:remote-client" || stored.Broker.Source.Protocol != message.ProtocolMQTT {
		t.Fatalf("typed source metadata was not preserved: %+v", stored.Broker.Source)
	}
	if stored.Broker.Trace.TraceID != "trace-remote" {
		t.Fatalf("typed trace metadata was not preserved: %+v", stored.Broker.Trace)
	}
	if _, leaked := stored.User.Properties[message.PropertyClientID]; leaked {
		t.Fatal("broker source metadata leaked into user properties")
	}
	if _, leaked := stored.User.Properties[message.PropertyTraceID]; leaked {
		t.Fatal("broker trace metadata leaked into user properties")
	}
}

func TestSubscriptionTrackingReferenceCounts(t *testing.T) {
	manager := NewManager(
		memlog.New(),
		newMockGroupStore(),
		DeliveryTargetFunc(func(context.Context, string, *message.Envelope) error { return nil }),
		DefaultConfig(),
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		nil,
	)

	manager.trackSubscription(testClientOneID, "orders", "workers@#")
	manager.trackSubscription(testClientOneID, "orders", "workers@#")

	targets := manager.getSubscriptionTargets(testClientOneID)
	if len(targets) != 1 {
		t.Fatalf("expected 1 tracked target after duplicate subscriptions, got %d", len(targets))
	}

	manager.untrackSubscription(testClientOneID, "orders", "workers@#")
	targets = manager.getSubscriptionTargets(testClientOneID)
	if len(targets) != 1 {
		t.Fatalf("expected tracked target to remain after first untrack, got %d", len(targets))
	}

	manager.untrackSubscription(testClientOneID, "orders", "workers@#")
	targets = manager.getSubscriptionTargets(testClientOneID)
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
		DeliveryTargetFunc(func(context.Context, string, *message.Envelope) error { return nil }),
		cfg,
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		nil,
	)

	manager.trackSubscription(testClientOneID, "orders", "workers@#")

	key := manager.subscriptionRefKey("orders", "workers@#")
	manager.subscriptionsMu.Lock()
	manager.subscriptions[testClientOneID][key].lastSeen = time.Now().Add(-time.Minute)
	manager.subscriptionsMu.Unlock()

	manager.pruneStaleSubscriptions()

	targets := manager.getSubscriptionTargets(testClientOneID)
	if len(targets) != 0 {
		t.Fatalf("expected stale tracked target to be pruned, got %d entries", len(targets))
	}
}

func TestUpdateHeartbeatRemovesStaleTrackedTargets(t *testing.T) {
	manager := NewManager(
		memlog.New(),
		newMockGroupStore(),
		DeliveryTargetFunc(func(context.Context, string, *message.Envelope) error { return nil }),
		DefaultConfig(),
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		nil,
	)

	manager.trackSubscription(testClientOneID, "orders", "workers@#")

	if err := manager.UpdateHeartbeat(context.Background(), testClientOneID); err != nil {
		t.Fatalf("UpdateHeartbeat failed: %v", err)
	}

	targets := manager.getSubscriptionTargets(testClientOneID)
	if len(targets) != 0 {
		t.Fatalf("expected stale tracked target to be removed after heartbeat update, got %d entries", len(targets))
	}
}

func TestOnConsumerRemovedCallbackFires(t *testing.T) {
	logStore := memlog.New()
	groupStore := newMockGroupStore()

	var mu sync.Mutex
	var gotQueue, gotGroup string
	var gotConsumerIDs []string

	cfg := DefaultConfig()
	cfg.ConsumerTimeout = 20 * time.Millisecond
	cfg.OnConsumerRemoved = func(queueName, groupID string, consumerIDs []string) {
		mu.Lock()
		defer mu.Unlock()
		gotQueue = queueName
		gotGroup = groupID
		gotConsumerIDs = consumerIDs
	}

	manager := NewManager(
		logStore,
		groupStore,
		DeliveryTargetFunc(func(context.Context, string, *message.Envelope) error { return nil }),
		cfg,
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		nil,
	)

	ctx := context.Background()
	queueCfg := types.DefaultQueueConfig(testQueueEvents, "$queue/events/#")
	if err := manager.CreateQueue(ctx, queueCfg); err != nil {
		t.Fatalf("CreateQueue failed: %v", err)
	}

	group := types.NewConsumerGroupState(testQueueEvents, testGroupWorkers, "")
	if err := groupStore.CreateConsumerGroup(ctx, group); err != nil {
		t.Fatalf("CreateConsumerGroup failed: %v", err)
	}

	staleTime := time.Now().Add(-time.Hour)
	info := &types.ConsumerInfo{
		ID:            testRemoteAMQP091,
		ClientID:      testRemoteAMQP091,
		RegisteredAt:  staleTime,
		LastHeartbeat: staleTime,
	}
	if err := groupStore.RegisterConsumer(ctx, testQueueEvents, testGroupWorkers, info); err != nil {
		t.Fatalf("RegisterConsumer failed: %v", err)
	}

	manager.cleanupStaleConsumers()

	mu.Lock()
	defer mu.Unlock()
	if gotQueue != testQueueEvents {
		t.Fatalf("expected queue 'events', got %q", gotQueue)
	}
	if gotGroup != testGroupWorkers {
		t.Fatalf("expected group 'workers', got %q", gotGroup)
	}
	if len(gotConsumerIDs) != 1 || gotConsumerIDs[0] != testRemoteAMQP091 {
		t.Fatalf("expected [amqp091-10.0.0.1:5000], got %v", gotConsumerIDs)
	}
}

func TestUpdateConsumerHeartbeat(t *testing.T) {
	logStore := memlog.New()
	groupStore := newMockGroupStore()
	manager := NewManager(
		logStore,
		groupStore,
		DeliveryTargetFunc(func(context.Context, string, *message.Envelope) error { return nil }),
		DefaultConfig(),
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		nil,
	)

	ctx := context.Background()
	queueCfg := types.DefaultQueueConfig("orders", "$queue/orders/#")
	if err := manager.CreateQueue(ctx, queueCfg); err != nil {
		t.Fatalf("CreateQueue failed: %v", err)
	}

	group := types.NewConsumerGroupState("orders", testGroupWorkers, "")
	if err := groupStore.CreateConsumerGroup(ctx, group); err != nil {
		t.Fatalf("CreateConsumerGroup failed: %v", err)
	}

	before := time.Now().Add(-time.Hour)
	info := &types.ConsumerInfo{
		ID:            testConsumerOne,
		ClientID:      testConsumerOne,
		RegisteredAt:  before,
		LastHeartbeat: before,
	}
	if err := groupStore.RegisterConsumer(ctx, "orders", testGroupWorkers, info); err != nil {
		t.Fatalf("RegisterConsumer failed: %v", err)
	}

	if err := manager.UpdateConsumerHeartbeat(ctx, "orders", testGroupWorkers, testConsumerOne); err != nil {
		t.Fatalf("UpdateConsumerHeartbeat failed: %v", err)
	}

	updatedGroup, err := groupStore.GetConsumerGroup(ctx, "orders", testGroupWorkers)
	if err != nil {
		t.Fatalf("GetConsumerGroup failed: %v", err)
	}
	updated := updatedGroup.GetConsumer(testConsumerOne)
	if updated == nil {
		t.Fatalf("expected consumer to exist")
	}
	if !updated.LastHeartbeat.After(before) {
		t.Fatalf("expected heartbeat to advance, got %v <= %v", updated.LastHeartbeat, before)
	}
}

func TestPELCapRejectsClaim(t *testing.T) {
	logStore := memlog.New()
	groupStore := newMockGroupStore()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	config := DefaultConfig()
	config.MaxPELSize = 3
	mgr := NewManager(logStore, groupStore, nil, config, logger, nil)

	ctx := context.Background()

	queueCfg := types.DefaultQueueConfig("pelcap", "$queue/pelcap/#")
	if err := mgr.CreateQueue(ctx, queueCfg); err != nil {
		t.Fatalf("CreateQueue failed: %v", err)
	}

	// Publish more messages than MaxPELSize
	for i := 0; i < 5; i++ {
		if err := mgr.Publish(ctx, types.PublishRequest{
			Topic:   "$queue/pelcap/test",
			Payload: []byte("msg"),
		}); err != nil {
			t.Fatalf("Publish failed: %v", err)
		}
	}

	// Set up consumer group + consumer via Subscribe
	if err := mgr.Subscribe(ctx, "pelcap", "", "c1", "g1", ""); err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}

	// Claim exactly MaxPELSize messages (should succeed)
	msgs, err := mgr.consumerManager.ClaimBatch(ctx, "pelcap", "g1", "c1", nil, 3)
	if err != nil {
		t.Fatalf("ClaimBatch should succeed: %v", err)
	}
	if len(msgs) != 3 {
		t.Fatalf("expected 3 messages, got %d", len(msgs))
	}

	// Next claim should fail — PEL is full, so ClaimBatch returns ErrNoMessages
	_, err = mgr.consumerManager.ClaimBatch(ctx, "pelcap", "g1", "c1", nil, 1)
	if err != consumer.ErrNoMessages {
		t.Fatalf("expected ErrNoMessages (PEL full), got: %v", err)
	}

	// Ack one message to free PEL space (message ID format is queueName:offset)
	ackID := fmt.Sprintf("pelcap:%d", msgs[0].Broker.Queue.Offset)
	if err := mgr.Ack(ctx, "pelcap", ackID, "g1"); err != nil {
		t.Fatalf("Ack failed: %v", err)
	}

	// Now claim should succeed again
	msgs2, err := mgr.consumerManager.ClaimBatch(ctx, "pelcap", "g1", "c1", nil, 1)
	if err != nil {
		t.Fatalf("ClaimBatch after ack should succeed: %v", err)
	}
	if len(msgs2) != 1 {
		t.Fatalf("expected 1 message after ack, got %d", len(msgs2))
	}
}

func TestMoveToDLQCreatesQueueAndAppendsMessage(t *testing.T) {
	logStore := memlog.New()
	groupStore := newMockGroupStore()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	ctx := context.Background()

	mgr := NewManager(
		logStore, groupStore,
		DeliveryTargetFunc(func(context.Context, string, *message.Envelope) error { return nil }),
		DefaultConfig(),
		logger, nil,
	)

	// Source queue with DLQ enabled
	queueCfg := types.DefaultQueueConfig("tasks", "$queue/tasks/#")
	queueCfg.DLQConfig.Enabled = true
	if err := mgr.CreateQueue(ctx, queueCfg); err != nil {
		t.Fatalf("CreateQueue failed: %v", err)
	}

	poisonMsg := newQueueEnvelope("bad-msg-1", "$queue/tasks/process", []byte("poison-payload"))
	poisonMsg.User.Properties = map[string]string{"custom-key": "custom-val"}

	require.NoError(t, mgr.moveToDLQ(ctx, "tasks", testGroupWorkers, poisonMsg, 42, 6, "decode failed", "$dlq/"))

	// Verify the DLQ queue was auto-created
	dlqCfg, err := logStore.GetQueue(ctx, "$dlq/tasks")
	if err != nil {
		t.Fatalf("DLQ queue not created: %v", err)
	}
	if dlqCfg.DLQConfig.Enabled {
		t.Fatal("DLQ queue should have DLQ disabled to prevent chains")
	}

	// Read the message from the DLQ queue
	msg, err := logStore.Read(ctx, "$dlq/tasks", 0)
	if err != nil {
		t.Fatalf("failed to read DLQ message: %v", err)
	}
	if string(msg.PayloadBytes()) != "poison-payload" {
		t.Fatalf("expected poison-payload, got %s", string(msg.PayloadBytes()))
	}
	if msg.Broker.Transfer.SourceQueue != "tasks" {
		t.Fatalf("expected original queue 'tasks', got %q", msg.Broker.Transfer.SourceQueue)
	}
	if msg.Broker.Source.Topic != "$queue/tasks/process" {
		t.Fatalf("expected original topic, got %q", msg.Broker.Source.Topic)
	}
	if msg.Broker.Transfer.SourceGroup != testGroupWorkers {
		t.Fatalf("expected group 'workers', got %q", msg.Broker.Transfer.SourceGroup)
	}
	if msg.Broker.Transfer.DeliveryCount != 6 {
		t.Fatalf("expected delivery count 6, got %d", msg.Broker.Transfer.DeliveryCount)
	}
	if msg.Broker.Transfer.SourceOffset != 42 {
		t.Fatalf("expected original offset 42, got %d", msg.Broker.Transfer.SourceOffset)
	}
	if msg.Broker.Transfer.ID == "" || msg.Broker.Queue.MessageID != msg.Broker.Transfer.ID {
		t.Fatalf("expected stable transfer identity, id=%q transfer=%q", msg.Broker.Queue.MessageID, msg.Broker.Transfer.ID)
	}
	if msg.Broker.Transfer.FailureReason != "decode failed" {
		t.Fatalf("expected reject reason, got %q", msg.Broker.Transfer.FailureReason)
	}
	if msg.User.Properties["custom-key"] != "custom-val" {
		t.Fatalf("expected original property preserved, got %q", msg.User.Properties["custom-key"])
	}
	if msg.Broker.Queue.State != message.QueueStateDLQ {
		t.Fatalf("expected state DLQ, got %q", msg.Broker.Queue.State)
	}
}

func TestMoveToDLQDisabledSkipsPublish(t *testing.T) {
	logStore := memlog.New()
	groupStore := newMockGroupStore()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	ctx := context.Background()

	mgr := NewManager(
		logStore, groupStore,
		DeliveryTargetFunc(func(context.Context, string, *message.Envelope) error { return nil }),
		DefaultConfig(),
		logger, nil,
	)

	// Source queue with DLQ disabled
	queueCfg := types.DefaultQueueConfig("tasks", "$queue/tasks/#")
	queueCfg.DLQConfig.Enabled = false
	if err := mgr.CreateQueue(ctx, queueCfg); err != nil {
		t.Fatalf("CreateQueue failed: %v", err)
	}

	err := mgr.moveToDLQ(ctx, "tasks", testGroupWorkers,
		newQueueEnvelope("msg-1", "$queue/tasks/test", []byte("data")), 0, 5, "", "$dlq/")
	require.ErrorIs(t, err, ErrDLQDisabled)

	// DLQ queue should not be created
	_, err = logStore.GetQueue(ctx, "$dlq/tasks")
	if err == nil {
		t.Fatal("expected DLQ queue not to be created when DLQ is disabled")
	}
}

func TestMoveToDLQCustomTopic(t *testing.T) {
	logStore := memlog.New()
	groupStore := newMockGroupStore()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	ctx := context.Background()

	mgr := NewManager(
		logStore, groupStore,
		DeliveryTargetFunc(func(context.Context, string, *message.Envelope) error { return nil }),
		DefaultConfig(),
		logger, nil,
	)

	queueCfg := types.DefaultQueueConfig("tasks", "$queue/tasks/#")
	queueCfg.DLQConfig.Enabled = true
	queueCfg.DLQConfig.Topic = "errors/tasks"
	if err := mgr.CreateQueue(ctx, queueCfg); err != nil {
		t.Fatalf("CreateQueue failed: %v", err)
	}

	require.NoError(t, mgr.moveToDLQ(ctx, "tasks", testGroupWorkers,
		newQueueEnvelope("msg-1", "$queue/tasks/test", []byte("data")), 0, 5, "", "$dlq/"))

	// Should use custom topic as queue name
	_, err := logStore.GetQueue(ctx, "errors/tasks")
	if err != nil {
		t.Fatalf("expected DLQ queue at custom topic: %v", err)
	}
	msg, err := logStore.Read(ctx, "errors/tasks", 0)
	if err != nil {
		t.Fatalf("failed to read from custom DLQ: %v", err)
	}
	if string(msg.PayloadBytes()) != "data" {
		t.Fatalf("expected 'data', got %s", string(msg.PayloadBytes()))
	}
}

// appendFailingStore fails Append for one queue, standing in for a queue whose
// storage is unavailable while the rest of the broker is healthy.
type appendFailingStore struct {
	storage.QueueStore
	failQueue string
}

func (s *appendFailingStore) Append(ctx context.Context, queueName string, msg *message.Envelope) (uint64, error) {
	if queueName == s.failQueue {
		return 0, errors.New("storage unavailable")
	}
	return s.QueueStore.Append(ctx, queueName, msg)
}

// Capture never fails the publish, so the counter is the only durable signal
// that a queue is dropping the traffic its pattern binds. A capture that cannot
// be stored must therefore be both reported to the caller and counted.
func TestPublishToMatchingQueuesCountsCaptureFailures(t *testing.T) {
	logStore := memlog.New()
	store := &appendFailingStore{QueueStore: logStore, failQueue: testCaptureQueue}
	mgr := NewManager(store, newMockGroupStore(), nil, DefaultConfig(), nil, nil)
	ctx := context.Background()

	if err := mgr.CreateQueue(ctx, types.DefaultQueueConfig(testCaptureQueue, "m/#")); err != nil {
		t.Fatalf("CreateQueue messages failed: %v", err)
	}
	if err := mgr.CreateQueue(ctx, types.DefaultQueueConfig("healthy", "h/#")); err != nil {
		t.Fatalf("CreateQueue healthy failed: %v", err)
	}

	if got := mgr.GetMetrics().CaptureFailures; got != 0 {
		t.Fatalf("initial capture failures = %d, want 0", got)
	}

	// The append now happens off the publish path, so the caller is told
	// nothing; the counter is the report.
	flushCapture(t, mgr, func() {
		if err := mgr.PublishToMatchingQueues(ctx, types.PublishRequest{
			Topic:   testCapturedTopic,
			Payload: []byte("payload"),
		}); err != nil {
			t.Fatalf("capture must not fail the publish: %v", err)
		}
	})
	if got := mgr.GetMetrics().CaptureFailures; got != 1 {
		t.Fatalf("capture failures = %d, want 1", got)
	}

	mgr.capture = newCaptureDispatcher(0, 0, 0, mgr.metrics, mgr.logger, mgr.applyCaptureJob)
	flushCapture(t, mgr, func() {
		if err := mgr.PublishToMatchingQueues(ctx, types.PublishRequest{
			Topic:   "h/acme/temp",
			Payload: []byte("payload"),
		}); err != nil {
			t.Fatalf("healthy queue capture failed: %v", err)
		}
	})
	if got := mgr.GetMetrics().CaptureFailures; got != 1 {
		t.Fatalf("a healthy capture changed the failure counter to %d", got)
	}
	count, err := logStore.Count(ctx, "healthy")
	if err != nil {
		t.Fatalf("Count healthy failed: %v", err)
	}
	if count != 1 {
		t.Fatalf("healthy queue stored %d messages, want 1", count)
	}
}

// PublishToMatchingQueues stores what it is given, so it must take ownership of
// the caller's payload, key, headers, and properties. An empty non-nil map is
// still the caller's map, and normalizing the request writes a client ID into
// whatever it is handed, so cloning only non-empty maps would mutate a protocol
// broker's own state.
func TestPublishToMatchingQueuesDoesNotAliasCallerState(t *testing.T) {
	logStore := memlog.New()
	mgr := NewManager(logStore, newMockGroupStore(), nil, DefaultConfig(), nil, nil)
	ctx := context.Background()

	if err := mgr.CreateQueue(ctx, types.DefaultQueueConfig(testCaptureQueue, "m/#")); err != nil {
		t.Fatalf("CreateQueue failed: %v", err)
	}

	properties := map[string]string{}
	payload := []byte("original")
	key := []byte("key")
	headers := map[string][]byte{"binary": {0x00, 0xff}}
	flushCapture(t, mgr, func() {
		if err := mgr.PublishToMatchingQueues(ctx, types.PublishRequest{
			Source:     message.SourceMetadata{ClientID: testCapturePublisher, Protocol: message.ProtocolMQTT},
			Topic:      testCapturedTopic,
			Payload:    payload,
			Key:        key,
			Headers:    headers,
			Properties: properties,
		}); err != nil {
			t.Fatalf("PublishToMatchingQueues failed: %v", err)
		}
	})

	if len(properties) != 0 {
		t.Fatalf("caller's property map was written to: %v", properties)
	}

	// The stored copy must survive the caller reusing its buffer.
	payload[0] = 'X'
	key[0] = 'X'
	headers["binary"][0] = 0xff
	headers["new"] = []byte("new")
	stored, err := logStore.Read(ctx, testCaptureQueue, 0)
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}
	if got := string(stored.PayloadBytes()); got != "original" {
		t.Fatalf("stored payload = %q, want original", got)
	}
	if got := string(stored.User.Key); got != "key" {
		t.Fatalf("stored key = %q, want key", got)
	}
	if got := stored.User.Headers["binary"]; !bytes.Equal(got, []byte{0x00, 0xff}) {
		t.Fatalf("stored binary header = %v, want [0 255]", got)
	}
	if _, ok := stored.User.Headers["new"]; ok {
		t.Fatal("stored headers alias the caller's map")
	}
	if got := stored.Broker.Source.ClientID; got != testCapturePublisher {
		t.Fatalf("stored client ID = %q, want mqtt-publisher", got)
	}
}

// Matching queues are independent targets. A publish reaching one that cannot
// accept it must still reach the others: capture is broker policy applied to
// every matching queue, so letting the first failure decide the outcome would
// let one unavailable queue silently suppress capture into healthy ones.
func TestPublishToMatchingQueuesAttemptsEveryTarget(t *testing.T) {
	logStore := memlog.New()
	coordinator := &mockQueueCoordinator{
		enabled:           true,
		replicatedByQueue: map[string]bool{testReplicatedQueue: true},
		leaderByQueue:     map[string]bool{testReplicatedQueue: false},
	}

	config := DefaultConfig()
	config.WritePolicy = WritePolicyReject
	mgr := NewManager(logStore, newMockGroupStore(), nil, config, slog.New(slog.NewTextHandler(io.Discard, nil)), nil)
	mgr.raftCoordinator = coordinator
	ctx := context.Background()

	// A replicated queue with no reachable leader, which the reject policy
	// refuses, alongside two ordinary queues that must still be appended to.
	replicated := types.DefaultQueueConfig(testReplicatedQueue, "m/#")
	replicated.Replication.Enabled = true
	// Inject persisted legacy state so this test reaches the publish-time
	// no-leader guard; new queue creation now rejects the same state earlier.
	if err := logStore.CreateQueue(ctx, replicated); err != nil {
		t.Fatalf("inject replicated queue failed: %v", err)
	}
	for _, name := range []string{"healthy-a", "healthy-b"} {
		if err := mgr.CreateQueue(ctx, types.DefaultQueueConfig(name, "m/#")); err != nil {
			t.Fatalf("CreateQueue %s failed: %v", name, err)
		}
	}

	flushCapture(t, mgr, func() {
		if err := mgr.PublishToMatchingQueues(ctx, types.PublishRequest{
			Topic:   testCapturedTopic,
			Payload: []byte("payload"),
		}); err != nil {
			t.Fatalf("capture must not fail the publish: %v", err)
		}
	})

	for _, name := range []string{"healthy-a", "healthy-b"} {
		count, countErr := logStore.Count(ctx, name)
		if countErr != nil {
			t.Fatalf("Count %s failed: %v", name, countErr)
		}
		if count != 1 {
			t.Fatalf("queue %s stored %d messages, want 1; a failing target suppressed a healthy one", name, count)
		}
	}
	if got := mgr.GetMetrics().CaptureFailures; got != 1 {
		t.Fatalf("capture failures = %d, want 1", got)
	}
}

// unreadableQueueStore resolves a queue in the topic index but fails to return
// its configuration, so the target is dropped before any append is attempted.
type unreadableQueueStore struct {
	storage.QueueStore
	failQueue string
}

func (s *unreadableQueueStore) GetQueue(ctx context.Context, queueName string) (*types.QueueConfig, error) {
	if queueName == s.failQueue {
		return nil, errors.New("configuration unreadable")
	}
	return s.QueueStore.GetQueue(ctx, queueName)
}

// A target dropped before the append is as much a lost message as a failed
// append, so it must move the counter the monitoring contract points operators at.
func TestPublishToMatchingQueuesCountsUnresolvedTargets(t *testing.T) {
	logStore := memlog.New()
	store := &unreadableQueueStore{QueueStore: logStore, failQueue: testCaptureQueue}
	mgr := NewManager(store, newMockGroupStore(), nil, DefaultConfig(), slog.New(slog.NewTextHandler(io.Discard, nil)), nil)
	ctx := context.Background()

	if err := mgr.CreateQueue(ctx, types.DefaultQueueConfig(testCaptureQueue, "m/#")); err != nil {
		t.Fatalf("CreateQueue failed: %v", err)
	}

	if err := mgr.PublishToMatchingQueues(ctx, types.PublishRequest{
		Topic:   testCapturedTopic,
		Payload: []byte("payload"),
	}); err != nil {
		t.Fatalf("PublishToMatchingQueues failed: %v", err)
	}

	if got := mgr.GetMetrics().CaptureFailures; got != 1 {
		t.Fatalf("capture failures = %d, want 1; a dropped target was invisible", got)
	}
}

// An addressed publish under the reject write policy promises that nothing was
// written: the caller is expected to retry against the leader, so a local append
// that happened anyway would be duplicated by that retry. Capture shares the
// fanout code but not that promise.
func TestPublishRejectWritePolicyWritesNothing(t *testing.T) {
	logStore := memlog.New()
	coordinator := &mockQueueCoordinator{
		enabled:           true,
		replicatedByQueue: map[string]bool{testReplicatedQueue: true},
		leaderByQueue:     map[string]bool{testReplicatedQueue: false},
	}

	config := DefaultConfig()
	config.WritePolicy = WritePolicyReject
	mgr := NewManager(logStore, newMockGroupStore(), nil, config, slog.New(slog.NewTextHandler(io.Discard, nil)), nil)
	mgr.raftCoordinator = coordinator
	ctx := context.Background()

	replicated := types.DefaultQueueConfig(testReplicatedQueue, "$queue/replicated/#")
	replicated.Replication.Enabled = true
	// Inject persisted legacy state so this test reaches the publish-time
	// no-leader guard; new queue creation now rejects the same state earlier.
	if err := logStore.CreateQueue(ctx, replicated); err != nil {
		t.Fatalf("inject replicated queue failed: %v", err)
	}
	if err := mgr.CreateQueue(ctx, types.DefaultQueueConfig("local", "$queue/replicated/#")); err != nil {
		t.Fatalf("CreateQueue local failed: %v", err)
	}

	err := mgr.Publish(ctx, types.PublishRequest{
		Topic:   "$queue/replicated/item",
		Payload: []byte("payload"),
	})
	if err == nil {
		t.Fatal("expected the reject write policy to refuse the publish")
	}

	count, countErr := logStore.Count(ctx, "local")
	if countErr != nil {
		t.Fatalf("Count local failed: %v", countErr)
	}
	if count != 0 {
		t.Fatalf("local queue stored %d messages; a rejected publish must write nothing", count)
	}
}

func TestReplicatedQueueCreationFailsClosed(t *testing.T) {
	queueCfg := types.DefaultQueueConfig(testReplicatedQueue, "$queue/replicated/#")
	queueCfg.Replication.Enabled = true
	readyCoordinator := func() *mockQueueCoordinator {
		return &mockQueueCoordinator{
			enabled:           true,
			replicatedByQueue: map[string]bool{testReplicatedQueue: true},
			leaderByQueue:     map[string]bool{testReplicatedQueue: true},
		}
	}
	tests := []struct {
		name        string
		policy      WritePolicy
		coordinator *mockQueueCoordinator
		want        error
	}{
		{name: "raft missing", policy: WritePolicyReject, want: ErrReplicationUnavailable},
		{name: "raft disabled", policy: WritePolicyReject, coordinator: &mockQueueCoordinator{}, want: ErrReplicationUnavailable},
		{name: "group missing", policy: WritePolicyReject, coordinator: &mockQueueCoordinator{enabled: true}, want: ErrReplicationUnavailable},
		{name: "leader missing", policy: WritePolicyReject, coordinator: &mockQueueCoordinator{enabled: true, replicatedByQueue: map[string]bool{testReplicatedQueue: true}}, want: ErrReplicationUnavailable},
		{name: "local policy", policy: WritePolicyLocal, coordinator: readyCoordinator(), want: ErrReplicationWritePolicy},
		{name: "unknown policy", policy: WritePolicy("mystery"), coordinator: readyCoordinator(), want: ErrReplicationWritePolicy},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := DefaultConfig()
			cfg.WritePolicy = tt.policy
			store := memlog.New()
			mgr := NewManager(store, newMockGroupStore(), nil, cfg, slog.New(slog.NewTextHandler(io.Discard, nil)), nil)
			if tt.coordinator != nil {
				mgr.SetRaftCoordinator(tt.coordinator)
			}
			err := mgr.CreateQueue(context.Background(), queueCfg)
			if !errors.Is(err, tt.want) {
				t.Fatalf("CreateQueue() error = %v, want %v", err, tt.want)
			}
			if _, getErr := store.GetQueue(context.Background(), queueCfg.Name); !errors.Is(getErr, storage.ErrQueueNotFound) {
				t.Fatalf("failed creation persisted queue: %v", getErr)
			}
		})
	}
}

func TestReplicatedPublishNeverFallsBackToLocalAppend(t *testing.T) {
	ctx := context.Background()
	store := memlog.New()
	queueCfg := types.DefaultQueueConfig(testReplicatedQueue, "$queue/replicated/#")
	queueCfg.Replication.Enabled = true
	require.NoError(t, store.CreateQueue(ctx, queueCfg))
	cfg := DefaultConfig()
	cfg.WritePolicy = WritePolicyReject
	mgr := NewManager(store, newMockGroupStore(), nil, cfg, slog.New(slog.NewTextHandler(io.Discard, nil)), nil)

	err := mgr.Publish(ctx, types.PublishRequest{Topic: "$queue/replicated/item", Payload: []byte("payload")})
	require.ErrorIs(t, err, ErrReplicationUnavailable)
	count, err := store.Count(ctx, testReplicatedQueue)
	require.NoError(t, err)
	require.Zero(t, count)
}

func TestStartupRejectsPersistedReplicatedQueueWithoutRaft(t *testing.T) {
	store := memlog.New()
	queueCfg := types.DefaultQueueConfig(testReplicatedQueue, "$queue/replicated/#")
	queueCfg.Replication.Enabled = true
	require.NoError(t, store.CreateQueue(context.Background(), queueCfg))
	cfg := DefaultConfig()
	cfg.WritePolicy = WritePolicyReject
	mgr := NewManager(store, newMockGroupStore(), nil, cfg, slog.New(slog.NewTextHandler(io.Discard, nil)), nil)
	require.ErrorIs(t, mgr.Start(context.Background()), ErrReplicationUnavailable)
}

// Each matching queue a captured publish fails to reach counts. Capture jobs are
// dispatched per queue, so the unit is the queue rather than the publish: a
// publish that misses two of its matching queues counts twice.
func TestPublishToMatchingQueuesCountsEachLostTarget(t *testing.T) {
	logStore := memlog.New()
	store := &unreadableQueueStore{QueueStore: &appendFailingStore{QueueStore: logStore, failQueue: "broken"}, failQueue: testCaptureQueue}
	mgr := NewManager(store, newMockGroupStore(), nil, DefaultConfig(), slog.New(slog.NewTextHandler(io.Discard, nil)), nil)
	ctx := context.Background()

	// One target whose configuration cannot be read and one whose append fails,
	// so both counted paths are exercised by a single publish.
	for _, name := range []string{testCaptureQueue, "broken"} {
		if err := mgr.CreateQueue(ctx, types.DefaultQueueConfig(name, "m/#")); err != nil {
			t.Fatalf("CreateQueue %s failed: %v", name, err)
		}
	}

	flushCapture(t, mgr, func() {
		if err := mgr.PublishToMatchingQueues(ctx, types.PublishRequest{
			Topic:   testCapturedTopic,
			Payload: []byte("payload"),
		}); err != nil {
			t.Fatalf("capture must not fail the publish: %v", err)
		}
	})

	if got := mgr.GetMetrics().CaptureFailures; got != 2 {
		t.Fatalf("capture failures = %d, want 2; each lost queue counts", got)
	}
}

// flushCapture runs fn with the capture dispatcher live, then drains every job
// it queued. Stop drains synchronously and waits for its workers, so assertions
// afterwards see the finished state without polling or sleeping.
func flushCapture(t *testing.T, mgr *Manager, fn func()) {
	t.Helper()
	mgr.capture.Start(context.Background())
	fn()
	mgr.capture.Stop()
}

// Configuration load is not the only way a queue is created: the admin API goes
// through the manager, and production runs on the disk-backed store rather than
// the in-memory one whose CreateQueue happened to validate. A filter that can
// never match has to be refused here, or a queue is created bound to nothing and
// silently receives no traffic.
func TestCreateQueueRejectsFiltersThatCannotMatch(t *testing.T) {
	mgr := NewManager(memlog.New(), newMockGroupStore(), nil, DefaultConfig(), slog.New(slog.NewTextHandler(io.Discard, nil)), nil)
	ctx := context.Background()

	malformed := types.DefaultQueueConfig("black-holed", "#/events")
	err := mgr.CreateQueue(ctx, malformed)
	if err == nil {
		t.Fatal("CreateQueue accepted a filter that can never match")
	}
	if !errors.Is(err, types.ErrInvalidConfig) {
		t.Fatalf("CreateQueue error = %v, want it to wrap ErrInvalidConfig", err)
	}
	if _, getErr := mgr.GetQueue(ctx, "black-holed"); getErr == nil {
		t.Fatal("the refused queue was created anyway")
	}

	// An update must not be able to unbind a working queue either.
	if err := mgr.CreateQueue(ctx, types.DefaultQueueConfig("working", "m/#")); err != nil {
		t.Fatalf("CreateQueue failed: %v", err)
	}
	if err := mgr.UpdateQueue(ctx, types.DefaultQueueConfig("working", "m/#/events")); err == nil {
		t.Fatal("UpdateQueue accepted a filter that can never match")
	}
}
