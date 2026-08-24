// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package queue

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	queuev1 "github.com/absmach/fluxmq/pkg/proto/queue/v1"
	queuepkg "github.com/absmach/fluxmq/queue"
	queueraft "github.com/absmach/fluxmq/queue/raft"
	qstorage "github.com/absmach/fluxmq/queue/storage"
	memlog "github.com/absmach/fluxmq/queue/storage/memory/log"
	"github.com/absmach/fluxmq/queue/types"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	testGroupHotPath  = "hot-path"
	testGroupJobsRaft = "jobs-raft"
	testConsumer1     = "consumer-1"
	testQueueEvents   = "events"
	testQueuePrimary  = "primary"
)

type readyQueueCoordinator struct {
	queueraft.QueueCoordinator
	store qstorage.QueueStore
}

func (c *readyQueueCoordinator) IsEnabled() bool                                      { return true }
func (c *readyQueueCoordinator) IsQueueReplicated(string) bool                        { return true }
func (c *readyQueueCoordinator) IsLeaderForQueue(string) bool                         { return true }
func (c *readyQueueCoordinator) LeaderForQueue(string) string                         { return "127.0.0.1:7100" }
func (c *readyQueueCoordinator) LeaderIDForQueue(string) string                       { return "node-1" }
func (c *readyQueueCoordinator) EnsureQueue(context.Context, types.QueueConfig) error { return nil }
func (c *readyQueueCoordinator) ApplyCreateQueue(ctx context.Context, cfg types.QueueConfig) error {
	return c.store.CreateQueue(ctx, cfg)
}

func TestListQueuesFilteringAndPagination(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := memlog.New()
	h := NewHandler(nil, store, nil, nil)

	for _, name := range []string{"alpha", "beta", "delta", "gamma"} {
		cfg := types.DefaultQueueConfig(name, "$queue/"+name+"/#")
		if err := store.CreateQueue(ctx, cfg); err != nil {
			t.Fatalf("create queue %q: %v", name, err)
		}
	}

	filteredResp, err := h.ListQueues(ctx, connect.NewRequest(&queuev1.ListQueuesRequest{
		Prefix: "g",
	}))
	if err != nil {
		t.Fatalf("list filtered queues: %v", err)
	}
	if len(filteredResp.Msg.Queues) != 1 || filteredResp.Msg.Queues[0].Name != "gamma" {
		t.Fatalf("unexpected filtered queues: %#v", filteredResp.Msg.Queues)
	}

	page1Resp, err := h.ListQueues(ctx, connect.NewRequest(&queuev1.ListQueuesRequest{
		Limit: 2,
	}))
	if err != nil {
		t.Fatalf("list queues page 1: %v", err)
	}
	if got := len(page1Resp.Msg.Queues); got != 2 {
		t.Fatalf("unexpected page 1 size: %d", got)
	}
	page1Names := []string{page1Resp.Msg.Queues[0].Name, page1Resp.Msg.Queues[1].Name}
	if !sort.StringsAreSorted(page1Names) {
		t.Fatalf("page 1 not sorted: %#v", page1Names)
	}
	if page1Resp.Msg.NextPageToken == "" {
		t.Fatalf("expected next_page_token on first page")
	}

	page2Resp, err := h.ListQueues(ctx, connect.NewRequest(&queuev1.ListQueuesRequest{
		Limit:     2,
		PageToken: page1Resp.Msg.NextPageToken,
	}))
	if err != nil {
		t.Fatalf("list queues page 2: %v", err)
	}
	if got := len(page2Resp.Msg.Queues); got != 2 {
		t.Fatalf("unexpected page 2 size: %d", got)
	}
	if page2Resp.Msg.NextPageToken != "" {
		t.Fatalf("expected empty next_page_token on last page, got %q", page2Resp.Msg.NextPageToken)
	}
	page2Names := []string{page2Resp.Msg.Queues[0].Name, page2Resp.Msg.Queues[1].Name}
	if !sort.StringsAreSorted(page2Names) {
		t.Fatalf("page 2 not sorted: %#v", page2Names)
	}
}

func TestAppendContractUsesExactOffsetsAndPreservesBytes(t *testing.T) {
	ctx := context.Background()
	store := memlog.New()
	groupStore := noopGroupStore{}
	manager := queuepkg.NewManager(store, groupStore, nil, queuepkg.DefaultConfig(), nil, nil)

	for _, name := range []string{testQueuePrimary, "same-pattern"} {
		cfg := types.DefaultQueueConfig(name, "shared/#")
		if err := manager.CreateQueue(ctx, cfg); err != nil {
			t.Fatalf("create queue %q: %v", name, err)
		}
	}
	h := NewHandler(manager, store, groupStore, nil)

	appendResp, err := h.Append(ctx, connect.NewRequest(&queuev1.AppendRequest{
		QueueName: testQueuePrimary,
		Key:       []byte{0x00, 0xff},
		Value:     []byte("one"),
		Headers:   map[string][]byte{"binary": {0x00, 0xff}},
	}))
	if err != nil {
		t.Fatalf("append: %v", err)
	}
	if appendResp.Msg.Offset != 0 {
		t.Fatalf("append offset = %d, want 0", appendResp.Msg.Offset)
	}

	stored, err := store.Read(ctx, testQueuePrimary, appendResp.Msg.Offset)
	if err != nil {
		t.Fatalf("read appended message: %v", err)
	}
	if !bytes.Equal(stored.Key, []byte{0x00, 0xff}) || !bytes.Equal(stored.Headers["binary"], []byte{0x00, 0xff}) {
		t.Fatalf("binary key/headers changed: key=%v headers=%v", stored.Key, stored.Headers)
	}
	if count, err := store.Count(ctx, "same-pattern"); err != nil || count != 0 {
		t.Fatalf("exact append routed to same-pattern queue: count=%d err=%v", count, err)
	}

	batchResp, err := h.AppendBatch(ctx, connect.NewRequest(&queuev1.AppendBatchRequest{
		QueueName: testQueuePrimary,
		Messages: []*queuev1.BatchMessage{
			{Key: []byte("k2"), Value: []byte("two")},
			{Key: []byte("k3"), Value: []byte("three")},
		},
	}))
	if err != nil {
		t.Fatalf("append batch: %v", err)
	}
	if batchResp.Msg.FirstOffset != 1 || batchResp.Msg.LastOffset != 2 || batchResp.Msg.Count != 2 {
		t.Fatalf("batch range = [%d,%d] count=%d, want [1,2] count=2",
			batchResp.Msg.FirstOffset, batchResp.Msg.LastOffset, batchResp.Msg.Count)
	}
}

func TestUpdateQueueAppliesConfig(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := memlog.New()
	h := NewHandler(nil, store, nil, nil)

	cfg := types.DefaultQueueConfig("orders", "$queue/orders/#")
	if err := store.CreateQueue(ctx, cfg); err != nil {
		t.Fatalf("create queue: %v", err)
	}

	retention := 2 * time.Hour
	updateResp, err := h.UpdateQueue(ctx, connect.NewRequest(&queuev1.UpdateQueueRequest{
		Name: "orders",
		Config: &queuev1.QueueConfig{
			Retention: &queuev1.RetentionConfig{
				MaxAge:      durationpb.New(retention),
				MaxBytes:    2048,
				MinMessages: 10,
			},
			MaxMessageSize: 4096,
			Replication: &queuev1.ReplicationConfig{
				Enabled:           true,
				ReplicationFactor: 3,
				Mode:              queuev1.ReplicationMode_REPLICATION_MODE_ASYNC,
				MinInSyncReplicas: 2,
				AckTimeout:        durationpb.New(2 * time.Second),
				Group:             testGroupHotPath,
			},
		},
	}))
	if err != nil {
		t.Fatalf("update queue: %v", err)
	}

	updated, err := store.GetQueue(ctx, "orders")
	if err != nil {
		t.Fatalf("read updated queue: %v", err)
	}

	if updated.MessageTTL != retention {
		t.Fatalf("unexpected message ttl: got %v want %v", updated.MessageTTL, retention)
	}
	if updated.Retention.RetentionTime != retention {
		t.Fatalf("unexpected retention time: got %v want %v", updated.Retention.RetentionTime, retention)
	}
	if updated.Retention.RetentionBytes != 2048 {
		t.Fatalf("unexpected retention bytes: got %d", updated.Retention.RetentionBytes)
	}
	if updated.Retention.RetentionMessages != 10 {
		t.Fatalf("unexpected retention messages: got %d", updated.Retention.RetentionMessages)
	}
	if updated.MaxMessageSize != 4096 {
		t.Fatalf("unexpected max message size: got %d", updated.MaxMessageSize)
	}
	if !updated.Replication.Enabled {
		t.Fatalf("expected replication enabled")
	}
	if updated.Replication.ReplicationFactor != 3 {
		t.Fatalf("unexpected replication factor: got %d", updated.Replication.ReplicationFactor)
	}
	if updated.Replication.Mode != types.ReplicationAsync {
		t.Fatalf("unexpected replication mode: got %s", updated.Replication.Mode)
	}
	if updated.Replication.MinInSyncReplicas != 2 {
		t.Fatalf("unexpected min ISR: got %d", updated.Replication.MinInSyncReplicas)
	}
	if updated.Replication.AckTimeout != 2*time.Second {
		t.Fatalf("unexpected ack timeout: got %s", updated.Replication.AckTimeout)
	}
	if updated.Replication.Group != testGroupHotPath {
		t.Fatalf("unexpected replication group: got %q", updated.Replication.Group)
	}

	if got := updateResp.Msg.Config.GetRetention().GetMaxAge().AsDuration(); got != retention {
		t.Fatalf("response retention mismatch: got %v want %v", got, retention)
	}
	if got := updateResp.Msg.Config.GetReplication(); got == nil || !got.Enabled {
		t.Fatalf("expected replication in response")
	}
	if got := updateResp.Msg.Config.GetReplication().GetMode(); got != queuev1.ReplicationMode_REPLICATION_MODE_ASYNC {
		t.Fatalf("response replication mode mismatch: got %v", got)
	}
	if got := updateResp.Msg.Config.GetReplication().GetGroup(); got != testGroupHotPath {
		t.Fatalf("response replication group mismatch: got %q", got)
	}
}

func TestProtectedQueueAdminUpdateAndDeleteFailPrecondition(t *testing.T) {
	ctx := context.Background()
	store := memlog.New()
	contract := types.DefaultQueueConfig("audit.events", "$queue/audit.events/#")
	contract.Type = types.QueueTypeStream
	contract.Reserved = true
	contract.Retention.RetentionTime = 30 * 24 * time.Hour
	contract.Retention.RetentionBytes = 10 * 1024 * 1024 * 1024
	contract.MessageTTL = 30 * 24 * time.Hour
	managerConfig := queuepkg.DefaultConfig()
	managerConfig.ProtectedQueueContracts = []types.QueueConfig{contract}
	manager := queuepkg.NewManager(store, noopGroupStore{}, nil, managerConfig, nil, nil)
	if err := manager.CreateQueue(ctx, contract); err != nil {
		t.Fatalf("create protected queue: %v", err)
	}
	h := NewHandler(manager, nil, nil, nil)

	_, err := h.UpdateQueue(ctx, connect.NewRequest(&queuev1.UpdateQueueRequest{
		Name: contract.Name,
		Config: &queuev1.QueueConfig{
			MaxMessageSize: uint32(contract.MaxMessageSize - 1),
		},
	}))
	if got := connect.CodeOf(err); got != connect.CodeFailedPrecondition {
		t.Fatalf("UpdateQueue() code = %s, error = %v, want failed_precondition", got, err)
	}
	persisted, getErr := store.GetQueue(ctx, contract.Name)
	if getErr != nil {
		t.Fatalf("GetQueue() error = %v", getErr)
	}
	if persisted.MaxMessageSize != contract.MaxMessageSize {
		t.Fatalf("rejected update changed max message size to %d", persisted.MaxMessageSize)
	}

	_, err = h.DeleteQueue(ctx, connect.NewRequest(&queuev1.DeleteQueueRequest{Name: contract.Name}))
	if got := connect.CodeOf(err); got != connect.CodeFailedPrecondition {
		t.Fatalf("DeleteQueue() code = %s, error = %v, want failed_precondition", got, err)
	}
	if _, getErr := store.GetQueue(ctx, contract.Name); getErr != nil {
		t.Fatalf("protected queue was deleted: %v", getErr)
	}
}

func TestCreateQueueAppliesReplicationConfig(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := memlog.New()
	groupStore := noopGroupStore{}
	managerConfig := queuepkg.DefaultConfig()
	managerConfig.WritePolicy = queuepkg.WritePolicyReject
	manager := queuepkg.NewManager(store, groupStore, nil, managerConfig, nil, nil)
	manager.SetRaftCoordinator(&readyQueueCoordinator{store: store})
	h := NewHandler(manager, store, groupStore, nil)

	createResp, err := h.CreateQueue(ctx, connect.NewRequest(&queuev1.CreateQueueRequest{
		Name:   "jobs",
		Topics: []string{"$queue/jobs/#"},
		Config: &queuev1.QueueConfig{
			Replication: &queuev1.ReplicationConfig{
				Enabled:           true,
				ReplicationFactor: 5,
				Mode:              queuev1.ReplicationMode_REPLICATION_MODE_SYNC,
				MinInSyncReplicas: 3,
				AckTimeout:        durationpb.New(4 * time.Second),
				Group:             testGroupJobsRaft,
			},
		},
	}))
	if err != nil {
		t.Fatalf("create queue: %v", err)
	}

	stored, err := store.GetQueue(ctx, "jobs")
	if err != nil {
		t.Fatalf("get queue: %v", err)
	}

	if !stored.Replication.Enabled {
		t.Fatalf("expected replication enabled")
	}
	if stored.Replication.ReplicationFactor != 5 {
		t.Fatalf("unexpected replication factor: %d", stored.Replication.ReplicationFactor)
	}
	if stored.Replication.Mode != types.ReplicationSync {
		t.Fatalf("unexpected replication mode: %s", stored.Replication.Mode)
	}
	if stored.Replication.MinInSyncReplicas != 3 {
		t.Fatalf("unexpected min ISR: %d", stored.Replication.MinInSyncReplicas)
	}
	if stored.Replication.AckTimeout != 4*time.Second {
		t.Fatalf("unexpected ack timeout: %s", stored.Replication.AckTimeout)
	}
	if stored.Replication.Group != testGroupJobsRaft {
		t.Fatalf("unexpected replication group: %q", stored.Replication.Group)
	}

	if got := createResp.Msg.Config.GetReplication(); got == nil || !got.Enabled {
		t.Fatalf("expected replication in create response")
	}
	if got := createResp.Msg.Config.GetReplication().GetReplicationFactor(); got != 5 {
		t.Fatalf("unexpected response replication factor: %d", got)
	}
	if got := createResp.Msg.Config.GetReplication().GetGroup(); got != testGroupJobsRaft {
		t.Fatalf("unexpected response replication group: %q", got)
	}
}

func TestHeartbeatUsesManagerPath(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	queueStore := memlog.New()
	groupStore := newStatefulGroupStore()
	manager := queuepkg.NewManager(queueStore, groupStore, nil, queuepkg.DefaultConfig(), nil, nil)
	h := NewHandler(manager, nil, nil, nil)

	cfg := types.DefaultQueueConfig("orders", "$queue/orders/#")
	if err := manager.CreateQueue(ctx, cfg); err != nil {
		t.Fatalf("create queue: %v", err)
	}

	group := types.NewConsumerGroupState("orders", "workers", "")
	if err := groupStore.CreateConsumerGroup(ctx, group); err != nil {
		t.Fatalf("create group: %v", err)
	}

	before := time.Now().Add(-time.Hour)
	consumer := &types.ConsumerInfo{
		ID:            testConsumer1,
		ClientID:      testConsumer1,
		RegisteredAt:  before,
		LastHeartbeat: before,
	}
	if err := groupStore.RegisterConsumer(ctx, "orders", "workers", consumer); err != nil {
		t.Fatalf("register consumer: %v", err)
	}

	_, err := h.Heartbeat(ctx, connect.NewRequest(&queuev1.HeartbeatRequest{
		QueueName:  "orders",
		GroupId:    "workers",
		ConsumerId: testConsumer1,
	}))
	if err != nil {
		t.Fatalf("heartbeat: %v", err)
	}

	updatedGroup, err := groupStore.GetConsumerGroup(ctx, "orders", "workers")
	if err != nil {
		t.Fatalf("get group: %v", err)
	}
	updatedConsumer := updatedGroup.GetConsumer(testConsumer1)
	if updatedConsumer == nil {
		t.Fatalf("expected consumer to exist")
	}
	if !updatedConsumer.LastHeartbeat.After(before) {
		t.Fatalf("expected heartbeat to advance, got %v <= %v", updatedConsumer.LastHeartbeat, before)
	}
}

type statefulGroupStore struct {
	noopGroupStore

	mu     sync.RWMutex
	groups map[string]map[string]*types.ConsumerGroup
}

func newStatefulGroupStore() *statefulGroupStore {
	return &statefulGroupStore{
		groups: make(map[string]map[string]*types.ConsumerGroup),
	}
}

func (s *statefulGroupStore) CreateConsumerGroup(_ context.Context, group *types.ConsumerGroup) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.groups[group.QueueName] == nil {
		s.groups[group.QueueName] = make(map[string]*types.ConsumerGroup)
	}
	if _, exists := s.groups[group.QueueName][group.ID]; exists {
		return qstorage.ErrConsumerGroupExists
	}
	s.groups[group.QueueName][group.ID] = group
	return nil
}

func (s *statefulGroupStore) GetConsumerGroup(_ context.Context, queueName, groupID string) (*types.ConsumerGroup, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	groups, ok := s.groups[queueName]
	if !ok {
		return nil, qstorage.ErrConsumerNotFound
	}
	group, ok := groups[groupID]
	if !ok {
		return nil, qstorage.ErrConsumerNotFound
	}
	return group, nil
}

func (s *statefulGroupStore) UpdateConsumerGroup(_ context.Context, group *types.ConsumerGroup) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.groups[group.QueueName] == nil {
		s.groups[group.QueueName] = make(map[string]*types.ConsumerGroup)
	}
	s.groups[group.QueueName][group.ID] = group
	return nil
}

func (s *statefulGroupStore) RegisterConsumer(_ context.Context, queueName, groupID string, consumer *types.ConsumerInfo) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	groups, ok := s.groups[queueName]
	if !ok {
		return qstorage.ErrConsumerNotFound
	}
	group, ok := groups[groupID]
	if !ok {
		return qstorage.ErrConsumerNotFound
	}

	group.SetConsumer(consumer.ID, consumer)
	return nil
}

type noopGroupStore struct{}

func (noopGroupStore) CreateConsumerGroup(context.Context, *types.ConsumerGroup) error {
	return nil
}

func (noopGroupStore) GetConsumerGroup(context.Context, string, string) (*types.ConsumerGroup, error) {
	return nil, qstorage.ErrConsumerNotFound
}

func (noopGroupStore) UpdateConsumerGroup(context.Context, *types.ConsumerGroup) error {
	return nil
}

func (noopGroupStore) DeleteConsumerGroup(context.Context, string, string) error {
	return nil
}

func (noopGroupStore) ListConsumerGroups(context.Context, string) ([]*types.ConsumerGroup, error) {
	return nil, nil
}

func (noopGroupStore) AddPendingEntry(context.Context, string, string, *types.PendingEntry) error {
	return nil
}

func (noopGroupStore) RemovePendingEntry(context.Context, string, string, string, uint64) error {
	return nil
}

func (noopGroupStore) GetPendingEntries(context.Context, string, string, string) ([]*types.PendingEntry, error) {
	return nil, nil
}

func (noopGroupStore) GetAllPendingEntries(context.Context, string, string) ([]*types.PendingEntry, error) {
	return nil, nil
}

func (noopGroupStore) TransferPendingEntry(context.Context, string, string, uint64, string, string) error {
	return nil
}

func (noopGroupStore) UpdateCursor(context.Context, string, string, uint64) error {
	return nil
}

func (noopGroupStore) UpdateCommitted(context.Context, string, string, uint64) error {
	return nil
}

func (noopGroupStore) RegisterConsumer(context.Context, string, string, *types.ConsumerInfo) error {
	return nil
}

func (noopGroupStore) UnregisterConsumer(context.Context, string, string, string) error {
	return nil
}

func (noopGroupStore) ListConsumers(context.Context, string, string) ([]*types.ConsumerInfo, error) {
	return nil, nil
}

func TestSeekToTimestamp(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := memlog.New()
	h := NewHandler(nil, store, nil, nil)

	cfg := types.DefaultQueueConfig(testQueueEvents, "$queue/events/#")
	if err := store.CreateQueue(ctx, cfg); err != nil {
		t.Fatalf("create queue: %v", err)
	}

	base := time.Unix(1700000000, 0).UTC()
	points := []time.Time{
		base,
		base.Add(1 * time.Minute),
		base.Add(2 * time.Minute),
	}
	for i, ts := range points {
		if _, err := store.Append(ctx, testQueueEvents, &types.Message{
			ID:        "m",
			Topic:     testQueueEvents,
			Payload:   []byte{byte(i)},
			CreatedAt: ts,
		}); err != nil {
			t.Fatalf("append message %d: %v", i, err)
		}
	}

	exactResp, err := h.SeekToTimestamp(ctx, connect.NewRequest(&queuev1.SeekToTimestampRequest{
		QueueName: testQueueEvents,
		Timestamp: timestamppb.New(points[1]),
	}))
	if err != nil {
		t.Fatalf("seek exact: %v", err)
	}
	if exactResp.Msg.Offset != 1 || !exactResp.Msg.ExactMatch {
		t.Fatalf("unexpected exact seek response: %+v", exactResp.Msg)
	}

	between := points[1].Add(30 * time.Second)
	betweenResp, err := h.SeekToTimestamp(ctx, connect.NewRequest(&queuev1.SeekToTimestampRequest{
		QueueName: testQueueEvents,
		Timestamp: timestamppb.New(between),
	}))
	if err != nil {
		t.Fatalf("seek in-between: %v", err)
	}
	if betweenResp.Msg.Offset != 2 || betweenResp.Msg.ExactMatch {
		t.Fatalf("unexpected in-between seek response: %+v", betweenResp.Msg)
	}

	afterLast := points[2].Add(1 * time.Second)
	afterResp, err := h.SeekToTimestamp(ctx, connect.NewRequest(&queuev1.SeekToTimestampRequest{
		QueueName: testQueueEvents,
		Timestamp: timestamppb.New(afterLast),
	}))
	if err != nil {
		t.Fatalf("seek after last: %v", err)
	}
	if afterResp.Msg.Offset != 3 || afterResp.Msg.ExactMatch {
		t.Fatalf("unexpected tail seek response: %+v", afterResp.Msg)
	}
	if got := afterResp.Msg.Timestamp.AsTime(); !got.Equal(afterLast) {
		t.Fatalf("unexpected tail seek timestamp: got %v want %v", got, afterLast)
	}

	_, err = h.SeekToTimestamp(ctx, connect.NewRequest(&queuev1.SeekToTimestampRequest{
		QueueName: testQueueEvents,
	}))
	if got := connect.CodeOf(err); got != connect.CodeInvalidArgument {
		t.Fatalf("unexpected code for missing timestamp: got %s want %s", got, connect.CodeInvalidArgument)
	}
}

// A filter the broker will not accept is the caller's mistake. It has to reach
// the client as InvalidArgument: AlreadyExists says the name is taken and
// Internal says the server broke, and a client acting on either would retry or
// rename instead of fixing the filter it sent.
func TestQueueMutationsReportInvalidFiltersAsInvalidArgument(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := memlog.New()
	groupStore := noopGroupStore{}
	manager := queuepkg.NewManager(store, groupStore, nil, queuepkg.DefaultConfig(), nil, nil)
	h := NewHandler(manager, store, groupStore, nil)

	t.Run("create", func(t *testing.T) {
		_, err := h.CreateQueue(ctx, connect.NewRequest(&queuev1.CreateQueueRequest{
			Name:   "black-holed",
			Topics: []string{"#/events"},
		}))
		if err == nil {
			t.Fatal("CreateQueue accepted a filter that can never match")
		}
		if got := connect.CodeOf(err); got != connect.CodeInvalidArgument {
			t.Fatalf("CreateQueue code = %v, want %v", got, connect.CodeInvalidArgument)
		}
	})

	t.Run("create with a name already taken still reports AlreadyExists", func(t *testing.T) {
		req := &queuev1.CreateQueueRequest{Name: "taken", Topics: []string{"$queue/taken/#"}}
		if _, err := h.CreateQueue(ctx, connect.NewRequest(req)); err != nil {
			t.Fatalf("CreateQueue failed: %v", err)
		}
		_, err := h.CreateQueue(ctx, connect.NewRequest(req))
		if err == nil {
			t.Fatal("CreateQueue accepted a duplicate name")
		}
		if got := connect.CodeOf(err); got != connect.CodeAlreadyExists {
			t.Fatalf("CreateQueue code = %v, want %v", got, connect.CodeAlreadyExists)
		}
	})
}

// Every QueueService method shares one domain mapping. Method implementations
// may supply a fallback only for errors the queue domain has not classified.
func TestQueueMutationErrorCodes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		err  error
		want connect.Code
	}{
		{
			name: "invalid configuration is the caller's mistake",
			err:  fmt.Errorf("wrapped: %w", types.ErrInvalidConfig),
			want: connect.CodeInvalidArgument,
		},
		{
			name: "protected queue mutation is a precondition",
			err:  queuepkg.ErrProtectedQueueMutation,
			want: connect.CodeFailedPrecondition,
		},
		{
			name: "name already taken",
			err:  qstorage.ErrQueueAlreadyExists,
			want: connect.CodeAlreadyExists,
		},
		{
			name: "queue missing",
			err:  qstorage.ErrQueueNotFound,
			want: connect.CodeNotFound,
		},
		{
			name: "anything else is a server fault",
			err:  errors.New("disk on fire"),
			want: connect.CodeInternal,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := newConnectError(connect.CodeInternal, tt.err).Code(); got != tt.want {
				t.Fatalf("newConnectError code = %v, want %v", got, tt.want)
			}
		})
	}
}
