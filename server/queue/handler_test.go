// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package queue

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/absmach/fluxmq/logstorage"
	"github.com/absmach/fluxmq/message"
	queuev1 "github.com/absmach/fluxmq/pkg/proto/queue/v1"
	queuepkg "github.com/absmach/fluxmq/queue"
	queueraft "github.com/absmach/fluxmq/queue/raft"
	qstorage "github.com/absmach/fluxmq/queue/storage"
	memlog "github.com/absmach/fluxmq/queue/storage/memory/log"
	"github.com/absmach/fluxmq/queue/types"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	testGroupHotPath  = "hot-path"
	testGroupJobsRaft = "jobs-raft"
	testConsumer1     = "consumer-1"
	testConsumer2     = "consumer-2"
	testGroupWorkers  = "workers"
	testQueueJobs     = "jobs"
	testQueueOrders   = "orders"
	testQueueEvents   = "events"
	testQueuePrimary  = "primary"
)

type readyQueueCoordinator struct {
	queueraft.QueueCoordinator
	store qstorage.QueueStore
}

func (c *readyQueueCoordinator) IsEnabled() bool { return true }

func (c *readyQueueCoordinator) IsQueueReplicated(string) bool { return true }

func (c *readyQueueCoordinator) IsLeaderForQueue(string) bool { return true }

func (c *readyQueueCoordinator) LeaderForQueue(string) string { return "127.0.0.1:7100" }

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
	header, ok := stored.PublisherMeta.Headers.Get("binary")
	if !stored.PublisherMeta.Key.Equal([]byte{0x00, 0xff}) || !ok || !header.Equal([]byte{0x00, 0xff}) {
		t.Fatalf("binary key/headers changed: key=%v headers=%v", stored.PublisherMeta.Key, stored.PublisherMeta.Headers)
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

func TestConnectAdapterUsesSharedQueueStateMachine(t *testing.T) {
	ctx := context.Background()
	store, err := logstorage.NewAdapter(t.TempDir(), logstorage.DefaultAdapterConfig())
	if err != nil {
		t.Fatalf("create logstorage adapter: %v", err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Errorf("close logstorage adapter: %v", err)
		}
	})
	manager := queuepkg.NewManager(store, store, nil, queuepkg.DefaultConfig(), nil, nil)
	h := NewHandler(manager, nil, nil, nil)
	if err := manager.CreateQueue(ctx, types.DefaultQueueConfig(testQueueJobs, "jobs/#")); err != nil {
		t.Fatalf("create queue: %v", err)
	}
	group := types.NewConsumerGroupState(testQueueJobs, testGroupWorkers, "")
	if err := store.CreateConsumerGroup(ctx, group); err != nil {
		t.Fatalf("create group: %v", err)
	}
	for _, id := range []string{testConsumer1, testConsumer2} {
		if err := store.RegisterConsumer(ctx, testQueueJobs, testGroupWorkers, &types.ConsumerInfo{ID: id, ClientID: id}); err != nil {
			t.Fatalf("register %s: %v", id, err)
		}
	}

	appended, err := h.AppendBatch(ctx, connect.NewRequest(&queuev1.AppendBatchRequest{
		QueueName: testQueueJobs,
		Messages: []*queuev1.BatchMessage{
			{Value: []byte("zero")},
			{Value: []byte("one")},
			{Value: []byte("two")},
		},
	}))
	if err != nil {
		t.Fatalf("append: %v", err)
	}
	if appended.Msg.FirstOffset != 0 || appended.Msg.LastOffset != 2 || appended.Msg.Count != 3 {
		t.Fatalf("append outcome = %+v", appended.Msg)
	}

	consumed, err := h.Consume(ctx, connect.NewRequest(&queuev1.ConsumeRequest{
		QueueName: testQueueJobs, GroupId: testGroupWorkers, ConsumerId: testConsumer1, MaxMessages: 3,
	}))
	if err != nil {
		t.Fatalf("consume: %v", err)
	}
	if len(consumed.Msg.Messages) != 3 {
		t.Fatalf("consumed %d messages, want 3", len(consumed.Msg.Messages))
	}

	acked, err := h.Ack(ctx, connect.NewRequest(&queuev1.AckRequest{
		QueueName: testQueueJobs, GroupId: testGroupWorkers, ConsumerId: testConsumer1, Offsets: []uint64{0},
	}))
	if err != nil {
		t.Fatalf("ack offset 0: %v", err)
	}
	if acked.Msg.AckedCount != 1 || acked.Msg.Committed.GetCommitted() != 1 {
		t.Fatalf("ack outcome = %+v", acked.Msg)
	}

	if _, err := h.Nack(ctx, connect.NewRequest(&queuev1.NackRequest{
		QueueName: testQueueJobs, GroupId: testGroupWorkers, ConsumerId: testConsumer1, Offsets: []uint64{1}, Delay: durationpb.New(time.Second),
	})); err != nil {
		t.Fatalf("delayed nack: %v", err)
	}
	claimed, err := h.Claim(ctx, connect.NewRequest(&queuev1.ClaimRequest{
		QueueName: testQueueJobs, GroupId: testGroupWorkers, ConsumerId: testConsumer2, MinIdleTime: durationpb.New(time.Hour), Limit: 1,
	}))
	if err != nil {
		t.Fatalf("claim delayed message: %v", err)
	}
	if len(claimed.Msg.Messages) != 0 {
		t.Fatalf("delayed nack was claimable immediately: %+v", claimed.Msg.Messages)
	}
	if _, err := h.Nack(ctx, connect.NewRequest(&queuev1.NackRequest{
		QueueName: testQueueJobs, GroupId: testGroupWorkers, ConsumerId: testConsumer1, Offsets: []uint64{1},
	})); err != nil {
		t.Fatalf("immediate nack: %v", err)
	}
	claimed, err = h.Claim(ctx, connect.NewRequest(&queuev1.ClaimRequest{
		QueueName: testQueueJobs, GroupId: testGroupWorkers, ConsumerId: testConsumer2, MinIdleTime: durationpb.New(time.Second), Limit: 1,
	}))
	if err != nil {
		t.Fatalf("claim immediate nack: %v", err)
	}
	if len(claimed.Msg.Messages) != 1 || claimed.Msg.Messages[0].Offset != 1 {
		t.Fatalf("claim outcome = %+v, want offset 1", claimed.Msg.Messages)
	}

	_, err = h.Ack(ctx, connect.NewRequest(&queuev1.AckRequest{
		QueueName: testQueueJobs, GroupId: testGroupWorkers, ConsumerId: testConsumer2, Offsets: []uint64{2},
	}))
	if got := connect.CodeOf(err); got != connect.CodeNotFound {
		t.Fatalf("wrong-owner ack code = %s, want not_found; error=%v", got, err)
	}
	for consumerID, offset := range map[string]uint64{testConsumer2: 1, testConsumer1: 2} {
		if _, err := h.Ack(ctx, connect.NewRequest(&queuev1.AckRequest{
			QueueName: testQueueJobs, GroupId: testGroupWorkers, ConsumerId: consumerID, Offsets: []uint64{offset},
		})); err != nil {
			t.Fatalf("ack offset %d as %s: %v", offset, consumerID, err)
		}
	}
	seek, err := h.SeekToOffset(ctx, connect.NewRequest(&queuev1.SeekToOffsetRequest{QueueName: testQueueJobs, Offset: 99}))
	if err != nil || seek.Msg.Offset != 3 {
		t.Fatalf("seek outcome = %+v, error = %v; want tail 3", seek, err)
	}
}

func TestUpdateQueueAppliesConfig(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := memlog.New()
	h := NewHandler(nil, store, nil, nil)

	cfg := types.DefaultQueueConfig(testQueueOrders, "$queue/orders/#")
	if err := store.CreateQueue(ctx, cfg); err != nil {
		t.Fatalf("create queue: %v", err)
	}

	retention := 2 * time.Hour
	updateResp, err := h.UpdateQueue(ctx, connect.NewRequest(&queuev1.UpdateQueueRequest{
		Name: testQueueOrders,
		Config: &queuev1.QueueConfig{
			Retention: &queuev1.RetentionConfig{
				MaxAge:      durationpb.New(retention),
				MaxBytes:    2048,
				MinMessages: 10,
			},
			MaxMessageSize: 4096,
		},
	}))
	if err != nil {
		t.Fatalf("update queue: %v", err)
	}

	updated, err := store.GetQueue(ctx, testQueueOrders)
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
	// Placement is not touched by an ordinary update; that it stayed local is
	// asserted by TestUpdateQueueRefusesReplicationPlacement.
	if updated.Replication.Enabled {
		t.Fatalf("an update without replication must leave the queue local")
	}

	if got := updateResp.Msg.Config.GetRetention().GetMaxAge().AsDuration(); got != retention {
		t.Fatalf("response retention mismatch: got %v want %v", got, retention)
	}
}

// Moving a queue into, out of, or between replication groups relocates its
// records and offsets. The previous shape assigned enabled and group
// unconditionally, so an update meaning to change one unrelated setting could
// disable replication and move the queue to the default group with it.
// Get returns the replication message for a replicated queue, so a client that
// reads a queue, changes retention and sends the config back must not be refused
// for restating placement it did not touch. Only an actual move is a migration.
func TestUpdateQueueAcceptsUnchangedReplicationPlacement(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	store := memlog.New()
	h := NewHandler(nil, store, noopGroupStore{}, nil)

	cfg := types.DefaultQueueConfig(testQueueOrders, "$queue/orders/#")
	cfg.Replication.Enabled = true
	cfg.Replication.Group = testGroupHotPath
	if err := store.CreateQueue(ctx, cfg); err != nil {
		t.Fatalf("create queue: %v", err)
	}

	readBack, err := h.GetQueue(ctx, connect.NewRequest(&queuev1.GetQueueRequest{Name: testQueueOrders}))
	if err != nil {
		t.Fatalf("get queue: %v", err)
	}

	// Exactly what a read-modify-write client sends: the config it was given,
	// with one unrelated field changed.
	config := readBack.Msg.Config
	config.Retention.MaxAge = durationpb.New(3 * time.Hour)

	if _, err := h.UpdateQueue(ctx, connect.NewRequest(&queuev1.UpdateQueueRequest{
		Name:   testQueueOrders,
		Config: config,
	})); err != nil {
		t.Fatalf("round-tripping a replicated queue must be accepted: %v", err)
	}

	updated, err := store.GetQueue(ctx, testQueueOrders)
	if err != nil {
		t.Fatalf("read updated queue: %v", err)
	}
	if updated.Retention.RetentionTime != 3*time.Hour {
		t.Fatalf("unexpected retention: %v", updated.Retention.RetentionTime)
	}
	if !updated.Replication.Enabled || updated.Replication.Group != testGroupHotPath {
		t.Fatalf("placement must be unchanged: %+v", updated.Replication)
	}
}

// Moving a queue into, out of, or between replication groups relocates its
// records and offsets, so it is refused. The request is well formed and
// conflicts with current state, which makes it a failed precondition.
func TestUpdateQueueRefusesReplicationPlacementChange(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		replicate bool
		group     string
		request   *queuev1.ReplicationConfig
	}{
		{
			name:    "local to replicated",
			request: &queuev1.ReplicationConfig{Group: testGroupHotPath},
		},
		{
			name:      "replicated to another group",
			replicate: true,
			group:     testGroupHotPath,
			request:   &queuev1.ReplicationConfig{Group: testGroupJobsRaft},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			store := memlog.New()
			h := NewHandler(nil, store, noopGroupStore{}, nil)

			cfg := types.DefaultQueueConfig(testQueueOrders, "$queue/orders/#")
			cfg.Replication.Enabled = tc.replicate
			cfg.Replication.Group = tc.group
			if err := store.CreateQueue(ctx, cfg); err != nil {
				t.Fatalf("create queue: %v", err)
			}

			_, err := h.UpdateQueue(ctx, connect.NewRequest(&queuev1.UpdateQueueRequest{
				Name:   testQueueOrders,
				Config: &queuev1.QueueConfig{Replication: tc.request},
			}))
			if err == nil {
				t.Fatal("expected a placement change to be refused")
			}
			if got := connect.CodeOf(err); got != connect.CodeFailedPrecondition {
				t.Fatalf("error code = %v, want FailedPrecondition", got)
			}

			stored, err := store.GetQueue(ctx, testQueueOrders)
			if err != nil {
				t.Fatalf("read queue: %v", err)
			}
			if stored.Replication.Enabled != tc.replicate || strings.TrimSpace(stored.Replication.Group) != tc.group {
				t.Fatalf("a refused update must not change placement: %+v", stored.Replication)
			}
		})
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
		Name:   testQueueJobs,
		Topics: []string{"$queue/jobs/#"},
		Config: &queuev1.QueueConfig{
			Replication: &queuev1.ReplicationConfig{Group: testGroupJobsRaft},
		},
	}))
	if err != nil {
		t.Fatalf("create queue: %v", err)
	}

	stored, err := store.GetQueue(ctx, testQueueJobs)
	if err != nil {
		t.Fatalf("get queue: %v", err)
	}

	// Presence of the message is what marks the queue replicated; the group is
	// the only thing it states.
	if !stored.Replication.Enabled {
		t.Fatalf("expected replication enabled")
	}
	if stored.Replication.Group != testGroupJobsRaft {
		t.Fatalf("unexpected replication group: %q", stored.Replication.Group)
	}

	if got := createResp.Msg.Config.GetReplication(); got == nil {
		t.Fatal("expected replication in create response")
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

	cfg := types.DefaultQueueConfig(testQueueOrders, "$queue/orders/#")
	if err := manager.CreateQueue(ctx, cfg); err != nil {
		t.Fatalf("create queue: %v", err)
	}

	group := types.NewConsumerGroupState("orders", testGroupWorkers, "")
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
	if err := groupStore.RegisterConsumer(ctx, "orders", testGroupWorkers, consumer); err != nil {
		t.Fatalf("register consumer: %v", err)
	}

	_, err := h.Heartbeat(ctx, connect.NewRequest(&queuev1.HeartbeatRequest{
		QueueName:  "orders",
		GroupId:    testGroupWorkers,
		ConsumerId: testConsumer1,
	}))
	if err != nil {
		t.Fatalf("heartbeat: %v", err)
	}

	updatedGroup, err := groupStore.GetConsumerGroup(ctx, "orders", testGroupWorkers)
	if err != nil {
		t.Fatalf("get group: %v", err)
	}
	updatedConsumer, registered := updatedGroup.GetConsumer(testConsumer1)
	if !registered {
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
		envelope := message.New(testQueueEvents, []byte{byte(i)})
		envelope.BrokerMeta.Queue.CreatedAt = ts
		if _, err := store.Append(ctx, testQueueEvents, envelope); err != nil {
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
func TestMessageToProtoDetachesPooledEnvelopeData(t *testing.T) {
	source := message.New("jobs", []byte("value"))
	source.PublisherMeta.Key = message.NewBinary([]byte("key"))
	source.PublisherMeta.Headers = message.NewHeaderMap(map[string][]byte{"binary": []byte("header")})

	converted := (&Handler{}).messageToProto(source)
	source.SetPayload([]byte("other"))
	source.PublisherMeta.Key = message.NewBinary([]byte("Xey"))
	source.PublisherMeta.Headers = source.PublisherMeta.Headers.With("binary", []byte("Xeader"))
	message.Release(source)

	if string(converted.Value) != "value" || string(converted.Key) != "key" || string(converted.Headers["binary"]) != "header" {
		t.Fatalf("protobuf response aliases pooled envelope data: %+v", converted)
	}
}

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
			if got := newConnectError(queuepkg.ErrorCodeInternal, tt.err).Code(); got != tt.want {
				t.Fatalf("newConnectError code = %v, want %v", got, tt.want)
			}
		})
	}
}

// max_bytes = 0 is documented as unlimited, so an update that selects it has to
// apply the zero. Skipping zeros made the documented way to say "unlimited" the
// one value that meant "leave the limit alone".
func TestUpdateQueueMaskAppliesZeroValues(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	store := memlog.New()
	h := NewHandler(nil, store, noopGroupStore{}, nil)

	cfg := types.DefaultQueueConfig(testQueueOrders, "$queue/orders/#")
	cfg.Retention.RetentionBytes = 4096
	cfg.Retention.RetentionMessages = 100
	if err := store.CreateQueue(ctx, cfg); err != nil {
		t.Fatalf("create queue: %v", err)
	}

	if _, err := h.UpdateQueue(ctx, connect.NewRequest(&queuev1.UpdateQueueRequest{
		Name:       testQueueOrders,
		Config:     &queuev1.QueueConfig{Retention: &queuev1.RetentionConfig{MaxBytes: 0}},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{updatePathRetentionMaxBytes}},
	})); err != nil {
		t.Fatalf("update queue: %v", err)
	}

	updated, err := store.GetQueue(ctx, testQueueOrders)
	if err != nil {
		t.Fatalf("read queue: %v", err)
	}
	if updated.Retention.RetentionBytes != 0 {
		t.Fatalf("max_bytes = 0 must clear the limit, got %d", updated.Retention.RetentionBytes)
	}
	if updated.Retention.RetentionMessages != 100 {
		t.Fatalf("a field outside the mask must be preserved, got %d", updated.Retention.RetentionMessages)
	}
}

// Omitting replication under a full replacement, and naming it in the mask with
// no message, are the same request: make this queue local. Both are a migration
// on a replicated queue.
func TestUpdateQueueRefusesImplicitAndExplicitLocalisation(t *testing.T) {
	t.Parallel()

	tests := map[string]*queuev1.UpdateQueueRequest{
		"omitted under full replacement": {
			Name:   testQueueOrders,
			Config: &queuev1.QueueConfig{MaxMessageSize: 4096},
		},
		"named in mask with no message": {
			Name:       testQueueOrders,
			Config:     &queuev1.QueueConfig{},
			UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"config.replication"}},
		},
	}

	for name, req := range tests {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			store := memlog.New()
			h := NewHandler(nil, store, noopGroupStore{}, nil)

			cfg := types.DefaultQueueConfig(testQueueOrders, "$queue/orders/#")
			cfg.Replication.Enabled = true
			cfg.Replication.Group = testGroupHotPath
			if err := store.CreateQueue(ctx, cfg); err != nil {
				t.Fatalf("create queue: %v", err)
			}

			_, err := h.UpdateQueue(ctx, connect.NewRequest(req))
			if err == nil {
				t.Fatal("expected localising a replicated queue to be refused")
			}
			if got := connect.CodeOf(err); got != connect.CodeFailedPrecondition {
				t.Fatalf("error code = %v, want FailedPrecondition", got)
			}

			stored, err := store.GetQueue(ctx, testQueueOrders)
			if err != nil {
				t.Fatalf("read queue: %v", err)
			}
			if !stored.Replication.Enabled {
				t.Fatal("a refused update must not localise the queue")
			}
		})
	}
}

// A mask naming a field the server does not know is a caller expecting a change
// that would silently not happen.
func TestUpdateQueueRejectsUnknownMaskPath(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	store := memlog.New()
	h := NewHandler(nil, store, noopGroupStore{}, nil)
	if err := store.CreateQueue(ctx, types.DefaultQueueConfig(testQueueOrders, "$queue/orders/#")); err != nil {
		t.Fatalf("create queue: %v", err)
	}

	_, err := h.UpdateQueue(ctx, connect.NewRequest(&queuev1.UpdateQueueRequest{
		Name:       testQueueOrders,
		Config:     &queuev1.QueueConfig{},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"config.retention.max_octets"}},
	}))
	if err == nil {
		t.Fatal("expected an unknown mask path to be rejected")
	}
	if got := connect.CodeOf(err); got != connect.CodeInvalidArgument {
		t.Fatalf("error code = %v, want InvalidArgument", got)
	}
}

// An update carrying no config cannot be a no-op: without a mask it asks for a
// full replacement, and with one it names paths whose values are not there.
// Accepting it reported success while changing nothing.
func TestUpdateQueueRequiresConfig(t *testing.T) {
	t.Parallel()

	tests := map[string]*fieldmaskpb.FieldMask{
		"no mask":            nil,
		"mask naming a path": {Paths: []string{updatePathRetentionMaxBytes}},
	}

	for name, mask := range tests {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			store := memlog.New()
			h := NewHandler(nil, store, noopGroupStore{}, nil)

			cfg := types.DefaultQueueConfig(testQueueOrders, "$queue/orders/#")
			cfg.Retention.RetentionBytes = 4096
			if err := store.CreateQueue(ctx, cfg); err != nil {
				t.Fatalf("create queue: %v", err)
			}

			_, err := h.UpdateQueue(ctx, connect.NewRequest(&queuev1.UpdateQueueRequest{
				Name:       testQueueOrders,
				UpdateMask: mask,
			}))
			if err == nil {
				t.Fatal("expected an update without a config to be rejected")
			}
			if got := connect.CodeOf(err); got != connect.CodeInvalidArgument {
				t.Fatalf("error code = %v, want InvalidArgument", got)
			}

			stored, err := store.GetQueue(ctx, testQueueOrders)
			if err != nil {
				t.Fatalf("read queue: %v", err)
			}
			if stored.Retention.RetentionBytes != 4096 {
				t.Fatalf("a rejected update must not change the queue, got %d", stored.Retention.RetentionBytes)
			}
		})
	}
}
