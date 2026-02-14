// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package queue

import (
	"context"
	"sort"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	queuev1 "github.com/absmach/fluxmq/pkg/proto/queue/v1"
	queuepkg "github.com/absmach/fluxmq/queue"
	qstorage "github.com/absmach/fluxmq/queue/storage"
	memlog "github.com/absmach/fluxmq/queue/storage/memory/log"
	"github.com/absmach/fluxmq/queue/types"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

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
				Group:             "hot-path",
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
	if updated.Replication.Group != "hot-path" {
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
	if got := updateResp.Msg.Config.GetReplication().GetGroup(); got != "hot-path" {
		t.Fatalf("response replication group mismatch: got %q", got)
	}
}

func TestCreateQueueAppliesReplicationConfig(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := memlog.New()
	groupStore := noopGroupStore{}
	manager := queuepkg.NewManager(store, groupStore, nil, queuepkg.DefaultConfig(), nil, nil)
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
				Group:             "jobs-raft",
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
	if stored.Replication.Group != "jobs-raft" {
		t.Fatalf("unexpected replication group: %q", stored.Replication.Group)
	}

	if got := createResp.Msg.Config.GetReplication(); got == nil || !got.Enabled {
		t.Fatalf("expected replication in create response")
	}
	if got := createResp.Msg.Config.GetReplication().GetReplicationFactor(); got != 5 {
		t.Fatalf("unexpected response replication factor: %d", got)
	}
	if got := createResp.Msg.Config.GetReplication().GetGroup(); got != "jobs-raft" {
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
		ID:            "consumer-1",
		ClientID:      "consumer-1",
		RegisteredAt:  before,
		LastHeartbeat: before,
	}
	if err := groupStore.RegisterConsumer(ctx, "orders", "workers", consumer); err != nil {
		t.Fatalf("register consumer: %v", err)
	}

	_, err := h.Heartbeat(ctx, connect.NewRequest(&queuev1.HeartbeatRequest{
		QueueName:  "orders",
		GroupId:    "workers",
		ConsumerId: "consumer-1",
	}))
	if err != nil {
		t.Fatalf("heartbeat: %v", err)
	}

	updatedGroup, err := groupStore.GetConsumerGroup(ctx, "orders", "workers")
	if err != nil {
		t.Fatalf("get group: %v", err)
	}
	updatedConsumer := updatedGroup.GetConsumer("consumer-1")
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

	cfg := types.DefaultQueueConfig("events", "$queue/events/#")
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
		if _, err := store.Append(ctx, "events", &types.Message{
			ID:        "m",
			Topic:     "events",
			Payload:   []byte{byte(i)},
			CreatedAt: ts,
		}); err != nil {
			t.Fatalf("append message %d: %v", i, err)
		}
	}

	exactResp, err := h.SeekToTimestamp(ctx, connect.NewRequest(&queuev1.SeekToTimestampRequest{
		QueueName: "events",
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
		QueueName: "events",
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
		QueueName: "events",
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
		QueueName: "events",
	}))
	if got := connect.CodeOf(err); got != connect.CodeInvalidArgument {
		t.Fatalf("unexpected code for missing timestamp: got %s want %s", got, connect.CodeInvalidArgument)
	}
}
