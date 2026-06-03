// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package queue

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	corebroker "github.com/absmach/fluxmq/broker"
	"github.com/absmach/fluxmq/cluster"
	"github.com/absmach/fluxmq/queue/consumer"
	memlog "github.com/absmach/fluxmq/queue/storage/memory/log"
	"github.com/absmach/fluxmq/queue/types"
	brokerstorage "github.com/absmach/fluxmq/storage"
)

func newTestEngine(t *testing.T, local Deliverer, remote RemoteRouter) (*DeliveryEngine, *memlog.Store, *mockGroupStore) {
	t.Helper()

	logStore := memlog.New()
	groupStore := newMockGroupStore()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	consumerCfg := consumer.Config{
		ClaimBatchSize:     10,
		MaxDeliveryCount:   5,
		MaxPELSize:         100_000,
		AutoCommitInterval: DefaultConfig().AutoCommitInterval,
		VisibilityTimeout:  DefaultConfig().VisibilityTimeout,
	}
	consumerMgr := consumer.NewManager(logStore, groupStore, consumerCfg)

	nodeID := ""
	if remote != nil {
		nodeID = "node-1" //nolint:goconst // test value
	}

	engine := NewDeliveryEngine(
		logStore, groupStore, consumerMgr,
		local, remote, nodeID,
		DistributionForward, 100, logger,
	)

	return engine, logStore, groupStore
}

type checkingDeliverer struct {
	mu         sync.Mutex
	targets    map[string]bool
	connected  map[string]bool
	delivered  []*brokerstorage.Message
	deliverErr error
}

func (d *checkingDeliverer) Deliver(ctx context.Context, clientID string, msg *brokerstorage.Message) error {
	if d.deliverErr != nil {
		return d.deliverErr
	}

	d.mu.Lock()
	defer d.mu.Unlock()
	d.delivered = append(d.delivered, msg)
	return nil
}

func (d *checkingDeliverer) HasDeliveryTarget(clientID string) bool {
	if d.targets != nil {
		return d.targets[clientID]
	}
	return d.IsClientConnected(clientID)
}

func (d *checkingDeliverer) IsClientConnected(clientID string) bool {
	if d.connected == nil {
		return true
	}
	return d.connected[clientID]
}

func (d *checkingDeliverer) deliveredMessages() []*brokerstorage.Message {
	d.mu.Lock()
	defer d.mu.Unlock()
	result := make([]*brokerstorage.Message, len(d.delivered))
	copy(result, d.delivered)
	return result
}

func TestScheduleDedup(t *testing.T) {
	engine, _, _ := newTestEngine(t, nil, nil)

	engine.Schedule("q1")
	engine.Schedule("q1") // duplicate — should be coalesced

	// Only one item should be in the channel.
	select {
	case name := <-engine.queue:
		if name != "q1" {
			t.Fatalf("expected q1, got %s", name)
		}
	default:
		t.Fatal("expected one item in the queue channel")
	}

	// Channel should now be empty.
	select {
	case name := <-engine.queue:
		t.Fatalf("expected empty channel, got %s", name)
	default:
		// ok
	}
}

func TestScheduleEmptyQueueName(t *testing.T) {
	engine, _, _ := newTestEngine(t, nil, nil)

	engine.Schedule("")

	select {
	case <-engine.queue:
		t.Fatal("expected empty string to be ignored")
	default:
		// ok
	}
}

func TestScheduleAllQueues(t *testing.T) {
	engine, logStore, _ := newTestEngine(t, nil, nil)
	ctx := context.Background()

	for _, name := range []string{"a", "b", "c"} {
		logStore.CreateQueue(ctx, types.DefaultQueueConfig(name, "$queue/"+name+"/#")) //nolint:errcheck // test setup
	}

	engine.ScheduleAll(ctx)

	seen := make(map[string]bool)
	for i := 0; i < 3; i++ {
		select {
		case name := <-engine.queue:
			seen[name] = true
		default:
			t.Fatalf("expected 3 items, got %d", i)
		}
	}

	for _, name := range []string{"a", "b", "c"} {
		if !seen[name] {
			t.Fatalf("expected queue %s to be scheduled", name)
		}
	}
}

func TestUnschedule(t *testing.T) {
	engine, _, _ := newTestEngine(t, nil, nil)

	// Manually add to enqueued set to simulate a pending schedule.
	engine.mu.Lock()
	engine.enqueued["q1"] = struct{}{}
	engine.mu.Unlock()

	engine.Unschedule("q1")

	engine.mu.Lock()
	_, exists := engine.enqueued["q1"]
	engine.mu.Unlock()

	if exists {
		t.Fatal("expected q1 to be removed from enqueued set")
	}
}

func TestDeliverQueueLocalConsumer(t *testing.T) {
	var mu sync.Mutex
	var delivered []*brokerstorage.Message

	local := DeliveryTargetFunc(func(ctx context.Context, clientID string, msg *brokerstorage.Message) error {
		mu.Lock()
		delivered = append(delivered, msg)
		mu.Unlock()
		return nil
	})

	engine, logStore, groupStore := newTestEngine(t, local, nil)
	ctx := context.Background()

	queueCfg := types.DefaultQueueConfig("tasks", "$queue/tasks/#")
	logStore.CreateQueue(ctx, queueCfg) //nolint:errcheck // test setup

	group := types.NewConsumerGroupState("tasks", testGroupWorkers, "")
	group.SetConsumer("c1", &types.ConsumerInfo{
		ID:       "c1",
		ClientID: "c1",
	})
	groupStore.CreateConsumerGroup(ctx, group) //nolint:errcheck // test setup

	logStore.Append(ctx, "tasks", &types.Message{ //nolint:errcheck // test setup
		ID:      "1",
		Topic:   testQueueTasksNew,
		Payload: []byte("job1"),
	})

	ok := engine.DeliverQueue(ctx, "tasks")
	if !ok {
		t.Fatal("expected DeliverQueue to return true")
	}

	mu.Lock()
	count := len(delivered)
	mu.Unlock()

	if count != 1 {
		t.Fatalf("expected 1 delivered message, got %d", count)
	}

	if delivered[0].Properties[types.PropQueueName] != "tasks" { //nolint:goconst // test value
		t.Fatalf("expected queue name tasks, got %s", delivered[0].Properties[types.PropQueueName])
	}
}

func TestDeliverQueueRemoteConsumer(t *testing.T) {
	mockRemote := &mockRemoteRouter{}

	engine, logStore, _ := newTestEngine(t, nil, mockRemote)
	ctx := context.Background()

	queueCfg := types.DefaultQueueConfig("tasks", "$queue/tasks/#")
	logStore.CreateQueue(ctx, queueCfg) //nolint:errcheck // test setup

	// Register a remote consumer via cluster listing.
	mockRemote.mu.Lock()
	mockRemote.consumers = []*cluster.QueueConsumerInfo{
		{
			QueueName:   "tasks",
			GroupID:     testGroupWorkers,
			ConsumerID:  "remote-c1",
			ClientID:    "remote-client",
			Pattern:     "",
			Mode:        string(types.GroupModeQueue),
			ProxyNodeID: testNode2,
		},
	}
	mockRemote.mu.Unlock()

	logStore.Append(ctx, "tasks", &types.Message{ //nolint:errcheck // test setup
		ID:      "1",
		Topic:   testQueueTasksNew,
		Payload: []byte("job1"),
	})

	ok := engine.DeliverQueue(ctx, "tasks")
	if !ok {
		t.Fatal("expected DeliverQueue to return true")
	}

	mockRemote.mu.Lock()
	count := len(mockRemote.routed)
	mockRemote.mu.Unlock()

	if count != 1 {
		t.Fatalf("expected 1 routed message, got %d", count)
	}

	if mockRemote.routed[0].msg.QueueName != "tasks" {
		t.Fatalf("expected queue tasks, got %s", mockRemote.routed[0].msg.QueueName)
	}
	if mockRemote.routed[0].nodeID != testNode2 { //nolint:goconst // test value
		t.Fatalf("expected node-2, got %s", mockRemote.routed[0].nodeID)
	}
}

func TestDeliverAllSweep(t *testing.T) {
	var mu sync.Mutex
	deliveryCount := 0

	local := DeliveryTargetFunc(func(ctx context.Context, clientID string, msg *brokerstorage.Message) error {
		mu.Lock()
		deliveryCount++
		mu.Unlock()
		return nil
	})

	engine, logStore, groupStore := newTestEngine(t, local, nil)
	ctx := context.Background()

	for _, name := range []string{"q1", "q2"} {
		logStore.CreateQueue(ctx, types.DefaultQueueConfig(name, "$queue/"+name+"/#")) //nolint:errcheck // test setup

		group := types.NewConsumerGroupState(name, "g-"+name, "")
		group.SetConsumer("c1", &types.ConsumerInfo{
			ID:       "c1",
			ClientID: "c1",
		})
		groupStore.CreateConsumerGroup(ctx, group) //nolint:errcheck // test setup

		logStore.Append(ctx, name, &types.Message{ //nolint:errcheck // test setup
			ID:      "msg-" + name,
			Topic:   "$queue/" + name + "/test",
			Payload: []byte("data"),
		})
	}

	engine.DeliverAll(ctx)

	mu.Lock()
	defer mu.Unlock()
	if deliveryCount != 2 {
		t.Fatalf("expected 2 deliveries across 2 queues, got %d", deliveryCount)
	}
}

func TestDeliverQueueNilRemoteRouter(t *testing.T) {
	var mu sync.Mutex
	deliveryCount := 0

	local := DeliveryTargetFunc(func(ctx context.Context, clientID string, msg *brokerstorage.Message) error {
		mu.Lock()
		deliveryCount++
		mu.Unlock()
		return nil
	})

	engine, logStore, groupStore := newTestEngine(t, local, nil)
	ctx := context.Background()

	logStore.CreateQueue(ctx, types.DefaultQueueConfig("q1", "$queue/q1/#")) //nolint:errcheck // test setup

	group := types.NewConsumerGroupState("q1", "g1", "")
	group.SetConsumer("c1", &types.ConsumerInfo{
		ID:          "c1",
		ClientID:    "c1",
		ProxyNodeID: testNode2, // remote proxy, but no remote router
	})
	groupStore.CreateConsumerGroup(ctx, group) //nolint:errcheck // test setup

	logStore.Append(ctx, "q1", &types.Message{ //nolint:errcheck // test setup
		ID:      "1",
		Topic:   "$queue/q1/test",
		Payload: []byte("data"),
	})

	ok := engine.DeliverQueue(ctx, "q1")
	if !ok {
		t.Fatal("expected DeliverQueue to return true")
	}

	// With nil remote, the engine should deliver locally instead of remote routing.
	mu.Lock()
	defer mu.Unlock()
	if deliveryCount != 1 {
		t.Fatalf("expected 1 local delivery (nil remote falls through to local), got %d", deliveryCount)
	}
}

func TestDeliverQueueEmptyName(t *testing.T) {
	engine, _, _ := newTestEngine(t, nil, nil)

	if engine.DeliverQueue(context.Background(), "") {
		t.Fatal("expected false for empty queue name")
	}
}

// --- mock RemoteRouter ---

type routedEntry struct {
	nodeID    string
	clientID  string
	queueName string
	msg       *cluster.QueueMessage
}

type mockRemoteRouter struct {
	mu        sync.Mutex
	consumers []*cluster.QueueConsumerInfo
	routed    []routedEntry
	removed   []routedEntry
}

func (r *mockRemoteRouter) ListQueueConsumers(ctx context.Context, queueName string) ([]*cluster.QueueConsumerInfo, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	var result []*cluster.QueueConsumerInfo
	for _, c := range r.consumers {
		if c.QueueName == queueName {
			result = append(result, c)
		}
	}
	return result, nil
}

func (r *mockRemoteRouter) RouteQueueMessage(ctx context.Context, nodeID, clientID, queueName string, msg *cluster.QueueMessage) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.routed = append(r.routed, routedEntry{
		nodeID:    nodeID,
		clientID:  clientID,
		queueName: queueName,
		msg:       msg,
	})
	return nil
}

func (r *mockRemoteRouter) UnregisterQueueConsumer(ctx context.Context, queueName, groupID, consumerID string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.removed = append(r.removed, routedEntry{
		nodeID:    groupID,
		clientID:  consumerID,
		queueName: queueName,
	})
	return nil
}

type batchRemoteRouter struct {
	mockRemoteRouter
	batchErr error
}

func (r *batchRemoteRouter) RouteQueueBatch(ctx context.Context, nodeID string, deliveries []cluster.QueueDelivery) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, delivery := range deliveries {
		r.routed = append(r.routed, routedEntry{
			nodeID:    nodeID,
			clientID:  delivery.ClientID,
			queueName: delivery.QueueName,
			msg:       delivery.Message,
		})
	}
	return r.batchErr
}

func TestDLQCallbackOnMaxDeliveryCount(t *testing.T) {
	var mu sync.Mutex
	var dlqCalls []struct {
		queueName     string
		groupID       string
		msgID         string
		deliveryCount int
	}

	local := DeliveryTargetFunc(func(ctx context.Context, clientID string, msg *brokerstorage.Message) error {
		return nil
	})

	logStore := memlog.New()
	groupStore := newMockGroupStore()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	ctx := context.Background()

	consumerCfg := consumer.Config{
		ClaimBatchSize:     10,
		MaxDeliveryCount:   3,
		MaxPELSize:         100_000,
		AutoCommitInterval: DefaultConfig().AutoCommitInterval,
		VisibilityTimeout:  1 * time.Millisecond, // very short so entries are immediately stealable
		OnDLQ: func(ctx context.Context, queueName, groupID string, msg *types.Message, deliveryCount int) {
			mu.Lock()
			dlqCalls = append(dlqCalls, struct {
				queueName     string
				groupID       string
				msgID         string
				deliveryCount int
			}{queueName, groupID, msg.ID, deliveryCount})
			mu.Unlock()
		},
	}
	consumerMgr := consumer.NewManager(logStore, groupStore, consumerCfg)

	engine := NewDeliveryEngine(
		logStore, groupStore, consumerMgr,
		local, nil, "",
		DistributionForward, 100, logger,
	)
	_ = engine

	queueCfg := types.DefaultQueueConfig("tasks", "$queue/tasks/#")
	logStore.CreateQueue(ctx, queueCfg) //nolint:errcheck // test setup

	// Create group with two consumers
	group := types.NewConsumerGroupState("tasks", testGroupWorkers, "")
	group.SetConsumer("c1", &types.ConsumerInfo{ID: "c1", ClientID: "c1"})
	group.SetConsumer("c2", &types.ConsumerInfo{ID: "c2", ClientID: "c2"})
	groupStore.CreateConsumerGroup(ctx, group) //nolint:errcheck // test setup

	logStore.Append(ctx, "tasks", &types.Message{ //nolint:errcheck // test setup
		ID:      testPoisonMsg,
		Topic:   "$queue/tasks/job",
		Payload: []byte("bad-job"),
	})

	// Claim as c1 to put it in PEL
	_, err := consumerMgr.Claim(ctx, "tasks", testGroupWorkers, "c1", nil)
	if err != nil {
		t.Fatalf("initial claim failed: %v", err)
	}

	// Manually set delivery count to exceed max (simulate repeated redeliveries)
	group.PEL["c1"][0].DeliveryCount = 5
	group.PEL["c1"][0].ClaimedAt = time.Now().Add(-time.Hour) // make stealable

	// c2 tries to claim — triggers stealWork which should fire DLQ callback
	_, err = consumerMgr.Claim(ctx, "tasks", testGroupWorkers, "c2", nil)
	if err == nil {
		t.Fatal("expected no messages (poison should go to DLQ, not be delivered)")
	}

	mu.Lock()
	defer mu.Unlock()
	if len(dlqCalls) != 1 {
		t.Fatalf("expected 1 DLQ callback, got %d", len(dlqCalls))
	}
	if dlqCalls[0].queueName != "tasks" {
		t.Fatalf("expected queue 'tasks', got %q", dlqCalls[0].queueName)
	}
	if dlqCalls[0].groupID != testGroupWorkers { //nolint:goconst // test value
		t.Fatalf("expected group 'workers', got %q", dlqCalls[0].groupID)
	}
	if dlqCalls[0].msgID != testPoisonMsg {
		t.Fatalf("expected msg ID 'poison-msg', got %q", dlqCalls[0].msgID)
	}
	if dlqCalls[0].deliveryCount != 5 {
		t.Fatalf("expected delivery count 5, got %d", dlqCalls[0].deliveryCount)
	}

	// Verify the PEL entry was removed
	entries, _ := groupStore.GetPendingEntries(ctx, "tasks", testGroupWorkers, "c1")
	if len(entries) != 0 {
		t.Fatalf("expected PEL entry to be removed, got %d entries", len(entries))
	}
}

func TestDLQCallbackNilHandlerSilentlyDrops(t *testing.T) {
	local := DeliveryTargetFunc(func(ctx context.Context, clientID string, msg *brokerstorage.Message) error {
		return nil
	})

	logStore := memlog.New()
	groupStore := newMockGroupStore()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	ctx := context.Background()

	consumerCfg := consumer.Config{
		ClaimBatchSize:     10,
		MaxDeliveryCount:   3,
		MaxPELSize:         100_000,
		AutoCommitInterval: DefaultConfig().AutoCommitInterval,
		VisibilityTimeout:  1 * time.Millisecond,
		// OnDLQ is nil — should silently remove from PEL
	}
	consumerMgr := consumer.NewManager(logStore, groupStore, consumerCfg)

	engine := NewDeliveryEngine(
		logStore, groupStore, consumerMgr,
		local, nil, "",
		DistributionForward, 100, logger,
	)
	_ = engine

	queueCfg := types.DefaultQueueConfig("tasks", "$queue/tasks/#")
	logStore.CreateQueue(ctx, queueCfg) //nolint:errcheck // test setup

	group := types.NewConsumerGroupState("tasks", testGroupWorkers, "")
	group.SetConsumer("c1", &types.ConsumerInfo{ID: "c1", ClientID: "c1"})
	group.SetConsumer("c2", &types.ConsumerInfo{ID: "c2", ClientID: "c2"})
	groupStore.CreateConsumerGroup(ctx, group) //nolint:errcheck // test setup

	logStore.Append(ctx, "tasks", &types.Message{ //nolint:errcheck // test setup
		ID:      testPoisonMsg,
		Topic:   "$queue/tasks/job",
		Payload: []byte("bad-job"),
	})

	// Claim as c1, then simulate poison
	consumerMgr.Claim(ctx, "tasks", testGroupWorkers, "c1", nil) //nolint:errcheck // test setup
	group.PEL["c1"][0].DeliveryCount = 5
	group.PEL["c1"][0].ClaimedAt = time.Now().Add(-time.Hour)

	// Should not panic with nil OnDLQ
	_, err := consumerMgr.Claim(ctx, "tasks", testGroupWorkers, "c2", nil)
	if err == nil {
		t.Fatal("expected no messages")
	}

	// PEL entry should still be removed
	entries, _ := groupStore.GetPendingEntries(ctx, "tasks", testGroupWorkers, "c1")
	if len(entries) != 0 {
		t.Fatalf("expected PEL entry removed, got %d", len(entries))
	}
}

func TestDeliverQueueSkipsExpiredMessages(t *testing.T) {
	var mu sync.Mutex
	var delivered []*brokerstorage.Message

	local := DeliveryTargetFunc(func(ctx context.Context, clientID string, msg *brokerstorage.Message) error {
		mu.Lock()
		delivered = append(delivered, msg)
		mu.Unlock()
		return nil
	})

	engine, logStore, groupStore := newTestEngine(t, local, nil)
	ctx := context.Background()

	queueCfg := types.DefaultQueueConfig("tasks", "$queue/tasks/#")
	logStore.CreateQueue(ctx, queueCfg) //nolint:errcheck // test setup

	group := types.NewConsumerGroupState("tasks", testGroupWorkers, "")
	group.SetConsumer("c1", &types.ConsumerInfo{ID: "c1", ClientID: "c1"})
	groupStore.CreateConsumerGroup(ctx, group) //nolint:errcheck // test setup

	// Append an expired message then a valid one.
	logStore.Append(ctx, "tasks", &types.Message{ //nolint:errcheck // test setup
		ID:        "expired",
		Topic:     "$queue/tasks/old",
		Payload:   []byte("stale"),
		ExpiresAt: time.Now().Add(-time.Second),
	})
	logStore.Append(ctx, "tasks", &types.Message{ //nolint:errcheck // test setup
		ID:      "valid",
		Topic:   testQueueTasksNew,
		Payload: []byte("fresh"),
	})

	engine.DeliverQueue(ctx, "tasks")

	mu.Lock()
	count := len(delivered)
	mu.Unlock()

	if count != 1 {
		t.Fatalf("expected 1 delivered message (expired skipped), got %d", count)
	}
	if string(delivered[0].GetPayload()) != "fresh" {
		t.Fatalf("expected fresh payload, got %s", string(delivered[0].GetPayload()))
	}
}

func TestDeliverStreamSkipsExpiredMessages(t *testing.T) {
	var mu sync.Mutex
	var delivered []*brokerstorage.Message

	local := DeliveryTargetFunc(func(ctx context.Context, clientID string, msg *brokerstorage.Message) error {
		mu.Lock()
		delivered = append(delivered, msg)
		mu.Unlock()
		return nil
	})

	engine, logStore, groupStore := newTestEngine(t, local, nil)
	ctx := context.Background()

	queueCfg := types.DefaultQueueConfig(testQueueEvents, "$queue/events/#")
	queueCfg.Type = types.QueueTypeStream
	logStore.CreateQueue(ctx, queueCfg) //nolint:errcheck // test setup

	group := types.NewConsumerGroupState(testQueueEvents, "readers", "")
	group.Mode = types.GroupModeStream
	group.SetConsumer("c1", &types.ConsumerInfo{ID: "c1", ClientID: "c1"})
	groupStore.CreateConsumerGroup(ctx, group) //nolint:errcheck // test setup

	logStore.Append(ctx, testQueueEvents, &types.Message{ //nolint:errcheck // test setup
		ID:        "expired",
		Topic:     "$queue/events/old",
		Payload:   []byte("stale"),
		ExpiresAt: time.Now().Add(-time.Second),
	})
	logStore.Append(ctx, testQueueEvents, &types.Message{ //nolint:errcheck // test setup
		ID:      "valid",
		Topic:   "$queue/events/new",
		Payload: []byte("fresh"),
	})

	engine.DeliverQueue(ctx, testQueueEvents)

	mu.Lock()
	count := len(delivered)
	mu.Unlock()

	if count != 1 {
		t.Fatalf("expected 1 delivered message (expired skipped), got %d", count)
	}
	if string(delivered[0].GetPayload()) != "fresh" {
		t.Fatalf("expected fresh payload, got %s", string(delivered[0].GetPayload()))
	}
}

func TestDeliverStreamDoesNotAdvanceCursorForMissingLocalTarget(t *testing.T) {
	local := &checkingDeliverer{
		targets: map[string]bool{"dead-client": false},
	}

	engine, logStore, groupStore := newTestEngine(t, local, nil)
	ctx := context.Background()

	queueCfg := types.DefaultQueueConfig("events", "$queue/events/#")
	queueCfg.Type = types.QueueTypeStream
	logStore.CreateQueue(ctx, queueCfg) //nolint:errcheck // test setup

	group := types.NewConsumerGroupState("events", "readers", "")
	group.Mode = types.GroupModeStream
	group.SetConsumer("c1", &types.ConsumerInfo{ID: "c1", ClientID: "dead-client"})
	groupStore.CreateConsumerGroup(ctx, group) //nolint:errcheck // test setup

	logStore.Append(ctx, "events", &types.Message{ //nolint:errcheck // test setup
		ID:      "1",
		Topic:   "$queue/events/new",
		Payload: []byte("payload"),
	})

	if engine.DeliverQueue(ctx, "events") {
		t.Fatal("expected no delivery for disconnected consumer")
	}

	if got := len(local.deliveredMessages()); got != 0 {
		t.Fatalf("expected 0 deliveries, got %d", got)
	}

	updated, err := groupStore.GetConsumerGroup(ctx, "events", "readers")
	if err != nil {
		t.Fatalf("get group failed: %v", err)
	}
	if updated.GetConsumer("c1") != nil {
		t.Fatal("expected disconnected consumer to be removed")
	}
	if cursor := updated.GetCursor().Cursor; cursor != 0 {
		t.Fatalf("expected cursor to stay at 0, got %d", cursor)
	}
}

func TestDeliverStreamKeepsConsumerForQueueableOfflineTarget(t *testing.T) {
	local := &checkingDeliverer{
		targets:   map[string]bool{"offline-client": true},
		connected: map[string]bool{"offline-client": false},
	}

	engine, logStore, groupStore := newTestEngine(t, local, nil)
	ctx := context.Background()

	queueCfg := types.DefaultQueueConfig("events", "$queue/events/#")
	queueCfg.Type = types.QueueTypeStream
	logStore.CreateQueue(ctx, queueCfg) //nolint:errcheck // test setup

	group := types.NewConsumerGroupState("events", "readers", "")
	group.Mode = types.GroupModeStream
	group.SetConsumer("c1", &types.ConsumerInfo{ID: "c1", ClientID: "offline-client"})
	groupStore.CreateConsumerGroup(ctx, group) //nolint:errcheck // test setup

	logStore.Append(ctx, "events", &types.Message{ //nolint:errcheck // test setup
		ID:      "1",
		Topic:   "$queue/events/new",
		Payload: []byte("payload"),
	})

	if !engine.DeliverQueue(ctx, "events") {
		t.Fatal("expected delivery to queueable offline target")
	}

	updated, err := groupStore.GetConsumerGroup(ctx, "events", "readers")
	if err != nil {
		t.Fatalf("get group failed: %v", err)
	}
	if updated.GetConsumer("c1") == nil {
		t.Fatal("expected queueable offline consumer to remain registered")
	}
	if cursor := updated.GetCursor().Cursor; cursor != 1 {
		t.Fatalf("expected cursor to advance to 1, got %d", cursor)
	}
}

func TestDeliverStreamCommitsCursorAfterSuccessfulDelivery(t *testing.T) {
	local := &checkingDeliverer{
		targets: map[string]bool{"client-1": true},
	}

	engine, logStore, groupStore := newTestEngine(t, local, nil)
	ctx := context.Background()

	queueCfg := types.DefaultQueueConfig("events", "$queue/events/#")
	queueCfg.Type = types.QueueTypeStream
	logStore.CreateQueue(ctx, queueCfg) //nolint:errcheck // test setup

	group := types.NewConsumerGroupState("events", "readers", "")
	group.Mode = types.GroupModeStream
	group.SetConsumer("c1", &types.ConsumerInfo{ID: "c1", ClientID: "client-1"})
	groupStore.CreateConsumerGroup(ctx, group) //nolint:errcheck // test setup

	logStore.Append(ctx, "events", &types.Message{ //nolint:errcheck // test setup
		ID:      "1",
		Topic:   "$queue/events/new",
		Payload: []byte("payload"),
	})

	if !engine.DeliverQueue(ctx, "events") {
		t.Fatal("expected successful delivery")
	}

	if got := len(local.deliveredMessages()); got != 1 {
		t.Fatalf("expected 1 delivery, got %d", got)
	}

	updated, err := groupStore.GetConsumerGroup(ctx, "events", "readers")
	if err != nil {
		t.Fatalf("get group failed: %v", err)
	}
	if cursor := updated.GetCursor().Cursor; cursor != 1 {
		t.Fatalf("expected cursor to advance to 1, got %d", cursor)
	}
}

func TestDeliverStreamRemovesConsumerOnClientNotConnectedError(t *testing.T) {
	local := &checkingDeliverer{
		targets:    map[string]bool{"client-1": true},
		deliverErr: corebroker.ErrClientNotConnected,
	}

	engine, logStore, groupStore := newTestEngine(t, local, nil)
	ctx := context.Background()

	queueCfg := types.DefaultQueueConfig("events", "$queue/events/#")
	queueCfg.Type = types.QueueTypeStream
	logStore.CreateQueue(ctx, queueCfg) //nolint:errcheck // test setup

	group := types.NewConsumerGroupState("events", "readers", "")
	group.Mode = types.GroupModeStream
	group.SetConsumer("c1", &types.ConsumerInfo{ID: "c1", ClientID: "client-1"})
	groupStore.CreateConsumerGroup(ctx, group) //nolint:errcheck // test setup

	logStore.Append(ctx, "events", &types.Message{ //nolint:errcheck // test setup
		ID:      "1",
		Topic:   "$queue/events/new",
		Payload: []byte("payload"),
	})

	if engine.DeliverQueue(ctx, "events") {
		t.Fatal("expected no successful delivery")
	}

	updated, err := groupStore.GetConsumerGroup(ctx, "events", "readers")
	if err != nil {
		t.Fatalf("get group failed: %v", err)
	}
	if updated.GetConsumer("c1") != nil {
		t.Fatal("expected stale consumer to be removed")
	}
	if cursor := updated.GetCursor().Cursor; cursor != 0 {
		t.Fatalf("expected cursor to stay at 0, got %d", cursor)
	}
}

func TestDeliverStreamRemovesRemoteConsumerOnBatchClientNotConnectedError(t *testing.T) {
	remote := &batchRemoteRouter{
		batchErr: errors.New("route queue batch failed after 3 retries: 1 deliveries still failing: client remote-client queue events: client not connected"),
	}

	engine, logStore, groupStore := newTestEngine(t, nil, remote)
	ctx := context.Background()

	queueCfg := types.DefaultQueueConfig("events", "$queue/events/#")
	queueCfg.Type = types.QueueTypeStream
	logStore.CreateQueue(ctx, queueCfg) //nolint:errcheck // test setup

	group := types.NewConsumerGroupState("events", "readers", "")
	group.Mode = types.GroupModeStream
	group.SetConsumer("remote-c1", &types.ConsumerInfo{
		ID:          "remote-c1",
		ClientID:    "remote-client",
		ProxyNodeID: "node-2",
	})
	groupStore.CreateConsumerGroup(ctx, group) //nolint:errcheck // test setup

	logStore.Append(ctx, "events", &types.Message{ //nolint:errcheck // test setup
		ID:      "1",
		Topic:   "$queue/events/new",
		Payload: []byte("payload"),
	})

	if engine.DeliverQueue(ctx, "events") {
		t.Fatal("expected no successful delivery")
	}

	updated, err := groupStore.GetConsumerGroup(ctx, "events", "readers")
	if err != nil {
		t.Fatalf("get group failed: %v", err)
	}
	if updated.GetConsumer("remote-c1") != nil {
		t.Fatal("expected stale remote consumer to be removed")
	}
	if cursor := updated.GetCursor().Cursor; cursor != 0 {
		t.Fatalf("expected cursor to stay at 0, got %d", cursor)
	}
	if len(remote.removed) != 1 {
		t.Fatalf("expected remote unregister, got %d", len(remote.removed))
	}
}

func TestDeliverQueueAllExpiredReturnsNoDelivery(t *testing.T) {
	var mu sync.Mutex
	deliveryCount := 0

	local := DeliveryTargetFunc(func(ctx context.Context, clientID string, msg *brokerstorage.Message) error {
		mu.Lock()
		deliveryCount++
		mu.Unlock()
		return nil
	})

	engine, logStore, groupStore := newTestEngine(t, local, nil)
	ctx := context.Background()

	queueCfg := types.DefaultQueueConfig("tasks", "$queue/tasks/#")
	logStore.CreateQueue(ctx, queueCfg) //nolint:errcheck // test setup

	group := types.NewConsumerGroupState("tasks", testGroupWorkers, "")
	group.SetConsumer("c1", &types.ConsumerInfo{ID: "c1", ClientID: "c1"})
	groupStore.CreateConsumerGroup(ctx, group) //nolint:errcheck // test setup

	logStore.Append(ctx, "tasks", &types.Message{ //nolint:errcheck // test setup
		ID:        "e1",
		Topic:     "$queue/tasks/a",
		Payload:   []byte("old1"),
		ExpiresAt: time.Now().Add(-time.Minute),
	})
	logStore.Append(ctx, "tasks", &types.Message{ //nolint:errcheck // test setup
		ID:        "e2",
		Topic:     "$queue/tasks/b",
		Payload:   []byte("old2"),
		ExpiresAt: time.Now().Add(-time.Minute),
	})

	ok := engine.DeliverQueue(ctx, "tasks")
	if ok {
		t.Fatal("expected DeliverQueue to return false when all messages are expired")
	}

	mu.Lock()
	defer mu.Unlock()
	if deliveryCount != 0 {
		t.Fatalf("expected 0 deliveries, got %d", deliveryCount)
	}
}
