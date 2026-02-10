// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package queue

import (
	"context"
	"io"
	"log/slog"
	"sync"
	"testing"

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
		nodeID = "node-1"
	}

	engine := NewDeliveryEngine(
		logStore, groupStore, consumerMgr,
		local, remote, nodeID,
		DistributionForward, 100, logger,
	)

	return engine, logStore, groupStore
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
		logStore.CreateQueue(ctx, types.DefaultQueueConfig(name, "$queue/"+name+"/#"))
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
	logStore.CreateQueue(ctx, queueCfg)

	group := types.NewConsumerGroupState("tasks", "workers", "")
	group.SetConsumer("c1", &types.ConsumerInfo{
		ID:       "c1",
		ClientID: "c1",
	})
	groupStore.CreateConsumerGroup(ctx, group)

	logStore.Append(ctx, "tasks", &types.Message{
		ID:      "1",
		Topic:   "$queue/tasks/new",
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

	if delivered[0].Properties[types.PropQueueName] != "tasks" {
		t.Fatalf("expected queue name tasks, got %s", delivered[0].Properties[types.PropQueueName])
	}
}

func TestDeliverQueueRemoteConsumer(t *testing.T) {
	mockRemote := &mockRemoteRouter{}

	engine, logStore, _ := newTestEngine(t, nil, mockRemote)
	ctx := context.Background()

	queueCfg := types.DefaultQueueConfig("tasks", "$queue/tasks/#")
	logStore.CreateQueue(ctx, queueCfg)

	// Register a remote consumer via cluster listing.
	mockRemote.mu.Lock()
	mockRemote.consumers = []*cluster.QueueConsumerInfo{
		{
			QueueName:   "tasks",
			GroupID:     "workers",
			ConsumerID:  "remote-c1",
			ClientID:    "remote-client",
			Pattern:     "",
			Mode:        string(types.GroupModeQueue),
			ProxyNodeID: "node-2",
		},
	}
	mockRemote.mu.Unlock()

	logStore.Append(ctx, "tasks", &types.Message{
		ID:      "1",
		Topic:   "$queue/tasks/new",
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
	if mockRemote.routed[0].nodeID != "node-2" {
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
		logStore.CreateQueue(ctx, types.DefaultQueueConfig(name, "$queue/"+name+"/#"))

		group := types.NewConsumerGroupState(name, "g-"+name, "")
		group.SetConsumer("c1", &types.ConsumerInfo{
			ID:       "c1",
			ClientID: "c1",
		})
		groupStore.CreateConsumerGroup(ctx, group)

		logStore.Append(ctx, name, &types.Message{
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

	logStore.CreateQueue(ctx, types.DefaultQueueConfig("q1", "$queue/q1/#"))

	group := types.NewConsumerGroupState("q1", "g1", "")
	group.SetConsumer("c1", &types.ConsumerInfo{
		ID:          "c1",
		ClientID:    "c1",
		ProxyNodeID: "node-2", // remote proxy, but no remote router
	})
	groupStore.CreateConsumerGroup(ctx, group)

	logStore.Append(ctx, "q1", &types.Message{
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
