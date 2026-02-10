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
	"github.com/absmach/fluxmq/queue/types"
)

func TestNoopClusterAdapter(t *testing.T) {
	var a ClusterAdapter = noopClusterAdapter{}
	ctx := context.Background()

	if err := a.RegisterConsumer(ctx, &cluster.QueueConsumerInfo{}); err != nil {
		t.Fatalf("expected nil, got %v", err)
	}
	if err := a.UnregisterConsumer(ctx, "q", "g", "c"); err != nil {
		t.Fatalf("expected nil, got %v", err)
	}
	if err := a.ForwardPublish(ctx, "n", "t", nil, nil, false); err != nil {
		t.Fatalf("expected nil, got %v", err)
	}
	if err := a.RouteMessage(ctx, "n", "c", "q", nil); err != nil {
		t.Fatalf("expected nil, got %v", err)
	}

	consumers, err := a.ListConsumers(ctx, "q")
	if err != nil || consumers != nil {
		t.Fatalf("expected (nil, nil), got (%v, %v)", consumers, err)
	}

	all, err := a.ListAllConsumers(ctx)
	if err != nil || all != nil {
		t.Fatalf("expected (nil, nil), got (%v, %v)", all, err)
	}

	// ForwardToRemoteNodes should be a no-op (no panic)
	a.ForwardToRemoteNodes(ctx, types.PublishRequest{}, func(string) bool { return false })

	if id := a.LocalNodeID(); id != "" {
		t.Fatalf("expected empty, got %q", id)
	}
	if a.IsRemote("any-node") {
		t.Fatal("noop adapter should never report remote")
	}
	if a.IsRemote("") {
		t.Fatal("noop adapter should not report empty string as remote")
	}
	if mode := a.DistributionMode(); mode != "" {
		t.Fatalf("expected empty, got %q", mode)
	}
}

func TestClusterAdapterDelegates(t *testing.T) {
	spy := newMockCluster("node-1")
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	a := &clusterAdapter{
		cluster:          spy,
		localNodeID:      "node-1",
		distributionMode: DistributionForward,
		logger:           logger,
	}

	ctx := context.Background()

	// RegisterConsumer
	info := &cluster.QueueConsumerInfo{QueueName: "q", ConsumerID: "c"}
	if err := a.RegisterConsumer(ctx, info); err != nil {
		t.Fatalf("RegisterConsumer: %v", err)
	}
	registered := spy.GetRegisteredQueueConsumers()
	if len(registered) != 1 || registered[0].QueueName != "q" {
		t.Fatalf("expected 1 registered consumer, got %d", len(registered))
	}

	// ForwardPublish
	if err := a.ForwardPublish(ctx, "node-2", "topic", []byte("p"), nil, true); err != nil {
		t.Fatalf("ForwardPublish: %v", err)
	}
	calls := spy.GetForwardCalls()
	if len(calls) != 1 || calls[0].nodeID != "node-2" || !calls[0].forwardToLeader {
		t.Fatalf("unexpected forward call: %+v", calls)
	}

	// RouteMessage
	if err := a.RouteMessage(ctx, "node-2", "client-1", "q", &cluster.QueueMessage{MessageID: "m1"}); err != nil {
		t.Fatalf("RouteMessage: %v", err)
	}
	routed := spy.GetRoutedMessages()
	if len(routed) != 1 || routed[0].nodeID != "node-2" {
		t.Fatalf("unexpected routed messages: %+v", routed)
	}

	// ListConsumers
	spy.SetQueueConsumers([]*cluster.QueueConsumerInfo{
		{QueueName: "q", ProxyNodeID: "node-1"},
		{QueueName: "q", ProxyNodeID: "node-2"},
		{QueueName: "other", ProxyNodeID: "node-3"},
	})
	consumers, err := a.ListConsumers(ctx, "q")
	if err != nil {
		t.Fatalf("ListConsumers: %v", err)
	}
	if len(consumers) != 2 {
		t.Fatalf("expected 2 consumers for queue q, got %d", len(consumers))
	}

	// ListAllConsumers
	all, err := a.ListAllConsumers(ctx)
	if err != nil {
		t.Fatalf("ListAllConsumers: %v", err)
	}
	if len(all) != 3 {
		t.Fatalf("expected 3 total consumers, got %d", len(all))
	}

	// LocalNodeID / IsRemote
	if a.LocalNodeID() != "node-1" {
		t.Fatalf("expected node-1, got %q", a.LocalNodeID())
	}
	if a.IsRemote("node-1") {
		t.Fatal("local node should not be remote")
	}
	if !a.IsRemote("node-2") {
		t.Fatal("node-2 should be remote")
	}
	if a.IsRemote("") {
		t.Fatal("empty nodeID should not be remote")
	}

	// DistributionMode
	if a.DistributionMode() != DistributionForward {
		t.Fatalf("expected forward, got %q", a.DistributionMode())
	}
}

func TestClusterAdapterForwardToRemoteNodes(t *testing.T) {
	spy := newMockCluster("node-1")
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	a := &clusterAdapter{
		cluster:          spy,
		localNodeID:      "node-1",
		distributionMode: DistributionForward,
		logger:           logger,
	}

	spy.SetQueueConsumers([]*cluster.QueueConsumerInfo{
		{QueueName: "orders", ProxyNodeID: "node-1"}, // local — should be skipped
		{QueueName: "orders", ProxyNodeID: "node-2"}, // remote — should forward
	})

	ctx := context.Background()
	a.ForwardToRemoteNodes(ctx, types.PublishRequest{
		Topic:   "$queue/orders/new",
		Payload: []byte("data"),
	}, func(string) bool { return false })

	calls := spy.GetForwardCalls()
	if len(calls) != 1 {
		t.Fatalf("expected 1 forward call, got %d", len(calls))
	}
	if calls[0].nodeID != "node-2" {
		t.Fatalf("expected forward to node-2, got %s", calls[0].nodeID)
	}
	if calls[0].forwardToLeader {
		t.Fatal("expected forwardToLeader=false for remote forwarding")
	}
}

func TestClusterAdapterForwardToRemoteNodesReplicateMode(t *testing.T) {
	spy := newMockCluster("node-1")
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	a := &clusterAdapter{
		cluster:          spy,
		localNodeID:      "node-1",
		distributionMode: DistributionReplicate,
		logger:           logger,
	}

	spy.SetQueueConsumers([]*cluster.QueueConsumerInfo{
		{QueueName: "orders", ProxyNodeID: "node-2"},
	})

	var mu sync.Mutex
	knownQueues := map[string]bool{"orders": true}

	ctx := context.Background()
	a.ForwardToRemoteNodes(ctx, types.PublishRequest{
		Topic:   "$queue/orders/new",
		Payload: []byte("data"),
	}, func(name string) bool {
		mu.Lock()
		defer mu.Unlock()
		return knownQueues[name]
	})

	calls := spy.GetForwardCalls()
	if len(calls) != 0 {
		t.Fatalf("replicate mode should skip known queues, got %d forward calls", len(calls))
	}

	// Unknown queue should forward
	spy.SetQueueConsumers([]*cluster.QueueConsumerInfo{
		{QueueName: "unknown", ProxyNodeID: "node-2"},
	})

	a.ForwardToRemoteNodes(ctx, types.PublishRequest{
		Topic:   "$queue/unknown/msg",
		Payload: []byte("data"),
	}, func(name string) bool {
		mu.Lock()
		defer mu.Unlock()
		return knownQueues[name]
	})

	calls = spy.GetForwardCalls()
	if len(calls) != 1 {
		t.Fatalf("replicate mode should forward unknown queues, got %d forward calls", len(calls))
	}
}
