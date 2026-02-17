// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package cluster

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/absmach/fluxmq/broker/router"
	clusterv1 "github.com/absmach/fluxmq/pkg/proto/cluster/v1"
	"github.com/absmach/fluxmq/storage"
)

func setupRoutePublishCluster(t *testing.T, stopCh chan struct{}, sendFn func(ctx context.Context, nodeID string, items []*clusterv1.ForwardPublishRequest) error) *EtcdCluster {
	t.Helper()

	c := &EtcdCluster{
		nodeID:     "node-local",
		transport:  &Transport{},
		logger:     slog.New(slog.NewTextHandler(io.Discard, nil)),
		stopCh:     stopCh,
		subTrie:    router.NewRouter(),
		ownerCache: map[string]string{},
	}

	clients := []struct {
		id   string
		node string
	}{
		{id: "client-a-1", node: "node-a"},
		{id: "client-a-2", node: "node-a"},
		{id: "client-b-1", node: "node-b"},
		{id: "client-local", node: "node-local"},
	}
	for _, cinfo := range clients {
		if err := c.subTrie.Subscribe(cinfo.id, "sensor/temp", 1, storage.SubscribeOptions{}); err != nil {
			t.Fatalf("subscribe failed: %v", err)
		}
		c.ownerCache[cinfo.id] = cinfo.node
	}

	c.forwardBatcher = newNodeBatcher(
		1,
		5*time.Millisecond,
		1,
		stopCh,
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		"test-forward",
		sendFn,
	)

	return c
}

func TestRoutePublishQoS1ForwardsSync(t *testing.T) {
	stopCh := make(chan struct{})
	defer close(stopCh)

	var (
		mu    sync.Mutex
		calls = make(map[string]int)
	)
	c := setupRoutePublishCluster(t, stopCh, func(ctx context.Context, nodeID string, items []*clusterv1.ForwardPublishRequest) error {
		mu.Lock()
		calls[nodeID] += len(items)
		mu.Unlock()
		return nil
	})

	err := c.RoutePublish(context.Background(), "sensor/temp", []byte("42"), 1, false, map[string]string{"k": "v"})
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if calls["node-a"] != 1 {
		t.Fatalf("expected one forward to node-a, got %d", calls["node-a"])
	}
	if calls["node-b"] != 1 {
		t.Fatalf("expected one forward to node-b, got %d", calls["node-b"])
	}
	if calls["node-local"] != 0 {
		t.Fatalf("expected no forwards to local node, got %d", calls["node-local"])
	}
}

func TestRoutePublishQoS1PropagatesError(t *testing.T) {
	stopCh := make(chan struct{})
	defer close(stopCh)

	errNodeB := errors.New("node-b unavailable")
	c := setupRoutePublishCluster(t, stopCh, func(ctx context.Context, nodeID string, items []*clusterv1.ForwardPublishRequest) error {
		if nodeID == "node-b" {
			return errNodeB
		}
		return nil
	})

	err := c.RoutePublish(context.Background(), "sensor/temp", []byte("42"), 1, false, nil)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !errors.Is(err, errNodeB) {
		t.Fatalf("expected node-b error in chain, got %v", err)
	}
}

func TestRoutePublishQoS0ForwardsAsync(t *testing.T) {
	stopCh := make(chan struct{})
	defer close(stopCh)

	var (
		mu    sync.Mutex
		calls = make(map[string]int)
	)
	c := setupRoutePublishCluster(t, stopCh, func(ctx context.Context, nodeID string, items []*clusterv1.ForwardPublishRequest) error {
		mu.Lock()
		calls[nodeID] += len(items)
		mu.Unlock()
		return nil
	})

	err := c.RoutePublish(context.Background(), "sensor/temp", []byte("42"), 0, false, nil)
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}

	deadline := time.Now().Add(200 * time.Millisecond)
	for {
		mu.Lock()
		done := calls["node-a"] == 1 && calls["node-b"] == 1
		mu.Unlock()
		if done {
			break
		}
		if time.Now().After(deadline) {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}

	mu.Lock()
	defer mu.Unlock()
	if calls["node-a"] != 1 {
		t.Fatalf("expected one forward to node-a, got %d", calls["node-a"])
	}
	if calls["node-b"] != 1 {
		t.Fatalf("expected one forward to node-b, got %d", calls["node-b"])
	}
}

func TestRoutePublishQoS0DoesNotPropagateFlushError(t *testing.T) {
	stopCh := make(chan struct{})
	defer close(stopCh)

	c := setupRoutePublishCluster(t, stopCh, func(ctx context.Context, nodeID string, items []*clusterv1.ForwardPublishRequest) error {
		return errors.New("transport down")
	})

	err := c.RoutePublish(context.Background(), "sensor/temp", []byte("42"), 0, false, nil)
	if err != nil {
		t.Fatalf("QoS 0 should not propagate flush errors, got %v", err)
	}
}

func TestRoutePublishNoRemoteNodesSkipsForwarding(t *testing.T) {
	stopCh := make(chan struct{})
	defer close(stopCh)

	c := &EtcdCluster{
		nodeID:     "node-local",
		transport:  &Transport{},
		logger:     slog.New(slog.NewTextHandler(io.Discard, nil)),
		stopCh:     stopCh,
		subTrie:    router.NewRouter(),
		ownerCache: map[string]string{},
	}

	if err := c.subTrie.Subscribe("client-local", "sensor/temp", 1, storage.SubscribeOptions{}); err != nil {
		t.Fatalf("subscribe failed: %v", err)
	}
	c.ownerCache["client-local"] = "node-local"

	called := false
	c.forwardBatcher = newNodeBatcher(
		1,
		time.Second,
		1,
		stopCh,
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		"test-forward",
		func(ctx context.Context, nodeID string, items []*clusterv1.ForwardPublishRequest) error {
			called = true
			return nil
		},
	)

	if err := c.RoutePublish(context.Background(), "sensor/temp", []byte("42"), 1, false, nil); err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if called {
		t.Fatal("expected no forwarding calls for local-only subscribers")
	}
}
