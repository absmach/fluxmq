// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package cluster

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/absmach/fluxmq/broker/router"
	clusterv1 "github.com/absmach/fluxmq/pkg/proto/cluster/v1"
	"github.com/absmach/fluxmq/storage"
)

func benchmarkRoutePublishCluster(b *testing.B, subscribers int) *EtcdCluster {
	stopCh := make(chan struct{})
	b.Cleanup(func() { close(stopCh) })

	c := &EtcdCluster{
		nodeID:     "node-local",
		transport:  &Transport{},
		logger:     slog.New(slog.NewTextHandler(io.Discard, nil)),
		stopCh:     stopCh,
		subTrie:    router.NewRouter(),
		ownerCache: make(map[string]string, subscribers),
	}

	for i := 0; i < subscribers; i++ {
		clientID := fmt.Sprintf("client-%d", i)
		if err := c.subTrie.Subscribe(clientID, "sensor/temp", 1, storage.SubscribeOptions{}); err != nil {
			b.Fatalf("subscribe failed: %v", err)
		}
		// Spread ownership over two remote nodes and one local node.
		switch i % 3 {
		case 0:
			c.ownerCache[clientID] = "node-a"
		case 1:
			c.ownerCache[clientID] = "node-b"
		default:
			c.ownerCache[clientID] = "node-local"
		}
	}

	c.forwardBatcher = newNodeBatcher(
		1,
		time.Second,
		1,
		stopCh,
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		"bench-forward",
		func(ctx context.Context, nodeID string, items []*clusterv1.ForwardPublishRequest) error {
			return nil
		},
	)

	return c
}

func BenchmarkRoutePublish_QoS1_1kSubscribers(b *testing.B) {
	c := benchmarkRoutePublishCluster(b, 1000)
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if err := c.RoutePublish(ctx, "sensor/temp", []byte("42"), 1, false, nil); err != nil {
			b.Fatalf("route publish failed: %v", err)
		}
	}
}
