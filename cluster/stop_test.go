// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package cluster

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/absmach/fluxmq/storage/memory"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// newEtcdTrio starts three clustered nodes and returns them once each has
// entered the leader election.
func newEtcdTrio(t *testing.T) []*EtcdCluster {
	t.Helper()

	const size = 3
	ids := make([]string, size)
	peers := make([]string, size)
	members := make([]string, size)
	for i := range size {
		ids[i] = fmt.Sprintf("stop-node-%d", i)
		peers[i] = fmt.Sprintf("127.0.0.1:%d", freeLocalPort(t))
		members[i] = fmt.Sprintf("%s=http://%s", ids[i], peers[i])
	}

	nodes := make([]*EtcdCluster, size)
	errs := make([]error, size)
	var wg sync.WaitGroup
	for i := range size {
		wg.Go(func() {
			cfg := &EtcdConfig{
				NodeID:         ids[i],
				DataDir:        filepath.Join(t.TempDir(), "etcd"),
				BindAddr:       peers[i],
				ClientAddr:     fmt.Sprintf("127.0.0.1:%d", freeLocalPort(t)),
				AdvertiseAddr:  peers[i],
				InitialCluster: strings.Join(members, ","),
				Bootstrap:      true,
				AllowInsecure:  true,
			}
			node, err := NewEtcdCluster(cfg, memory.New(), slog.New(slog.NewTextHandler(io.Discard, nil)))
			if err == nil {
				err = node.Start()
			}
			nodes[i], errs[i] = node, err
		})
	}
	wg.Wait()
	for i := range size {
		require.NoError(t, errs[i], "node %s", ids[i])
	}

	// Every node campaigns a few seconds after Start; the followers then wait
	// inside Election.Campaign for the leader's key to go away.
	require.Eventually(t, func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		resp, err := nodes[0].client.Get(ctx, electionPrefix, clientv3.WithPrefix(), clientv3.WithCountOnly())
		return err == nil && resp.Count == size
	}, 15*time.Second, 100*time.Millisecond)

	return nodes
}

// crash halts the given nodes' etcd members without the teardown Stop does, as
// killing their processes would, and stops what is left of them at cleanup.
func crash(t *testing.T, nodes ...*EtcdCluster) {
	t.Helper()

	var wg sync.WaitGroup
	for _, node := range nodes {
		// HardStop skips the leadership transfer a graceful close waits for;
		// Close then shuts the listeners, so peers see refused connections.
		node.etcd.Server.HardStop()
		wg.Go(node.etcd.Close)
	}
	wg.Wait()
	for _, node := range nodes {
		node.etcd = nil
	}
	t.Cleanup(func() {
		var wg sync.WaitGroup
		for _, node := range nodes {
			wg.Go(func() { _ = node.Stop() })
		}
		wg.Wait()
	})
}

func TestStopReturnsWithoutQuorum(t *testing.T) {
	nodes := newEtcdTrio(t)

	var follower *EtcdCluster
	for _, node := range nodes {
		if !node.IsLeader(context.Background()) {
			follower = node
			break
		}
	}
	require.NotNil(t, follower, "two of three nodes must be followers")

	var others []*EtcdCluster
	for _, node := range nodes {
		if node != follower {
			others = append(others, node)
		}
	}
	crash(t, others...)

	stopped := make(chan struct{})
	start := time.Now()
	go func() {
		_ = follower.Stop()
		close(stopped)
	}()

	select {
	case <-stopped:
		t.Logf("Stop returned after %v", time.Since(start).Round(time.Millisecond))
	case <-time.After(20 * time.Second):
		t.Fatal("Stop did not return after the cluster lost quorum")
	}
}
