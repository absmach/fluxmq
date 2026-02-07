// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package cluster

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"path/filepath"
	"testing"
	"time"

	"github.com/absmach/fluxmq/storage/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func freeLocalPort(t *testing.T) int {
	t.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer ln.Close()

	return ln.Addr().(*net.TCPAddr).Port
}

func newSingleNodeEtcdCluster(t *testing.T) *EtcdCluster {
	t.Helper()

	peerPort := freeLocalPort(t)
	clientPort := freeLocalPort(t)
	nodeID := "lease-test-node"

	cfg := &EtcdConfig{
		NodeID:         nodeID,
		DataDir:        filepath.Join(t.TempDir(), "etcd"),
		BindAddr:       fmt.Sprintf("127.0.0.1:%d", peerPort),
		ClientAddr:     fmt.Sprintf("127.0.0.1:%d", clientPort),
		AdvertiseAddr:  fmt.Sprintf("127.0.0.1:%d", peerPort),
		InitialCluster: fmt.Sprintf("%s=http://127.0.0.1:%d", nodeID, peerPort),
		Bootstrap:      true,
	}

	c, err := NewEtcdCluster(cfg, memory.New(), slog.New(slog.NewTextHandler(io.Discard, nil)))
	require.NoError(t, err)
	require.NoError(t, c.Start())

	t.Cleanup(func() {
		_ = c.Stop()
	})

	return c
}

func TestRegisterQueueConsumerStoresLeasedKey(t *testing.T) {
	c := newSingleNodeEtcdCluster(t)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	info := &QueueConsumerInfo{
		QueueName:    "events",
		GroupID:      "workers@#",
		ConsumerID:   "consumer-1",
		ClientID:     "client-1",
		Pattern:      "#",
		Mode:         "stream",
		ProxyNodeID:  c.nodeID,
		RegisteredAt: time.Now(),
	}
	require.NoError(t, c.RegisterQueueConsumer(ctx, info))

	key := fmt.Sprintf("%s%s/%s/%s", queueConsumersPrefix, info.QueueName, info.GroupID, info.ConsumerID)
	resp, err := c.client.Get(ctx, key)
	require.NoError(t, err)
	require.Len(t, resp.Kvs, 1)

	c.leaseMu.Lock()
	leaseID := c.sessionLease
	c.leaseMu.Unlock()

	assert.NotZero(t, resp.Kvs[0].Lease)
	assert.Equal(t, int64(leaseID), resp.Kvs[0].Lease)
}

func TestRegisterQueueConsumerRefreshesExpiredLease(t *testing.T) {
	c := newSingleNodeEtcdCluster(t)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	c.leaseMu.Lock()
	oldLease := c.sessionLease
	c.leaseMu.Unlock()

	_, err := c.client.Revoke(ctx, oldLease)
	require.NoError(t, err)

	info := &QueueConsumerInfo{
		QueueName:    "events",
		GroupID:      "workers@#",
		ConsumerID:   "consumer-2",
		ClientID:     "client-2",
		Pattern:      "#",
		Mode:         "stream",
		ProxyNodeID:  c.nodeID,
		RegisteredAt: time.Now(),
	}
	require.NoError(t, c.RegisterQueueConsumer(ctx, info))

	c.leaseMu.Lock()
	newLease := c.sessionLease
	c.leaseMu.Unlock()

	assert.NotEqual(t, oldLease, newLease)

	key := fmt.Sprintf("%s%s/%s/%s", queueConsumersPrefix, info.QueueName, info.GroupID, info.ConsumerID)
	resp, err := c.client.Get(ctx, key)
	require.NoError(t, err)
	require.Len(t, resp.Kvs, 1)
	assert.Equal(t, int64(newLease), resp.Kvs[0].Lease)
}
