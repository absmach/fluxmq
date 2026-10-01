// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package raft

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/absmach/fluxmq/cluster"
	"github.com/absmach/fluxmq/internal/testcert"
	"github.com/absmach/fluxmq/logstorage"
	"github.com/absmach/fluxmq/message"
	"github.com/absmach/fluxmq/queue/types"
	"github.com/stretchr/testify/require"
)

// threeNodeCluster exercises the production manager, disk stores, snapshots,
// and mTLS transport. Node directories and addresses survive a fixture restart.
type threeNodeCluster struct {
	nodes     [3]*threeNodeClusterNode
	tlsConfig *cluster.TransportTLSConfig
}

type threeNodeClusterNode struct {
	id      string
	address string
	dataDir string
	peers   map[string]string
	manager *Manager
	store   *logstorage.Adapter
}

func threeNodeTestDirs(t *testing.T) (string, [3]string) {
	t.Helper()
	root := t.TempDir()

	// Hold all listeners until every address is allocated so the OS cannot
	// accidentally return the same ephemeral port twice.
	var listeners [3]net.Listener
	var addresses [3]string
	for i := range listeners {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		listeners[i] = listener
		addresses[i] = listener.Addr().String()
	}
	for _, listener := range listeners {
		require.NoError(t, listener.Close())
	}
	return root, addresses
}

func newThreeNodeCluster(t *testing.T) *threeNodeCluster {
	t.Helper()
	root, addresses := threeNodeTestDirs(t)
	_, err := testcert.Generate(root)
	require.NoError(t, err)
	return newThreeNodeClusterAt(t, root, addresses)
}

func newThreeNodeClusterAt(t *testing.T, root string, addresses [3]string) *threeNodeCluster {
	t.Helper()
	c := &threeNodeCluster{tlsConfig: &cluster.TransportTLSConfig{
		CertFile: filepath.Join(root, "node.crt"),
		KeyFile:  filepath.Join(root, "node.key"),
		CAFile:   filepath.Join(root, "ca.crt"),
	}}
	t.Cleanup(func() {
		for _, node := range c.nodes {
			if node != nil {
				if err := node.stop(); err != nil {
					t.Errorf("stop %s: %v", node.id, err)
				}
			}
		}
	})

	for i := range c.nodes {
		c.nodes[i] = &threeNodeClusterNode{
			id:      fmt.Sprintf("node-%d", i+1),
			address: addresses[i],
			dataDir: filepath.Join(root, fmt.Sprintf("node-%d", i+1)),
		}
	}
	for _, node := range c.nodes {
		node.peers = make(map[string]string, len(c.nodes)-1)
		for _, peer := range c.nodes {
			if peer != node {
				node.peers[peer.id] = peer.address
			}
		}
	}
	c.startNodes(t, c.nodes[:]...)
	return c
}

func (c *threeNodeCluster) startNodes(t *testing.T, nodes ...*threeNodeClusterNode) {
	t.Helper()
	// Start concurrently so each manager's peer-readiness check finds the
	// other listeners before its bootstrap deadline.
	var starts sync.WaitGroup
	startErrors := make(chan error, len(nodes))
	for _, node := range nodes {
		starts.Add(1)
		go func() {
			defer starts.Done()
			if err := node.start(c.tlsConfig); err != nil {
				startErrors <- fmt.Errorf("%s: %w", node.id, err)
			}
		}()
	}
	starts.Wait()
	close(startErrors)
	for err := range startErrors {
		require.NoError(t, err)
	}
}

func (n *threeNodeClusterNode) start(tlsConfig *cluster.TransportTLSConfig) error {
	if n.manager != nil || n.store != nil {
		return fmt.Errorf("node already started")
	}
	store, err := logstorage.NewAdapter(filepath.Join(n.dataDir, "queues"), logstorage.DefaultAdapterConfig())
	if err != nil {
		return err
	}
	n.store = store
	config := DefaultManagerConfig()
	config.Enabled = true
	n.manager = NewManager(n.id, n.address, n.dataDir, store, store, n.peers, config, tlsConfig,
		slog.New(slog.NewTextHandler(io.Discard, nil)))
	return n.manager.Start(context.Background())
}

func (n *threeNodeClusterNode) stop() error {
	var errs []error
	if n.manager != nil {
		errs = append(errs, n.manager.Stop())
		n.manager = nil
	}
	if n.store != nil {
		errs = append(errs, n.store.Close())
		n.store = nil
	}
	return errors.Join(errs...)
}

func (c *threeNodeCluster) stopNodes(t *testing.T, nodes ...*threeNodeClusterNode) {
	t.Helper()
	for _, node := range nodes {
		require.NoError(t, node.stop(), "stop %s", node.id)
	}
}

func (c *threeNodeCluster) waitForLeader(t *testing.T) *threeNodeClusterNode {
	t.Helper()
	var leader *threeNodeClusterNode
	require.Eventually(t, func() bool {
		leader = nil
		for _, node := range c.nodes {
			if node.manager != nil && node.manager.IsLeader(context.Background()) {
				if leader != nil {
					return false
				}
				leader = node
			}
		}
		if leader == nil {
			return false
		}
		for _, node := range c.nodes {
			if node.manager != nil && node.manager.LeaderID() != leader.id {
				return false
			}
		}
		return true
	}, 15*time.Second, 25*time.Millisecond, "running nodes should agree on one Raft leader")
	return leader
}

func (c *threeNodeCluster) waitForRecord(t *testing.T, queueName string, offset uint64, id string, payload []byte) {
	t.Helper()
	for _, node := range c.nodes {
		if node.store == nil {
			continue
		}
		require.Eventuallyf(t, func() bool {
			record, err := node.store.Read(context.Background(), queueName, offset)
			if err != nil {
				return false
			}
			defer message.Release(record)
			return record.PublisherMeta.MessageID == id && bytes.Equal(record.PayloadBytes(), payload)
		}, 15*time.Second, 25*time.Millisecond, "%s should apply record at offset %d", node.id, offset)
	}
}

func appendTestRecord(t *testing.T, leader *threeNodeClusterNode, queueName, id string, payload []byte) uint64 {
	t.Helper()
	record := newQueuedEnvelope(id, "jobs/ready", payload)
	offset, err := leader.manager.ApplyAppend(context.Background(), queueName, record)
	message.Release(record)
	require.NoError(t, err)
	return offset
}

func createTestReplicatedQueue(t *testing.T, leader *threeNodeClusterNode) string {
	t.Helper()
	queue := types.DefaultQueueConfig("replicated-jobs", "jobs/#")
	queue.Replication.Enabled = true
	queue.Replication.Group = DefaultGroupID
	require.NoError(t, leader.manager.ApplyCreateQueue(context.Background(), queue))
	return queue.Name
}

func TestThreeNodeRaftReplicatesAppend(t *testing.T) {
	c := newThreeNodeCluster(t)
	leader := c.waitForLeader(t)
	queueName := createTestReplicatedQueue(t, leader)

	payload := []byte("replicated payload")
	offset := appendTestRecord(t, leader, queueName, "replicated-1", payload)
	require.Equal(t, uint64(0), offset)
	c.waitForRecord(t, queueName, offset, "replicated-1", payload)
}

func TestThreeNodeRaftLeaderFailoverAndRestart(t *testing.T) {
	c := newThreeNodeCluster(t)
	leader := c.waitForLeader(t)
	queueName := createTestReplicatedQueue(t, leader)
	payload := []byte("survive leader loss")
	offset := appendTestRecord(t, leader, queueName, "failover-1", payload)
	require.Equal(t, uint64(0), offset)

	c.stopNodes(t, leader)
	// Stop must release the Raft listener, or the restart below cannot bind.
	listener, err := net.Listen("tcp", leader.address)
	require.NoError(t, err, "stopped manager should release %s", leader.address)
	require.NoError(t, listener.Close())
	replacement := c.waitForLeader(t)
	require.NotEqual(t, leader.id, replacement.id)
	c.waitForRecord(t, queueName, offset, "failover-1", payload)

	c.startNodes(t, leader)
	c.waitForRecord(t, queueName, offset, "failover-1", payload)
	for _, node := range c.nodes {
		count, err := node.store.Count(context.Background(), queueName)
		require.NoError(t, err)
		require.Equal(t, uint64(1), count, "%s should hold the record exactly once", node.id)
	}
}

func TestThreeNodeRaftRejectsWriteWithoutQuorum(t *testing.T) {
	c := newThreeNodeCluster(t)
	leader := c.waitForLeader(t)
	queueName := createTestReplicatedQueue(t, leader)

	var followers []*threeNodeClusterNode
	for _, node := range c.nodes {
		if node != leader {
			followers = append(followers, node)
		}
	}
	c.stopNodes(t, followers...)
	record := newQueuedEnvelope("no-quorum-1", "jobs/ready", []byte("unconfirmed"))
	_, err := leader.manager.ApplyAppendWithOptions(context.Background(), queueName, record, ApplyOptions{AckTimeout: time.Second})
	message.Release(record)
	require.Error(t, err, "a one-node minority must not report a committed write")
	// A timed-out submission may still commit after quorum returns. Its result
	// is unconfirmed, not proof that the record is absent or safe to retry.
}

// A queue's settings count as Raft state only once the log or a snapshot
// carries them. A node-local copy, or a create that found one, does not.
func TestThreeNodeRaftTracksRecordedQueueConfig(t *testing.T) {
	c := newThreeNodeCluster(t)
	leader := c.waitForLeader(t)
	ctx := context.Background()

	configured := types.DefaultQueueConfig("configured-jobs", "configured/#")
	configured.Replication.Enabled = true
	configured.Replication.Group = DefaultGroupID
	for _, node := range c.nodes {
		require.NoError(t, node.store.CreateQueue(ctx, configured))
	}
	appendTestRecord(t, leader, configured.Name, "configured-1", []byte("before settings"))
	c.waitForRecord(t, configured.Name, 0, "configured-1", []byte("before settings"))
	c.requireConfigRecorded(t, configured.Name, false, "a node-local queue is not raft state")

	require.NoError(t, leader.manager.ApplyCreateQueue(ctx, configured))
	c.requireConfigRecorded(t, configured.Name, false, "a create that found the queue did not set its settings")

	require.NoError(t, leader.manager.ApplyUpdateQueue(ctx, configured))
	c.eventuallyConfigRecorded(t, configured.Name, true, "an applied update records the settings")

	require.NoError(t, leader.manager.ApplyDeleteQueue(ctx, configured.Name))
	c.eventuallyConfigRecorded(t, configured.Name, false, "a deleted queue has no recorded settings")
}

// A create applies its settings only on replicas that lacked the queue. Until
// the update lands, a replica holding other settings keeps them, so the queue
// must not count as recorded anywhere, the leader included.
func TestThreeNodeRaftCreateAloneDoesNotRecordQueueConfig(t *testing.T) {
	c := newThreeNodeCluster(t)
	leader := c.waitForLeader(t)
	ctx := context.Background()

	var stale *threeNodeClusterNode
	for _, node := range c.nodes {
		if node != leader {
			stale = node
			break
		}
	}
	configured := types.DefaultQueueConfig("configured-jobs", "configured/#")
	configured.Replication.Enabled = true
	configured.Replication.Group = DefaultGroupID
	staleSettings := configured
	staleSettings.Topics = []string{"$queue/configured-jobs/#"}
	staleSettings.Durable = false
	require.NoError(t, stale.store.CreateQueue(ctx, staleSettings))

	// The leader lacks the queue, so this create makes it there; the update
	// that would follow it is taken to have failed.
	require.NoError(t, leader.manager.ApplyCreateQueue(ctx, configured))
	require.Eventually(t, func() bool {
		_, err := c.nodes[0].store.GetQueue(ctx, configured.Name)
		_, err2 := c.nodes[1].store.GetQueue(ctx, configured.Name)
		_, err3 := c.nodes[2].store.GetQueue(ctx, configured.Name)
		return err == nil && err2 == nil && err3 == nil
	}, 15*time.Second, 25*time.Millisecond, "create should reach every node")
	staleNow, err := stale.store.GetQueue(ctx, configured.Name)
	require.NoError(t, err)
	require.Equal(t, staleSettings.Topics, staleNow.Topics, "create must not have repaired the stale replica")
	c.requireConfigRecorded(t, configured.Name, false, "a create alone must not open the write gate")

	require.NoError(t, leader.manager.ApplyUpdateQueue(ctx, configured))
	c.eventuallyConfigRecorded(t, configured.Name, true, "the update records the settings everywhere")
	repaired, err := stale.store.GetQueue(ctx, configured.Name)
	require.NoError(t, err)
	require.Equal(t, configured.Topics, repaired.Topics, "the update should repair the stale replica")
}

func (c *threeNodeCluster) requireConfigRecorded(t *testing.T, queueName string, want bool, msg string) {
	t.Helper()
	for _, node := range c.nodes {
		require.Equal(t, want, node.manager.IsQueueConfigRecorded(queueName), "%s: %s", node.id, msg)
	}
}

func (c *threeNodeCluster) eventuallyConfigRecorded(t *testing.T, queueName string, want bool, msg string) {
	t.Helper()
	for _, node := range c.nodes {
		require.Eventuallyf(t, func() bool {
			return node.manager.IsQueueConfigRecorded(queueName) == want
		}, 15*time.Second, 25*time.Millisecond, "%s: %s", node.id, msg)
	}
}

// Queue settings operations report success or trigger a retry on the answer, so
// an async-configured group must still wait for the commit.
func TestThreeNodeRaftQueueSettingsAreSynchronousInAsyncMode(t *testing.T) {
	c := newThreeNodeCluster(t)
	leader := c.waitForLeader(t)
	leader.manager.config.SyncMode = false
	ctx := context.Background()

	configured := types.DefaultQueueConfig("async-jobs", "async/#")
	configured.Replication.Enabled = true
	configured.Replication.Group = DefaultGroupID

	require.NoError(t, leader.manager.ApplyCreateQueue(ctx, configured))
	require.NoError(t, leader.manager.ApplyUpdateQueue(ctx, configured))
	require.True(t, leader.manager.IsQueueConfigRecorded(configured.Name),
		"the update returned before the leader applied it")

	require.NoError(t, leader.manager.ApplyDeleteQueue(ctx, configured.Name))
	require.False(t, leader.manager.IsQueueConfigRecorded(configured.Name),
		"the delete returned before the leader applied it")
}
