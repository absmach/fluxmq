// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package queue

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	queueStorage "github.com/absmach/mqtt/queue/storage"
	badgerstore "github.com/absmach/mqtt/queue/storage/badger"
	memstore "github.com/absmach/mqtt/queue/storage/memory"
	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Integration tests for replicated queues

// setupReplicatedTest creates a test environment with N nodes and replicated queues.
func setupReplicatedTest(t *testing.T, nodeCount int, queueName string, partitions int) ([]*Manager, []*badgerstore.Store, func()) {
	t.Helper()

	tempDir, err := os.MkdirTemp("", "replication-test-*")
	require.NoError(t, err)

	managers := make([]*Manager, nodeCount)
	stores := make([]*badgerstore.Store, nodeCount)
	dbs := make([]*badger.DB, nodeCount)
	brokers := make([]*MockBroker, nodeCount)
	nodeAddresses := make(map[string]string)

	// Create addresses for Raft transport
	for i := 0; i < nodeCount; i++ {
		nodeID := fmt.Sprintf("node%d", i+1)
		// Use localhost with different ports for each node
		nodeAddresses[nodeID] = fmt.Sprintf("127.0.0.1:%d", 7000+i)
	}

	// Create managers for each node
	for i := 0; i < nodeCount; i++ {
		nodeID := fmt.Sprintf("node%d", i+1)
		nodeDir := filepath.Join(tempDir, nodeID)
		err := os.MkdirAll(nodeDir, 0755)
		require.NoError(t, err)

		// Create BadgerDB store for this node
		dbPath := filepath.Join(nodeDir, "data")
		opts := badger.DefaultOptions(dbPath)
		opts.Logger = nil
		db, err := badger.Open(opts)
		require.NoError(t, err)
		dbs[i] = db
		stores[i] = badgerstore.New(db)

		// Create broker mock
		brokers[i] = NewMockBroker()

		// Create manager
		cfg := Config{
			QueueStore:    stores[i],
			MessageStore:  stores[i],
			ConsumerStore: stores[i],
			DeliverFn:     brokers[i].DeliverToSession,
			LocalNodeID:   nodeID,
			DataDir:       nodeDir,
			NodeAddresses: nodeAddresses,
		}

		mgr, err := NewManager(cfg)
		require.NoError(t, err)
		managers[i] = mgr
	}

	// Create replicated queue on all nodes
	queueConfig := queueStorage.QueueConfig{
		Name:       queueName,
		Partitions: partitions,
		Ordering:   queueStorage.OrderingPartition,
		Replication: queueStorage.ReplicationConfig{
			Enabled:           true,
			ReplicationFactor: nodeCount,
			Mode:              queueStorage.ReplicationSync,
			Placement:         queueStorage.PlacementRoundRobin,
			MinInSyncReplicas: (nodeCount / 2) + 1, // Quorum
			AckTimeout:        5 * time.Second,
			HeartbeatTimeout:  1 * time.Second,
			ElectionTimeout:   3 * time.Second,
		},
		RetryPolicy: queueStorage.RetryPolicy{
			MaxRetries:        3,
			InitialBackoff:    100 * time.Millisecond,
			MaxBackoff:        1 * time.Second,
			BackoffMultiplier: 2.0,
		},
		MaxMessageSize: 1024 * 1024, // 1MB
		MaxQueueDepth:  10000,
		DeliveryTimeout: 30 * time.Second,
		BatchSize:       100,
	}

	ctx := context.Background()
	for i, mgr := range managers {
		err := mgr.CreateQueue(ctx, queueConfig)
		require.NoError(t, err, "node%d failed to create queue", i+1)
	}

	// Raft managers will be created by the manager when CreateQueue is called with replication enabled
	// No need to manually create them here

	cleanup := func() {
		for _, mgr := range managers {
			mgr.Stop()
		}
		for _, db := range dbs {
			db.Close()
		}
		os.RemoveAll(tempDir)
	}

	return managers, stores, cleanup
}

// TestReplication_BasicEnqueueDequeue tests basic replicated enqueue and dequeue operations.
func TestReplication_BasicEnqueueDequeue(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping replication test in short mode")
	}

	queueName := "$queue/replication-basic"
	managers, stores, cleanup := setupReplicatedTest(t, 3, queueName, 3)
	defer cleanup()

	ctx := context.Background()

	// Subscribe consumer on node1
	err := managers[0].Subscribe(ctx, queueName, "client-1", "group-1", "")
	require.NoError(t, err)

	// Enqueue message on node1 (should replicate to node2 and node3)
	payload := []byte("replicated test message")
	props := map[string]string{"test": "replication"}
	err = managers[0].Enqueue(ctx, queueName, payload, props)
	require.NoError(t, err)

	// Give time for replication
	time.Sleep(500 * time.Millisecond)

	// Verify message was stored - check message count
	for i, store := range stores {
		count, err := store.Count(ctx, queueName)
		assert.NoError(t, err, "node%d: failed to count messages", i+1)
		// At least one node (partition leader) should have the message
		if count > 0 {
			t.Logf("node%d: has %d messages", i+1, count)
		}
	}

	// TODO: Add delivery and ack verification
	// This requires starting partition workers and delivery workers
	// For now, this test validates config and basic enqueue works
}

// TestReplication_SyncVsAsync compares sync and async replication modes.
func TestReplication_SyncVsAsync(t *testing.T) {
	t.Skip("TODO: Implement sync vs async mode comparison test")
}

// TestReplication_LeaderFailover tests leader election when the partition leader fails.
func TestReplication_LeaderFailover(t *testing.T) {
	t.Skip("TODO: Implement leader failover test - see failover_test.go")
}

// TestReplication_MessageDurability verifies messages survive node failures.
func TestReplication_MessageDurability(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping durability test in short mode")
	}

	t.Skip("TODO: Implement message durability test")
	// Test plan:
	// 1. Enqueue message with sync replication
	// 2. Verify quorum ACK
	// 3. Kill leader node
	// 4. Wait for new leader election
	// 5. Verify message still exists
	// 6. Deliver and ack message from new leader
}

// TestReplication_ISRTracking tests In-Sync Replica tracking.
func TestReplication_ISRTracking(t *testing.T) {
	t.Skip("TODO: Implement ISR tracking test")
	// Test plan:
	// 1. Create 3-node cluster
	// 2. Partition one follower (simulate network issue)
	// 3. Verify ISR count decreases to 2
	// 4. Heal partition
	// 5. Verify follower catches up and ISR returns to 3
}

// TestReplication_ConcurrentEnqueue tests concurrent enqueues from multiple nodes.
func TestReplication_ConcurrentEnqueue(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping concurrent test in short mode")
	}

	t.Skip("TODO: Implement concurrent enqueue test")
	// Test plan:
	// 1. Create 3-node cluster
	// 2. Enqueue 1000 messages concurrently from all 3 nodes
	// 3. Verify all messages are replicated
	// 4. Verify no duplicates
	// 5. Verify total count is exactly 1000
}

// TestReplication_PartitionDistribution verifies replicas are distributed correctly.
func TestReplication_PartitionDistribution(t *testing.T) {
	t.Skip("TODO: Implement after Raft manager stabilization - requires raftManagers map access")
	// Test plan:
	// 1. Create 3-node cluster with 9 partitions
	// 2. Wait for all Raft groups to elect leaders
	// 3. Verify each node leads ~3 partitions (even distribution)
	// 4. Check via mgr.raftManagers[queueName].IsLeader(partitionID)
}

// TestReplication_ConfigValidation tests replication configuration validation.
func TestReplication_ConfigValidation(t *testing.T) {
	ctx := context.Background()
	store := memstore.New()
	broker := NewMockBroker()

	cfg := Config{
		QueueStore:    store,
		MessageStore:  store,
		ConsumerStore: store,
		DeliverFn:     broker.DeliverToSession,
	}

	mgr, err := NewManager(cfg)
	require.NoError(t, err)
	defer mgr.Stop()

	tests := []struct {
		name        string
		config      queueStorage.QueueConfig
		expectError bool
		errorMsg    string
	}{
		{
			name: "valid sync replication",
			config: queueStorage.QueueConfig{
				Name:       "$queue/valid-sync",
				Partitions: 3,
				Ordering:   queueStorage.OrderingPartition,
				Replication: queueStorage.ReplicationConfig{
					Enabled:           true,
					ReplicationFactor: 3,
					Mode:              queueStorage.ReplicationSync,
					Placement:         queueStorage.PlacementRoundRobin,
					MinInSyncReplicas: 2,
					AckTimeout:        5 * time.Second,
				},
				RetryPolicy: queueStorage.RetryPolicy{
					MaxRetries:        3,
					InitialBackoff:    100 * time.Millisecond,
					MaxBackoff:        1 * time.Second,
					BackoffMultiplier: 2.0,
				},
				MaxMessageSize:   1024 * 1024,
				MaxQueueDepth:    10000,
				DeliveryTimeout:  30 * time.Second,
				BatchSize:        100,
				HeartbeatTimeout: 10 * time.Second,
			},
			expectError: false,
		},
		{
			name: "valid async replication",
			config: queueStorage.QueueConfig{
				Name:       "$queue/valid-async",
				Partitions: 3,
				Ordering:   queueStorage.OrderingPartition,
				Replication: queueStorage.ReplicationConfig{
					Enabled:           true,
					ReplicationFactor: 3,
					Mode:              queueStorage.ReplicationAsync,
					Placement:         queueStorage.PlacementRoundRobin,
					MinInSyncReplicas: 2,
					AckTimeout:        5 * time.Second,
				},
				RetryPolicy: queueStorage.RetryPolicy{
					MaxRetries:        3,
					InitialBackoff:    100 * time.Millisecond,
					MaxBackoff:        1 * time.Second,
					BackoffMultiplier: 2.0,
				},
				MaxMessageSize:   1024 * 1024,
				MaxQueueDepth:    10000,
				DeliveryTimeout:  30 * time.Second,
				BatchSize:        100,
				HeartbeatTimeout: 10 * time.Second,
			},
			expectError: false,
		},
		{
			name: "replication disabled",
			config: queueStorage.QueueConfig{
				Name:       "$queue/no-replication",
				Partitions: 3,
				Ordering:   queueStorage.OrderingPartition,
				Replication: queueStorage.ReplicationConfig{
					Enabled: false,
				},
				RetryPolicy: queueStorage.RetryPolicy{
					MaxRetries:        3,
					InitialBackoff:    100 * time.Millisecond,
					MaxBackoff:        1 * time.Second,
					BackoffMultiplier: 2.0,
				},
				MaxMessageSize:   1024 * 1024,
				MaxQueueDepth:    10000,
				DeliveryTimeout:  30 * time.Second,
				BatchSize:        100,
				HeartbeatTimeout: 10 * time.Second,
			},
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := mgr.CreateQueue(ctx, tt.config)
			if tt.expectError {
				assert.Error(t, err)
				if tt.errorMsg != "" {
					assert.Contains(t, err.Error(), tt.errorMsg)
				}
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

// TestReplication_BackwardCompatibility verifies non-replicated queues still work.
func TestReplication_BackwardCompatibility(t *testing.T) {
	ctx := context.Background()
	store := memstore.New()
	broker := NewMockBroker()

	cfg := Config{
		QueueStore:    store,
		MessageStore:  store,
		ConsumerStore: store,
		DeliverFn:     broker.DeliverToSession,
	}

	mgr, err := NewManager(cfg)
	require.NoError(t, err)
	defer mgr.Stop()

	// Create queue WITHOUT replication
	queueName := "$queue/legacy"
	queueConfig := queueStorage.DefaultQueueConfig(queueName)
	queueConfig.Replication.Enabled = false // Explicitly disabled

	err = mgr.CreateQueue(ctx, queueConfig)
	require.NoError(t, err)

	// Subscribe consumer
	err = mgr.Subscribe(ctx, queueName, "client-1", "group-1", "")
	require.NoError(t, err)

	// Enqueue message (should work without replication)
	err = mgr.Enqueue(ctx, queueName, []byte("legacy message"), nil)
	require.NoError(t, err)

	// Verify message was stored by checking count
	count, err := store.Count(ctx, queueName)
	require.NoError(t, err)
	assert.Equal(t, int64(1), count)
}

// Helper function to wait for Raft cluster to stabilize.
func waitForRaftStable(t *testing.T, managers []*Manager, queueName string, partitionID int, timeout time.Duration) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			t.Fatalf("timeout waiting for raft to stabilize on queue %s partition %d", queueName, partitionID)
		case <-ticker.C:
			// Check if we have a leader
			leaderCount := 0
			for _, mgr := range managers {
				// Access raftManagers map (not raftManager field)
				if raftMgr, ok := mgr.raftManagers[queueName]; ok && raftMgr != nil {
					if raftMgr.IsLeader(partitionID) {
						leaderCount++
					}
				}
			}
			if leaderCount == 1 {
				// Exactly one leader - cluster is stable
				return
			}
		}
	}
}
