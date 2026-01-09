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
	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestRetention_ReplicationBasic tests that size-based retention operations are replicated.
func TestRetention_ReplicationBasic(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping retention integration test in short mode")
	}

	queueName := "$queue/retention-repl-basic"
	managers, stores, cleanup := setupReplicatedTestWithRetention(t, 3, queueName, 1)
	defer cleanup()

	ctx := context.Background()

	// Find the leader for partition 0
	var leaderMgr *Manager
	for _, mgr := range managers {
		if raftMgr, ok := mgr.raftManagers[queueName]; ok && raftMgr != nil {
			if raftMgr.IsLeader(0) {
				leaderMgr = mgr
				break
			}
		}
	}
	require.NotNil(t, leaderMgr, "no leader found")

	// Enqueue 10 messages on the leader
	for i := 0; i < 10; i++ {
		err := leaderMgr.Enqueue(ctx, queueName, []byte(fmt.Sprintf("message %d", i)), nil)
		require.NoError(t, err)
	}

	// Wait for replication
	time.Sleep(200 * time.Millisecond)

	// Verify all nodes have 10 messages
	for i, store := range stores {
		count, err := store.Count(ctx, queueName)
		require.NoError(t, err, "node%d: failed to get count", i+1)
		assert.Equal(t, int64(10), count, "node%d: wrong message count", i+1)
	}

	// Trigger retention on leader (should delete 5 oldest messages to get to limit of 5)
	retentionMgr := leaderMgr.retentionManagers[queueName]
	require.NotNil(t, retentionMgr, "retention manager not found")

	// Call CheckSizeRetention 5 times to trigger the check (SizeCheckEvery=5 in setup)
	var totalDeleted int64
	for i := 0; i < 5; i++ {
		deleted, _, err := retentionMgr.CheckSizeRetention(ctx, 0)
		require.NoError(t, err)
		totalDeleted += deleted
	}

	assert.Equal(t, int64(5), totalDeleted, "should have deleted 5 messages")

	// Wait for replication of deletions
	time.Sleep(500 * time.Millisecond)

	// Verify all nodes now have 5 messages (deletions replicated)
	for i, store := range stores {
		count, err := store.Count(ctx, queueName)
		require.NoError(t, err, "node%d: failed to get count after retention", i+1)
		assert.Equal(t, int64(5), count, "node%d: deletions not replicated", i+1)
	}

	// Verify the oldest messages were deleted on all nodes
	for i, store := range stores {
		messages, err := store.ListOldestMessages(ctx, queueName, 0, 10)
		require.NoError(t, err, "node%d: failed to list messages", i+1)
		assert.Len(t, messages, 5, "node%d: wrong number of messages", i+1)
		// First remaining message should be sequence 6 (sequences 1-5 were deleted)
		if len(messages) > 0 {
			assert.Equal(t, uint64(6), messages[0].Sequence, "node%d: wrong oldest message", i+1)
		}
	}
}

// TestRetention_LeaderOnly tests that only the leader runs retention cleanup.
func TestRetention_LeaderOnly(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping retention integration test in short mode")
	}

	queueName := "$queue/retention-leader-only"
	managers, stores, cleanup := setupReplicatedTestWithRetention(t, 3, queueName, 1)
	defer cleanup()

	ctx := context.Background()

	// Find leader and followers
	var leaderMgr *Manager
	var followerMgrs []*Manager
	for _, mgr := range managers {
		if raftMgr, ok := mgr.raftManagers[queueName]; ok && raftMgr != nil {
			if raftMgr.IsLeader(0) {
				leaderMgr = mgr
			} else {
				followerMgrs = append(followerMgrs, mgr)
			}
		}
	}
	require.NotNil(t, leaderMgr, "no leader found")
	require.Len(t, followerMgrs, 2, "should have 2 followers")

	// Enqueue messages
	for i := 0; i < 10; i++ {
		err := leaderMgr.Enqueue(ctx, queueName, []byte(fmt.Sprintf("message %d", i)), nil)
		require.NoError(t, err)
	}

	time.Sleep(200 * time.Millisecond)

	// Try to run retention on a follower - should fail or be no-op
	followerRetentionMgr := followerMgrs[0].retentionManagers[queueName]
	require.NotNil(t, followerRetentionMgr)

	// Call CheckSizeRetention on follower - should not delete anything
	// because followers don't execute retention operations
	for i := 0; i < 5; i++ {
		deleted, _, _ := followerRetentionMgr.CheckSizeRetention(ctx, 0)
		// On follower, this should either return 0 or the operation should be rejected
		assert.Equal(t, int64(0), deleted, "follower should not delete messages")
	}

	// Verify all nodes still have 10 messages
	for i, store := range stores {
		count, err := store.Count(ctx, queueName)
		require.NoError(t, err, "node%d", i+1)
		assert.Equal(t, int64(10), count, "node%d: follower retention should not affect count", i+1)
	}

	// Now run retention on leader
	leaderRetentionMgr := leaderMgr.retentionManagers[queueName]
	var totalDeleted int64
	for i := 0; i < 5; i++ {
		deleted, _, err := leaderRetentionMgr.CheckSizeRetention(ctx, 0)
		require.NoError(t, err)
		totalDeleted += deleted
	}
	assert.Greater(t, totalDeleted, int64(0), "leader should delete messages")

	time.Sleep(500 * time.Millisecond)

	// Verify all nodes now have 5 messages
	for i, store := range stores {
		count, err := store.Count(ctx, queueName)
		require.NoError(t, err, "node%d", i+1)
		assert.Equal(t, int64(5), count, "node%d: leader retention should replicate", i+1)
	}
}

// TestRetention_TimeBasedReplication tests time-based retention replication.
func TestRetention_TimeBasedReplication(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping retention integration test in short mode")
	}

	queueName := "$queue/retention-time-repl"
	managers, stores, cleanup := setupReplicatedTestWithTimeRetention(t, 3, queueName, 1)
	defer cleanup()

	ctx := context.Background()

	// Find the leader
	var leaderMgr *Manager
	for _, mgr := range managers {
		if raftMgr, ok := mgr.raftManagers[queueName]; ok && raftMgr != nil {
			if raftMgr.IsLeader(0) {
				leaderMgr = mgr
				break
			}
		}
	}
	require.NotNil(t, leaderMgr, "no leader found")

	// Enqueue old messages (200ms ago) directly to ALL stores to control CreatedAt
	// Since we're bypassing Raft, we need to insert on all nodes manually
	oldTime := time.Now().Add(-200 * time.Millisecond)
	for i := 0; i < 3; i++ {
		for _, store := range stores {
			msg := &queueStorage.Message{
				ID:          fmt.Sprintf("old-msg-%d", i),
				Topic:       queueName,
				Payload:     []byte("old message"),
				Sequence:    uint64(i + 1), // Sequences start at 1
				State:       queueStorage.StateQueued,
				CreatedAt:   oldTime,
				PartitionID: 0,
			}
			err := store.Enqueue(ctx, queueName, msg)
			require.NoError(t, err)
		}
	}

	// Enqueue new messages on all stores
	for i := 3; i < 6; i++ {
		for _, store := range stores {
			msg := &queueStorage.Message{
				ID:          fmt.Sprintf("new-msg-%d", i),
				Topic:       queueName,
				Payload:     []byte("new message"),
				Sequence:    uint64(i + 1), // Sequences start at 1
				State:       queueStorage.StateQueued,
				CreatedAt:   time.Now(),
				PartitionID: 0,
			}
			err := store.Enqueue(ctx, queueName, msg)
			require.NoError(t, err)
		}
	}

	// Verify all nodes have 6 messages
	for i, store := range stores {
		count, err := store.Count(ctx, queueName)
		require.NoError(t, err, "node%d", i+1)
		assert.Equal(t, int64(6), count, "node%d", i+1)
	}

	// Start retention manager on leader (time-based cleanup will run automatically)
	retentionMgr := leaderMgr.retentionManagers[queueName]
	require.NotNil(t, retentionMgr)

	ctxWithCancel, cancel := context.WithCancel(ctx)
	defer cancel()

	go retentionMgr.Start(ctxWithCancel, 0)

	// Wait for time-based cleanup to run (check interval is 50ms, wait only 70ms
	// so new messages don't become old)
	time.Sleep(70 * time.Millisecond)

	// Stop retention manager
	retentionMgr.Stop()

	// Wait for replication
	time.Sleep(500 * time.Millisecond)

	// Verify all nodes now have 3 messages (old messages deleted)
	for i, store := range stores {
		count, err := store.Count(ctx, queueName)
		require.NoError(t, err, "node%d: failed to get count", i+1)
		assert.Equal(t, int64(3), count, "node%d: time-based deletions not replicated", i+1)
	}
}

// setupReplicatedTestWithRetention creates a test environment with retention policies.
func setupReplicatedTestWithRetention(t *testing.T, nodeCount int, queueName string, partitions int) ([]*Manager, []*badgerstore.Store, func()) {
	t.Helper()

	tempDir, err := os.MkdirTemp("", "retention-repl-test-*")
	require.NoError(t, err)

	managers := make([]*Manager, nodeCount)
	stores := make([]*badgerstore.Store, nodeCount)
	dbs := make([]*badger.DB, nodeCount)
	brokers := make([]*MockBroker, nodeCount)
	nodeAddresses := make(map[string]string)

	// Create addresses for Raft transport
	for i := 0; i < nodeCount; i++ {
		nodeID := fmt.Sprintf("node%d", i+1)
		nodeAddresses[nodeID] = fmt.Sprintf("127.0.0.1:%d", 7100+i)
	}

	// Create managers for each node
	for i := 0; i < nodeCount; i++ {
		nodeID := fmt.Sprintf("node%d", i+1)
		nodeDir := filepath.Join(tempDir, nodeID)
		err := os.MkdirAll(nodeDir, 0755)
		require.NoError(t, err)

		// Create BadgerDB store
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

	// Create replicated queue with retention policy
	queueConfig := queueStorage.QueueConfig{
		Name:       queueName,
		Partitions: partitions,
		Ordering:   queueStorage.OrderingPartition,
		Replication: queueStorage.ReplicationConfig{
			Enabled:           true,
			ReplicationFactor: nodeCount,
			Mode:              queueStorage.ReplicationSync,
			Placement:         queueStorage.PlacementRoundRobin,
			MinInSyncReplicas: (nodeCount / 2) + 1,
			AckTimeout:        5 * time.Second,
			HeartbeatTimeout:  1 * time.Second,
			ElectionTimeout:   3 * time.Second,
		},
		Retention: queueStorage.RetentionPolicy{
			RetentionMessages: 5,              // Keep max 5 messages
			SizeCheckEvery:    5,              // Check every 5 enqueues
			RetentionBytes:    0,              // No byte limit
			RetentionTime:     0,              // No time limit
			TimeCheckInterval: 5 * time.Minute, // Not used for size-based
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
		MessageTTL:       24 * time.Hour,
	}

	ctx := context.Background()
	for i, mgr := range managers {
		err := mgr.CreateQueue(ctx, queueConfig)
		require.NoError(t, err, "node%d failed to create queue", i+1)
	}

	// Start Raft partitions on all nodes
	for i, mgr := range managers {
		raftMgr, exists := mgr.raftManagers[queueName]
		require.True(t, exists, "node%d: raft manager not found", i+1)
		require.NotNil(t, raftMgr, "node%d: raft manager is nil", i+1)

		for partID := 0; partID < partitions; partID++ {
			err := raftMgr.StartPartition(ctx, partID, partitions)
			require.NoError(t, err, "node%d: failed to start partition %d", i+1, partID)
		}
	}

	// Wait for leader election on all partitions
	for partID := 0; partID < partitions; partID++ {
		waitForRaftStable(t, managers, queueName, partID, 10*time.Second)
		t.Logf("Partition %d: leader elected", partID)
	}

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

// setupReplicatedTestWithTimeRetention creates a test environment with time-based retention.
func setupReplicatedTestWithTimeRetention(t *testing.T, nodeCount int, queueName string, partitions int) ([]*Manager, []*badgerstore.Store, func()) {
	t.Helper()

	tempDir, err := os.MkdirTemp("", "retention-time-repl-test-*")
	require.NoError(t, err)

	managers := make([]*Manager, nodeCount)
	stores := make([]*badgerstore.Store, nodeCount)
	dbs := make([]*badger.DB, nodeCount)
	brokers := make([]*MockBroker, nodeCount)
	nodeAddresses := make(map[string]string)

	for i := 0; i < nodeCount; i++ {
		nodeID := fmt.Sprintf("node%d", i+1)
		nodeAddresses[nodeID] = fmt.Sprintf("127.0.0.1:%d", 7200+i)
	}

	for i := 0; i < nodeCount; i++ {
		nodeID := fmt.Sprintf("node%d", i+1)
		nodeDir := filepath.Join(tempDir, nodeID)
		err := os.MkdirAll(nodeDir, 0755)
		require.NoError(t, err)

		dbPath := filepath.Join(nodeDir, "data")
		opts := badger.DefaultOptions(dbPath)
		opts.Logger = nil
		db, err := badger.Open(opts)
		require.NoError(t, err)
		dbs[i] = db
		stores[i] = badgerstore.New(db)

		brokers[i] = NewMockBroker()

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

	queueConfig := queueStorage.QueueConfig{
		Name:       queueName,
		Partitions: partitions,
		Ordering:   queueStorage.OrderingPartition,
		Replication: queueStorage.ReplicationConfig{
			Enabled:           true,
			ReplicationFactor: nodeCount,
			Mode:              queueStorage.ReplicationSync,
			Placement:         queueStorage.PlacementRoundRobin,
			MinInSyncReplicas: (nodeCount / 2) + 1,
			AckTimeout:        5 * time.Second,
			HeartbeatTimeout:  1 * time.Second,
			ElectionTimeout:   3 * time.Second,
		},
		Retention: queueStorage.RetentionPolicy{
			RetentionMessages: 0,                     // No message limit
			SizeCheckEvery:    100,                   // Not used for time-based
			RetentionBytes:    0,                     // No byte limit
			RetentionTime:     100 * time.Millisecond, // Messages older than 100ms are deleted
			TimeCheckInterval: 50 * time.Millisecond,  // Check every 50ms
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
		MessageTTL:       24 * time.Hour,
	}

	ctx := context.Background()
	for i, mgr := range managers {
		err := mgr.CreateQueue(ctx, queueConfig)
		require.NoError(t, err, "node%d failed to create queue", i+1)
	}

	for i, mgr := range managers {
		raftMgr, exists := mgr.raftManagers[queueName]
		require.True(t, exists, "node%d: raft manager not found", i+1)
		require.NotNil(t, raftMgr, "node%d: raft manager is nil", i+1)

		for partID := 0; partID < partitions; partID++ {
			err := raftMgr.StartPartition(ctx, partID, partitions)
			require.NoError(t, err, "node%d: failed to start partition %d", i+1, partID)
		}
	}

	for partID := 0; partID < partitions; partID++ {
		waitForRaftStable(t, managers, queueName, partID, 10*time.Second)
		t.Logf("Partition %d: leader elected", partID)
	}

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
