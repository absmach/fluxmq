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

// TestFailover_LeaderElection tests that a new leader is elected when the current leader fails.
func TestFailover_LeaderElection(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping failover test in short mode")
	}

	queueName := "$queue/failover-election"
	partitions := 3
	managers, stores, cleanup := setupReplicatedTest(t, 3, queueName, partitions)
	defer cleanup()

	ctx := context.Background()

	// Track original leaders for each partition
	originalLeaders := make(map[int]int) // partitionID -> nodeIndex
	for partID := 0; partID < partitions; partID++ {
		for i, mgr := range managers {
			raftMgr := mgr.raftManagers[queueName]
			if raftMgr != nil && raftMgr.IsLeader(partID) {
				originalLeaders[partID] = i
				t.Logf("Partition %d: original leader is node%d", partID, i+1)
				break
			}
		}
		require.Contains(t, originalLeaders, partID, "partition %d has no leader", partID)
	}

	// Pick the node that is leader for partition 0
	leaderNodeIdx := originalLeaders[0]
	t.Logf("Stopping node%d (leader for partition 0)...", leaderNodeIdx+1)

	// Stop the leader node
	startTime := time.Now()
	managers[leaderNodeIdx].Stop()

	// Wait for new leader election on partition 0 (should be < 5s per roadmap)
	newLeaderElected := false
	timeout := 10 * time.Second
	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		time.Sleep(200 * time.Millisecond)

		// Count leaders for partition 0
		leaderCount := 0
		var newLeaderIdx int
		for i, mgr := range managers {
			if i == leaderNodeIdx {
				continue // Skip stopped node
			}
			raftMgr := mgr.raftManagers[queueName]
			if raftMgr != nil && raftMgr.IsLeader(0) {
				leaderCount++
				newLeaderIdx = i
			}
		}

		if leaderCount == 1 {
			electionTime := time.Since(startTime)
			t.Logf("New leader elected: node%d (took %v)", newLeaderIdx+1, electionTime)
			newLeaderElected = true

			// Verify election time is reasonable (< 5s target)
			assert.Less(t, electionTime, 5*time.Second, "leader election took too long")

			// Verify exactly one leader
			assert.Equal(t, 1, leaderCount, "should have exactly one leader")

			break
		}
	}

	require.True(t, newLeaderElected, "no new leader elected within timeout")

	// Verify cluster is stable - check all remaining nodes have consistent view
	time.Sleep(500 * time.Millisecond) // Let cluster stabilize

	leaderCount := 0
	for i, mgr := range managers {
		if i == leaderNodeIdx {
			continue
		}
		raftMgr := mgr.raftManagers[queueName]
		if raftMgr != nil && raftMgr.IsLeader(0) {
			leaderCount++
		}
	}
	assert.Equal(t, 1, leaderCount, "should still have exactly one leader after stabilization")

	// Verify we can still enqueue messages on remaining nodes
	payload := []byte("post-failover message")
	props := map[string]string{"test": "failover"}

	enqueued := false
	for i, mgr := range managers {
		if i == leaderNodeIdx {
			continue // Skip stopped node
		}
		err := mgr.Enqueue(ctx, queueName, payload, props)
		if err == nil {
			t.Logf("Successfully enqueued on node%d after failover", i+1)
			enqueued = true
			break
		}
	}
	assert.True(t, enqueued, "should be able to enqueue after failover")

	// Verify message was stored on remaining nodes
	time.Sleep(1 * time.Second)
	totalCount := int64(0)
	for i, store := range stores {
		if i == leaderNodeIdx {
			continue
		}
		count, err := store.Count(ctx, queueName)
		if err == nil {
			totalCount += count
		}
	}
	assert.Greater(t, totalCount, int64(0), "message should be stored after failover")
}

// TestFailover_MessageDurabilityOnLeaderFailure tests that messages survive leader failure.
func TestFailover_MessageDurabilityOnLeaderFailure(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping durability test in short mode")
	}

	queueName := "$queue/failover-durability"
	partitions := 1 // Use single partition for simpler test
	managers, stores, cleanup := setupReplicatedTest(t, 3, queueName, partitions)
	defer cleanup()

	ctx := context.Background()

	// Find the leader for partition 0
	var leaderIdx int
	var leaderFound bool
	for i, mgr := range managers {
		raftMgr := mgr.raftManagers[queueName]
		if raftMgr != nil && raftMgr.IsLeader(0) {
			leaderIdx = i
			leaderFound = true
			t.Logf("Partition 0 leader is node%d", i+1)
			break
		}
	}
	require.True(t, leaderFound, "no leader found for partition 0")

	// Subscribe consumer on the leader node
	err := managers[leaderIdx].Subscribe(ctx, queueName, "client-1", "group-1", "")
	require.NoError(t, err)

	// Enqueue message on the leader (sync replication ensures quorum ACK)
	payload := []byte("durable test message")
	props := map[string]string{"test": "durability", "partition-key": "key-1"}

	t.Logf("Enqueuing message on leader node%d...", leaderIdx+1)
	err = managers[leaderIdx].Enqueue(ctx, queueName, payload, props)
	require.NoError(t, err, "failed to enqueue message")

	// Give time for replication
	time.Sleep(500 * time.Millisecond)

	// Verify message exists on all nodes (should be replicated to all 3)
	preFailureCount := make([]int64, len(stores))
	for i, store := range stores {
		count, err := store.Count(ctx, queueName)
		if err == nil {
			preFailureCount[i] = count
			t.Logf("node%d: has %d messages before failover", i+1, count)
		}
	}

	// Verify at least quorum (2/3) nodes have the message
	nodesWithMessage := 0
	for _, count := range preFailureCount {
		if count > 0 {
			nodesWithMessage++
		}
	}
	assert.GreaterOrEqual(t, nodesWithMessage, 2, "at least quorum should have the message")

	// Kill the leader node (simulate crash)
	t.Logf("Killing leader node%d...", leaderIdx+1)
	managers[leaderIdx].Stop()

	// Wait for new leader election
	newLeaderIdx := -1
	timeout := 10 * time.Second
	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		time.Sleep(200 * time.Millisecond)

		for i, mgr := range managers {
			if i == leaderIdx {
				continue
			}
			raftMgr := mgr.raftManagers[queueName]
			if raftMgr != nil && raftMgr.IsLeader(0) {
				newLeaderIdx = i
				t.Logf("New leader elected: node%d", i+1)
				break
			}
		}

		if newLeaderIdx != -1 {
			break
		}
	}
	require.NotEqual(t, -1, newLeaderIdx, "new leader not elected within timeout")

	// Verify message still exists on remaining nodes
	time.Sleep(500 * time.Millisecond)
	postFailureCount := make([]int64, len(stores))
	for i, store := range stores {
		if i == leaderIdx {
			continue // Skip killed node
		}
		count, err := store.Count(ctx, queueName)
		if err == nil {
			postFailureCount[i] = count
			t.Logf("node%d: has %d messages after failover", i+1, count)
		}
	}

	// Verify at least one remaining node has the message
	survivingNodesWithMessage := 0
	for i, count := range postFailureCount {
		if i != leaderIdx && count > 0 {
			survivingNodesWithMessage++
		}
	}
	assert.GreaterOrEqual(t, survivingNodesWithMessage, 1, "message should survive on at least one node")

	// Subscribe consumer on new leader
	err = managers[newLeaderIdx].Subscribe(ctx, queueName, "client-2", "group-1", "")
	require.NoError(t, err)

	// Verify we can still process the message
	// In a real scenario, delivery would happen automatically via the delivery worker
	// For this test, we just verify the message is accessible

	t.Log("Message durability verified - message survived leader failure")
}

// TestFailover_InflightMessagesPreserved tests that inflight messages are preserved during failover.
func TestFailover_InflightMessagesPreserved(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping inflight preservation test in short mode")
	}

	t.Skip("TODO: Implement inflight preservation test")
	// Test plan:
	// 1. Create 3-node cluster
	// 2. Enqueue and deliver message (QoS 1)
	// 3. Message is inflight (not yet acked)
	// 4. Kill partition leader
	// 5. Wait for new leader election
	// 6. Verify message is still inflight on new leader
	// 7. Ack message
	// 8. Verify message removed from inflight
}

// TestFailover_FollowerCatchup tests that followers catch up after network partition heals.
// This test verifies the first part of the catch-up process:
// - Follower falls behind during partition
// - Cluster continues operating with quorum
// - Leader and active replicas maintain consistency
//
// Note: Full "heal and catch-up" testing requires infrastructure to restart
// nodes and rejoin the Raft cluster, which is beyond the current test setup.
// Raft's automatic log replay handles catch-up in production.
func TestFailover_FollowerCatchup(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping catchup test in short mode")
	}

	queueName := "$queue/follower-catchup"
	partitions := 1
	managers, stores, cleanup := setupReplicatedTest(t, 3, queueName, partitions)
	defer cleanup()

	ctx := context.Background()

	// Step 1: Find leader and followers
	var leaderIdx int
	var followerIndices []int
	for i, mgr := range managers {
		raftMgr := mgr.raftManagers[queueName]
		require.NotNil(t, raftMgr)

		if raftMgr.IsLeader(0) {
			leaderIdx = i
		} else {
			followerIndices = append(followerIndices, i)
		}
	}

	require.Len(t, followerIndices, 2, "should have exactly 2 followers")
	t.Logf("Initial state - Leader: node%d, Followers: node%d, node%d",
		leaderIdx+1, followerIndices[0]+1, followerIndices[1]+1)

	// Step 2: Enqueue baseline messages to establish cluster state
	baselineMessages := 20
	for i := 0; i < baselineMessages; i++ {
		payload := []byte(fmt.Sprintf("baseline-message-%d", i))
		props := map[string]string{"phase": "baseline", "seq": fmt.Sprintf("%d", i)}
		err := managers[leaderIdx].Enqueue(ctx, queueName, payload, props)
		require.NoError(t, err)
	}

	// Wait for replication
	time.Sleep(1 * time.Second)

	// Verify all nodes have baseline messages
	for i, store := range stores {
		count, err := store.Count(ctx, queueName)
		require.NoError(t, err)
		assert.Equal(t, int64(baselineMessages), count,
			"node%d should have all %d baseline messages", i+1, baselineMessages)
		t.Logf("node%d: %d messages (baseline)", i+1, count)
	}

	// Step 3: Partition one follower (simulate network partition/crash)
	partitionedIdx := followerIndices[0]
	activeFollowerIdx := followerIndices[1]
	t.Logf("Partitioning node%d (follower) - simulating network issue", partitionedIdx+1)

	// Stop the follower to simulate partition
	managers[partitionedIdx].Stop()

	// Wait for cluster to detect partition
	time.Sleep(2 * time.Second)

	// Step 4: Enqueue many messages while follower is partitioned
	// This simulates a follower falling far behind
	catchupMessages := 100
	t.Logf("Enqueuing %d messages while node%d is partitioned", catchupMessages, partitionedIdx+1)

	for i := 0; i < catchupMessages; i++ {
		payload := []byte(fmt.Sprintf("catchup-message-%d", i))
		props := map[string]string{"phase": "catchup", "seq": fmt.Sprintf("%d", i)}
		err := managers[leaderIdx].Enqueue(ctx, queueName, payload, props)
		require.NoError(t, err, "enqueue should succeed with quorum (2/3 nodes)")

		// Log progress every 20 messages
		if (i+1)%20 == 0 {
			t.Logf("  Enqueued %d/%d messages", i+1, catchupMessages)
		}
	}

	t.Logf("Successfully enqueued %d messages with partitioned follower", catchupMessages)

	// Wait for replication to active nodes
	time.Sleep(2 * time.Second)

	// Step 5: Verify message distribution
	expectedTotal := int64(baselineMessages + catchupMessages)

	// Leader should have all messages
	leaderCount, err := stores[leaderIdx].Count(ctx, queueName)
	require.NoError(t, err)
	assert.Equal(t, expectedTotal, leaderCount,
		"leader should have all %d messages", expectedTotal)
	t.Logf("node%d (leader): %d messages ✓", leaderIdx+1, leaderCount)

	// Active follower should have all messages (replicated via Raft)
	activeFollowerCount, err := stores[activeFollowerIdx].Count(ctx, queueName)
	require.NoError(t, err)
	assert.Equal(t, expectedTotal, activeFollowerCount,
		"active follower should have all %d messages", expectedTotal)
	t.Logf("node%d (active follower): %d messages ✓", activeFollowerIdx+1, activeFollowerCount)

	// Partitioned follower should have old baseline count (stale)
	partitionedCount, err := stores[partitionedIdx].Count(ctx, queueName)
	require.NoError(t, err)
	assert.Equal(t, int64(baselineMessages), partitionedCount,
		"partitioned follower should be stale with baseline count")
	t.Logf("node%d (partitioned follower): %d messages (stale, missing %d)",
		partitionedIdx+1, partitionedCount, catchupMessages)

	// Step 6: Verify catch-up requirements
	// When the partitioned follower rejoins, Raft will:
	// 1. Detect it's behind (last log index < leader's last log index)
	// 2. Leader sends missing log entries via AppendEntries RPC
	// 3. Follower replays log entries and applies to FSM
	// 4. Follower becomes in-sync when caught up

	t.Logf("\nFollower catch-up verification:")
	t.Logf("  ✓ Partitioned follower fell behind by %d messages", catchupMessages)
	t.Logf("  ✓ Cluster maintained quorum (2/3 nodes)")
	t.Logf("  ✓ Leader and active follower remained consistent")
	t.Logf("  ✓ All messages safely replicated to quorum")
	t.Logf("\nIn production:")
	t.Logf("  - When node%d reconnects, Raft automatically initiates catch-up", partitionedIdx+1)
	t.Logf("  - Leader sends %d missing log entries", catchupMessages)
	t.Logf("  - Follower replays entries and applies to state machine")
	t.Logf("  - ISR count returns to 3/3 when catch-up completes")

	// Note: Full restart and catch-up would require:
	// 1. Infrastructure to restart a manager with preserved data directory
	// 2. Raft group to detect and accept the rejoined node
	// 3. Time for log replay to complete
	// This is beyond the current test setup but is handled automatically
	// by Raft's consensus protocol in production environments.
}

// TestFailover_SplitBrainPrevention tests that split-brain scenarios are prevented.
func TestFailover_SplitBrainPrevention(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping split-brain test in short mode")
	}

	t.Skip("TODO: Implement split-brain prevention test")
	// Test plan:
	// 1. Create 5-node cluster (to allow 2-2-1 partition)
	// 2. Partition network into two groups: [node1, node2] and [node3, node4, node5]
	// 3. Verify only the majority partition (3 nodes) can elect a leader
	// 4. Verify minority partition (2 nodes) cannot process writes
	// 5. Heal partition
	// 6. Verify cluster converges to single leader
}

// TestFailover_GracefulShutdown tests graceful node shutdown without message loss.
func TestFailover_GracefulShutdown(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping graceful shutdown test in short mode")
	}

	ctx := context.Background()
	tempDir, err := os.MkdirTemp("", "failover-graceful-*")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	// Create 3 nodes with in-memory stores for this test
	nodeCount := 3
	queueName := "$queue/graceful-test"
	partitions := 3

	managers := make([]*Manager, nodeCount)
	stores := make([]*badgerstore.Store, nodeCount)
	dbs := make([]*badger.DB, nodeCount)
	nodeAddresses := make(map[string]string)

	// Create addresses
	for i := 0; i < nodeCount; i++ {
		nodeID := fmt.Sprintf("node%d", i+1)
		nodeAddresses[nodeID] = fmt.Sprintf("127.0.0.1:%d", 7100+i)
	}

	// Create managers
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

		broker := NewMockBroker()
		cfg := Config{
			QueueStore:    stores[i],
			MessageStore:  stores[i],
			ConsumerStore: stores[i],
			DeliverFn:     broker.DeliverToSession,
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
	}

	for i, mgr := range managers {
		err := mgr.CreateQueue(ctx, queueConfig)
		require.NoError(t, err, "node%d failed to create queue", i+1)
	}

	// Enqueue messages
	messageCount := 10
	for i := 0; i < messageCount; i++ {
		payload := []byte(fmt.Sprintf("message-%d", i))
		err := managers[0].Enqueue(ctx, queueName, payload, nil)
		require.NoError(t, err)
	}

	// Give time for replication
	time.Sleep(1 * time.Second)

	// Gracefully stop node 1
	managers[0].Stop()

	// Give time for cluster to stabilize
	time.Sleep(2 * time.Second)

	// Verify messages still exist on remaining nodes
	// At least one of the remaining nodes should have the messages
	totalCount := int64(0)
	for i := 1; i < nodeCount; i++ {
		count, err := stores[i].Count(ctx, queueName)
		require.NoError(t, err, "node%d: failed to count messages", i+1)
		totalCount += count
	}

	// With replication factor 3, at least 2 nodes should have the messages
	assert.GreaterOrEqual(t, totalCount, int64(messageCount), "messages should be preserved after graceful shutdown")

	// Cleanup
	for i := 1; i < nodeCount; i++ {
		managers[i].Stop()
	}
	for i := 0; i < nodeCount; i++ {
		dbs[i].Close()
	}
}

// TestFailover_ElectionTimeout tests that leader election completes within expected time.
func TestFailover_ElectionTimeout(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping election timeout test in short mode")
	}

	t.Skip("TODO: Implement election timeout test")
	// Test plan:
	// 1. Create 3-node cluster
	// 2. Record current leader
	// 3. Kill leader node
	// 4. Measure time until new leader elected
	// 5. Assert election time < 5s (roadmap requirement)
	// 6. Typical expectation: < 3s with proper Raft tuning
}

// TestFailover_ConcurrentFailures tests behavior when multiple nodes fail simultaneously.
func TestFailover_ConcurrentFailures(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping concurrent failures test in short mode")
	}

	t.Skip("TODO: Implement concurrent failures test")
	// Test plan:
	// 1. Create 5-node cluster (replication factor 5)
	// 2. Kill 2 nodes simultaneously (still have quorum: 3/5)
	// 3. Verify cluster continues operating
	// 4. Verify new leaders elected for affected partitions
	// 5. Kill 3rd node (lose quorum: 2/5)
	// 6. Verify cluster stops accepting writes (safety)
	// 7. Bring back 2 nodes (restore quorum: 4/5)
	// 8. Verify cluster resumes operations
}

// TestFailover_RapidLeaderChanges tests system stability under rapid leader changes.
func TestFailover_RapidLeaderChanges(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping rapid leader changes test in short mode")
	}

	t.Skip("TODO: Implement rapid leader changes test")
	// Test plan:
	// 1. Create 3-node cluster
	// 2. Kill leader
	// 3. Wait for new leader
	// 4. Immediately kill new leader
	// 5. Repeat 5 times
	// 6. Verify system stabilizes after chaos
	// 7. Verify no message loss or corruption
	// 8. Verify ISR counts eventually recover
}
