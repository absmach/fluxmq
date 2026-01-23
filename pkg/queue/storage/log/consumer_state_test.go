// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package log

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConsumerState_NewAndClose(t *testing.T) {
	dir := t.TempDir()

	config := DefaultConsumerStateConfig()
	cs, err := NewConsumerState(dir, "test-group", config)
	require.NoError(t, err)
	require.NotNil(t, cs)

	// Verify directory structure
	groupDir := filepath.Join(dir, "test-group")
	_, err = os.Stat(groupDir)
	assert.NoError(t, err)

	// Close
	err = cs.Close()
	assert.NoError(t, err)
}

func TestConsumerState_DeliverAndAck(t *testing.T) {
	dir := t.TempDir()
	config := DefaultConsumerStateConfig()
	cs, err := NewConsumerState(dir, "test-group", config)
	require.NoError(t, err)
	defer cs.Close()

	// Deliver messages
	err = cs.Deliver(0, 1, "consumer-1")
	require.NoError(t, err)

	err = cs.Deliver(0, 2, "consumer-1")
	require.NoError(t, err)

	err = cs.Deliver(0, 3, "consumer-2")
	require.NoError(t, err)

	// Check partition state
	ps := cs.GetPartitionState(0)
	assert.Equal(t, uint64(4), ps.Cursor)
	assert.Equal(t, uint64(0), ps.AckFloor)
	assert.Equal(t, uint64(3), ps.Delivered)

	// Check pending count
	assert.Equal(t, 3, cs.PendingCount())

	// Ack in order
	err = cs.Ack(0, 1)
	require.NoError(t, err)

	ps = cs.GetPartitionState(0)
	assert.Equal(t, uint64(1), ps.AckFloor) // Should advance

	// Ack out of order (offset 3 before 2)
	err = cs.Ack(0, 3)
	require.NoError(t, err)

	ps = cs.GetPartitionState(0)
	assert.Equal(t, uint64(1), ps.AckFloor) // Should NOT advance (gap at 2)

	// Ack offset 2 to fill the gap
	err = cs.Ack(0, 2)
	require.NoError(t, err)

	ps = cs.GetPartitionState(0)
	assert.Equal(t, uint64(3), ps.AckFloor) // Should advance to 3

	// All acked
	assert.Equal(t, 0, cs.PendingCount())
}

func TestConsumerState_DeliverBatch(t *testing.T) {
	dir := t.TempDir()
	config := DefaultConsumerStateConfig()
	cs, err := NewConsumerState(dir, "test-group", config)
	require.NoError(t, err)
	defer cs.Close()

	offsets := []uint64{10, 11, 12, 13, 14}
	err = cs.DeliverBatch(0, offsets, "consumer-1")
	require.NoError(t, err)

	ps := cs.GetPartitionState(0)
	assert.Equal(t, uint64(15), ps.Cursor)
	assert.Equal(t, uint64(5), ps.Delivered)
	assert.Equal(t, 5, cs.PendingCount())
}

func TestConsumerState_AckBatch(t *testing.T) {
	dir := t.TempDir()
	config := DefaultConsumerStateConfig()
	cs, err := NewConsumerState(dir, "test-group", config)
	require.NoError(t, err)
	defer cs.Close()

	// Deliver
	for i := uint64(0); i < 10; i++ {
		err = cs.Deliver(0, i, "consumer-1")
		require.NoError(t, err)
	}

	// Ack batch (contiguous from start)
	err = cs.AckBatch(0, []uint64{0, 1, 2, 3, 4})
	require.NoError(t, err)

	ps := cs.GetPartitionState(0)
	assert.Equal(t, uint64(4), ps.AckFloor)
	assert.Equal(t, 5, cs.PendingCount())
}

func TestConsumerState_Nack(t *testing.T) {
	dir := t.TempDir()
	config := DefaultConsumerStateConfig()
	cs, err := NewConsumerState(dir, "test-group", config)
	require.NoError(t, err)
	defer cs.Close()

	err = cs.Deliver(0, 1, "consumer-1")
	require.NoError(t, err)

	// Get initial entry
	entry, ok := cs.GetPending(1)
	require.True(t, ok)
	assert.Equal(t, uint16(1), entry.DeliveryCount)

	// Nack increments delivery count
	err = cs.Nack(1)
	require.NoError(t, err)

	entry, ok = cs.GetPending(1)
	require.True(t, ok)
	assert.Equal(t, uint16(2), entry.DeliveryCount)
}

func TestConsumerState_Claim(t *testing.T) {
	dir := t.TempDir()
	config := DefaultConsumerStateConfig()
	cs, err := NewConsumerState(dir, "test-group", config)
	require.NoError(t, err)
	defer cs.Close()

	err = cs.Deliver(0, 1, "consumer-1")
	require.NoError(t, err)

	// Verify initial owner
	entry, ok := cs.GetPending(1)
	require.True(t, ok)
	assert.Equal(t, "consumer-1", entry.ConsumerID)

	// Claim for another consumer
	err = cs.Claim(1, "consumer-2")
	require.NoError(t, err)

	entry, ok = cs.GetPending(1)
	require.True(t, ok)
	assert.Equal(t, "consumer-2", entry.ConsumerID)
	assert.Equal(t, uint16(2), entry.DeliveryCount) // Incremented on claim
}

func TestConsumerState_ClaimBatch(t *testing.T) {
	dir := t.TempDir()
	config := DefaultConsumerStateConfig()
	cs, err := NewConsumerState(dir, "test-group", config)
	require.NoError(t, err)
	defer cs.Close()

	// Deliver multiple
	for i := uint64(1); i <= 5; i++ {
		err = cs.Deliver(0, i, "consumer-1")
		require.NoError(t, err)
	}

	// Claim batch
	err = cs.ClaimBatch([]uint64{1, 2, 3}, "consumer-2")
	require.NoError(t, err)

	// Verify claims
	for _, offset := range []uint64{1, 2, 3} {
		entry, ok := cs.GetPending(offset)
		require.True(t, ok)
		assert.Equal(t, "consumer-2", entry.ConsumerID)
	}

	// Verify unclaimed still with original consumer
	for _, offset := range []uint64{4, 5} {
		entry, ok := cs.GetPending(offset)
		require.True(t, ok)
		assert.Equal(t, "consumer-1", entry.ConsumerID)
	}
}

func TestConsumerState_GetPendingByConsumer(t *testing.T) {
	dir := t.TempDir()
	config := DefaultConsumerStateConfig()
	cs, err := NewConsumerState(dir, "test-group", config)
	require.NoError(t, err)
	defer cs.Close()

	// Deliver to different consumers
	err = cs.Deliver(0, 1, "consumer-1")
	require.NoError(t, err)
	err = cs.Deliver(0, 2, "consumer-1")
	require.NoError(t, err)
	err = cs.Deliver(0, 3, "consumer-2")
	require.NoError(t, err)

	// Get by consumer
	c1Entries := cs.GetPendingByConsumer("consumer-1")
	assert.Len(t, c1Entries, 2)

	c2Entries := cs.GetPendingByConsumer("consumer-2")
	assert.Len(t, c2Entries, 1)

	c3Entries := cs.GetPendingByConsumer("consumer-3")
	assert.Len(t, c3Entries, 0)
}

func TestConsumerState_GetPendingByShard(t *testing.T) {
	dir := t.TempDir()
	config := DefaultConsumerStateConfig()
	config.NumPELShards = 4
	cs, err := NewConsumerState(dir, "test-group", config)
	require.NoError(t, err)
	defer cs.Close()

	// Deliver many messages to distribute across shards
	for i := uint64(0); i < 100; i++ {
		err = cs.Deliver(0, i, "consumer-1")
		require.NoError(t, err)
	}

	// Check shard distribution
	totalFromShards := 0
	for shardID := 0; shardID < 4; shardID++ {
		entries := cs.GetPendingByShard(shardID)
		totalFromShards += len(entries)
		// Each shard should have some entries (roughly 25 each)
		assert.True(t, len(entries) > 0, "shard %d should have entries", shardID)
	}

	assert.Equal(t, 100, totalFromShards)
}

func TestConsumerState_RedeliveryCandidates(t *testing.T) {
	dir := t.TempDir()
	config := DefaultConsumerStateConfig()
	config.NumPELShards = 2
	cs, err := NewConsumerState(dir, "test-group", config)
	require.NoError(t, err)
	defer cs.Close()

	// Deliver messages
	for i := uint64(0); i < 10; i++ {
		err = cs.Deliver(0, i, "consumer-1")
		require.NoError(t, err)
	}

	// No candidates yet (just delivered)
	candidates := cs.GetAllRedeliveryCandidates(100*time.Millisecond, 100)
	assert.Len(t, candidates, 0)

	// Wait for timeout
	time.Sleep(150 * time.Millisecond)

	// Now should have candidates
	candidates = cs.GetAllRedeliveryCandidates(100*time.Millisecond, 100)
	assert.Len(t, candidates, 10)

	// Test per-shard candidates
	for shardID := 0; shardID < 2; shardID++ {
		shardCandidates := cs.GetRedeliveryCandidates(shardID, 100*time.Millisecond, 100)
		assert.True(t, len(shardCandidates.Entries) > 0)
	}
}

func TestConsumerState_DeadLetterCandidates(t *testing.T) {
	dir := t.TempDir()
	config := DefaultConsumerStateConfig()
	cs, err := NewConsumerState(dir, "test-group", config)
	require.NoError(t, err)
	defer cs.Close()

	// Deliver a message
	err = cs.Deliver(0, 1, "consumer-1")
	require.NoError(t, err)

	// No DLQ candidates yet
	dlqCandidates := cs.GetDeadLetterCandidates(3, 100)
	assert.Len(t, dlqCandidates, 0)

	// Nack multiple times to exceed delivery count
	for i := 0; i < 3; i++ {
		err = cs.Nack(1)
		require.NoError(t, err)
	}

	// Now should be a DLQ candidate (delivery count = 4)
	dlqCandidates = cs.GetDeadLetterCandidates(3, 100)
	assert.Len(t, dlqCandidates, 1)
	assert.Equal(t, uint64(1), dlqCandidates[0].Offset)
}

func TestConsumerState_PersistenceAndRecovery(t *testing.T) {
	dir := t.TempDir()
	config := DefaultConsumerStateConfig()

	// Create and populate
	cs, err := NewConsumerState(dir, "test-group", config)
	require.NoError(t, err)

	for i := uint64(0); i < 10; i++ {
		err = cs.Deliver(0, i, "consumer-1")
		require.NoError(t, err)
	}

	// Ack some
	err = cs.AckBatch(0, []uint64{0, 1, 2})
	require.NoError(t, err)

	// Sync and close
	err = cs.Sync()
	require.NoError(t, err)
	err = cs.Close()
	require.NoError(t, err)

	// Reopen and verify state
	cs2, err := NewConsumerState(dir, "test-group", config)
	require.NoError(t, err)
	defer cs2.Close()

	ps := cs2.GetPartitionState(0)
	assert.Equal(t, uint64(10), ps.Cursor)
	assert.Equal(t, uint64(2), ps.AckFloor)
	assert.Equal(t, 7, cs2.PendingCount())
}

func TestConsumerState_CompactAndRecovery(t *testing.T) {
	dir := t.TempDir()
	config := DefaultConsumerStateConfig()
	config.CompactThreshold = 100 // High threshold to avoid background compaction race

	cs, err := NewConsumerState(dir, "test-group", config)
	require.NoError(t, err)

	// Deliver messages
	for i := uint64(0); i < 20; i++ {
		err = cs.Deliver(0, i, "consumer-1")
		require.NoError(t, err)
	}

	// Manual compact
	err = cs.Compact()
	require.NoError(t, err)

	// Verify state file exists
	statePath := filepath.Join(dir, "test-group", "state.json")
	_, err = os.Stat(statePath)
	assert.NoError(t, err)

	cs.Close()

	// Reopen and verify
	cs2, err := NewConsumerState(dir, "test-group", config)
	require.NoError(t, err)
	defer cs2.Close()

	ps := cs2.GetPartitionState(0)
	assert.Equal(t, uint64(20), ps.Cursor)
	assert.Equal(t, 20, cs2.PendingCount())
}

func TestConsumerState_MultiplePartitions(t *testing.T) {
	dir := t.TempDir()
	config := DefaultConsumerStateConfig()
	cs, err := NewConsumerState(dir, "test-group", config)
	require.NoError(t, err)
	defer cs.Close()

	// Deliver to multiple partitions
	err = cs.Deliver(0, 1, "consumer-1")
	require.NoError(t, err)
	err = cs.Deliver(1, 1, "consumer-1")
	require.NoError(t, err)
	err = cs.Deliver(2, 1, "consumer-1")
	require.NoError(t, err)

	// Check each partition
	ps0 := cs.GetPartitionState(0)
	assert.Equal(t, uint64(2), ps0.Cursor)

	ps1 := cs.GetPartitionState(1)
	assert.Equal(t, uint64(2), ps1.Cursor)

	ps2 := cs.GetPartitionState(2)
	assert.Equal(t, uint64(2), ps2.Cursor)

	// Ack only partition 0
	err = cs.Ack(0, 1)
	require.NoError(t, err)

	ps0 = cs.GetPartitionState(0)
	assert.Equal(t, uint64(1), ps0.AckFloor)

	ps1 = cs.GetPartitionState(1)
	assert.Equal(t, uint64(0), ps1.AckFloor) // Unchanged
}

func TestConsumerState_ShardKey(t *testing.T) {
	dir := t.TempDir()
	config := DefaultConsumerStateConfig()
	config.NumPELShards = 8
	cs, err := NewConsumerState(dir, "test-group", config)
	require.NoError(t, err)
	defer cs.Close()

	// Verify consistent sharding
	for i := uint64(0); i < 100; i++ {
		shard1 := cs.ShardKey(i)
		shard2 := cs.ShardKey(i)
		assert.Equal(t, shard1, shard2)
		assert.True(t, shard1 >= 0 && shard1 < 8)
	}
}

func TestConsumerState_Stats(t *testing.T) {
	dir := t.TempDir()
	config := DefaultConsumerStateConfig()
	config.NumPELShards = 4
	cs, err := NewConsumerState(dir, "test-group", config)
	require.NoError(t, err)
	defer cs.Close()

	// Deliver some messages
	for i := uint64(0); i < 20; i++ {
		err = cs.Deliver(0, i, "consumer-1")
		require.NoError(t, err)
	}

	stats := cs.Stats()
	assert.Equal(t, "test-group", stats.GroupID)
	assert.Equal(t, 1, stats.NumPartitions)
	assert.Equal(t, 4, stats.NumShards)
	assert.Equal(t, 20, stats.TotalPending)
	assert.Len(t, stats.ShardStats, 4)
}

func TestConsumerState_PendingCountByShard(t *testing.T) {
	dir := t.TempDir()
	config := DefaultConsumerStateConfig()
	config.NumPELShards = 4
	cs, err := NewConsumerState(dir, "test-group", config)
	require.NoError(t, err)
	defer cs.Close()

	for i := uint64(0); i < 40; i++ {
		err = cs.Deliver(0, i, "consumer-1")
		require.NoError(t, err)
	}

	counts := cs.PendingCountByShard()
	assert.Len(t, counts, 4)

	total := 0
	for _, count := range counts {
		total += count
		assert.Equal(t, 10, count) // Should be evenly distributed
	}
	assert.Equal(t, 40, total)
}

func TestConsumerState_EmptyGroup(t *testing.T) {
	dir := t.TempDir()
	config := DefaultConsumerStateConfig()
	cs, err := NewConsumerState(dir, "test-group", config)
	require.NoError(t, err)
	defer cs.Close()

	// Empty group should work
	ps := cs.GetPartitionState(0)
	assert.Equal(t, uint64(0), ps.Cursor)
	assert.Equal(t, uint64(0), ps.AckFloor)

	assert.Equal(t, 0, cs.PendingCount())

	_, ok := cs.GetPending(1)
	assert.False(t, ok)

	entries := cs.GetPendingByConsumer("consumer-1")
	assert.Len(t, entries, 0)
}

func TestConsumerState_AckNonExistent(t *testing.T) {
	dir := t.TempDir()
	config := DefaultConsumerStateConfig()
	cs, err := NewConsumerState(dir, "test-group", config)
	require.NoError(t, err)
	defer cs.Close()

	// Ack non-existent should not error (idempotent)
	err = cs.Ack(0, 999)
	assert.NoError(t, err)
}

func TestConsumerState_ClaimNonExistent(t *testing.T) {
	dir := t.TempDir()
	config := DefaultConsumerStateConfig()
	cs, err := NewConsumerState(dir, "test-group", config)
	require.NoError(t, err)
	defer cs.Close()

	// Claim non-existent should not error (no-op)
	err = cs.Claim(999, "consumer-2")
	assert.NoError(t, err)
}

func TestConsumerState_NackNonExistent(t *testing.T) {
	dir := t.TempDir()
	config := DefaultConsumerStateConfig()
	cs, err := NewConsumerState(dir, "test-group", config)
	require.NoError(t, err)
	defer cs.Close()

	// Nack non-existent should not error (no-op)
	err = cs.Nack(999)
	assert.NoError(t, err)
}

func TestConsumerShardKey(t *testing.T) {
	// Test consumer-based sharding
	shard1 := ConsumerShardKey("consumer-1", 8)
	shard2 := ConsumerShardKey("consumer-1", 8)
	assert.Equal(t, shard1, shard2)

	// Different consumers may get different shards
	shardA := ConsumerShardKey("consumer-a", 8)
	shardB := ConsumerShardKey("consumer-b", 8)
	// They might be same or different, but both valid
	assert.True(t, shardA >= 0 && shardA < 8)
	assert.True(t, shardB >= 0 && shardB < 8)
}
