// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package log

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConsumerManager_NewAndClose(t *testing.T) {
	dir := t.TempDir()
	config := DefaultConsumerStateConfig()

	cm, err := NewConsumerManager(dir, config)
	require.NoError(t, err)
	require.NotNil(t, cm)

	err = cm.Close()
	assert.NoError(t, err)
}

func TestConsumerManager_GetOrCreate(t *testing.T) {
	dir := t.TempDir()
	config := DefaultConsumerStateConfig()

	cm, err := NewConsumerManager(dir, config)
	require.NoError(t, err)
	defer cm.Close()

	// Create new group
	state1, err := cm.GetOrCreate("group-1")
	require.NoError(t, err)
	require.NotNil(t, state1)

	// Get same group
	state2, err := cm.GetOrCreate("group-1")
	require.NoError(t, err)
	assert.Equal(t, state1, state2)

	// Create another group
	state3, err := cm.GetOrCreate("group-2")
	require.NoError(t, err)
	assert.NotEqual(t, state1, state3)
}

func TestConsumerManager_Get(t *testing.T) {
	dir := t.TempDir()
	config := DefaultConsumerStateConfig()

	cm, err := NewConsumerManager(dir, config)
	require.NoError(t, err)
	defer cm.Close()

	// Get non-existent
	state := cm.Get("group-1")
	assert.Nil(t, state)

	// Create
	_, err = cm.GetOrCreate("group-1")
	require.NoError(t, err)

	// Now Get should work
	state = cm.Get("group-1")
	assert.NotNil(t, state)
}

func TestConsumerManager_Delete(t *testing.T) {
	dir := t.TempDir()
	config := DefaultConsumerStateConfig()

	cm, err := NewConsumerManager(dir, config)
	require.NoError(t, err)
	defer cm.Close()

	// Create
	_, err = cm.GetOrCreate("group-1")
	require.NoError(t, err)
	assert.True(t, cm.Exists("group-1"))

	// Delete
	err = cm.Delete("group-1")
	require.NoError(t, err)

	assert.False(t, cm.Exists("group-1"))
	assert.Nil(t, cm.Get("group-1"))
}

func TestConsumerManager_List(t *testing.T) {
	dir := t.TempDir()
	config := DefaultConsumerStateConfig()

	cm, err := NewConsumerManager(dir, config)
	require.NoError(t, err)
	defer cm.Close()

	// Empty
	groups := cm.List()
	assert.Len(t, groups, 0)

	// Create groups
	_, err = cm.GetOrCreate("group-a")
	require.NoError(t, err)
	_, err = cm.GetOrCreate("group-b")
	require.NoError(t, err)
	_, err = cm.GetOrCreate("group-c")
	require.NoError(t, err)

	groups = cm.List()
	assert.Len(t, groups, 3)
	assert.Contains(t, groups, "group-a")
	assert.Contains(t, groups, "group-b")
	assert.Contains(t, groups, "group-c")
}

func TestConsumerManager_Exists(t *testing.T) {
	dir := t.TempDir()
	config := DefaultConsumerStateConfig()

	cm, err := NewConsumerManager(dir, config)
	require.NoError(t, err)
	defer cm.Close()

	assert.False(t, cm.Exists("group-1"))

	_, err = cm.GetOrCreate("group-1")
	require.NoError(t, err)

	assert.True(t, cm.Exists("group-1"))
}

func TestConsumerManager_Persistence(t *testing.T) {
	dir := t.TempDir()
	config := DefaultConsumerStateConfig()

	// Create manager and groups
	cm, err := NewConsumerManager(dir, config)
	require.NoError(t, err)

	state1, err := cm.GetOrCreate("group-1")
	require.NoError(t, err)

	// Add some data
	err = state1.Deliver(0, 1, "consumer-1")
	require.NoError(t, err)
	err = state1.Deliver(0, 2, "consumer-1")
	require.NoError(t, err)

	err = cm.Sync()
	require.NoError(t, err)
	err = cm.Close()
	require.NoError(t, err)

	// Reopen
	cm2, err := NewConsumerManager(dir, config)
	require.NoError(t, err)
	defer cm2.Close()

	// Group should still exist
	assert.True(t, cm2.Exists("group-1"))

	state2 := cm2.Get("group-1")
	require.NotNil(t, state2)

	// Data should be preserved
	ps := state2.GetPartitionState(0)
	assert.Equal(t, uint64(3), ps.Cursor)
	assert.Equal(t, 2, state2.PendingCount())
}

func TestConsumerManager_GetRedeliveryBatches(t *testing.T) {
	dir := t.TempDir()
	config := DefaultConsumerStateConfig()
	config.NumPELShards = 2

	cm, err := NewConsumerManager(dir, config)
	require.NoError(t, err)
	defer cm.Close()

	// Create groups with pending messages
	state1, err := cm.GetOrCreate("group-1")
	require.NoError(t, err)
	for i := uint64(0); i < 10; i++ {
		err = state1.Deliver(0, i, "consumer-1")
		require.NoError(t, err)
	}

	state2, err := cm.GetOrCreate("group-2")
	require.NoError(t, err)
	for i := uint64(0); i < 5; i++ {
		err = state2.Deliver(0, i, "consumer-2")
		require.NoError(t, err)
	}

	// No batches yet (just delivered)
	batches := cm.GetRedeliveryBatches(50*time.Millisecond, 100)
	assert.Len(t, batches, 0)

	// Wait for timeout
	time.Sleep(100 * time.Millisecond)

	// Now should have batches
	batches = cm.GetRedeliveryBatches(50*time.Millisecond, 100)
	assert.True(t, len(batches) > 0)

	// Count total entries across batches
	totalEntries := 0
	for _, batch := range batches {
		totalEntries += len(batch.Entries)
	}
	assert.Equal(t, 15, totalEntries)
}

func TestConsumerManager_GetRedeliveryBatchForShard(t *testing.T) {
	dir := t.TempDir()
	config := DefaultConsumerStateConfig()
	config.NumPELShards = 4

	cm, err := NewConsumerManager(dir, config)
	require.NoError(t, err)
	defer cm.Close()

	state, err := cm.GetOrCreate("group-1")
	require.NoError(t, err)

	for i := uint64(0); i < 40; i++ {
		err = state.Deliver(0, i, "consumer-1")
		require.NoError(t, err)
	}

	// Wait for timeout
	time.Sleep(100 * time.Millisecond)

	// Get batch for specific shard
	batch := cm.GetRedeliveryBatchForShard("group-1", 0, 50*time.Millisecond, 100)
	require.NotNil(t, batch)
	assert.Equal(t, "group-1", batch.GroupID)
	assert.Equal(t, 0, batch.ShardID)
	assert.Equal(t, 10, len(batch.Entries)) // 40/4 shards = 10 per shard

	// Non-existent group
	batch = cm.GetRedeliveryBatchForShard("non-existent", 0, 50*time.Millisecond, 100)
	assert.Nil(t, batch)
}

func TestConsumerManager_ClaimBatch(t *testing.T) {
	dir := t.TempDir()
	config := DefaultConsumerStateConfig()

	cm, err := NewConsumerManager(dir, config)
	require.NoError(t, err)
	defer cm.Close()

	state, err := cm.GetOrCreate("group-1")
	require.NoError(t, err)

	for i := uint64(0); i < 5; i++ {
		err = state.Deliver(0, i, "consumer-1")
		require.NoError(t, err)
	}

	// Claim batch
	claimed, err := cm.ClaimBatch("group-1", []uint64{0, 1, 2}, "consumer-2")
	require.NoError(t, err)
	assert.Len(t, claimed, 3)

	// Verify
	for _, offset := range []uint64{0, 1, 2} {
		entry, ok := state.GetPending(offset)
		require.True(t, ok)
		assert.Equal(t, "consumer-2", entry.ConsumerID)
	}

	// Non-existent group
	_, err = cm.ClaimBatch("non-existent", []uint64{0}, "consumer-2")
	assert.Error(t, err)
}

func TestConsumerManager_AckBatch(t *testing.T) {
	dir := t.TempDir()
	config := DefaultConsumerStateConfig()

	cm, err := NewConsumerManager(dir, config)
	require.NoError(t, err)
	defer cm.Close()

	state, err := cm.GetOrCreate("group-1")
	require.NoError(t, err)

	for i := uint64(0); i < 10; i++ {
		err = state.Deliver(0, i, "consumer-1")
		require.NoError(t, err)
	}

	assert.Equal(t, 10, state.PendingCount())

	// Ack batch
	err = cm.AckBatch("group-1", 0, []uint64{0, 1, 2, 3, 4})
	require.NoError(t, err)

	assert.Equal(t, 5, state.PendingCount())

	// Non-existent group
	err = cm.AckBatch("non-existent", 0, []uint64{0})
	assert.Error(t, err)
}

func TestConsumerManager_GetDeadLetterBatches(t *testing.T) {
	dir := t.TempDir()
	config := DefaultConsumerStateConfig()

	cm, err := NewConsumerManager(dir, config)
	require.NoError(t, err)
	defer cm.Close()

	state, err := cm.GetOrCreate("group-1")
	require.NoError(t, err)

	err = state.Deliver(0, 1, "consumer-1")
	require.NoError(t, err)

	// Nack multiple times
	for i := 0; i < 5; i++ {
		err = state.Nack(1)
		require.NoError(t, err)
	}

	// Get DLQ batches
	batches := cm.GetDeadLetterBatches(3, 100)
	assert.Len(t, batches, 1)
	assert.Equal(t, "group-1", batches[0].GroupID)
	assert.Len(t, batches[0].Entries, 1)
}

func TestConsumerManager_GetShardAssignments(t *testing.T) {
	dir := t.TempDir()
	config := DefaultConsumerStateConfig()
	config.NumPELShards = 8

	cm, err := NewConsumerManager(dir, config)
	require.NoError(t, err)
	defer cm.Close()

	_, err = cm.GetOrCreate("group-1")
	require.NoError(t, err)

	nodeIDs := []string{"node-1", "node-2", "node-3"}
	assignments := cm.GetShardAssignments("group-1", nodeIDs)

	assert.Len(t, assignments, 3)

	// Verify all shards assigned
	allShards := make(map[int]bool)
	for _, assignment := range assignments {
		for _, shardID := range assignment.ShardIDs {
			assert.False(t, allShards[shardID], "shard %d assigned twice", shardID)
			allShards[shardID] = true
		}
	}
	assert.Len(t, allShards, 8)

	// Non-existent group
	assignments = cm.GetShardAssignments("non-existent", nodeIDs)
	assert.Nil(t, assignments)
}

func TestConsumerManager_ForEachGroup(t *testing.T) {
	dir := t.TempDir()
	config := DefaultConsumerStateConfig()

	cm, err := NewConsumerManager(dir, config)
	require.NoError(t, err)
	defer cm.Close()

	_, err = cm.GetOrCreate("group-1")
	require.NoError(t, err)
	_, err = cm.GetOrCreate("group-2")
	require.NoError(t, err)

	visited := make(map[string]bool)
	err = cm.ForEachGroup(func(groupID string, state *ConsumerState) error {
		visited[groupID] = true
		return nil
	})
	require.NoError(t, err)

	assert.Len(t, visited, 2)
	assert.True(t, visited["group-1"])
	assert.True(t, visited["group-2"])
}

func TestConsumerManager_Stats(t *testing.T) {
	dir := t.TempDir()
	config := DefaultConsumerStateConfig()

	cm, err := NewConsumerManager(dir, config)
	require.NoError(t, err)
	defer cm.Close()

	state1, err := cm.GetOrCreate("group-1")
	require.NoError(t, err)
	for i := uint64(0); i < 10; i++ {
		err = state1.Deliver(0, i, "consumer-1")
		require.NoError(t, err)
	}

	state2, err := cm.GetOrCreate("group-2")
	require.NoError(t, err)
	for i := uint64(0); i < 5; i++ {
		err = state2.Deliver(0, i, "consumer-1")
		require.NoError(t, err)
	}

	stats := cm.Stats()
	assert.Equal(t, 2, stats.GroupCount)
	assert.Equal(t, 15, stats.TotalPending)
	assert.Len(t, stats.GroupStats, 2)
}

func TestConsumerManager_Compact(t *testing.T) {
	dir := t.TempDir()
	config := DefaultConsumerStateConfig()

	cm, err := NewConsumerManager(dir, config)
	require.NoError(t, err)
	defer cm.Close()

	state, err := cm.GetOrCreate("group-1")
	require.NoError(t, err)

	for i := uint64(0); i < 100; i++ {
		err = state.Deliver(0, i, "consumer-1")
		require.NoError(t, err)
	}

	err = cm.Compact()
	assert.NoError(t, err)
}

func TestConsumerManager_Sync(t *testing.T) {
	dir := t.TempDir()
	config := DefaultConsumerStateConfig()

	cm, err := NewConsumerManager(dir, config)
	require.NoError(t, err)
	defer cm.Close()

	state, err := cm.GetOrCreate("group-1")
	require.NoError(t, err)

	err = state.Deliver(0, 1, "consumer-1")
	require.NoError(t, err)

	err = cm.Sync()
	assert.NoError(t, err)
}
