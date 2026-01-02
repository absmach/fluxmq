// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package queue

import (
	"fmt"
	"testing"

	queueStorage "github.com/absmach/mqtt/queue/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOrderingEnforcer_OrderingNone(t *testing.T) {
	enforcer := NewOrderingEnforcer(queueStorage.OrderingNone)

	// With no ordering, all messages can be delivered
	msg1 := &queueStorage.Message{
		ID:          "msg-1",
		PartitionID: 0,
		Sequence:    5,
	}
	msg2 := &queueStorage.Message{
		ID:          "msg-2",
		PartitionID: 0,
		Sequence:    3, // Out of order
	}

	canDeliver, err := enforcer.CanDeliver(msg1)
	require.NoError(t, err)
	assert.True(t, canDeliver)

	enforcer.MarkDelivered(msg1)

	canDeliver, err = enforcer.CanDeliver(msg2)
	require.NoError(t, err)
	assert.True(t, canDeliver) // Can deliver even though out of order
}

func TestOrderingEnforcer_PartitionOrdering_InOrder(t *testing.T) {
	enforcer := NewOrderingEnforcer(queueStorage.OrderingPartition)

	messages := []*queueStorage.Message{
		{ID: "msg-1", PartitionID: 0, Sequence: 1},
		{ID: "msg-2", PartitionID: 0, Sequence: 2},
		{ID: "msg-3", PartitionID: 0, Sequence: 3},
	}

	for i, msg := range messages {
		canDeliver, err := enforcer.CanDeliver(msg)
		require.NoError(t, err, "message %d should be deliverable", i)
		assert.True(t, canDeliver, "message %d should be deliverable", i)

		enforcer.MarkDelivered(msg)
	}
}

func TestOrderingEnforcer_PartitionOrdering_OutOfOrder(t *testing.T) {
	enforcer := NewOrderingEnforcer(queueStorage.OrderingPartition)

	// Deliver message with sequence 3
	msg1 := &queueStorage.Message{
		ID:          "msg-1",
		PartitionID: 0,
		Sequence:    3,
	}
	canDeliver, err := enforcer.CanDeliver(msg1)
	require.NoError(t, err)
	assert.True(t, canDeliver)
	enforcer.MarkDelivered(msg1)

	// Try to deliver message with sequence 2 (out of order)
	msg2 := &queueStorage.Message{
		ID:          "msg-2",
		PartitionID: 0,
		Sequence:    2,
	}
	canDeliver, err = enforcer.CanDeliver(msg2)
	assert.Error(t, err)
	assert.False(t, canDeliver)
	assert.Contains(t, err.Error(), "out-of-order")
}

func TestOrderingEnforcer_PartitionOrdering_MultiplePartitions(t *testing.T) {
	enforcer := NewOrderingEnforcer(queueStorage.OrderingPartition)

	// Partition 0
	msg1 := &queueStorage.Message{
		ID:          "msg-1",
		PartitionID: 0,
		Sequence:    1,
	}
	canDeliver, err := enforcer.CanDeliver(msg1)
	require.NoError(t, err)
	assert.True(t, canDeliver)
	enforcer.MarkDelivered(msg1)

	// Partition 1 (independent ordering)
	msg2 := &queueStorage.Message{
		ID:          "msg-2",
		PartitionID: 1,
		Sequence:    1,
	}
	canDeliver, err = enforcer.CanDeliver(msg2)
	require.NoError(t, err)
	assert.True(t, canDeliver)
	enforcer.MarkDelivered(msg2)

	// Partition 0 continues
	msg3 := &queueStorage.Message{
		ID:          "msg-3",
		PartitionID: 0,
		Sequence:    2,
	}
	canDeliver, err = enforcer.CanDeliver(msg3)
	require.NoError(t, err)
	assert.True(t, canDeliver)
}

func TestOrderingEnforcer_PartitionOrdering_Gaps(t *testing.T) {
	enforcer := NewOrderingEnforcer(queueStorage.OrderingPartition)

	// Deliver message with sequence 1
	msg1 := &queueStorage.Message{
		ID:          "msg-1",
		PartitionID: 0,
		Sequence:    1,
	}
	canDeliver, err := enforcer.CanDeliver(msg1)
	require.NoError(t, err)
	assert.True(t, canDeliver)
	enforcer.MarkDelivered(msg1)

	// Deliver message with sequence 5 (gap is allowed - message 2-4 might have been acked/failed)
	msg2 := &queueStorage.Message{
		ID:          "msg-2",
		PartitionID: 0,
		Sequence:    5,
	}
	canDeliver, err = enforcer.CanDeliver(msg2)
	require.NoError(t, err)
	assert.True(t, canDeliver)
	enforcer.MarkDelivered(msg2)
}

func TestOrderingEnforcer_StrictOrdering_SinglePartition(t *testing.T) {
	enforcer := NewOrderingEnforcer(queueStorage.OrderingStrict)

	// Message in partition 0 is allowed
	msg1 := &queueStorage.Message{
		ID:          "msg-1",
		PartitionID: 0,
		Sequence:    1,
	}
	canDeliver, err := enforcer.CanDeliver(msg1)
	require.NoError(t, err)
	assert.True(t, canDeliver)

	// Message in partition 1 is NOT allowed for strict ordering
	msg2 := &queueStorage.Message{
		ID:          "msg-2",
		PartitionID: 1,
		Sequence:    1,
	}
	canDeliver, err = enforcer.CanDeliver(msg2)
	assert.Error(t, err)
	assert.False(t, canDeliver)
	assert.Contains(t, err.Error(), "strict ordering requires all messages in partition 0")
}

func TestOrderingEnforcer_Reset(t *testing.T) {
	enforcer := NewOrderingEnforcer(queueStorage.OrderingPartition)

	msg1 := &queueStorage.Message{
		ID:          "msg-1",
		PartitionID: 0,
		Sequence:    5,
	}
	enforcer.MarkDelivered(msg1)

	// Verify last delivered is tracked
	lastSeq, exists := enforcer.GetLastDelivered(0)
	assert.True(t, exists)
	assert.Equal(t, uint64(5), lastSeq)

	// Reset partition
	enforcer.Reset(0)

	// Verify cleared
	_, exists = enforcer.GetLastDelivered(0)
	assert.False(t, exists)

	// Can now deliver message with lower sequence
	msg2 := &queueStorage.Message{
		ID:          "msg-2",
		PartitionID: 0,
		Sequence:    1,
	}
	canDeliver, err := enforcer.CanDeliver(msg2)
	require.NoError(t, err)
	assert.True(t, canDeliver)
}

func TestOrderingEnforcer_ResetAll(t *testing.T) {
	enforcer := NewOrderingEnforcer(queueStorage.OrderingPartition)

	// Mark several partitions as delivered
	for i := 0; i < 5; i++ {
		msg := &queueStorage.Message{
			ID:          fmt.Sprintf("msg-%d", i),
			PartitionID: i,
			Sequence:    10,
		}
		enforcer.MarkDelivered(msg)
	}

	// Verify tracked
	stats := enforcer.Stats()
	assert.Equal(t, 5, stats.PartitionCount)

	// Reset all
	enforcer.ResetAll()

	// Verify all cleared
	stats = enforcer.Stats()
	assert.Equal(t, 0, stats.PartitionCount)
}

func TestOrderingEnforcer_Stats(t *testing.T) {
	enforcer := NewOrderingEnforcer(queueStorage.OrderingPartition)

	// Deliver messages to different partitions
	partitions := []int{0, 0, 1, 1, 2}
	sequences := []uint64{1, 5, 3, 7, 2}

	for i, partID := range partitions {
		msg := &queueStorage.Message{
			ID:          fmt.Sprintf("msg-%d", i),
			PartitionID: partID,
			Sequence:    sequences[i],
		}
		enforcer.MarkDelivered(msg)
	}

	stats := enforcer.Stats()
	assert.Equal(t, queueStorage.OrderingPartition, stats.Mode)
	assert.Equal(t, 3, stats.PartitionCount)
	assert.Equal(t, uint64(5), stats.PartitionSeqs[0])
	assert.Equal(t, uint64(7), stats.PartitionSeqs[1])
	assert.Equal(t, uint64(2), stats.PartitionSeqs[2])
}

func TestHashPartitionStrategy(t *testing.T) {
	strategy := &HashPartitionStrategy{}

	// Same key should always go to same partition
	partition1 := strategy.GetPartition("user-123", 10)
	partition2 := strategy.GetPartition("user-123", 10)
	assert.Equal(t, partition1, partition2)

	// Different keys should distribute across partitions
	partitions := make(map[int]int)
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("key-%d", i)
		partition := strategy.GetPartition(key, 10)
		assert.GreaterOrEqual(t, partition, 0)
		assert.Less(t, partition, 10)
		partitions[partition]++
	}

	// Should have reasonable distribution (at least 5 different partitions used)
	assert.GreaterOrEqual(t, len(partitions), 5)
}

func TestHashPartitionStrategy_EmptyKey(t *testing.T) {
	strategy := &HashPartitionStrategy{}

	// Empty key should return random partition
	// Call multiple times and expect variation
	partitions := make(map[int]bool)
	for i := 0; i < 50; i++ {
		partition := strategy.GetPartition("", 10)
		assert.GreaterOrEqual(t, partition, 0)
		assert.Less(t, partition, 10)
		partitions[partition] = true
	}

	// Should have some variation (probabilistically very likely)
	assert.Greater(t, len(partitions), 1)
}
