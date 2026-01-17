// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package lifecycle

import (
	"fmt"
	"testing"

	"github.com/absmach/fluxmq/queue/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const testGroupID = "test-group"

func TestOrderingEnforcer_OrderingNone(t *testing.T) {
	enforcer := NewOrderingEnforcer(types.OrderingNone)

	// With no ordering, all messages can be delivered
	msg1 := &types.Message{
		ID:          "msg-1",
		PartitionID: 0,
		Sequence:    5,
	}
	msg2 := &types.Message{
		ID:          "msg-2",
		PartitionID: 0,
		Sequence:    3, // Out of order
	}

	canDeliver, err := enforcer.CanDeliver(msg1, testGroupID)
	require.NoError(t, err)
	assert.True(t, canDeliver)

	enforcer.MarkDelivered(msg1, testGroupID)

	canDeliver, err = enforcer.CanDeliver(msg2, testGroupID)
	require.NoError(t, err)
	assert.True(t, canDeliver) // Can deliver even though out of order
}

func TestOrderingEnforcer_PartitionOrdering_InOrder(t *testing.T) {
	enforcer := NewOrderingEnforcer(types.OrderingPartition)

	messages := []*types.Message{
		{ID: "msg-1", PartitionID: 0, Sequence: 1},
		{ID: "msg-2", PartitionID: 0, Sequence: 2},
		{ID: "msg-3", PartitionID: 0, Sequence: 3},
	}

	for i, msg := range messages {
		canDeliver, err := enforcer.CanDeliver(msg, testGroupID)
		require.NoError(t, err, "message %d should be deliverable", i)
		assert.True(t, canDeliver, "message %d should be deliverable", i)

		enforcer.MarkDelivered(msg, testGroupID)
	}
}

func TestOrderingEnforcer_PartitionOrdering_OutOfOrder(t *testing.T) {
	enforcer := NewOrderingEnforcer(types.OrderingPartition)

	// Deliver message with sequence 3
	msg1 := &types.Message{
		ID:          "msg-1",
		PartitionID: 0,
		Sequence:    3,
	}
	canDeliver, err := enforcer.CanDeliver(msg1, testGroupID)
	require.NoError(t, err)
	assert.True(t, canDeliver)
	enforcer.MarkDelivered(msg1, testGroupID)

	// Try to deliver message with sequence 2 (out of order)
	msg2 := &types.Message{
		ID:          "msg-2",
		PartitionID: 0,
		Sequence:    2,
	}
	canDeliver, err = enforcer.CanDeliver(msg2, testGroupID)
	assert.Error(t, err)
	assert.False(t, canDeliver)
	assert.Contains(t, err.Error(), "out-of-order")
}

func TestOrderingEnforcer_PartitionOrdering_MultiplePartitions(t *testing.T) {
	enforcer := NewOrderingEnforcer(types.OrderingPartition)

	// Partition 0
	msg1 := &types.Message{
		ID:          "msg-1",
		PartitionID: 0,
		Sequence:    1,
	}
	canDeliver, err := enforcer.CanDeliver(msg1, testGroupID)
	require.NoError(t, err)
	assert.True(t, canDeliver)
	enforcer.MarkDelivered(msg1, testGroupID)

	// Partition 1 (independent ordering)
	msg2 := &types.Message{
		ID:          "msg-2",
		PartitionID: 1,
		Sequence:    1,
	}
	canDeliver, err = enforcer.CanDeliver(msg2, testGroupID)
	require.NoError(t, err)
	assert.True(t, canDeliver)
	enforcer.MarkDelivered(msg2, testGroupID)

	// Partition 0 continues
	msg3 := &types.Message{
		ID:          "msg-3",
		PartitionID: 0,
		Sequence:    2,
	}
	canDeliver, err = enforcer.CanDeliver(msg3, testGroupID)
	require.NoError(t, err)
	assert.True(t, canDeliver)
}

func TestOrderingEnforcer_PartitionOrdering_Gaps(t *testing.T) {
	enforcer := NewOrderingEnforcer(types.OrderingPartition)

	// Deliver message with sequence 1
	msg1 := &types.Message{
		ID:          "msg-1",
		PartitionID: 0,
		Sequence:    1,
	}
	canDeliver, err := enforcer.CanDeliver(msg1, testGroupID)
	require.NoError(t, err)
	assert.True(t, canDeliver)
	enforcer.MarkDelivered(msg1, testGroupID)

	// Deliver message with sequence 5 (gap is allowed - message 2-4 might have been acked/failed)
	msg2 := &types.Message{
		ID:          "msg-2",
		PartitionID: 0,
		Sequence:    5,
	}
	canDeliver, err = enforcer.CanDeliver(msg2, testGroupID)
	require.NoError(t, err)
	assert.True(t, canDeliver)
	enforcer.MarkDelivered(msg2, testGroupID)
}

func TestOrderingEnforcer_StrictOrdering_SinglePartition(t *testing.T) {
	enforcer := NewOrderingEnforcer(types.OrderingStrict)

	// Message in partition 0 is allowed
	msg1 := &types.Message{
		ID:          "msg-1",
		PartitionID: 0,
		Sequence:    1,
	}
	canDeliver, err := enforcer.CanDeliver(msg1, testGroupID)
	require.NoError(t, err)
	assert.True(t, canDeliver)

	// Message in partition 1 is NOT allowed for strict ordering
	msg2 := &types.Message{
		ID:          "msg-2",
		PartitionID: 1,
		Sequence:    1,
	}
	canDeliver, err = enforcer.CanDeliver(msg2, testGroupID)
	assert.Error(t, err)
	assert.False(t, canDeliver)
	assert.Contains(t, err.Error(), "strict ordering requires all messages in partition 0")
}

func TestOrderingEnforcer_Reset(t *testing.T) {
	enforcer := NewOrderingEnforcer(types.OrderingPartition)

	msg1 := &types.Message{
		ID:          "msg-1",
		PartitionID: 0,
		Sequence:    5,
	}
	enforcer.MarkDelivered(msg1, testGroupID)

	// Verify last delivered is tracked
	lastSeq, exists := enforcer.GetLastDelivered(testGroupID, 0)
	assert.True(t, exists)
	assert.Equal(t, uint64(5), lastSeq)

	// Reset partition
	enforcer.Reset(0)

	// Verify cleared
	_, exists = enforcer.GetLastDelivered(testGroupID, 0)
	assert.False(t, exists)

	// Can now deliver message with lower sequence
	msg2 := &types.Message{
		ID:          "msg-2",
		PartitionID: 0,
		Sequence:    1,
	}
	canDeliver, err := enforcer.CanDeliver(msg2, testGroupID)
	require.NoError(t, err)
	assert.True(t, canDeliver)
}

func TestOrderingEnforcer_ResetAll(t *testing.T) {
	enforcer := NewOrderingEnforcer(types.OrderingPartition)

	// Mark several partitions as delivered
	for i := 0; i < 5; i++ {
		msg := &types.Message{
			ID:          fmt.Sprintf("msg-%d", i),
			PartitionID: i,
			Sequence:    10,
		}
		enforcer.MarkDelivered(msg, testGroupID)
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
	enforcer := NewOrderingEnforcer(types.OrderingPartition)

	// Deliver messages to different partitions
	partitions := []int{0, 0, 1, 1, 2}
	sequences := []uint64{1, 5, 3, 7, 2}

	for i, partID := range partitions {
		msg := &types.Message{
			ID:          fmt.Sprintf("msg-%d", i),
			PartitionID: partID,
			Sequence:    sequences[i],
		}
		enforcer.MarkDelivered(msg, testGroupID)
	}

	stats := enforcer.Stats()
	assert.Equal(t, types.OrderingPartition, stats.Mode)
	assert.Equal(t, 3, stats.PartitionCount)
	assert.Equal(t, uint64(5), stats.GroupStats[testGroupID][0])
	assert.Equal(t, uint64(7), stats.GroupStats[testGroupID][1])
	assert.Equal(t, uint64(2), stats.GroupStats[testGroupID][2])
}

func TestOrderingEnforcer_MultipleGroups(t *testing.T) {
	enforcer := NewOrderingEnforcer(types.OrderingPartition)

	// Same message delivered to multiple groups should work
	msg := &types.Message{
		ID:          "msg-1",
		PartitionID: 0,
		Sequence:    1,
	}

	// Deliver to group1
	canDeliver, err := enforcer.CanDeliver(msg, "group1")
	require.NoError(t, err)
	assert.True(t, canDeliver)
	enforcer.MarkDelivered(msg, "group1")

	// Same message can be delivered to group2 (groups are independent)
	canDeliver, err = enforcer.CanDeliver(msg, "group2")
	require.NoError(t, err)
	assert.True(t, canDeliver)
	enforcer.MarkDelivered(msg, "group2")

	// Next message in group1
	msg2 := &types.Message{
		ID:          "msg-2",
		PartitionID: 0,
		Sequence:    2,
	}
	canDeliver, err = enforcer.CanDeliver(msg2, "group1")
	require.NoError(t, err)
	assert.True(t, canDeliver)

	// Out of order message in group1 should fail
	msgOld := &types.Message{
		ID:          "msg-0",
		PartitionID: 0,
		Sequence:    0,
	}
	canDeliver, err = enforcer.CanDeliver(msgOld, "group1")
	assert.Error(t, err)
	assert.False(t, canDeliver)

	// But out of order check for group2 is independent
	canDeliver, err = enforcer.CanDeliver(msgOld, "group2")
	assert.Error(t, err)
	assert.False(t, canDeliver)
}
