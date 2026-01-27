// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package consumer

import (
	"context"
	"testing"
	"time"

	memory "github.com/absmach/fluxmq/queue/storage/memory/log"
	"github.com/absmach/fluxmq/queue/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// MockPartition implements Partition interface for testing.
type MockPartition struct {
	id         int
	assignedTo string
}

func NewMockPartition(id int) *MockPartition {
	return &MockPartition{
		id: id,
	}
}

func (p *MockPartition) ID() int {
	return p.id
}

func (p *MockPartition) AssignTo(consumerID string) {
	p.assignedTo = consumerID
}

func (p *MockPartition) AssignedTo() string {
	return p.assignedTo
}

func (p *MockPartition) IsAssigned() bool {
	return p.assignedTo != ""
}

func toPartitions(mocks []*MockPartition) []Partition {
	parts := make([]Partition, len(mocks))
	for i, m := range mocks {
		parts[i] = m
	}
	return parts
}

func TestNewGroupManager(t *testing.T) {
	store := memory.New()
	mgr := NewGroupManager("$queue/test", store, time.Second, nil)

	assert.NotNil(t, mgr)
	assert.Equal(t, "$queue/test", mgr.queueName)
}

func TestGroupManager_AddConsumer(t *testing.T) {
	ctx := context.Background()
	store := memory.New()
	config := types.DefaultQueueConfig("$queue/test")
	err := store.CreateQueue(ctx, config)
	require.NoError(t, err)

	mgr := NewGroupManager("$queue/test", store, time.Second, nil)

	err = mgr.AddConsumer(ctx, "group1", "consumer1", "client1", "")
	require.NoError(t, err)

	// Verify consumer was added
	group, exists := mgr.GetGroup("group1")
	assert.True(t, exists)
	assert.NotNil(t, group)

	consumer, exists := group.GetConsumer("consumer1")
	assert.True(t, exists)
	assert.Equal(t, "consumer1", consumer.ID)
	assert.Equal(t, "client1", consumer.ClientID)
	assert.Equal(t, "group1", consumer.GroupID)
}

func TestGroupManager_AddConsumer_Duplicate(t *testing.T) {
	ctx := context.Background()
	store := memory.New()
	config := types.DefaultQueueConfig("$queue/test")
	err := store.CreateQueue(ctx, config)
	require.NoError(t, err)

	mgr := NewGroupManager("$queue/test", store, time.Second, nil)

	err = mgr.AddConsumer(ctx, "group1", "consumer1", "client1", "")
	require.NoError(t, err)

	// Try to add same consumer again
	err = mgr.AddConsumer(ctx, "group1", "consumer1", "client1", "")
	assert.NoError(t, err) // Should succeed (idempotent)
}

func TestGroupManager_RemoveConsumer(t *testing.T) {
	ctx := context.Background()
	store := memory.New()
	config := types.DefaultQueueConfig("$queue/test")
	err := store.CreateQueue(ctx, config)
	require.NoError(t, err)

	mgr := NewGroupManager("$queue/test", store, time.Second, nil)

	err = mgr.AddConsumer(ctx, "group1", "consumer1", "client1", "")
	require.NoError(t, err)

	err = mgr.RemoveConsumer(ctx, "group1", "consumer1")
	require.NoError(t, err)

	// Verify consumer was removed
	group, exists := mgr.GetGroup("group1")
	assert.False(t, exists) // Group should be removed when last consumer leaves
	assert.Nil(t, group)
}

func TestGroupManager_RemoveConsumer_NonExistent(t *testing.T) {
	ctx := context.Background()
	store := memory.New()
	mgr := NewGroupManager("$queue/test", store, time.Second, nil)

	err := mgr.RemoveConsumer(ctx, "nonexistent", "consumer1")
	assert.Error(t, err)
}

func TestGroupManager_GetGroup(t *testing.T) {
	ctx := context.Background()
	store := memory.New()
	config := types.DefaultQueueConfig("$queue/test")
	err := store.CreateQueue(ctx, config)
	require.NoError(t, err)

	mgr := NewGroupManager("$queue/test", store, time.Second, nil)

	// Non-existent group
	group, exists := mgr.GetGroup("nonexistent")
	assert.False(t, exists)
	assert.Nil(t, group)

	// Add consumer to create group
	err = mgr.AddConsumer(ctx, "group1", "consumer1", "client1", "")
	require.NoError(t, err)

	// Now group should exist
	group, exists = mgr.GetGroup("group1")
	assert.True(t, exists)
	assert.NotNil(t, group)
	assert.Equal(t, "group1", group.ID())
}

func TestGroupManager_ListGroups(t *testing.T) {
	ctx := context.Background()
	store := memory.New()
	config := types.DefaultQueueConfig("$queue/test")
	err := store.CreateQueue(ctx, config)
	require.NoError(t, err)

	mgr := NewGroupManager("$queue/test", store, time.Second, nil)

	// Empty initially
	groups := mgr.ListGroups()
	assert.Len(t, groups, 0)

	// Add consumers to multiple groups
	err = mgr.AddConsumer(ctx, "group1", "consumer1", "client1", "")
	require.NoError(t, err)
	err = mgr.AddConsumer(ctx, "group2", "consumer2", "client2", "")
	require.NoError(t, err)

	groups = mgr.ListGroups()
	assert.Len(t, groups, 2)
}

func TestGroupManager_Rebalance(t *testing.T) {
	ctx := context.Background()
	store := memory.New()
	config := types.DefaultQueueConfig("$queue/test")
	err := store.CreateQueue(ctx, config)
	require.NoError(t, err)

	mgr := NewGroupManager("$queue/test", store, time.Second, nil)

	// Add 3 consumers
	err = mgr.AddConsumer(ctx, "group1", "consumer1", "client1", "")
	require.NoError(t, err)
	err = mgr.AddConsumer(ctx, "group1", "consumer2", "client2", "")
	require.NoError(t, err)
	err = mgr.AddConsumer(ctx, "group1", "consumer3", "client3", "")
	require.NoError(t, err)

	// Create partitions
	partitions := make([]*MockPartition, 6)
	for i := 0; i < 6; i++ {
		partitions[i] = NewMockPartition(i)
	}

	// Rebalance
	err = mgr.Rebalance("group1", toPartitions(partitions))
	require.NoError(t, err)

	// Verify all partitions are assigned
	group, _ := mgr.GetGroup("group1")
	consumer1, _ := group.GetConsumer("consumer1")
	consumer2, _ := group.GetConsumer("consumer2")
	consumer3, _ := group.GetConsumer("consumer3")

	totalAssigned := len(consumer1.AssignedParts) + len(consumer2.AssignedParts) + len(consumer3.AssignedParts)
	assert.Equal(t, 6, totalAssigned)

	// Each consumer should have 2 partitions (evenly distributed)
	assert.Equal(t, 2, len(consumer1.AssignedParts))
	assert.Equal(t, 2, len(consumer2.AssignedParts))
	assert.Equal(t, 2, len(consumer3.AssignedParts))

	// Verify partitions know their assignment
	for i := 0; i < 6; i++ {
		assert.True(t, partitions[i].IsAssigned())
		assert.NotEmpty(t, partitions[i].AssignedTo())
	}
}

func TestGroupManager_Rebalance_UnevenDistribution(t *testing.T) {
	ctx := context.Background()
	store := memory.New()
	config := types.DefaultQueueConfig("$queue/test")
	err := store.CreateQueue(ctx, config)
	require.NoError(t, err)

	mgr := NewGroupManager("$queue/test", store, time.Second, nil)

	// Add 2 consumers
	err = mgr.AddConsumer(ctx, "group1", "consumer1", "client1", "")
	require.NoError(t, err)
	err = mgr.AddConsumer(ctx, "group1", "consumer2", "client2", "")
	require.NoError(t, err)

	// Create partitions
	partitions := make([]*MockPartition, 5)
	for i := 0; i < 5; i++ {
		partitions[i] = NewMockPartition(i)
	}

	// Rebalance
	err = mgr.Rebalance("group1", toPartitions(partitions))
	require.NoError(t, err)

	// Verify all partitions are assigned
	group, _ := mgr.GetGroup("group1")
	consumer1, _ := group.GetConsumer("consumer1")
	consumer2, _ := group.GetConsumer("consumer2")

	totalAssigned := len(consumer1.AssignedParts) + len(consumer2.AssignedParts)
	assert.Equal(t, 5, totalAssigned)

	// One consumer should have 3, one should have 2
	assert.True(t, len(consumer1.AssignedParts) == 3 || len(consumer2.AssignedParts) == 3)
	assert.True(t, len(consumer1.AssignedParts) == 2 || len(consumer2.AssignedParts) == 2)
}

func TestGroup_Consumers(t *testing.T) {
	ctx := context.Background()
	store := memory.New()
	config := types.DefaultQueueConfig("$queue/test")
	err := store.CreateQueue(ctx, config)
	require.NoError(t, err)

	mgr := NewGroupManager("$queue/test", store, time.Second, nil)

	err = mgr.AddConsumer(ctx, "group1", "consumer1", "client1", "")
	require.NoError(t, err)
	err = mgr.AddConsumer(ctx, "group1", "consumer2", "client2", "")
	require.NoError(t, err)

	group, _ := mgr.GetGroup("group1")
	consumers := group.ListConsumers()

	assert.Len(t, consumers, 2)
	ids := []string{consumers[0].ID, consumers[1].ID}
	assert.Contains(t, ids, "consumer1")
	assert.Contains(t, ids, "consumer2")
}

func TestGroup_Size(t *testing.T) {
	ctx := context.Background()
	store := memory.New()
	config := types.DefaultQueueConfig("$queue/test")
	err := store.CreateQueue(ctx, config)
	require.NoError(t, err)

	mgr := NewGroupManager("$queue/test", store, time.Second, nil)

	err = mgr.AddConsumer(ctx, "group1", "consumer1", "client1", "")
	require.NoError(t, err)

	group, _ := mgr.GetGroup("group1")
	assert.Equal(t, 1, group.Size())

	err = mgr.AddConsumer(ctx, "group1", "consumer2", "client2", "")
	require.NoError(t, err)

	assert.Equal(t, 2, group.Size())
}

func TestGroup_GetConsumerForPartition(t *testing.T) {
	ctx := context.Background()
	store := memory.New()
	config := types.DefaultQueueConfig("$queue/test")
	err := store.CreateQueue(ctx, config)
	require.NoError(t, err)

	mgr := NewGroupManager("$queue/test", store, time.Second, nil)

	err = mgr.AddConsumer(ctx, "group1", "consumer1", "client1", "")
	require.NoError(t, err)

	// Create and assign partitions
	partitions := make([]*MockPartition, 4)
	for i := 0; i < 4; i++ {
		partitions[i] = NewMockPartition(i)
	}
	err = mgr.Rebalance("group1", toPartitions(partitions))
	require.NoError(t, err)

	group, _ := mgr.GetGroup("group1")

	// All partitions should be assigned to consumer1
	for i := 0; i < 4; i++ {
		consumer, exists := group.GetConsumerForPartition(i)
		assert.True(t, exists)
		assert.NotNil(t, consumer)
		assert.Equal(t, "consumer1", consumer.ID)
	}

	// Non-existent partition
	consumer, exists := group.GetConsumerForPartition(999)
	assert.False(t, exists)
	assert.Nil(t, consumer)
}
