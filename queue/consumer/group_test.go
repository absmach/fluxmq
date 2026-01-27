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

func TestNewGroupManager(t *testing.T) {
	store := memory.New()
	mgr := NewGroupManager("$queue/test", store, time.Second)

	assert.NotNil(t, mgr)
	assert.Equal(t, "$queue/test", mgr.queueName)
}

func TestGroupManager_AddConsumer(t *testing.T) {
	ctx := context.Background()
	store := memory.New()
	config := types.DefaultQueueConfig("$queue/test")
	err := store.CreateQueue(ctx, config)
	require.NoError(t, err)

	mgr := NewGroupManager("$queue/test", store, time.Second)

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

	mgr := NewGroupManager("$queue/test", store, time.Second)

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

	mgr := NewGroupManager("$queue/test", store, time.Second)

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
	mgr := NewGroupManager("$queue/test", store, time.Second)

	err := mgr.RemoveConsumer(ctx, "nonexistent", "consumer1")
	assert.Error(t, err)
}

func TestGroupManager_GetGroup(t *testing.T) {
	ctx := context.Background()
	store := memory.New()
	config := types.DefaultQueueConfig("$queue/test")
	err := store.CreateQueue(ctx, config)
	require.NoError(t, err)

	mgr := NewGroupManager("$queue/test", store, time.Second)

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

	mgr := NewGroupManager("$queue/test", store, time.Second)

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

func TestGroupManager_GetNextConsumer(t *testing.T) {
	ctx := context.Background()
	store := memory.New()
	config := types.DefaultQueueConfig("$queue/test")
	err := store.CreateQueue(ctx, config)
	require.NoError(t, err)

	mgr := NewGroupManager("$queue/test", store, time.Second)

	// Add 3 consumers
	err = mgr.AddConsumer(ctx, "group1", "consumer1", "client1", "")
	require.NoError(t, err)
	err = mgr.AddConsumer(ctx, "group1", "consumer2", "client2", "")
	require.NoError(t, err)
	err = mgr.AddConsumer(ctx, "group1", "consumer3", "client3", "")
	require.NoError(t, err)

	group, exists := mgr.GetGroup("group1")
	require.True(t, exists)

	// GetNextConsumer should return consumers in round-robin fashion
	consumer1, ok1 := group.GetNextConsumer()
	require.True(t, ok1)
	require.NotNil(t, consumer1)

	consumer2, ok2 := group.GetNextConsumer()
	require.True(t, ok2)
	require.NotNil(t, consumer2)

	consumer3, ok3 := group.GetNextConsumer()
	require.True(t, ok3)
	require.NotNil(t, consumer3)

	// Back to first consumer (round-robin)
	consumer4, ok4 := group.GetNextConsumer()
	require.True(t, ok4)
	require.NotNil(t, consumer4)
	assert.Equal(t, consumer1.ID, consumer4.ID)
}

func TestGroup_Consumers(t *testing.T) {
	ctx := context.Background()
	store := memory.New()
	config := types.DefaultQueueConfig("$queue/test")
	err := store.CreateQueue(ctx, config)
	require.NoError(t, err)

	mgr := NewGroupManager("$queue/test", store, time.Second)

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

	mgr := NewGroupManager("$queue/test", store, time.Second)

	err = mgr.AddConsumer(ctx, "group1", "consumer1", "client1", "")
	require.NoError(t, err)

	group, _ := mgr.GetGroup("group1")
	assert.Equal(t, 1, group.Size())

	err = mgr.AddConsumer(ctx, "group1", "consumer2", "client2", "")
	require.NoError(t, err)

	assert.Equal(t, 2, group.Size())
}

func TestGroupManager_Rebalance(t *testing.T) {
	ctx := context.Background()
	store := memory.New()
	config := types.DefaultQueueConfig("$queue/test")
	err := store.CreateQueue(ctx, config)
	require.NoError(t, err)

	mgr := NewGroupManager("$queue/test", store, time.Second)

	// Add consumers
	err = mgr.AddConsumer(ctx, "group1", "consumer1", "client1", "")
	require.NoError(t, err)
	err = mgr.AddConsumer(ctx, "group1", "consumer2", "client2", "")
	require.NoError(t, err)

	// Rebalance should be a no-op now (no partitions)
	err = mgr.Rebalance("group1")
	assert.NoError(t, err)

	// Verify group still exists
	group, exists := mgr.GetGroup("group1")
	assert.True(t, exists)
	assert.NotNil(t, group)
	assert.Equal(t, 2, group.Size())
}

func TestGroupManager_Rebalance_NonExistentGroup(t *testing.T) {
	store := memory.New()
	mgr := NewGroupManager("$queue/test", store, time.Second)

	err := mgr.Rebalance("nonexistent")
	assert.Error(t, err)
}
