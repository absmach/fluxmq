// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package queue

import (
	"context"
	"testing"

	queueStorage "github.com/absmach/mqtt/queue/storage"
	"github.com/absmach/mqtt/queue/storage/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewQueue(t *testing.T) {
	store := memory.New()
	config := queueStorage.DefaultQueueConfig("$queue/test")

	queue := NewQueue(config, store, store)

	assert.Equal(t, "$queue/test", queue.Name())
	assert.Equal(t, config, queue.Config())
	assert.Len(t, queue.Partitions(), config.Partitions)
	assert.NotNil(t, queue.ConsumerGroups())
	assert.NotNil(t, queue.OrderingEnforcer())
}

func TestQueue_Name(t *testing.T) {
	store := memory.New()
	config := queueStorage.DefaultQueueConfig("$queue/myqueue")
	queue := NewQueue(config, store, store)

	assert.Equal(t, "$queue/myqueue", queue.Name())
}

func TestQueue_Config(t *testing.T) {
	store := memory.New()
	config := queueStorage.DefaultQueueConfig("$queue/test")
	config.Partitions = 5
	queue := NewQueue(config, store, store)

	retrievedConfig := queue.Config()
	assert.Equal(t, "$queue/test", retrievedConfig.Name)
	assert.Equal(t, 5, retrievedConfig.Partitions)
}

func TestQueue_UpdateConfig(t *testing.T) {
	store := memory.New()
	config := queueStorage.DefaultQueueConfig("$queue/test")
	queue := NewQueue(config, store, store)

	newConfig := config
	newConfig.MaxMessageSize = 2048
	queue.UpdateConfig(newConfig)

	retrievedConfig := queue.Config()
	assert.Equal(t, int64(2048), retrievedConfig.MaxMessageSize)
}

func TestQueue_Partitions(t *testing.T) {
	store := memory.New()
	config := queueStorage.DefaultQueueConfig("$queue/test")
	config.Partitions = 5
	queue := NewQueue(config, store, store)

	partitions := queue.Partitions()
	assert.Len(t, partitions, 5)
	for i, p := range partitions {
		assert.Equal(t, i, p.ID())
	}
}

func TestQueue_GetPartition(t *testing.T) {
	store := memory.New()
	config := queueStorage.DefaultQueueConfig("$queue/test")
	config.Partitions = 3
	queue := NewQueue(config, store, store)

	// Valid partition
	partition, exists := queue.GetPartition(1)
	assert.True(t, exists)
	assert.NotNil(t, partition)
	assert.Equal(t, 1, partition.ID())

	// Invalid partition (negative)
	partition, exists = queue.GetPartition(-1)
	assert.False(t, exists)
	assert.Nil(t, partition)

	// Invalid partition (out of range)
	partition, exists = queue.GetPartition(3)
	assert.False(t, exists)
	assert.Nil(t, partition)
}

func TestQueue_GetPartitionForMessage(t *testing.T) {
	store := memory.New()
	config := queueStorage.DefaultQueueConfig("$queue/test")
	config.Partitions = 10
	queue := NewQueue(config, store, store)

	// Same key should always return same partition
	key1 := "user-123"
	partition1 := queue.GetPartitionForMessage(key1)
	partition2 := queue.GetPartitionForMessage(key1)
	assert.Equal(t, partition1, partition2)
	assert.GreaterOrEqual(t, partition1, 0)
	assert.Less(t, partition1, 10)

	// Empty key returns random partition (but valid)
	partition := queue.GetPartitionForMessage("")
	assert.GreaterOrEqual(t, partition, 0)
	assert.Less(t, partition, 10)
}

func TestQueue_AddConsumer(t *testing.T) {
	ctx := context.Background()
	store := memory.New()
	config := queueStorage.DefaultQueueConfig("$queue/test")
	err := store.CreateQueue(ctx, config)
	require.NoError(t, err)

	queue := NewQueue(config, store, store)

	err = queue.AddConsumer(ctx, "group1", "consumer1", "client1", "")
	require.NoError(t, err)

	// Verify consumer was added
	group, exists := queue.ConsumerGroups().GetGroup("group1")
	assert.True(t, exists)
	assert.NotNil(t, group)

	consumers := group.ListConsumers()
	assert.Len(t, consumers, 1)
	assert.Equal(t, "consumer1", consumers[0].ID)
}

func TestQueue_AddConsumer_MultipleConsumers(t *testing.T) {
	ctx := context.Background()
	store := memory.New()
	config := queueStorage.DefaultQueueConfig("$queue/test")
	config.Partitions = 4
	err := store.CreateQueue(ctx, config)
	require.NoError(t, err)

	queue := NewQueue(config, store, store)

	// Add multiple consumers to same group
	err = queue.AddConsumer(ctx, "group1", "consumer1", "client1", "")
	require.NoError(t, err)
	err = queue.AddConsumer(ctx, "group1", "consumer2", "client2", "")
	require.NoError(t, err)

	group, exists := queue.ConsumerGroups().GetGroup("group1")
	assert.True(t, exists)
	consumers := group.ListConsumers()
	assert.Len(t, consumers, 2)

	// Verify partitions were assigned
	consumer1, _ := group.GetConsumer("consumer1")
	consumer2, _ := group.GetConsumer("consumer2")
	assert.NotEmpty(t, consumer1.AssignedParts)
	assert.NotEmpty(t, consumer2.AssignedParts)
}

func TestQueue_RemoveConsumer(t *testing.T) {
	ctx := context.Background()
	store := memory.New()
	config := queueStorage.DefaultQueueConfig("$queue/test")
	err := store.CreateQueue(ctx, config)
	require.NoError(t, err)

	queue := NewQueue(config, store, store)

	// Add consumer
	err = queue.AddConsumer(ctx, "group1", "consumer1", "client1", "")
	require.NoError(t, err)

	// Remove consumer
	err = queue.RemoveConsumer(ctx, "group1", "consumer1")
	require.NoError(t, err)

	// Verify consumer was removed (group may or may not exist after last consumer removed)
	group, exists := queue.ConsumerGroups().GetGroup("group1")
	if exists {
		consumers := group.ListConsumers()
		assert.Len(t, consumers, 0) // But no consumers
	}
}

func TestQueue_RemoveConsumer_RebalancesPartitions(t *testing.T) {
	ctx := context.Background()
	store := memory.New()
	config := queueStorage.DefaultQueueConfig("$queue/test")
	config.Partitions = 6
	err := store.CreateQueue(ctx, config)
	require.NoError(t, err)

	queue := NewQueue(config, store, store)

	// Add 3 consumers
	err = queue.AddConsumer(ctx, "group1", "consumer1", "client1", "")
	require.NoError(t, err)
	err = queue.AddConsumer(ctx, "group1", "consumer2", "client2", "")
	require.NoError(t, err)
	err = queue.AddConsumer(ctx, "group1", "consumer3", "client3", "")
	require.NoError(t, err)

	// Remove one consumer
	err = queue.RemoveConsumer(ctx, "group1", "consumer2")
	require.NoError(t, err)

	// Verify remaining consumers have partitions redistributed
	group, _ := queue.ConsumerGroups().GetGroup("group1")
	consumer1, _ := group.GetConsumer("consumer1")
	consumer3, _ := group.GetConsumer("consumer3")

	totalAssigned := len(consumer1.AssignedParts) + len(consumer3.AssignedParts)
	assert.Equal(t, 6, totalAssigned) // All partitions still assigned
}

func TestQueue_GetConsumerForPartition(t *testing.T) {
	ctx := context.Background()
	store := memory.New()
	config := queueStorage.DefaultQueueConfig("$queue/test")
	config.Partitions = 2
	err := store.CreateQueue(ctx, config)
	require.NoError(t, err)

	queue := NewQueue(config, store, store)

	// Add consumer
	err = queue.AddConsumer(ctx, "group1", "consumer1", "client1", "")
	require.NoError(t, err)

	// Consumer should be assigned to partitions
	consumer, exists := queue.GetConsumerForPartition("group1", 0)
	assert.True(t, exists)
	assert.NotNil(t, consumer)
	assert.Equal(t, "consumer1", consumer.ID)

	// Non-existent group
	consumer, exists = queue.GetConsumerForPartition("nonexistent", 0)
	assert.False(t, exists)
	assert.Nil(t, consumer)
}

func TestQueue_OrderingEnforcer(t *testing.T) {
	store := memory.New()
	config := queueStorage.DefaultQueueConfig("$queue/test")
	config.Ordering = queueStorage.OrderingPartition
	queue := NewQueue(config, store, store)

	enforcer := queue.OrderingEnforcer()
	assert.NotNil(t, enforcer)

	stats := enforcer.(*OrderingEnforcer).Stats()
	assert.Equal(t, queueStorage.OrderingPartition, stats.Mode)
}

func TestQueue_MultipleGroups(t *testing.T) {
	ctx := context.Background()
	store := memory.New()
	config := queueStorage.DefaultQueueConfig("$queue/test")
	config.Partitions = 4
	err := store.CreateQueue(ctx, config)
	require.NoError(t, err)

	queue := NewQueue(config, store, store)

	// Add consumers to different groups
	err = queue.AddConsumer(ctx, "group1", "consumer1", "client1", "")
	require.NoError(t, err)
	err = queue.AddConsumer(ctx, "group2", "consumer2", "client2", "")
	require.NoError(t, err)

	// Each group should exist independently
	group1, exists1 := queue.ConsumerGroups().GetGroup("group1")
	group2, exists2 := queue.ConsumerGroups().GetGroup("group2")
	assert.True(t, exists1)
	assert.True(t, exists2)

	// Each should have one consumer
	assert.Len(t, group1.ListConsumers(), 1)
	assert.Len(t, group2.ListConsumers(), 1)

	// Both groups should have all partitions assigned
	consumer1, _ := group1.GetConsumer("consumer1")
	consumer2, _ := group2.GetConsumer("consumer2")
	assert.Len(t, consumer1.AssignedParts, 4)
	assert.Len(t, consumer2.AssignedParts, 4)
}
