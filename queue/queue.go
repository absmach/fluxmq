// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package queue

import (
	"context"
	"sync"

	queueStorage "github.com/absmach/mqtt/queue/storage"
)

// Queue represents a single durable queue with partitions and consumer groups.
type Queue struct {
	name             string
	config           queueStorage.QueueConfig
	partitions       []*Partition
	strategy         PartitionStrategy
	consumerGroups   *ConsumerGroupManager
	messageStore     queueStorage.MessageStore
	orderingEnforcer *OrderingEnforcer
	mu               sync.RWMutex
}

// NewQueue creates a new queue instance.
func NewQueue(config queueStorage.QueueConfig, messageStore queueStorage.MessageStore, consumerStore queueStorage.ConsumerStore) *Queue {
	partitions := make([]*Partition, config.Partitions)
	for i := 0; i < config.Partitions; i++ {
		partitions[i] = NewPartition(i)
	}

	return &Queue{
		name:             config.Name,
		config:           config,
		partitions:       partitions,
		strategy:         &HashPartitionStrategy{},
		consumerGroups:   NewConsumerGroupManager(config.Name, consumerStore, config.HeartbeatTimeout, partitions),
		messageStore:     messageStore,
		orderingEnforcer: NewOrderingEnforcer(config.Ordering),
	}
}

// Name returns the queue name.
func (q *Queue) Name() string {
	return q.name
}

// Config returns the queue configuration.
func (q *Queue) Config() queueStorage.QueueConfig {
	q.mu.RLock()
	defer q.mu.RUnlock()

	return q.config
}

// UpdateConfig updates the queue configuration.
func (q *Queue) UpdateConfig(config queueStorage.QueueConfig) {
	q.mu.Lock()
	defer q.mu.Unlock()

	q.config = config
}

// Partitions returns all partitions.
func (q *Queue) Partitions() []*Partition {
	q.mu.RLock()
	defer q.mu.RUnlock()

	return q.partitions
}

// GetPartition returns a partition by ID.
func (q *Queue) GetPartition(id int) (*Partition, bool) {
	q.mu.RLock()
	defer q.mu.RUnlock()

	if id < 0 || id >= len(q.partitions) {
		return nil, false
	}

	return q.partitions[id], true
}

// GetPartitionForMessage returns the partition ID for a message based on its partition key.
func (q *Queue) GetPartitionForMessage(partitionKey string) int {
	q.mu.RLock()
	defer q.mu.RUnlock()

	return q.strategy.GetPartition(partitionKey, len(q.partitions))
}

// ConsumerGroups returns the consumer group manager.
func (q *Queue) ConsumerGroups() *ConsumerGroupManager {
	return q.consumerGroups
}

// AddConsumer adds a consumer to a consumer group.
func (q *Queue) AddConsumer(ctx context.Context, groupID, consumerID, clientID, proxyNodeID string) error {
	if err := q.consumerGroups.AddConsumer(ctx, groupID, consumerID, clientID, proxyNodeID); err != nil {
		return err
	}

	// Trigger rebalancing for this group
	return q.consumerGroups.Rebalance(groupID, q.partitions)
}

// RemoveConsumer removes a consumer from a consumer group.
func (q *Queue) RemoveConsumer(ctx context.Context, groupID, consumerID string) error {
	if err := q.consumerGroups.RemoveConsumer(ctx, groupID, consumerID); err != nil {
		return err
	}

	// Trigger rebalancing for this group if it still exists
	if group, exists := q.consumerGroups.GetGroup(groupID); exists {
		return q.consumerGroups.Rebalance(group.ID(), q.partitions)
	}

	return nil
}

// GetConsumerForPartition returns the consumer assigned to a partition in a specific group.
func (q *Queue) GetConsumerForPartition(groupID string, partitionID int) (*queueStorage.Consumer, bool) {
	group, exists := q.consumerGroups.GetGroup(groupID)
	if !exists {
		return nil, false
	}

	return group.GetConsumerForPartition(partitionID)
}

// OrderingEnforcer returns the ordering enforcer for this queue.
func (q *Queue) OrderingEnforcer() *OrderingEnforcer {
	return q.orderingEnforcer
}
