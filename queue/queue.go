// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package queue

import (
	"context"
	"sync"

	"github.com/absmach/fluxmq/queue/consumer"
	"github.com/absmach/fluxmq/queue/delivery"
	"github.com/absmach/fluxmq/queue/lifecycle"
	queueStorage "github.com/absmach/fluxmq/queue/storage"
	"github.com/absmach/fluxmq/queue/types"
)

// Queue represents a single durable queue with partitions and consumer groups.
type Queue struct {
	name             string
	config           types.QueueConfig
	partitions       []*Partition
	strategy         PartitionStrategy
	consumerGroups   *consumer.GroupManager
	messageStore     queueStorage.MessageStore
	orderingEnforcer *lifecycle.OrderingEnforcer
	mu               sync.RWMutex
}

// NewQueue creates a new queue instance.
func NewQueue(config types.QueueConfig, messageStore queueStorage.MessageStore, consumerStore queueStorage.ConsumerStore) *Queue {
	partitions := make([]*Partition, config.Partitions)
	for i := 0; i < config.Partitions; i++ {
		partitions[i] = NewPartition(i)
	}

	// Convert partitions to consumer.Partition interface slice
	consumerPartitions := make([]consumer.Partition, len(partitions))
	for i, p := range partitions {
		consumerPartitions[i] = p
	}

	return &Queue{
		name:             config.Name,
		config:           config,
		partitions:       partitions,
		strategy:         &HashPartitionStrategy{},
		consumerGroups:   consumer.NewGroupManager(config.Name, consumerStore, config.HeartbeatTimeout, consumerPartitions),
		messageStore:     messageStore,
		orderingEnforcer: lifecycle.NewOrderingEnforcer(config.Ordering),
	}
}

// Name returns the queue name.
func (q *Queue) Name() string {
	return q.name
}

// Config returns the queue configuration.
func (q *Queue) Config() types.QueueConfig {
	q.mu.RLock()
	defer q.mu.RUnlock()

	return q.config
}

// UpdateConfig updates the queue configuration.
func (q *Queue) UpdateConfig(config types.QueueConfig) {
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
func (q *Queue) ConsumerGroups() *consumer.GroupManager {
	return q.consumerGroups
}

// AddConsumer adds a consumer to a consumer group.
func (q *Queue) AddConsumer(ctx context.Context, groupID, consumerID, clientID, proxyNodeID string) error {
	if err := q.consumerGroups.AddConsumer(ctx, groupID, consumerID, clientID, proxyNodeID); err != nil {
		return err
	}

	// Trigger rebalancing for this group
	return q.consumerGroups.Rebalance(groupID, q.toConsumerPartitions())
}

// RemoveConsumer removes a consumer from a consumer group.
func (q *Queue) RemoveConsumer(ctx context.Context, groupID, consumerID string) error {
	if err := q.consumerGroups.RemoveConsumer(ctx, groupID, consumerID); err != nil {
		return err
	}

	// Trigger rebalancing for this group if it still exists
	if group, exists := q.consumerGroups.GetGroup(groupID); exists {
		return q.consumerGroups.Rebalance(group.ID(), q.toConsumerPartitions())
	}

	return nil
}

// GetConsumerForPartition returns the consumer assigned to a partition in a specific group.
func (q *Queue) GetConsumerForPartition(groupID string, partitionID int) (*types.Consumer, bool) {
	group, exists := q.consumerGroups.GetGroup(groupID)
	if !exists {
		return nil, false
	}

	return group.GetConsumerForPartition(partitionID)
}

// OrderingEnforcer returns the ordering enforcer for this queue.
func (q *Queue) OrderingEnforcer() delivery.OrderingEnforcer {
	return q.orderingEnforcer
}

// toConsumerPartitions converts the queue's partitions to a slice of consumer.Partition interface.
func (q *Queue) toConsumerPartitions() []consumer.Partition {
	q.mu.RLock()
	defer q.mu.RUnlock()

	consumerPartitions := make([]consumer.Partition, len(q.partitions))
	for i, p := range q.partitions {
		consumerPartitions[i] = p
	}
	return consumerPartitions
}
