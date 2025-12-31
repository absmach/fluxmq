// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package queue

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/absmach/mqtt/queue/storage"
)

// ConsumerGroupManager manages consumer groups for a queue.
type ConsumerGroupManager struct {
	queueName     string
	groups        map[string]*ConsumerGroup
	consumerStore storage.ConsumerStore
	mu            sync.RWMutex
}

// NewConsumerGroupManager creates a new consumer group manager.
func NewConsumerGroupManager(queueName string, consumerStore storage.ConsumerStore) *ConsumerGroupManager {
	return &ConsumerGroupManager{
		queueName:     queueName,
		groups:        make(map[string]*ConsumerGroup),
		consumerStore: consumerStore,
	}
}

// AddConsumer adds a consumer to a group.
func (cgm *ConsumerGroupManager) AddConsumer(ctx context.Context, groupID, consumerID, clientID, proxyNodeID string) error {
	cgm.mu.Lock()
	defer cgm.mu.Unlock()

	group, exists := cgm.groups[groupID]
	if !exists {
		group = NewConsumerGroup(groupID, cgm.queueName)
		cgm.groups[groupID] = group
	}

	consumer := &storage.Consumer{
		ID:            consumerID,
		ClientID:      clientID,
		GroupID:       groupID,
		QueueName:     cgm.queueName,
		RegisteredAt:  time.Now(),
		LastHeartbeat: time.Now(),
		ProxyNodeID:   proxyNodeID,
	}

	if err := cgm.consumerStore.RegisterConsumer(ctx, consumer); err != nil {
		return err
	}

	group.AddConsumer(consumer)
	return nil
}

// RemoveConsumer removes a consumer from a group.
func (cgm *ConsumerGroupManager) RemoveConsumer(ctx context.Context, groupID, consumerID string) error {
	cgm.mu.Lock()
	defer cgm.mu.Unlock()

	group, exists := cgm.groups[groupID]
	if !exists {
		return storage.ErrConsumerNotFound
	}

	if err := cgm.consumerStore.UnregisterConsumer(ctx, cgm.queueName, groupID, consumerID); err != nil {
		return err
	}

	group.RemoveConsumer(consumerID)

	// Remove empty groups
	if group.Size() == 0 {
		delete(cgm.groups, groupID)
	}

	return nil
}

// GetGroup returns a consumer group by ID.
func (cgm *ConsumerGroupManager) GetGroup(groupID string) (*ConsumerGroup, bool) {
	cgm.mu.RLock()
	defer cgm.mu.RUnlock()

	group, exists := cgm.groups[groupID]
	return group, exists
}

// ListGroups returns all consumer groups.
func (cgm *ConsumerGroupManager) ListGroups() []*ConsumerGroup {
	cgm.mu.RLock()
	defer cgm.mu.RUnlock()

	groups := make([]*ConsumerGroup, 0, len(cgm.groups))
	for _, group := range cgm.groups {
		groups = append(groups, group)
	}
	return groups
}

// Rebalance triggers rebalancing for a specific group.
func (cgm *ConsumerGroupManager) Rebalance(groupID string, partitions []*Partition) error {
	cgm.mu.Lock()
	defer cgm.mu.Unlock()

	group, exists := cgm.groups[groupID]
	if !exists {
		return fmt.Errorf("group %s not found", groupID)
	}

	group.Rebalance(partitions)
	return nil
}

// ConsumerGroup represents a group of consumers sharing a queue.
type ConsumerGroup struct {
	id        string
	queueName string
	consumers map[string]*storage.Consumer
	mu        sync.RWMutex
}

// NewConsumerGroup creates a new consumer group.
func NewConsumerGroup(id, queueName string) *ConsumerGroup {
	return &ConsumerGroup{
		id:        id,
		queueName: queueName,
		consumers: make(map[string]*storage.Consumer),
	}
}

// ID returns the group ID.
func (cg *ConsumerGroup) ID() string {
	return cg.id
}

// AddConsumer adds a consumer to the group.
func (cg *ConsumerGroup) AddConsumer(consumer *storage.Consumer) {
	cg.mu.Lock()
	defer cg.mu.Unlock()

	cg.consumers[consumer.ID] = consumer
}

// RemoveConsumer removes a consumer from the group.
func (cg *ConsumerGroup) RemoveConsumer(consumerID string) {
	cg.mu.Lock()
	defer cg.mu.Unlock()

	delete(cg.consumers, consumerID)
}

// GetConsumer returns a consumer by ID.
func (cg *ConsumerGroup) GetConsumer(consumerID string) (*storage.Consumer, bool) {
	cg.mu.RLock()
	defer cg.mu.RUnlock()

	consumer, exists := cg.consumers[consumerID]
	return consumer, exists
}

// ListConsumers returns all consumers in the group.
func (cg *ConsumerGroup) ListConsumers() []*storage.Consumer {
	cg.mu.RLock()
	defer cg.mu.RUnlock()

	consumers := make([]*storage.Consumer, 0, len(cg.consumers))
	for _, consumer := range cg.consumers {
		consumers = append(consumers, consumer)
	}
	return consumers
}

// Size returns the number of consumers in the group.
func (cg *ConsumerGroup) Size() int {
	cg.mu.RLock()
	defer cg.mu.RUnlock()

	return len(cg.consumers)
}

// Rebalance assigns partitions to consumers in the group.
// Uses simple round-robin strategy: partitions divided evenly among consumers.
func (cg *ConsumerGroup) Rebalance(partitions []*Partition) {
	cg.mu.Lock()
	defer cg.mu.Unlock()

	// Clear existing assignments
	for _, partition := range partitions {
		partition.Unassign()
	}

	// Clear consumer assignments
	for _, consumer := range cg.consumers {
		consumer.AssignedParts = []int{}
	}

	if len(cg.consumers) == 0 {
		return
	}

	// Convert to slice for indexing
	consumers := make([]*storage.Consumer, 0, len(cg.consumers))
	for _, consumer := range cg.consumers {
		consumers = append(consumers, consumer)
	}

	// Calculate partitions per consumer
	partitionsPerConsumer := len(partitions) / len(consumers)
	remainder := len(partitions) % len(consumers)

	partitionIdx := 0
	for consumerIdx, consumer := range consumers {
		count := partitionsPerConsumer
		if consumerIdx < remainder {
			count++
		}

		consumer.AssignedParts = make([]int, 0, count)
		for i := 0; i < count && partitionIdx < len(partitions); i++ {
			partition := partitions[partitionIdx]
			partition.AssignTo(consumer.ID)
			consumer.AssignedParts = append(consumer.AssignedParts, partition.ID())
			partitionIdx++
		}
	}
}

// GetConsumerForPartition returns the consumer assigned to a partition.
func (cg *ConsumerGroup) GetConsumerForPartition(partitionID int) (*storage.Consumer, bool) {
	cg.mu.RLock()
	defer cg.mu.RUnlock()

	for _, consumer := range cg.consumers {
		for _, assignedPart := range consumer.AssignedParts {
			if assignedPart == partitionID {
				return consumer, true
			}
		}
	}

	return nil, false
}
