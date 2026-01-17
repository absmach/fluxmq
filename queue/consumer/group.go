// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

// Package consumer provides consumer group management for durable queues.
package consumer

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	queueStorage "github.com/absmach/fluxmq/queue/storage"
	"github.com/absmach/fluxmq/queue/types"
)

// Partition is an interface for partition operations needed by consumer groups.
// This avoids circular dependency with the queue package.
type Partition interface {
	ID() int
	AssignTo(consumerID string)
}

// GroupManager manages consumer groups for a queue.
type GroupManager struct {
	queueName        string
	groups           map[string]*Group
	consumerStore    queueStorage.ConsumerStore
	heartbeatTimeout time.Duration
	partitions       []Partition
	ctx              context.Context
	cancel           context.CancelFunc
	mu               sync.RWMutex
}

// NewGroupManager creates a new consumer group manager.
func NewGroupManager(queueName string, consumerStore queueStorage.ConsumerStore, heartbeatTimeout time.Duration, partitions []Partition) *GroupManager {
	ctx, cancel := context.WithCancel(context.Background())
	cgm := &GroupManager{
		queueName:        queueName,
		groups:           make(map[string]*Group),
		consumerStore:    consumerStore,
		heartbeatTimeout: heartbeatTimeout,
		partitions:       partitions,
		ctx:              ctx,
		cancel:           cancel,
	}

	// Start heartbeat monitoring
	go cgm.monitorHeartbeats()

	return cgm
}

// AddConsumer adds a consumer to a group.
func (cgm *GroupManager) AddConsumer(ctx context.Context, groupID, consumerID, clientID, proxyNodeID string) error {
	cgm.mu.Lock()
	defer cgm.mu.Unlock()

	group, exists := cgm.groups[groupID]
	if !exists {
		group = NewGroup(groupID, cgm.queueName)
		cgm.groups[groupID] = group
	}

	consumer := &types.Consumer{
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
func (cgm *GroupManager) RemoveConsumer(ctx context.Context, groupID, consumerID string) error {
	cgm.mu.Lock()
	defer cgm.mu.Unlock()

	group, exists := cgm.groups[groupID]
	if !exists {
		return queueStorage.ErrConsumerNotFound
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
func (cgm *GroupManager) GetGroup(groupID string) (*Group, bool) {
	cgm.mu.RLock()
	defer cgm.mu.RUnlock()

	group, exists := cgm.groups[groupID]
	return group, exists
}

// ListGroups returns all consumer groups.
func (cgm *GroupManager) ListGroups() []*Group {
	cgm.mu.RLock()
	defer cgm.mu.RUnlock()

	groups := make([]*Group, 0, len(cgm.groups))
	for _, group := range cgm.groups {
		groups = append(groups, group)
	}

	// Sort groups by ID for deterministic ordering (important for round-robin)
	sort.Slice(groups, func(i, j int) bool {
		return groups[i].ID() < groups[j].ID()
	})

	return groups
}

// RestoreConsumer restores a consumer from persistent storage without re-persisting.
// This is used during startup to restore consumers that were saved before shutdown.
func (cgm *GroupManager) RestoreConsumer(consumer *types.Consumer) {
	cgm.mu.Lock()
	defer cgm.mu.Unlock()

	group, exists := cgm.groups[consumer.GroupID]
	if !exists {
		group = NewGroup(consumer.GroupID, cgm.queueName)
		cgm.groups[consumer.GroupID] = group
	}
	group.AddConsumer(consumer)
}

// Rebalance triggers rebalancing for a specific group.
func (cgm *GroupManager) Rebalance(groupID string, partitions []Partition) error {
	cgm.mu.Lock()
	defer cgm.mu.Unlock()

	group, exists := cgm.groups[groupID]
	if !exists {
		return fmt.Errorf("group %s not found", groupID)
	}

	group.Rebalance(partitions)
	return nil
}

// UpdateHeartbeat updates the heartbeat timestamp for a consumer.
// Uses write lock to prevent race condition when updating consumer.LastHeartbeat.
func (cgm *GroupManager) UpdateHeartbeat(ctx context.Context, groupID, consumerID string) error {
	cgm.mu.Lock()
	group, exists := cgm.groups[groupID]
	if !exists {
		cgm.mu.Unlock()
		return queueStorage.ErrConsumerNotFound
	}

	consumer, exists := group.GetConsumer(consumerID)
	if !exists {
		cgm.mu.Unlock()
		return queueStorage.ErrConsumerNotFound
	}

	now := time.Now()
	consumer.LastHeartbeat = now
	cgm.mu.Unlock()

	return cgm.consumerStore.UpdateHeartbeat(ctx, cgm.queueName, groupID, consumerID, now)
}

// monitorHeartbeats runs in the background to check for stale consumers.
func (cgm *GroupManager) monitorHeartbeats() {
	ticker := time.NewTicker(cgm.heartbeatTimeout / 3)
	defer ticker.Stop()

	for {
		select {
		case <-cgm.ctx.Done():
			return
		case <-ticker.C:
			cgm.checkStaleConsumers()
		}
	}
}

// checkStaleConsumers identifies and removes consumers with stale heartbeats.
func (cgm *GroupManager) checkStaleConsumers() {
	cgm.mu.RLock()
	groups := make([]*Group, 0, len(cgm.groups))
	for _, group := range cgm.groups {
		groups = append(groups, group)
	}
	cgm.mu.RUnlock()

	now := time.Now()
	timeout := cgm.heartbeatTimeout

	for _, group := range groups {
		consumers := group.ListConsumers()
		for _, consumer := range consumers {
			if now.Sub(consumer.LastHeartbeat) > timeout {
				// Consumer heartbeat is stale, remove it
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				err := cgm.RemoveConsumer(ctx, group.ID(), consumer.ID)
				cancel() // Always cancel immediately after use to prevent context leak
				if err == nil {
					// Rebalance partitions after removing stale consumer
					_ = cgm.Rebalance(group.ID(), cgm.partitions)
				}
			}
		}
	}
}

// Stop stops the heartbeat monitoring.
func (cgm *GroupManager) Stop() {
	if cgm.cancel != nil {
		cgm.cancel()
	}
}

// Group represents a group of consumers sharing a queue.
type Group struct {
	id        string
	queueName string
	consumers map[string]*types.Consumer
	mu        sync.RWMutex
}

// NewGroup creates a new consumer group.
func NewGroup(id, queueName string) *Group {
	return &Group{
		id:        id,
		queueName: queueName,
		consumers: make(map[string]*types.Consumer),
	}
}

// ID returns the group ID.
func (cg *Group) ID() string {
	return cg.id
}

// AddConsumer adds a consumer to the group.
func (cg *Group) AddConsumer(consumer *types.Consumer) {
	cg.mu.Lock()
	defer cg.mu.Unlock()

	cg.consumers[consumer.ID] = consumer
}

// RemoveConsumer removes a consumer from the group.
func (cg *Group) RemoveConsumer(consumerID string) {
	cg.mu.Lock()
	defer cg.mu.Unlock()

	delete(cg.consumers, consumerID)
}

// GetConsumer returns a consumer by ID.
func (cg *Group) GetConsumer(consumerID string) (*types.Consumer, bool) {
	cg.mu.RLock()
	defer cg.mu.RUnlock()

	consumer, exists := cg.consumers[consumerID]
	return consumer, exists
}

// ListConsumers returns all consumers in the group.
func (cg *Group) ListConsumers() []*types.Consumer {
	cg.mu.RLock()
	defer cg.mu.RUnlock()

	consumers := make([]*types.Consumer, 0, len(cg.consumers))
	for _, consumer := range cg.consumers {
		consumers = append(consumers, consumer)
	}
	return consumers
}

// Size returns the number of consumers in the group.
func (cg *Group) Size() int {
	cg.mu.RLock()
	defer cg.mu.RUnlock()

	return len(cg.consumers)
}

// Rebalance assigns partitions to consumers in the group.
// Uses simple round-robin strategy: partitions divided evenly among consumers.
// Note: Partition.assignedTo is per-partition global state and not used for actual delivery.
// Delivery uses consumer.AssignedParts instead. We only clear consumer assignments here
// to avoid affecting other groups' partition tracking.
func (cg *Group) Rebalance(partitions []Partition) {
	cg.mu.Lock()
	defer cg.mu.Unlock()

	// Clear consumer assignments (don't clear partition.assignedTo as it affects all groups)
	for _, consumer := range cg.consumers {
		consumer.AssignedParts = []int{}
	}

	if len(cg.consumers) == 0 {
		return
	}

	// Convert to slice for indexing
	consumers := make([]*types.Consumer, 0, len(cg.consumers))
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
func (cg *Group) GetConsumerForPartition(partitionID int) (*types.Consumer, bool) {
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
