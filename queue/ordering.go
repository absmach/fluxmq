// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package queue

import (
	"fmt"
	"sync"

	queueStorage "github.com/absmach/fluxmq/queue/storage"
)

// OrderingEnforcer enforces message ordering within partitions.
// Group-aware: each consumer group tracks its own ordering independently.
type OrderingEnforcer struct {
	mode          queueStorage.OrderingMode
	lastDelivered map[string]map[int]uint64 // groupID -> partitionID -> last delivered sequence
	mu            sync.RWMutex
}

// NewOrderingEnforcer creates a new ordering enforcer.
func NewOrderingEnforcer(mode queueStorage.OrderingMode) *OrderingEnforcer {
	return &OrderingEnforcer{
		mode:          mode,
		lastDelivered: make(map[string]map[int]uint64),
	}
}

// CanDeliver checks if a message can be delivered according to ordering rules.
// groupID identifies the consumer group - each group has independent ordering tracking.
func (oe *OrderingEnforcer) CanDeliver(msg *queueStorage.Message, groupID string) (bool, error) {
	switch oe.mode {
	case queueStorage.OrderingNone:
		// No ordering requirements
		return true, nil

	case queueStorage.OrderingPartition:
		// Partition-based ordering: messages within same partition must be delivered in order
		return oe.checkPartitionOrder(msg, groupID)

	case queueStorage.OrderingStrict:
		// Strict global ordering: all messages must be delivered in global sequence order
		// This requires single partition (partition 0)
		if msg.PartitionID != 0 {
			return false, fmt.Errorf("strict ordering requires all messages in partition 0, got partition %d", msg.PartitionID)
		}
		return oe.checkPartitionOrder(msg, groupID)

	default:
		return false, fmt.Errorf("unknown ordering mode: %s", oe.mode)
	}
}

// checkPartitionOrder checks if message is next in sequence for its partition within a group.
func (oe *OrderingEnforcer) checkPartitionOrder(msg *queueStorage.Message, groupID string) (bool, error) {
	oe.mu.RLock()
	groupMap, groupExists := oe.lastDelivered[groupID]
	if !groupExists {
		oe.mu.RUnlock()
		return true, nil
	}
	lastSeq, exists := groupMap[msg.PartitionID]
	oe.mu.RUnlock()

	if !exists {
		// First message in partition for this group, allow
		return true, nil
	}

	// Message sequence must be greater than last delivered
	// Allow gaps for now (messages might have been acked before delivery due to retries)
	if msg.Sequence <= lastSeq {
		return false, fmt.Errorf("out-of-order message: group %s, partition %d, expected seq > %d, got %d",
			groupID, msg.PartitionID, lastSeq, msg.Sequence)
	}

	return true, nil
}

// MarkDelivered records that a message was delivered to a consumer group.
func (oe *OrderingEnforcer) MarkDelivered(msg *queueStorage.Message, groupID string) {
	if oe.mode == queueStorage.OrderingNone {
		return
	}

	oe.mu.Lock()
	defer oe.mu.Unlock()

	// Ensure group map exists
	if _, exists := oe.lastDelivered[groupID]; !exists {
		oe.lastDelivered[groupID] = make(map[int]uint64)
	}

	// Update last delivered sequence for partition within group
	currentLast, exists := oe.lastDelivered[groupID][msg.PartitionID]
	if !exists || msg.Sequence > currentLast {
		oe.lastDelivered[groupID][msg.PartitionID] = msg.Sequence
	}
}

// Reset clears ordering state for a partition across all groups.
func (oe *OrderingEnforcer) Reset(partitionID int) {
	oe.mu.Lock()
	defer oe.mu.Unlock()
	for groupID := range oe.lastDelivered {
		delete(oe.lastDelivered[groupID], partitionID)
	}
}

// ResetGroup clears ordering state for a specific group.
func (oe *OrderingEnforcer) ResetGroup(groupID string) {
	oe.mu.Lock()
	defer oe.mu.Unlock()
	delete(oe.lastDelivered, groupID)
}

// ResetAll clears all ordering state.
func (oe *OrderingEnforcer) ResetAll() {
	oe.mu.Lock()
	defer oe.mu.Unlock()
	oe.lastDelivered = make(map[string]map[int]uint64)
}

// GetLastDelivered returns the last delivered sequence for a partition in a group.
func (oe *OrderingEnforcer) GetLastDelivered(groupID string, partitionID int) (uint64, bool) {
	oe.mu.RLock()
	defer oe.mu.RUnlock()
	groupMap, exists := oe.lastDelivered[groupID]
	if !exists {
		return 0, false
	}
	seq, exists := groupMap[partitionID]
	return seq, exists
}

// Stats returns ordering statistics.
func (oe *OrderingEnforcer) Stats() OrderingStats {
	oe.mu.RLock()
	defer oe.mu.RUnlock()

	groupStats := make(map[string]map[int]uint64, len(oe.lastDelivered))
	totalPartitions := 0
	for groupID, partMap := range oe.lastDelivered {
		groupStats[groupID] = make(map[int]uint64, len(partMap))
		for partID, seq := range partMap {
			groupStats[groupID][partID] = seq
		}
		totalPartitions += len(partMap)
	}

	return OrderingStats{
		Mode:           oe.mode,
		GroupCount:     len(oe.lastDelivered),
		PartitionCount: totalPartitions,
		GroupStats:     groupStats,
	}
}

// OrderingStats holds ordering statistics.
type OrderingStats struct {
	Mode           queueStorage.OrderingMode
	GroupCount     int
	PartitionCount int
	GroupStats     map[string]map[int]uint64
}
