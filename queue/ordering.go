// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package queue

import (
	"fmt"
	"sync"

	queueStorage "github.com/absmach/mqtt/queue/storage"
)

// OrderingEnforcer enforces message ordering within partitions.
type OrderingEnforcer struct {
	mode             queueStorage.OrderingMode
	lastDelivered    map[int]uint64 // partitionID -> last delivered sequence
	mu               sync.RWMutex
}

// NewOrderingEnforcer creates a new ordering enforcer.
func NewOrderingEnforcer(mode queueStorage.OrderingMode) *OrderingEnforcer {
	return &OrderingEnforcer{
		mode:          mode,
		lastDelivered: make(map[int]uint64),
	}
}

// CanDeliver checks if a message can be delivered according to ordering rules.
func (oe *OrderingEnforcer) CanDeliver(msg *queueStorage.QueueMessage) (bool, error) {
	switch oe.mode {
	case queueStorage.OrderingNone:
		// No ordering requirements
		return true, nil

	case queueStorage.OrderingPartition:
		// Partition-based ordering: messages within same partition must be delivered in order
		return oe.checkPartitionOrder(msg)

	case queueStorage.OrderingStrict:
		// Strict global ordering: all messages must be delivered in global sequence order
		// This requires single partition (partition 0)
		if msg.PartitionID != 0 {
			return false, fmt.Errorf("strict ordering requires all messages in partition 0, got partition %d", msg.PartitionID)
		}
		return oe.checkPartitionOrder(msg)

	default:
		return false, fmt.Errorf("unknown ordering mode: %s", oe.mode)
	}
}

// checkPartitionOrder checks if message is next in sequence for its partition.
func (oe *OrderingEnforcer) checkPartitionOrder(msg *queueStorage.QueueMessage) (bool, error) {
	oe.mu.RLock()
	lastSeq, exists := oe.lastDelivered[msg.PartitionID]
	oe.mu.RUnlock()

	if !exists {
		// First message in partition, allow
		return true, nil
	}

	// Message sequence must be greater than last delivered
	// Allow gaps for now (messages might have been acked before delivery due to retries)
	if msg.Sequence <= lastSeq {
		return false, fmt.Errorf("out-of-order message: partition %d, expected seq > %d, got %d",
			msg.PartitionID, lastSeq, msg.Sequence)
	}

	return true, nil
}

// MarkDelivered records that a message was delivered.
func (oe *OrderingEnforcer) MarkDelivered(msg *queueStorage.QueueMessage) {
	if oe.mode == queueStorage.OrderingNone {
		return
	}

	oe.mu.Lock()
	defer oe.mu.Unlock()

	// Update last delivered sequence for partition
	currentLast, exists := oe.lastDelivered[msg.PartitionID]
	if !exists || msg.Sequence > currentLast {
		oe.lastDelivered[msg.PartitionID] = msg.Sequence
	}
}

// Reset clears ordering state for a partition (used when partition is reassigned).
func (oe *OrderingEnforcer) Reset(partitionID int) {
	oe.mu.Lock()
	defer oe.mu.Unlock()
	delete(oe.lastDelivered, partitionID)
}

// ResetAll clears all ordering state.
func (oe *OrderingEnforcer) ResetAll() {
	oe.mu.Lock()
	defer oe.mu.Unlock()
	oe.lastDelivered = make(map[int]uint64)
}

// GetLastDelivered returns the last delivered sequence for a partition.
func (oe *OrderingEnforcer) GetLastDelivered(partitionID int) (uint64, bool) {
	oe.mu.RLock()
	defer oe.mu.RUnlock()
	seq, exists := oe.lastDelivered[partitionID]
	return seq, exists
}

// Stats returns ordering statistics.
func (oe *OrderingEnforcer) Stats() OrderingStats {
	oe.mu.RLock()
	defer oe.mu.RUnlock()

	partitionSeqs := make(map[int]uint64, len(oe.lastDelivered))
	for partID, seq := range oe.lastDelivered {
		partitionSeqs[partID] = seq
	}

	return OrderingStats{
		Mode:             oe.mode,
		PartitionCount:   len(oe.lastDelivered),
		PartitionSeqs:    partitionSeqs,
	}
}

// OrderingStats holds ordering statistics.
type OrderingStats struct {
	Mode           queueStorage.OrderingMode
	PartitionCount int
	PartitionSeqs  map[int]uint64
}
