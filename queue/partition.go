// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package queue

import (
	"hash"
	"hash/fnv"
	"math/rand"
	"sync"
)

// Hash pool for partition selection.
var hashPool = sync.Pool{
	New: func() interface{} {
		return fnv.New32a()
	},
}

// PartitionStrategy determines how messages are assigned to partitions.
type PartitionStrategy interface {
	GetPartition(partitionKey string, numPartitions int) int
}

// HashPartitionStrategy uses consistent hashing to assign partitions.
type HashPartitionStrategy struct{}

// GetPartition returns the partition ID for the given partition key.
func (h *HashPartitionStrategy) GetPartition(partitionKey string, numPartitions int) int {
	if partitionKey == "" {
		// No partition key = random assignment
		return rand.Intn(numPartitions)
	}

	// Get hash from pool
	hasher := hashPool.Get().(hash.Hash32)
	defer func() {
		hasher.Reset()
		hashPool.Put(hasher)
	}()

	hasher.Write([]byte(partitionKey))
	return int(hasher.Sum32()) % numPartitions
}

// Partition represents a single partition within a queue.
type Partition struct {
	id         int
	assignedTo string // consumerID currently assigned to this partition
}

// NewPartition creates a new partition.
func NewPartition(id int) *Partition {
	return &Partition{
		id: id,
	}
}

// ID returns the partition ID.
func (p *Partition) ID() int {
	return p.id
}

// AssignTo assigns this partition to a consumer.
func (p *Partition) AssignTo(consumerID string) {
	p.assignedTo = consumerID
}

// Unassign removes the consumer assignment from this partition.
func (p *Partition) Unassign() {
	p.assignedTo = ""
}

// AssignedTo returns the consumer ID this partition is assigned to.
func (p *Partition) AssignedTo() string {
	return p.assignedTo
}

// IsAssigned returns true if this partition is assigned to a consumer.
func (p *Partition) IsAssigned() bool {
	return p.assignedTo != ""
}
