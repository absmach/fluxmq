// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package consumer

import (
	"context"
	"fmt"

	"github.com/absmach/fluxmq/cluster"
)

// PartitionAssigner determines which node owns a partition.
// Supports multiple strategies for different use cases.
type PartitionAssigner interface {
	// GetOwner returns the node ID that should own the partition.
	GetOwner(ctx context.Context, queueName string, partitionID int, nodes []cluster.NodeInfo) (string, error)
}

// HashPartitionAssigner assigns partitions using static hash-based distribution.
// Uses simple modulo: partitionID % len(nodes) determines the owner node.
// Zero coordination overhead, deterministic routing.
type HashPartitionAssigner struct{}

// NewHashPartitionAssigner creates a new hash-based partition assigner.
func NewHashPartitionAssigner() *HashPartitionAssigner {
	return &HashPartitionAssigner{}
}

// GetOwner returns the node ID using hash-based assignment.
func (h *HashPartitionAssigner) GetOwner(ctx context.Context, queueName string, partitionID int, nodes []cluster.NodeInfo) (string, error) {
	if len(nodes) == 0 {
		return "", fmt.Errorf("no nodes available")
	}

	// Simple modulo distribution
	nodeIndex := partitionID % len(nodes)
	return nodes[nodeIndex].ID, nil
}

// DynamicPartitionAssigner assigns partitions using etcd-based dynamic assignment.
// Queries etcd for current partition owner, claims unclaimed partitions.
// Supports rebalancing and flexible reassignment.
type DynamicPartitionAssigner struct {
	cluster   cluster.Cluster
	localNode string
}

// NewDynamicPartitionAssigner creates a new etcd-based partition assigner.
func NewDynamicPartitionAssigner(c cluster.Cluster, localNodeID string) *DynamicPartitionAssigner {
	return &DynamicPartitionAssigner{
		cluster:   c,
		localNode: localNodeID,
	}
}

// GetOwner returns the node ID using dynamic etcd-based assignment.
func (d *DynamicPartitionAssigner) GetOwner(ctx context.Context, queueName string, partitionID int, nodes []cluster.NodeInfo) (string, error) {
	if len(nodes) == 0 {
		return "", fmt.Errorf("no nodes available")
	}

	if d.cluster == nil {
		// Fallback to local node if no cluster
		return d.localNode, nil
	}

	// Query etcd for current owner
	owner, exists, err := d.cluster.GetPartitionOwner(ctx, queueName, partitionID)
	if err != nil {
		return "", fmt.Errorf("failed to get partition owner from etcd: %w", err)
	}

	if exists {
		// Verify the owner is still in the cluster
		for _, node := range nodes {
			if node.ID == owner && node.Healthy {
				return owner, nil
			}
		}
		// Owner left the cluster, fall through to claim it
	}

	// Partition is unclaimed or owner is gone, claim it for this node
	if err := d.cluster.AcquirePartition(ctx, queueName, partitionID, d.localNode); err != nil {
		return "", fmt.Errorf("failed to acquire partition: %w", err)
	}

	return d.localNode, nil
}
