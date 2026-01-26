// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package types

import (
	"errors"
	"strings"
	"time"
)

// ErrInvalidConfig indicates an invalid queue configuration.
var ErrInvalidConfig = errors.New("invalid queue configuration")

// OrderingMode defines message ordering guarantees.
type OrderingMode string

const (
	OrderingNone      OrderingMode = "none"      // No ordering guarantees
	OrderingPartition OrderingMode = "partition" // FIFO per partition key
	OrderingStrict    OrderingMode = "strict"    // Global FIFO (single partition)
)

// ReplicationMode defines the replication behavior for queue messages.
type ReplicationMode string

const (
	ReplicationSync  ReplicationMode = "sync"  // Wait for quorum ACK before returning
	ReplicationAsync ReplicationMode = "async" // Return immediately after leader accepts
)

// PlacementStrategy defines how replicas are assigned to nodes.
type PlacementStrategy string

const (
	PlacementRoundRobin PlacementStrategy = "round-robin" // Distribute replicas evenly
	PlacementManual     PlacementStrategy = "manual"      // Operator-specified placement
)

// QueueConfig defines configuration for a queue.
type QueueConfig struct {
	Name       string
	Partitions int
	Ordering   OrderingMode

	RetryPolicy RetryPolicy
	DLQConfig   DLQConfig
	Replication ReplicationConfig
	Retention   RetentionPolicy

	// Limits
	MaxMessageSize int64
	MaxQueueDepth  int64
	MessageTTL     time.Duration

	// Performance
	DeliveryTimeout  time.Duration
	BatchSize        int
	HeartbeatTimeout time.Duration
}

// RetryPolicy defines retry behavior for failed messages.
type RetryPolicy struct {
	MaxRetries        int
	InitialBackoff    time.Duration
	MaxBackoff        time.Duration
	BackoffMultiplier float64
	TotalTimeout      time.Duration
}

// DLQConfig defines dead-letter queue configuration.
type DLQConfig struct {
	Enabled      bool
	Topic        string
	AlertWebhook string
}

// ReplicationConfig defines Raft-based replication for queue partitions.
type ReplicationConfig struct {
	Enabled           bool
	ReplicationFactor int               // Number of replicas per partition (default: 3)
	Mode              ReplicationMode   // sync or async
	Placement         PlacementStrategy // How to assign replicas to nodes
	ManualReplicas    map[int][]string  // For manual placement: partitionID -> []nodeID
	MinInSyncReplicas int               // Min replicas that must ACK (default: 2)
	AckTimeout        time.Duration     // Timeout for sync mode operations (default: 5s)

	// Raft tuning (optional, uses defaults if zero)
	HeartbeatTimeout  time.Duration // Raft heartbeat interval (default: 1s)
	ElectionTimeout   time.Duration // Raft election timeout (default: 3s)
	SnapshotInterval  time.Duration // Snapshot frequency (default: 5m)
	SnapshotThreshold uint64        // Snapshot after N log entries (default: 8192)
}

// RetentionPolicy defines Kafka-style retention policies for automatic message cleanup.
type RetentionPolicy struct {
	// Time-based retention (background cleanup)
	RetentionTime     time.Duration // Delete messages older than this (0 = disabled)
	TimeCheckInterval time.Duration // How often to run cleanup (default: 5m)

	// Size-based retention (active check on enqueue)
	RetentionBytes    int64 // Max total queue size in bytes (0 = unlimited)
	RetentionMessages int64 // Max message count (0 = unlimited)
	SizeCheckEvery    int   // Check size every N enqueues (default: 100, optimization)

	// Log compaction (Kafka-style)
	CompactionEnabled  bool          // Enable log compaction
	CompactionKey      string        // Message property to use as compaction key
	CompactionLag      time.Duration // Wait before compacting new messages (default: 5m)
	CompactionInterval time.Duration // How often to run compaction (default: 10m)
}

// DefaultQueueConfig returns default queue configuration.
func DefaultQueueConfig(name string) QueueConfig {
	return QueueConfig{
		Name:             name,
		Partitions:       10,
		Ordering:         OrderingPartition,
		MaxMessageSize:   10 * 1024 * 1024, // 1MB
		MaxQueueDepth:    100000,
		MessageTTL:       7 * 24 * time.Hour,
		DeliveryTimeout:  30 * time.Second,
		BatchSize:        100,
		HeartbeatTimeout: 30 * time.Second,

		RetryPolicy: RetryPolicy{
			MaxRetries:        10,
			InitialBackoff:    5 * time.Second,
			MaxBackoff:        5 * time.Minute,
			BackoffMultiplier: 2.0,
			TotalTimeout:      3 * time.Hour,
		},

		DLQConfig: DLQConfig{
			Enabled: true,
			Topic:   "", // Auto-generated
		},

		Replication: ReplicationConfig{
			Enabled:           false, // Disabled by default for backward compatibility
			ReplicationFactor: 3,
			Mode:              ReplicationSync,
			Placement:         PlacementRoundRobin,
			MinInSyncReplicas: 2,
			AckTimeout:        5 * time.Second,
		},
	}
}

// Validate validates queue configuration.
func (c *QueueConfig) Validate() error {
	switch {
	case c.Name == "":
		return ErrInvalidConfig
	case !strings.HasPrefix(c.Name, "$queue/"):
		// Queue name must start with $queue/
		return ErrInvalidConfig
	case c.Partitions < 1:
		return ErrInvalidConfig
	case c.Partitions > 1000:
		// Reasonable upper limit
		return ErrInvalidConfig
	case c.Ordering != OrderingNone && c.Ordering != OrderingPartition && c.Ordering != OrderingStrict:
		return ErrInvalidConfig
	case c.Ordering == OrderingStrict && c.Partitions != 1:
		return ErrInvalidConfig
	case c.MaxMessageSize <= 0:
		return ErrInvalidConfig
	case c.MaxQueueDepth <= 0:
		return ErrInvalidConfig
	case c.DeliveryTimeout <= 0:
		return ErrInvalidConfig
	case c.BatchSize <= 0:
		return ErrInvalidConfig
	case c.HeartbeatTimeout <= 0:
		return ErrInvalidConfig
	case c.RetryPolicy.MaxRetries < 0:
		return ErrInvalidConfig
	case c.RetryPolicy.InitialBackoff < 0 || c.RetryPolicy.MaxBackoff < c.RetryPolicy.InitialBackoff:
		return ErrInvalidConfig
	case c.RetryPolicy.BackoffMultiplier < 1.0:
		return ErrInvalidConfig
	case c.RetryPolicy.TotalTimeout < 0:
		return ErrInvalidConfig
	}

	// Validate replication config if enabled
	if c.Replication.Enabled {
		switch {
		case c.Replication.ReplicationFactor < 1 || c.Replication.ReplicationFactor > 10:
			return ErrInvalidConfig
		case c.Replication.MinInSyncReplicas < 1 || c.Replication.MinInSyncReplicas > c.Replication.ReplicationFactor:
			return ErrInvalidConfig
		case c.Replication.Mode != ReplicationSync && c.Replication.Mode != ReplicationAsync:
			return ErrInvalidConfig
		case c.Replication.Placement != PlacementRoundRobin && c.Replication.Placement != PlacementManual:
			return ErrInvalidConfig
		case c.Replication.Placement == PlacementManual && len(c.Replication.ManualReplicas) != c.Partitions:
			return ErrInvalidConfig
		case c.Replication.AckTimeout <= 0:
			return ErrInvalidConfig
		}

		// Validate manual replica assignments
		if c.Replication.Placement == PlacementManual {
			for partID, replicas := range c.Replication.ManualReplicas {
				if partID < 0 || partID >= c.Partitions {
					return ErrInvalidConfig
				}
				if len(replicas) != c.Replication.ReplicationFactor {
					return ErrInvalidConfig
				}
			}
		}
	}

	return nil
}
