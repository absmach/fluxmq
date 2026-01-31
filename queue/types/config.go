// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package types

import (
	"errors"
	"time"
)

// ErrInvalidConfig indicates an invalid queue configuration.
var ErrInvalidConfig = errors.New("invalid queue configuration")

// Queue constants
const (
	MQTTQueueName  = "mqtt"
	MQTTQueueTopic = "$queue/#"
)

// ReplicationMode defines the replication behavior for queue messages.
type ReplicationMode string

const (
	ReplicationSync  ReplicationMode = "sync"  // Wait for quorum ACK before returning
	ReplicationAsync ReplicationMode = "async" // Return immediately after leader accepts
)

// QueueConfig defines configuration for a queue.
type QueueConfig struct {
	Name     string
	Topics   []string // Topic patterns that route to this queue (e.g., "sensors/#", "orders/+/created")
	Reserved bool     // True for system queues like "mqtt" that cannot be deleted

	RetryPolicy RetryPolicy
	DLQConfig   DLQConfig
	Replication ReplicationConfig
	Retention   RetentionPolicy

	// Limits
	MaxMessageSize int64
	MaxDepth       int64
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

// ReplicationConfig defines Raft-based replication for queues.
type ReplicationConfig struct {
	Enabled           bool
	ReplicationFactor int             // Number of replicas (default: 3)
	Mode              ReplicationMode // sync or async
	MinInSyncReplicas int             // Min replicas that must ACK (default: 2)
	AckTimeout        time.Duration   // Timeout for sync mode operations (default: 5s)

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
func DefaultQueueConfig(name string, topics ...string) QueueConfig {
	if len(topics) == 0 {
		topics = []string{"#"} // Match all topics by default
	}
	return QueueConfig{
		Name:             name,
		Topics:           topics,
		Reserved:         false,
		MaxMessageSize:   10 * 1024 * 1024, // 10MB
		MaxDepth:         100000,
		MessageTTL:       7 * 24 * time.Hour,
		DeliveryTimeout:  30 * time.Second,
		BatchSize:        100,
		HeartbeatTimeout: 2 * time.Minute,

		RetryPolicy: RetryPolicy{
			MaxRetries:        10,
			InitialBackoff:    5 * time.Second,
			MaxBackoff:        5 * time.Minute,
			BackoffMultiplier: 2.0,
			TotalTimeout:      24 * time.Hour,
		},

		DLQConfig: DLQConfig{
			Enabled: true,
			Topic:   "", // Auto-generated
		},

		Replication: ReplicationConfig{
			Enabled:           false, // Disabled by default for backward compatibility
			ReplicationFactor: 3,
			Mode:              ReplicationSync,
			MinInSyncReplicas: 2,
			AckTimeout:        5 * time.Second,
		},
	}
}

// MQTTQueueConfig returns the reserved mqtt queue configuration.
func MQTTQueueConfig() QueueConfig {
	config := DefaultQueueConfig(MQTTQueueName, MQTTQueueTopic)
	config.Reserved = true
	return config
}

// QueueConfigInput is a simplified queue configuration from the main config file.
type QueueConfigInput struct {
	Name           string
	Topics         []string
	Reserved       bool
	MaxMessageSize int64
	MaxDepth       int64
	MessageTTL     time.Duration
	MaxRetries     int
	InitialBackoff time.Duration
	MaxBackoff     time.Duration
	Multiplier     float64
	DLQEnabled     bool
	DLQTopic       string
}

// FromInput creates a QueueConfig from a simplified input config.
func FromInput(input QueueConfigInput) QueueConfig {
	cfg := DefaultQueueConfig(input.Name, input.Topics...)
	cfg.Reserved = input.Reserved

	if input.MaxMessageSize > 0 {
		cfg.MaxMessageSize = input.MaxMessageSize
	}
	if input.MaxDepth > 0 {
		cfg.MaxDepth = input.MaxDepth
	}
	if input.MessageTTL > 0 {
		cfg.MessageTTL = input.MessageTTL
	}
	if input.MaxRetries > 0 {
		cfg.RetryPolicy.MaxRetries = input.MaxRetries
	}
	if input.InitialBackoff > 0 {
		cfg.RetryPolicy.InitialBackoff = input.InitialBackoff
	}
	if input.MaxBackoff > 0 {
		cfg.RetryPolicy.MaxBackoff = input.MaxBackoff
	}
	if input.Multiplier > 0 {
		cfg.RetryPolicy.BackoffMultiplier = input.Multiplier
	}

	cfg.DLQConfig.Enabled = input.DLQEnabled
	if input.DLQTopic != "" {
		cfg.DLQConfig.Topic = input.DLQTopic
	}

	return cfg
}

// Validate validates queue configuration.
func (c *QueueConfig) Validate() error {
	switch {
	case c.Name == "":
		return ErrInvalidConfig
	case len(c.Topics) == 0:
		return ErrInvalidConfig
	case c.MaxMessageSize <= 0:
		return ErrInvalidConfig
	case c.MaxDepth <= 0:
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
		case c.Replication.AckTimeout <= 0:
			return ErrInvalidConfig
		}
	}

	return nil
}
