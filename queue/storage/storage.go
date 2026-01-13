// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package storage

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/absmach/mqtt/core"
)

var (
	ErrQueueNotFound      = errors.New("queue not found")
	ErrMessageNotFound    = errors.New("message not found")
	ErrConsumerNotFound   = errors.New("consumer not found")
	ErrQueueAlreadyExists = errors.New("queue already exists")
	ErrInvalidConfig      = errors.New("invalid queue configuration")
)

// OrderingMode defines message ordering guarantees.
type OrderingMode string

const (
	OrderingNone      OrderingMode = "none"      // No ordering guarantees
	OrderingPartition OrderingMode = "partition" // FIFO per partition key
	OrderingStrict    OrderingMode = "strict"    // Global FIFO (single partition)
)

// MessageState represents the lifecycle state of a queue message.
type MessageState string

const (
	StateQueued    MessageState = "queued"
	StateDelivered MessageState = "delivered"
	StateAcked     MessageState = "acked"
	StateRetry     MessageState = "retry"
	StateDLQ       MessageState = "dlq"
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

// Message represents a message in the queue system.
type Message struct {
	ID           string
	Payload      []byte                 // Deprecated: Use PayloadBuf for zero-copy
	PayloadBuf   *core.RefCountedBuffer // Zero-copy payload buffer (preferred)
	Topic        string
	PartitionKey string
	PartitionID  int
	Sequence     uint64
	Properties   map[string]string

	// Lifecycle tracking
	State       MessageState
	CreatedAt   time.Time
	DeliveredAt time.Time
	NextRetryAt time.Time
	RetryCount  int

	// DLQ metadata
	FailureReason string
	FirstAttempt  time.Time
	LastAttempt   time.Time
	MovedToDLQAt  time.Time
	ExpiresAt     time.Time
}

// GetPayload returns the message payload, preferring PayloadBuf if available.
// This provides backward compatibility during migration to zero-copy.
func (m *Message) GetPayload() []byte {
	if m.PayloadBuf != nil {
		return m.PayloadBuf.Bytes()
	}
	return m.Payload
}

// SetPayloadFromBuffer sets the payload from a RefCountedBuffer.
// The message takes ownership of one reference.
func (m *Message) SetPayloadFromBuffer(buf *core.RefCountedBuffer) {
	if m.PayloadBuf != nil {
		m.PayloadBuf.Release() // Release previous buffer
	}
	m.PayloadBuf = buf
	m.Payload = nil // Clear legacy field
}

// SetPayloadFromBytes creates a new buffer from bytes (for backward compatibility).
// This will eventually be phased out in favor of direct buffer creation.
func (m *Message) SetPayloadFromBytes(data []byte) {
	if m.PayloadBuf != nil {
		m.PayloadBuf.Release()
	}
	if len(data) > 0 {
		m.PayloadBuf = core.GetBufferWithData(data)
	} else {
		m.PayloadBuf = nil
	}
	m.Payload = nil
}

// ReleasePayload releases the buffer reference if PayloadBuf is set.
// This should be called when the message is no longer needed.
func (m *Message) ReleasePayload() {
	if m.PayloadBuf != nil {
		m.PayloadBuf.Release()
		m.PayloadBuf = nil
	}
	m.Payload = nil
}

// DeliveryState tracks inflight message delivery.
type DeliveryState struct {
	MessageID   string
	QueueName   string
	PartitionID int
	ConsumerID  string
	DeliveredAt time.Time
	Timeout     time.Time
	RetryCount  int
}

// Consumer represents a queue consumer.
type Consumer struct {
	ID            string
	ClientID      string
	GroupID       string
	QueueName     string
	AssignedParts []int
	RegisteredAt  time.Time
	LastHeartbeat time.Time
	ProxyNodeID   string // For cluster routing
}

// ConsumerGroup represents a group of consumers.
type ConsumerGroup struct {
	ID        string
	QueueName string
	Consumers map[string]*Consumer
}

// QueueStore manages queue metadata and configuration.
type QueueStore interface {
	CreateQueue(ctx context.Context, config QueueConfig) error
	GetQueue(ctx context.Context, queueName string) (*QueueConfig, error)
	UpdateQueue(ctx context.Context, config QueueConfig) error
	DeleteQueue(ctx context.Context, queueName string) error
	ListQueues(ctx context.Context) ([]QueueConfig, error)
}

// MessageStore manages queue messages and delivery state.
type MessageStore interface {
	// Message operations
	Enqueue(ctx context.Context, queueName string, msg *Message) error
	// Count returns the number of messages in the queue (across all partitions).
	Count(ctx context.Context, queueName string) (int64, error)
	Dequeue(ctx context.Context, queueName string, partitionID int) (*Message, error)
	DequeueBatch(ctx context.Context, queueName string, partitionID int, limit int) ([]*Message, error)
	UpdateMessage(ctx context.Context, queueName string, msg *Message) error
	DeleteMessage(ctx context.Context, queueName string, messageID string) error
	GetMessage(ctx context.Context, queueName string, messageID string) (*Message, error)

	// Inflight tracking
	MarkInflight(ctx context.Context, state *DeliveryState) error
	GetInflight(ctx context.Context, queueName string) ([]*DeliveryState, error)
	GetInflightMessage(ctx context.Context, queueName, messageID string) (*DeliveryState, error)
	RemoveInflight(ctx context.Context, queueName, messageID string) error

	// DLQ operations
	EnqueueDLQ(ctx context.Context, dlqTopic string, msg *Message) error
	ListDLQ(ctx context.Context, dlqTopic string, limit int) ([]*Message, error)
	DeleteDLQMessage(ctx context.Context, dlqTopic, messageID string) error

	// Retry operations
	ListRetry(ctx context.Context, queueName string, partitionID int) ([]*Message, error)

	// Partition operations
	GetNextSequence(ctx context.Context, queueName string, partitionID int) (uint64, error)
	UpdateOffset(ctx context.Context, queueName string, partitionID int, offset uint64) error
	GetOffset(ctx context.Context, queueName string, partitionID int) (uint64, error)

	// Batch operations
	ListQueued(ctx context.Context, queueName string, partitionID int, limit int) ([]*Message, error)

	// Retention operations
	ListOldestMessages(ctx context.Context, queueName string, partitionID int, limit int) ([]*Message, error)
	ListMessagesBefore(ctx context.Context, queueName string, partitionID int, cutoffTime time.Time, limit int) ([]*Message, error)
	DeleteMessageBatch(ctx context.Context, queueName string, messageIDs []string) (int64, error)
	GetQueueSize(ctx context.Context, queueName string) (int64, error) // Total size in bytes

	// Compaction operations
	ListAllMessages(ctx context.Context, queueName string, partitionID int) ([]*Message, error)
}

// ConsumerStore manages consumer group state.
type ConsumerStore interface {
	RegisterConsumer(ctx context.Context, consumer *Consumer) error
	UnregisterConsumer(ctx context.Context, queueName, groupID, consumerID string) error
	GetConsumer(ctx context.Context, queueName, groupID, consumerID string) (*Consumer, error)
	ListConsumers(ctx context.Context, queueName, groupID string) ([]*Consumer, error)
	ListGroups(ctx context.Context, queueName string) ([]string, error)
	UpdateHeartbeat(ctx context.Context, queueName, groupID, consumerID string, timestamp time.Time) error
}

// DefaultQueueConfig returns default queue configuration.
func DefaultQueueConfig(name string) QueueConfig {
	return QueueConfig{
		Name:             name,
		Partitions:       10,
		Ordering:         OrderingPartition,
		MaxMessageSize:   1024 * 1024, // 1MB
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
