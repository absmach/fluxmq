// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package storage

import (
	"context"
	"errors"
	"strings"
	"time"
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

// QueueConfig defines configuration for a queue.
type QueueConfig struct {
	Name       string
	Partitions int
	Ordering   OrderingMode

	RetryPolicy RetryPolicy
	DLQConfig   DLQConfig

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

// Message represents a message in the queue system.
type Message struct {
	ID           string
	Payload      []byte
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
	return nil
}
