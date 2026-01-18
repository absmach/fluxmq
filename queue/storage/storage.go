// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package storage

import (
	"context"
	"errors"
	"time"

	"github.com/absmach/fluxmq/queue/types"
)

var (
	ErrQueueNotFound      = errors.New("queue not found")
	ErrMessageNotFound    = errors.New("message not found")
	ErrConsumerNotFound   = errors.New("consumer not found")
	ErrQueueAlreadyExists = errors.New("queue already exists")
)

// QueueStore manages queue metadata and configuration.
type QueueStore interface {
	CreateQueue(ctx context.Context, config types.QueueConfig) error
	GetQueue(ctx context.Context, queueName string) (*types.QueueConfig, error)
	UpdateQueue(ctx context.Context, config types.QueueConfig) error
	DeleteQueue(ctx context.Context, queueName string) error
	ListQueues(ctx context.Context) ([]types.QueueConfig, error)
}

// MessageStore manages queue messages and delivery state.
type MessageStore interface {
	// Message operations
	Enqueue(ctx context.Context, queueName string, msg *types.Message) error
	// Count returns the number of messages in the queue (across all partitions).
	Count(ctx context.Context, queueName string) (int64, error)
	Dequeue(ctx context.Context, queueName string, partitionID int) (*types.Message, error)
	DequeueBatch(ctx context.Context, queueName string, partitionID int, limit int) ([]*types.Message, error)
	UpdateMessage(ctx context.Context, queueName string, msg *types.Message) error
	DeleteMessage(ctx context.Context, queueName string, messageID string) error
	GetMessage(ctx context.Context, queueName string, messageID string) (*types.Message, error)

	// Inflight tracking (group-aware for fan-out support)
	MarkInflight(ctx context.Context, state *types.DeliveryState) error
	GetInflight(ctx context.Context, queueName string) ([]*types.DeliveryState, error)
	GetInflightMessage(ctx context.Context, queueName, messageID, groupID string) (*types.DeliveryState, error)
	GetInflightForMessage(ctx context.Context, queueName, messageID string) ([]*types.DeliveryState, error)
	RemoveInflight(ctx context.Context, queueName, messageID, groupID string) error

	// DLQ operations
	EnqueueDLQ(ctx context.Context, dlqTopic string, msg *types.Message) error
	ListDLQ(ctx context.Context, dlqTopic string, limit int) ([]*types.Message, error)
	DeleteDLQMessage(ctx context.Context, dlqTopic, messageID string) error

	// Retry operations
	ListRetry(ctx context.Context, queueName string, partitionID int) ([]*types.Message, error)

	// Partition operations
	GetNextSequence(ctx context.Context, queueName string, partitionID int) (uint64, error)
	UpdateOffset(ctx context.Context, queueName string, partitionID int, offset uint64) error
	GetOffset(ctx context.Context, queueName string, partitionID int) (uint64, error)

	// Batch operations
	ListQueued(ctx context.Context, queueName string, partitionID int, limit int) ([]*types.Message, error)

	// Retention operations
	ListOldestMessages(ctx context.Context, queueName string, partitionID int, limit int) ([]*types.Message, error)
	ListMessagesBefore(ctx context.Context, queueName string, partitionID int, cutoffTime time.Time, limit int) ([]*types.Message, error)
	DeleteMessageBatch(ctx context.Context, queueName string, messageIDs []string) (int64, error)
	GetQueueSize(ctx context.Context, queueName string) (int64, error) // Total size in bytes

	// Compaction operations
	ListAllMessages(ctx context.Context, queueName string, partitionID int) ([]*types.Message, error)
}

// ConsumerStore manages consumer group state.
type ConsumerStore interface {
	RegisterConsumer(ctx context.Context, consumer *types.Consumer) error
	UnregisterConsumer(ctx context.Context, queueName, groupID, consumerID string) error
	GetConsumer(ctx context.Context, queueName, groupID, consumerID string) (*types.Consumer, error)
	ListConsumers(ctx context.Context, queueName, groupID string) ([]*types.Consumer, error)
	ListGroups(ctx context.Context, queueName string) ([]string, error)
	UpdateHeartbeat(ctx context.Context, queueName, groupID, consumerID string, timestamp time.Time) error
}
