// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package storage

import (
	"context"
	"errors"

	"github.com/absmach/fluxmq/queue/types"
)

// Log storage errors.
var (
	ErrOffsetOutOfRange   = errors.New("offset out of range")
	ErrPartitionNotFound  = errors.New("partition not found")
	ErrLogFull            = errors.New("log is full")
	ErrInvalidOffset      = errors.New("invalid offset")
	ErrConsumerGroupExists = errors.New("consumer group already exists")
	ErrPendingEntryNotFound = errors.New("pending entry not found")
)

// LogStore provides append-only log storage with offset-based access.
// This is the foundation for the Kafka/Redis Streams-style queue model.
type LogStore interface {
	// Queue lifecycle
	CreateQueue(ctx context.Context, config types.QueueConfig) error
	GetQueue(ctx context.Context, queueName string) (*types.QueueConfig, error)
	DeleteQueue(ctx context.Context, queueName string) error
	ListQueues(ctx context.Context) ([]types.QueueConfig, error)

	// Append adds a message to the end of a partition's log.
	// Returns the assigned offset.
	Append(ctx context.Context, queueName string, partitionID int, msg *types.Message) (uint64, error)

	// AppendBatch adds multiple messages to a partition's log.
	// Returns the first assigned offset.
	AppendBatch(ctx context.Context, queueName string, partitionID int, msgs []*types.Message) (uint64, error)

	// Read retrieves a message at a specific offset.
	Read(ctx context.Context, queueName string, partitionID int, offset uint64) (*types.Message, error)

	// ReadBatch reads messages starting from offset up to limit.
	// Returns messages in offset order.
	ReadBatch(ctx context.Context, queueName string, partitionID int, startOffset uint64, limit int) ([]*types.Message, error)

	// Head returns the first valid offset in the partition (after truncation).
	Head(ctx context.Context, queueName string, partitionID int) (uint64, error)

	// Tail returns the next offset that will be assigned (one past the last message).
	Tail(ctx context.Context, queueName string, partitionID int) (uint64, error)

	// Truncate removes all messages with offset < minOffset.
	// Used for retention policy enforcement.
	Truncate(ctx context.Context, queueName string, partitionID int, minOffset uint64) error

	// Count returns the number of messages in a partition (tail - head).
	Count(ctx context.Context, queueName string, partitionID int) (uint64, error)

	// TotalCount returns total messages across all partitions.
	TotalCount(ctx context.Context, queueName string) (uint64, error)
}

// ConsumerGroupStore manages cursor-based consumer groups with PEL tracking.
type ConsumerGroupStore interface {
	// CreateConsumerGroup creates a new consumer group for a queue.
	CreateConsumerGroup(ctx context.Context, group *types.ConsumerGroupState) error

	// GetConsumerGroup retrieves a consumer group's state.
	GetConsumerGroup(ctx context.Context, queueName, groupID string) (*types.ConsumerGroupState, error)

	// UpdateConsumerGroup updates a consumer group's state (cursors, PEL).
	UpdateConsumerGroup(ctx context.Context, group *types.ConsumerGroupState) error

	// DeleteConsumerGroup removes a consumer group.
	DeleteConsumerGroup(ctx context.Context, queueName, groupID string) error

	// ListConsumerGroups lists all consumer groups for a queue.
	ListConsumerGroups(ctx context.Context, queueName string) ([]*types.ConsumerGroupState, error)

	// AddPendingEntry adds an entry to a consumer's PEL.
	AddPendingEntry(ctx context.Context, queueName, groupID string, entry *types.PendingEntry) error

	// RemovePendingEntry removes an entry from a consumer's PEL.
	RemovePendingEntry(ctx context.Context, queueName, groupID, consumerID string, partitionID int, offset uint64) error

	// GetPendingEntries retrieves all pending entries for a consumer.
	GetPendingEntries(ctx context.Context, queueName, groupID, consumerID string) ([]*types.PendingEntry, error)

	// GetAllPendingEntries retrieves all pending entries for a group (across all consumers).
	GetAllPendingEntries(ctx context.Context, queueName, groupID string) ([]*types.PendingEntry, error)

	// TransferPendingEntry moves a pending entry from one consumer to another (work stealing).
	TransferPendingEntry(ctx context.Context, queueName, groupID string, partitionID int, offset uint64, fromConsumer, toConsumer string) error

	// UpdateCursor updates the cursor position for a partition.
	UpdateCursor(ctx context.Context, queueName, groupID string, partitionID int, cursor uint64) error

	// UpdateCommitted updates the committed offset for a partition.
	UpdateCommitted(ctx context.Context, queueName, groupID string, partitionID int, committed uint64) error

	// RegisterConsumer adds a consumer to a group.
	RegisterConsumer(ctx context.Context, queueName, groupID string, consumer *types.ConsumerInfo) error

	// UnregisterConsumer removes a consumer from a group.
	UnregisterConsumer(ctx context.Context, queueName, groupID, consumerID string) error

	// ListConsumers lists all consumers in a group.
	ListConsumers(ctx context.Context, queueName, groupID string) ([]*types.ConsumerInfo, error)
}
