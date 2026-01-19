// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

// Package log provides a hybrid log store combining in-memory and persistent storage.
// The memory store serves as a hot cache while BadgerDB provides durability.
package log

import (
	"context"
	"sync"
	"time"

	"github.com/absmach/fluxmq/queue/storage"
	badgerlog "github.com/absmach/fluxmq/queue/storage/badger/log"
	memlog "github.com/absmach/fluxmq/queue/storage/memory/log"
	"github.com/absmach/fluxmq/queue/types"
	"github.com/dgraph-io/badger/v4"
)

// Store is a hybrid log store combining memory (hot path) with BadgerDB (durability).
type Store struct {
	memory     *memlog.Store
	persistent *badgerlog.Store
	config     Config
	mu         sync.RWMutex
}

// Config defines hybrid log store configuration.
type Config struct {
	// Memory store capacity per partition
	MemoryCapacity int

	// Enable write-through to persistent storage
	WriteThrough bool

	// Enable read-through from persistent storage on cache miss
	ReadThrough bool
}

// DefaultConfig returns default configuration.
func DefaultConfig() Config {
	return Config{
		MemoryCapacity: 10000,
		WriteThrough:   true,
		ReadThrough:    true,
	}
}

// New creates a new hybrid log store.
func New(db *badger.DB) *Store {
	return NewWithConfig(db, DefaultConfig())
}

// NewWithConfig creates a new hybrid log store with custom configuration.
func NewWithConfig(db *badger.DB, cfg Config) *Store {
	return &Store{
		memory:     memlog.NewWithConfig(memlog.Config{InitialCapacity: cfg.MemoryCapacity}),
		persistent: badgerlog.New(db),
		config:     cfg,
	}
}

// --- LogStore Implementation ---

// CreateQueue creates a new queue in both stores.
func (s *Store) CreateQueue(ctx context.Context, config types.QueueConfig) error {
	// Create in persistent first (source of truth)
	if err := s.persistent.CreateQueue(ctx, config); err != nil {
		return err
	}

	// Create in memory
	if err := s.memory.CreateQueue(ctx, config); err != nil {
		// Rollback persistent
		s.persistent.DeleteQueue(ctx, config.Name)
		return err
	}

	return nil
}

// GetQueue retrieves queue configuration.
func (s *Store) GetQueue(ctx context.Context, queueName string) (*types.QueueConfig, error) {
	// Try memory first
	config, err := s.memory.GetQueue(ctx, queueName)
	if err == nil {
		return config, nil
	}

	// Fall back to persistent
	return s.persistent.GetQueue(ctx, queueName)
}

// DeleteQueue removes a queue from both stores.
func (s *Store) DeleteQueue(ctx context.Context, queueName string) error {
	s.memory.DeleteQueue(ctx, queueName)
	return s.persistent.DeleteQueue(ctx, queueName)
}

// ListQueues returns all queue configurations.
func (s *Store) ListQueues(ctx context.Context) ([]types.QueueConfig, error) {
	// Use persistent as source of truth
	return s.persistent.ListQueues(ctx)
}

// Append adds a message to the log.
func (s *Store) Append(ctx context.Context, queueName string, partitionID int, msg *types.Message) (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Write to persistent first if write-through enabled
	if s.config.WriteThrough {
		offset, err := s.persistent.Append(ctx, queueName, partitionID, msg)
		if err != nil {
			return 0, err
		}

		// Update message with assigned offset
		msg.Sequence = offset

		// Also write to memory cache
		s.memory.Append(ctx, queueName, partitionID, msg)

		return offset, nil
	}

	// Memory-only mode
	return s.memory.Append(ctx, queueName, partitionID, msg)
}

// AppendBatch adds multiple messages to the log.
func (s *Store) AppendBatch(ctx context.Context, queueName string, partitionID int, msgs []*types.Message) (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.config.WriteThrough {
		offset, err := s.persistent.AppendBatch(ctx, queueName, partitionID, msgs)
		if err != nil {
			return 0, err
		}

		// Update messages with assigned offsets
		for i, msg := range msgs {
			msg.Sequence = offset + uint64(i)
		}

		// Also write to memory cache
		s.memory.AppendBatch(ctx, queueName, partitionID, msgs)

		return offset, nil
	}

	return s.memory.AppendBatch(ctx, queueName, partitionID, msgs)
}

// Read retrieves a message at a specific offset.
func (s *Store) Read(ctx context.Context, queueName string, partitionID int, offset uint64) (*types.Message, error) {
	// Try memory first
	msg, err := s.memory.Read(ctx, queueName, partitionID, offset)
	if err == nil {
		return msg, nil
	}

	// Fall back to persistent if read-through enabled
	if s.config.ReadThrough {
		return s.persistent.Read(ctx, queueName, partitionID, offset)
	}

	return nil, err
}

// ReadBatch reads messages starting from offset.
func (s *Store) ReadBatch(ctx context.Context, queueName string, partitionID int, startOffset uint64, limit int) ([]*types.Message, error) {
	// Try memory first
	msgs, err := s.memory.ReadBatch(ctx, queueName, partitionID, startOffset, limit)
	if err == nil && len(msgs) == limit {
		return msgs, nil
	}

	// Fall back to persistent for complete result
	if s.config.ReadThrough {
		return s.persistent.ReadBatch(ctx, queueName, partitionID, startOffset, limit)
	}

	return msgs, err
}

// Head returns the first valid offset.
func (s *Store) Head(ctx context.Context, queueName string, partitionID int) (uint64, error) {
	// Use persistent as source of truth
	return s.persistent.Head(ctx, queueName, partitionID)
}

// Tail returns the next offset to be assigned.
func (s *Store) Tail(ctx context.Context, queueName string, partitionID int) (uint64, error) {
	// Use persistent as source of truth
	return s.persistent.Tail(ctx, queueName, partitionID)
}

// Truncate removes messages before minOffset.
func (s *Store) Truncate(ctx context.Context, queueName string, partitionID int, minOffset uint64) error {
	// Truncate both stores
	s.memory.Truncate(ctx, queueName, partitionID, minOffset)
	return s.persistent.Truncate(ctx, queueName, partitionID, minOffset)
}

// Count returns the number of messages in a partition.
func (s *Store) Count(ctx context.Context, queueName string, partitionID int) (uint64, error) {
	return s.persistent.Count(ctx, queueName, partitionID)
}

// TotalCount returns total messages across all partitions.
func (s *Store) TotalCount(ctx context.Context, queueName string) (uint64, error) {
	return s.persistent.TotalCount(ctx, queueName)
}

// --- ConsumerGroupStore Implementation ---

// CreateConsumerGroup creates a new consumer group.
func (s *Store) CreateConsumerGroup(ctx context.Context, group *types.ConsumerGroupState) error {
	// Write to persistent first
	if err := s.persistent.CreateConsumerGroup(ctx, group); err != nil {
		return err
	}

	// Also cache in memory
	return s.memory.CreateConsumerGroup(ctx, group)
}

// GetConsumerGroup retrieves a consumer group's state.
func (s *Store) GetConsumerGroup(ctx context.Context, queueName, groupID string) (*types.ConsumerGroupState, error) {
	// Try memory first
	group, err := s.memory.GetConsumerGroup(ctx, queueName, groupID)
	if err == nil {
		return group, nil
	}

	// Fall back to persistent
	group, err = s.persistent.GetConsumerGroup(ctx, queueName, groupID)
	if err != nil {
		return nil, err
	}

	// Cache in memory
	s.memory.CreateConsumerGroup(ctx, group)

	return group, nil
}

// UpdateConsumerGroup updates a consumer group's state.
func (s *Store) UpdateConsumerGroup(ctx context.Context, group *types.ConsumerGroupState) error {
	group.UpdatedAt = time.Now()

	// Update memory first (fast path)
	s.memory.UpdateConsumerGroup(ctx, group)

	// Persist
	return s.persistent.UpdateConsumerGroup(ctx, group)
}

// DeleteConsumerGroup removes a consumer group.
func (s *Store) DeleteConsumerGroup(ctx context.Context, queueName, groupID string) error {
	s.memory.DeleteConsumerGroup(ctx, queueName, groupID)
	return s.persistent.DeleteConsumerGroup(ctx, queueName, groupID)
}

// ListConsumerGroups lists all consumer groups for a queue.
func (s *Store) ListConsumerGroups(ctx context.Context, queueName string) ([]*types.ConsumerGroupState, error) {
	return s.persistent.ListConsumerGroups(ctx, queueName)
}

// AddPendingEntry adds an entry to a consumer's PEL.
func (s *Store) AddPendingEntry(ctx context.Context, queueName, groupID string, entry *types.PendingEntry) error {
	s.memory.AddPendingEntry(ctx, queueName, groupID, entry)
	return s.persistent.AddPendingEntry(ctx, queueName, groupID, entry)
}

// RemovePendingEntry removes an entry from a consumer's PEL.
func (s *Store) RemovePendingEntry(ctx context.Context, queueName, groupID, consumerID string, partitionID int, offset uint64) error {
	s.memory.RemovePendingEntry(ctx, queueName, groupID, consumerID, partitionID, offset)
	return s.persistent.RemovePendingEntry(ctx, queueName, groupID, consumerID, partitionID, offset)
}

// GetPendingEntries retrieves all pending entries for a consumer.
func (s *Store) GetPendingEntries(ctx context.Context, queueName, groupID, consumerID string) ([]*types.PendingEntry, error) {
	entries, err := s.memory.GetPendingEntries(ctx, queueName, groupID, consumerID)
	if err == nil && len(entries) > 0 {
		return entries, nil
	}
	return s.persistent.GetPendingEntries(ctx, queueName, groupID, consumerID)
}

// GetAllPendingEntries retrieves all pending entries for a group.
func (s *Store) GetAllPendingEntries(ctx context.Context, queueName, groupID string) ([]*types.PendingEntry, error) {
	entries, err := s.memory.GetAllPendingEntries(ctx, queueName, groupID)
	if err == nil && len(entries) > 0 {
		return entries, nil
	}
	return s.persistent.GetAllPendingEntries(ctx, queueName, groupID)
}

// TransferPendingEntry moves a pending entry between consumers.
func (s *Store) TransferPendingEntry(ctx context.Context, queueName, groupID string, partitionID int, offset uint64, fromConsumer, toConsumer string) error {
	s.memory.TransferPendingEntry(ctx, queueName, groupID, partitionID, offset, fromConsumer, toConsumer)
	return s.persistent.TransferPendingEntry(ctx, queueName, groupID, partitionID, offset, fromConsumer, toConsumer)
}

// UpdateCursor updates the cursor position for a partition.
func (s *Store) UpdateCursor(ctx context.Context, queueName, groupID string, partitionID int, cursor uint64) error {
	s.memory.UpdateCursor(ctx, queueName, groupID, partitionID, cursor)
	return s.persistent.UpdateCursor(ctx, queueName, groupID, partitionID, cursor)
}

// UpdateCommitted updates the committed offset for a partition.
func (s *Store) UpdateCommitted(ctx context.Context, queueName, groupID string, partitionID int, committed uint64) error {
	s.memory.UpdateCommitted(ctx, queueName, groupID, partitionID, committed)
	return s.persistent.UpdateCommitted(ctx, queueName, groupID, partitionID, committed)
}

// RegisterConsumer adds a consumer to a group.
func (s *Store) RegisterConsumer(ctx context.Context, queueName, groupID string, consumer *types.ConsumerInfo) error {
	s.memory.RegisterConsumer(ctx, queueName, groupID, consumer)
	return s.persistent.RegisterConsumer(ctx, queueName, groupID, consumer)
}

// UnregisterConsumer removes a consumer from a group.
func (s *Store) UnregisterConsumer(ctx context.Context, queueName, groupID, consumerID string) error {
	s.memory.UnregisterConsumer(ctx, queueName, groupID, consumerID)
	return s.persistent.UnregisterConsumer(ctx, queueName, groupID, consumerID)
}

// ListConsumers lists all consumers in a group.
func (s *Store) ListConsumers(ctx context.Context, queueName, groupID string) ([]*types.ConsumerInfo, error) {
	consumers, err := s.memory.ListConsumers(ctx, queueName, groupID)
	if err == nil && len(consumers) > 0 {
		return consumers, nil
	}
	return s.persistent.ListConsumers(ctx, queueName, groupID)
}

// Warmup loads recent data from persistent storage into memory.
func (s *Store) Warmup(ctx context.Context, queueName string, messagesPerPartition int) error {
	config, err := s.persistent.GetQueue(ctx, queueName)
	if err != nil {
		return err
	}

	// Load recent messages for each partition
	for i := 0; i < config.Partitions; i++ {
		tail, err := s.persistent.Tail(ctx, queueName, i)
		if err != nil {
			continue
		}

		startOffset := uint64(0)
		if tail > uint64(messagesPerPartition) {
			startOffset = tail - uint64(messagesPerPartition)
		}

		msgs, err := s.persistent.ReadBatch(ctx, queueName, i, startOffset, messagesPerPartition)
		if err != nil {
			continue
		}

		for _, msg := range msgs {
			s.memory.Append(ctx, queueName, i, msg)
		}
	}

	// Load consumer groups
	groups, err := s.persistent.ListConsumerGroups(ctx, queueName)
	if err == nil {
		for _, group := range groups {
			s.memory.CreateConsumerGroup(ctx, group)
		}
	}

	return nil
}

// Compile-time interface assertions
var (
	_ storage.LogStore           = (*Store)(nil)
	_ storage.ConsumerGroupStore = (*Store)(nil)
)
