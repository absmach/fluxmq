// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

// Package log provides an in-memory implementation of the log-based queue storage.
// This implementation uses append-only logs with offset-based access, inspired by
// Kafka and Redis Streams.
package log

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/absmach/fluxmq/queue/storage"
	"github.com/absmach/fluxmq/queue/types"
)

// Store implements LogStore and ConsumerGroupStore using in-memory data structures.
type Store struct {
	queues sync.Map // map[string]*queueLog
	groups sync.Map // map[string]map[string]*types.ConsumerGroupState (queueName -> groupID -> state)
	config Config
}

// queueLog holds the log data for a single queue.
type queueLog struct {
	config     types.QueueConfig
	partitions []*partitionLog
	mu         sync.RWMutex
}

// partitionLog is an append-only log for a single partition.
type partitionLog struct {
	messages []*types.Message // Append-only message log
	head     uint64           // First valid offset (after truncation)
	tail     uint64           // Next offset to assign (atomic)
	mu       sync.RWMutex
}

// Config defines configuration for the memory log store.
type Config struct {
	InitialCapacity int // Initial slice capacity per partition
}

// DefaultConfig returns default configuration.
func DefaultConfig() Config {
	return Config{
		InitialCapacity: 10000,
	}
}

// New creates a new memory log store with default configuration.
func New() *Store {
	return NewWithConfig(DefaultConfig())
}

// NewWithConfig creates a new memory log store with custom configuration.
func NewWithConfig(cfg Config) *Store {
	return &Store{
		config: cfg,
	}
}

// CreateQueue creates a new queue with the specified configuration.
func (s *Store) CreateQueue(ctx context.Context, config types.QueueConfig) error {
	if err := config.Validate(); err != nil {
		return err
	}

	if _, exists := s.queues.Load(config.Name); exists {
		return storage.ErrQueueAlreadyExists
	}

	partitions := make([]*partitionLog, config.Partitions)
	for i := range partitions {
		partitions[i] = &partitionLog{
			messages: make([]*types.Message, 0, s.config.InitialCapacity),
			head:     0,
			tail:     0,
		}
	}

	ql := &queueLog{
		config:     config,
		partitions: partitions,
	}

	s.queues.Store(config.Name, ql)

	// Initialize empty groups map for this queue
	s.groups.Store(config.Name, &sync.Map{})

	return nil
}

// GetQueue retrieves queue configuration.
func (s *Store) GetQueue(ctx context.Context, queueName string) (*types.QueueConfig, error) {
	val, exists := s.queues.Load(queueName)
	if !exists {
		return nil, storage.ErrQueueNotFound
	}

	ql := val.(*queueLog)
	configCopy := ql.config
	return &configCopy, nil
}

// DeleteQueue removes a queue and all its data.
func (s *Store) DeleteQueue(ctx context.Context, queueName string) error {
	s.queues.Delete(queueName)
	s.groups.Delete(queueName)
	return nil
}

// ListQueues returns all queue configurations.
func (s *Store) ListQueues(ctx context.Context) ([]types.QueueConfig, error) {
	var configs []types.QueueConfig

	s.queues.Range(func(key, value interface{}) bool {
		ql := value.(*queueLog)
		configs = append(configs, ql.config)
		return true
	})

	return configs, nil
}

// Append adds a message to the end of a partition's log.
func (s *Store) Append(ctx context.Context, queueName string, partitionID int, msg *types.Message) (uint64, error) {
	ql, err := s.getQueueLog(queueName)
	if err != nil {
		return 0, err
	}

	if partitionID < 0 || partitionID >= len(ql.partitions) {
		return 0, storage.ErrPartitionNotFound
	}

	pl := ql.partitions[partitionID]
	pl.mu.Lock()
	defer pl.mu.Unlock()

	offset := pl.tail
	pl.tail++

	// Set message metadata
	msg.Sequence = offset
	msg.PartitionID = partitionID

	// Append to log
	pl.messages = append(pl.messages, msg)

	return offset, nil
}

// AppendBatch adds multiple messages to a partition's log.
func (s *Store) AppendBatch(ctx context.Context, queueName string, partitionID int, msgs []*types.Message) (uint64, error) {
	if len(msgs) == 0 {
		return 0, nil
	}

	ql, err := s.getQueueLog(queueName)
	if err != nil {
		return 0, err
	}

	if partitionID < 0 || partitionID >= len(ql.partitions) {
		return 0, storage.ErrPartitionNotFound
	}

	pl := ql.partitions[partitionID]
	pl.mu.Lock()
	defer pl.mu.Unlock()

	firstOffset := pl.tail

	for i, msg := range msgs {
		offset := pl.tail
		pl.tail++

		msg.Sequence = offset
		msg.PartitionID = partitionID
		msgs[i] = msg
	}

	pl.messages = append(pl.messages, msgs...)

	return firstOffset, nil
}

// Read retrieves a message at a specific offset.
func (s *Store) Read(ctx context.Context, queueName string, partitionID int, offset uint64) (*types.Message, error) {
	ql, err := s.getQueueLog(queueName)
	if err != nil {
		return nil, err
	}

	if partitionID < 0 || partitionID >= len(ql.partitions) {
		return nil, storage.ErrPartitionNotFound
	}

	pl := ql.partitions[partitionID]
	pl.mu.RLock()
	defer pl.mu.RUnlock()

	if offset < pl.head || offset >= pl.tail {
		return nil, storage.ErrOffsetOutOfRange
	}

	// Calculate index in slice (accounting for truncation)
	idx := int(offset - pl.head)
	if idx < 0 || idx >= len(pl.messages) {
		return nil, storage.ErrOffsetOutOfRange
	}

	return pl.messages[idx], nil
}

// ReadBatch reads messages starting from offset up to limit.
func (s *Store) ReadBatch(ctx context.Context, queueName string, partitionID int, startOffset uint64, limit int) ([]*types.Message, error) {
	ql, err := s.getQueueLog(queueName)
	if err != nil {
		return nil, err
	}

	if partitionID < 0 || partitionID >= len(ql.partitions) {
		return nil, storage.ErrPartitionNotFound
	}

	pl := ql.partitions[partitionID]
	pl.mu.RLock()
	defer pl.mu.RUnlock()

	// Adjust start offset if before head
	if startOffset < pl.head {
		startOffset = pl.head
	}

	if startOffset >= pl.tail {
		return []*types.Message{}, nil
	}

	startIdx := int(startOffset - pl.head)
	endIdx := startIdx + limit
	if endIdx > len(pl.messages) {
		endIdx = len(pl.messages)
	}

	result := make([]*types.Message, endIdx-startIdx)
	copy(result, pl.messages[startIdx:endIdx])

	return result, nil
}

// Head returns the first valid offset in the partition.
func (s *Store) Head(ctx context.Context, queueName string, partitionID int) (uint64, error) {
	ql, err := s.getQueueLog(queueName)
	if err != nil {
		return 0, err
	}

	if partitionID < 0 || partitionID >= len(ql.partitions) {
		return 0, storage.ErrPartitionNotFound
	}

	pl := ql.partitions[partitionID]
	pl.mu.RLock()
	defer pl.mu.RUnlock()

	return pl.head, nil
}

// Tail returns the next offset that will be assigned.
func (s *Store) Tail(ctx context.Context, queueName string, partitionID int) (uint64, error) {
	ql, err := s.getQueueLog(queueName)
	if err != nil {
		return 0, err
	}

	if partitionID < 0 || partitionID >= len(ql.partitions) {
		return 0, storage.ErrPartitionNotFound
	}

	pl := ql.partitions[partitionID]
	return atomic.LoadUint64(&pl.tail), nil
}

// Truncate removes all messages with offset < minOffset.
func (s *Store) Truncate(ctx context.Context, queueName string, partitionID int, minOffset uint64) error {
	ql, err := s.getQueueLog(queueName)
	if err != nil {
		return err
	}

	if partitionID < 0 || partitionID >= len(ql.partitions) {
		return storage.ErrPartitionNotFound
	}

	pl := ql.partitions[partitionID]
	pl.mu.Lock()
	defer pl.mu.Unlock()

	if minOffset <= pl.head {
		return nil // Nothing to truncate
	}

	if minOffset > pl.tail {
		minOffset = pl.tail
	}

	// Calculate how many messages to remove
	removeCount := int(minOffset - pl.head)
	if removeCount > len(pl.messages) {
		removeCount = len(pl.messages)
	}

	// Release payload buffers for removed messages
	for i := 0; i < removeCount; i++ {
		if pl.messages[i] != nil {
			pl.messages[i].ReleasePayload()
		}
	}

	// Truncate slice
	pl.messages = pl.messages[removeCount:]
	pl.head = minOffset

	return nil
}

// Count returns the number of messages in a partition.
func (s *Store) Count(ctx context.Context, queueName string, partitionID int) (uint64, error) {
	ql, err := s.getQueueLog(queueName)
	if err != nil {
		return 0, err
	}

	if partitionID < 0 || partitionID >= len(ql.partitions) {
		return 0, storage.ErrPartitionNotFound
	}

	pl := ql.partitions[partitionID]
	pl.mu.RLock()
	defer pl.mu.RUnlock()

	return pl.tail - pl.head, nil
}

// TotalCount returns total messages across all partitions.
func (s *Store) TotalCount(ctx context.Context, queueName string) (uint64, error) {
	ql, err := s.getQueueLog(queueName)
	if err != nil {
		return 0, err
	}

	var total uint64
	for _, pl := range ql.partitions {
		pl.mu.RLock()
		total += pl.tail - pl.head
		pl.mu.RUnlock()
	}

	return total, nil
}

// getQueueLog retrieves the queue log, returning an error if not found.
func (s *Store) getQueueLog(queueName string) (*queueLog, error) {
	val, exists := s.queues.Load(queueName)
	if !exists {
		return nil, storage.ErrQueueNotFound
	}
	return val.(*queueLog), nil
}

// --- ConsumerGroupStore Implementation ---

// CreateConsumerGroup creates a new consumer group for a queue.
func (s *Store) CreateConsumerGroup(ctx context.Context, group *types.ConsumerGroupState) error {
	groupsVal, exists := s.groups.Load(group.QueueName)
	if !exists {
		return storage.ErrQueueNotFound
	}

	groups := groupsVal.(*sync.Map)

	if _, exists := groups.Load(group.ID); exists {
		return storage.ErrConsumerGroupExists
	}

	groups.Store(group.ID, group)
	return nil
}

// GetConsumerGroup retrieves a consumer group's state.
func (s *Store) GetConsumerGroup(ctx context.Context, queueName, groupID string) (*types.ConsumerGroupState, error) {
	groupsVal, exists := s.groups.Load(queueName)
	if !exists {
		return nil, storage.ErrQueueNotFound
	}

	groups := groupsVal.(*sync.Map)

	val, exists := groups.Load(groupID)
	if !exists {
		return nil, storage.ErrConsumerNotFound
	}

	return val.(*types.ConsumerGroupState), nil
}

// UpdateConsumerGroup updates a consumer group's state.
func (s *Store) UpdateConsumerGroup(ctx context.Context, group *types.ConsumerGroupState) error {
	groupsVal, exists := s.groups.Load(group.QueueName)
	if !exists {
		return storage.ErrQueueNotFound
	}

	groups := groupsVal.(*sync.Map)
	groups.Store(group.ID, group)
	return nil
}

// DeleteConsumerGroup removes a consumer group.
func (s *Store) DeleteConsumerGroup(ctx context.Context, queueName, groupID string) error {
	groupsVal, exists := s.groups.Load(queueName)
	if !exists {
		return storage.ErrQueueNotFound
	}

	groups := groupsVal.(*sync.Map)
	groups.Delete(groupID)
	return nil
}

// ListConsumerGroups lists all consumer groups for a queue.
func (s *Store) ListConsumerGroups(ctx context.Context, queueName string) ([]*types.ConsumerGroupState, error) {
	groupsVal, exists := s.groups.Load(queueName)
	if !exists {
		return nil, storage.ErrQueueNotFound
	}

	groups := groupsVal.(*sync.Map)
	var result []*types.ConsumerGroupState

	groups.Range(func(key, value interface{}) bool {
		result = append(result, value.(*types.ConsumerGroupState))
		return true
	})

	return result, nil
}

// AddPendingEntry adds an entry to a consumer's PEL.
func (s *Store) AddPendingEntry(ctx context.Context, queueName, groupID string, entry *types.PendingEntry) error {
	group, err := s.GetConsumerGroup(ctx, queueName, groupID)
	if err != nil {
		return err
	}

	group.AddPending(entry.ConsumerID, entry)
	return nil
}

// RemovePendingEntry removes an entry from a consumer's PEL.
func (s *Store) RemovePendingEntry(ctx context.Context, queueName, groupID, consumerID string, partitionID int, offset uint64) error {
	group, err := s.GetConsumerGroup(ctx, queueName, groupID)
	if err != nil {
		return err
	}

	if !group.RemovePending(consumerID, partitionID, offset) {
		return storage.ErrPendingEntryNotFound
	}

	return nil
}

// GetPendingEntries retrieves all pending entries for a consumer.
func (s *Store) GetPendingEntries(ctx context.Context, queueName, groupID, consumerID string) ([]*types.PendingEntry, error) {
	group, err := s.GetConsumerGroup(ctx, queueName, groupID)
	if err != nil {
		return nil, err
	}

	entries, ok := group.PEL[consumerID]
	if !ok {
		return []*types.PendingEntry{}, nil
	}

	// Return copy
	result := make([]*types.PendingEntry, len(entries))
	copy(result, entries)
	return result, nil
}

// GetAllPendingEntries retrieves all pending entries for a group.
func (s *Store) GetAllPendingEntries(ctx context.Context, queueName, groupID string) ([]*types.PendingEntry, error) {
	group, err := s.GetConsumerGroup(ctx, queueName, groupID)
	if err != nil {
		return nil, err
	}

	var result []*types.PendingEntry
	for _, entries := range group.PEL {
		result = append(result, entries...)
	}

	return result, nil
}

// TransferPendingEntry moves a pending entry from one consumer to another.
func (s *Store) TransferPendingEntry(ctx context.Context, queueName, groupID string, partitionID int, offset uint64, fromConsumer, toConsumer string) error {
	group, err := s.GetConsumerGroup(ctx, queueName, groupID)
	if err != nil {
		return err
	}

	if !group.TransferPending(partitionID, offset, fromConsumer, toConsumer) {
		return storage.ErrPendingEntryNotFound
	}

	return nil
}

// UpdateCursor updates the cursor position for a partition.
func (s *Store) UpdateCursor(ctx context.Context, queueName, groupID string, partitionID int, cursor uint64) error {
	group, err := s.GetConsumerGroup(ctx, queueName, groupID)
	if err != nil {
		return err
	}

	pc := group.GetCursor(partitionID)
	pc.Cursor = cursor
	group.UpdatedAt = time.Now()

	return nil
}

// UpdateCommitted updates the committed offset for a partition.
func (s *Store) UpdateCommitted(ctx context.Context, queueName, groupID string, partitionID int, committed uint64) error {
	group, err := s.GetConsumerGroup(ctx, queueName, groupID)
	if err != nil {
		return err
	}

	pc := group.GetCursor(partitionID)
	pc.Committed = committed
	group.UpdatedAt = time.Now()

	return nil
}

// RegisterConsumer adds a consumer to a group.
func (s *Store) RegisterConsumer(ctx context.Context, queueName, groupID string, consumer *types.ConsumerInfo) error {
	group, err := s.GetConsumerGroup(ctx, queueName, groupID)
	if err != nil {
		return err
	}

	group.Consumers[consumer.ID] = consumer
	group.UpdatedAt = time.Now()

	return nil
}

// UnregisterConsumer removes a consumer from a group.
func (s *Store) UnregisterConsumer(ctx context.Context, queueName, groupID, consumerID string) error {
	group, err := s.GetConsumerGroup(ctx, queueName, groupID)
	if err != nil {
		return err
	}

	delete(group.Consumers, consumerID)

	// Also remove any pending entries for this consumer
	delete(group.PEL, consumerID)

	group.UpdatedAt = time.Now()

	return nil
}

// ListConsumers lists all consumers in a group.
func (s *Store) ListConsumers(ctx context.Context, queueName, groupID string) ([]*types.ConsumerInfo, error) {
	group, err := s.GetConsumerGroup(ctx, queueName, groupID)
	if err != nil {
		return nil, err
	}

	result := make([]*types.ConsumerInfo, 0, len(group.Consumers))
	for _, c := range group.Consumers {
		result = append(result, c)
	}

	return result, nil
}

// Compile-time interface assertions
var (
	_ storage.LogStore           = (*Store)(nil)
	_ storage.ConsumerGroupStore = (*Store)(nil)
)
