// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

// Package log provides an in-memory implementation of the queue-based storage.
// This implementation uses append-only logs with offset-based access, inspired by
// NATS JetStream.
package log

import (
	"context"
	"sync"
	"time"

	"github.com/absmach/fluxmq/queue/storage"
	"github.com/absmach/fluxmq/queue/types"
)

// Store implements queueStore and ConsumerGroupStore using in-memory data structures.
type Store struct {
	logs       sync.Map // map[string]*Log
	groups     sync.Map // map[string]map[string]*types.ConsumerGroupState (queueName -> groupID -> state)
	consumers  sync.Map // map[string]map[string]map[string]*types.Consumer (queueName -> groupID -> consumerID -> consumer)
	topicIndex *storage.TopicIndex
	config     Config
}

type log struct {
	config   types.QueueConfig
	messages []*types.Message // Append-only message log
	head     uint64           // First valid offset (after truncation)
	tail     uint64           // Next offset to assign
	mu       sync.RWMutex
}

// Config defines configuration for the memory log store.
type Config struct {
	InitialCapacity int
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
		config:     cfg,
		topicIndex: storage.NewTopicIndex(),
	}
}

// CreateQueue creates a new queue with the specified configuration.
func (s *Store) CreateQueue(ctx context.Context, config types.QueueConfig) error {
	if err := config.Validate(); err != nil {
		return err
	}

	if _, exists := s.logs.Load(config.Name); exists {
		return storage.ErrQueueAlreadyExists
	}

	sl := &log{
		config:   config,
		messages: make([]*types.Message, 0, s.config.InitialCapacity),
		head:     0,
		tail:     0,
	}

	s.logs.Store(config.Name, sl)

	// Index topics for topic routing
	s.topicIndex.AddQueue(config.Name, config.Topics)

	// Initialize empty groups map for this queue
	s.groups.Store(config.Name, &sync.Map{})

	// Initialize empty consumers map for this queue
	s.consumers.Store(config.Name, &sync.Map{})

	return nil
}

// UpdateQueue updates an existing queue's configuration.
func (s *Store) UpdateQueue(ctx context.Context, config types.QueueConfig) error {
	val, exists := s.logs.Load(config.Name)
	if !exists {
		return storage.ErrQueueNotFound
	}

	sl := val.(*log)
	sl.mu.Lock()
	sl.config = config
	sl.mu.Unlock()
	return nil
}

// GetQueue retrieves queue configuration.
func (s *Store) GetQueue(ctx context.Context, queueName string) (*types.QueueConfig, error) {
	val, exists := s.logs.Load(queueName)
	if !exists {
		return nil, storage.ErrQueueNotFound
	}

	sl := val.(*log)
	configCopy := sl.config
	return &configCopy, nil
}

// DeleteQueue removes a queue and all its data.
func (s *Store) DeleteQueue(ctx context.Context, queueName string) error {
	val, exists := s.logs.Load(queueName)
	if exists {
		sl := val.(*log)
		if sl.config.Reserved {
			return storage.ErrQueueNotFound // Cannot delete reserved queue
		}
	}

	s.logs.Delete(queueName)
	s.groups.Delete(queueName)
	s.consumers.Delete(queueName)
	s.topicIndex.RemoveQueue(queueName)
	return nil
}

// ListQueues returns all queue configurations.
func (s *Store) ListQueues(ctx context.Context) ([]types.QueueConfig, error) {
	var configs []types.QueueConfig

	s.logs.Range(func(key, value interface{}) bool {
		sl := value.(*log)
		configs = append(configs, sl.config)
		return true
	})

	return configs, nil
}

// FindMatchingQueues returns all queues whose topic patterns match the given topic.
func (s *Store) FindMatchingQueues(ctx context.Context, topic string) ([]string, error) {
	return s.topicIndex.FindMatching(topic), nil
}

// Append adds a message to the end of a queue's log.
func (s *Store) Append(ctx context.Context, queueName string, msg *types.Message) (uint64, error) {
	sl, err := s.getQueueLog(queueName)
	if err != nil {
		return 0, err
	}

	sl.mu.Lock()
	defer sl.mu.Unlock()

	offset := sl.tail
	sl.tail++

	// Set message metadata
	msg.Sequence = offset

	// Append to log
	sl.messages = append(sl.messages, msg)

	return offset, nil
}

// AppendBatch adds multiple messages to a queue's log.
func (s *Store) AppendBatch(ctx context.Context, queueName string, msgs []*types.Message) (uint64, error) {
	if len(msgs) == 0 {
		return 0, nil
	}

	sl, err := s.getQueueLog(queueName)
	if err != nil {
		return 0, err
	}

	sl.mu.Lock()
	defer sl.mu.Unlock()

	firstOffset := sl.tail

	for _, msg := range msgs {
		offset := sl.tail
		sl.tail++

		msg.Sequence = offset
	}

	sl.messages = append(sl.messages, msgs...)

	return firstOffset, nil
}

// Read retrieves a message at a specific offset.
func (s *Store) Read(ctx context.Context, queueName string, offset uint64) (*types.Message, error) {
	sl, err := s.getQueueLog(queueName)
	if err != nil {
		return nil, err
	}

	sl.mu.RLock()
	defer sl.mu.RUnlock()

	if offset < sl.head || offset >= sl.tail {
		return nil, storage.ErrOffsetOutOfRange
	}

	// Calculate index in slice (accounting for truncation)
	idx := int(offset - sl.head)
	if idx < 0 || idx >= len(sl.messages) {
		return nil, storage.ErrOffsetOutOfRange
	}

	return sl.messages[idx], nil
}

// ReadBatch reads messages starting from offset up to limit.
func (s *Store) ReadBatch(ctx context.Context, queueName string, startOffset uint64, limit int) ([]*types.Message, error) {
	sl, err := s.getQueueLog(queueName)
	if err != nil {
		return nil, err
	}

	sl.mu.RLock()
	defer sl.mu.RUnlock()

	// Adjust start offset if before head
	if startOffset < sl.head {
		startOffset = sl.head
	}

	if startOffset >= sl.tail {
		return []*types.Message{}, nil
	}

	startIdx := int(startOffset - sl.head)
	endIdx := startIdx + limit
	if endIdx > len(sl.messages) {
		endIdx = len(sl.messages)
	}

	result := make([]*types.Message, endIdx-startIdx)
	copy(result, sl.messages[startIdx:endIdx])

	return result, nil
}

// Head returns the first valid offset in the queue.
func (s *Store) Head(ctx context.Context, queueName string) (uint64, error) {
	sl, err := s.getQueueLog(queueName)
	if err != nil {
		return 0, err
	}

	sl.mu.RLock()
	defer sl.mu.RUnlock()

	return sl.head, nil
}

// Tail returns the next offset that will be assigned.
func (s *Store) Tail(ctx context.Context, queueName string) (uint64, error) {
	sl, err := s.getQueueLog(queueName)
	if err != nil {
		return 0, err
	}

	sl.mu.RLock()
	defer sl.mu.RUnlock()

	return sl.tail, nil
}

// Truncate removes all messages with offset < minOffset.
func (s *Store) Truncate(ctx context.Context, queueName string, minOffset uint64) error {
	sl, err := s.getQueueLog(queueName)
	if err != nil {
		return err
	}

	sl.mu.Lock()
	defer sl.mu.Unlock()

	if minOffset <= sl.head {
		return nil // Nothing to truncate
	}

	if minOffset > sl.tail {
		minOffset = sl.tail
	}

	// Calculate how many messages to remove
	removeCount := int(minOffset - sl.head)
	if removeCount > len(sl.messages) {
		removeCount = len(sl.messages)
	}

	// Release payload buffers for removed messages
	for i := 0; i < removeCount; i++ {
		if sl.messages[i] != nil {
			sl.messages[i].ReleasePayload()
		}
	}

	// Truncate slice
	sl.messages = sl.messages[removeCount:]
	sl.head = minOffset

	return nil
}

// Count returns the number of messages in a queue.
func (s *Store) Count(ctx context.Context, queueName string) (uint64, error) {
	sl, err := s.getQueueLog(queueName)
	if err != nil {
		return 0, err
	}

	sl.mu.RLock()
	defer sl.mu.RUnlock()

	return sl.tail - sl.head, nil
}

// getQueueLog retrieves the queue log, returning an error if not found.
func (s *Store) getQueueLog(queueName string) (*log, error) {
	val, exists := s.logs.Load(queueName)
	if !exists {
		return nil, storage.ErrQueueNotFound
	}
	return val.(*log), nil
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
func (s *Store) RemovePendingEntry(ctx context.Context, queueName, groupID, consumerID string, offset uint64) error {
	group, err := s.GetConsumerGroup(ctx, queueName, groupID)
	if err != nil {
		return err
	}

	if !group.RemovePending(consumerID, offset) {
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
func (s *Store) TransferPendingEntry(ctx context.Context, queueName, groupID string, offset uint64, fromConsumer, toConsumer string) error {
	group, err := s.GetConsumerGroup(ctx, queueName, groupID)
	if err != nil {
		return err
	}

	if !group.TransferPending(offset, fromConsumer, toConsumer) {
		return storage.ErrPendingEntryNotFound
	}

	return nil
}

// UpdateCursor updates the cursor position.
func (s *Store) UpdateCursor(ctx context.Context, queueName, groupID string, cursor uint64) error {
	group, err := s.GetConsumerGroup(ctx, queueName, groupID)
	if err != nil {
		return err
	}

	c := group.GetCursor()
	c.Cursor = cursor
	group.UpdatedAt = time.Now()

	return nil
}

// UpdateCommitted updates the committed offset.
func (s *Store) UpdateCommitted(ctx context.Context, queueName, groupID string, committed uint64) error {
	group, err := s.GetConsumerGroup(ctx, queueName, groupID)
	if err != nil {
		return err
	}

	c := group.GetCursor()
	c.Committed = committed
	group.UpdatedAt = time.Now()

	return nil
}

// --- ConsumerStore Implementation ---

// RegisterConsumer registers a consumer in a group.
func (s *Store) RegisterConsumer(ctx context.Context, consumer *types.Consumer) error {
	queueConsumersVal, exists := s.consumers.Load(consumer.QueueName)
	if !exists {
		return storage.ErrQueueNotFound
	}

	queueConsumers := queueConsumersVal.(*sync.Map)

	// Get or create the group map
	groupConsumersVal, _ := queueConsumers.LoadOrStore(consumer.GroupID, &sync.Map{})
	groupConsumers := groupConsumersVal.(*sync.Map)

	// Store the consumer
	groupConsumers.Store(consumer.ID, consumer)
	return nil
}

// UnregisterConsumer removes a consumer from a group.
func (s *Store) UnregisterConsumer(ctx context.Context, queueName, groupID, consumerID string) error {
	queueConsumersVal, exists := s.consumers.Load(queueName)
	if !exists {
		return storage.ErrQueueNotFound
	}

	queueConsumers := queueConsumersVal.(*sync.Map)

	groupConsumersVal, exists := queueConsumers.Load(groupID)
	if !exists {
		return storage.ErrConsumerNotFound
	}

	groupConsumers := groupConsumersVal.(*sync.Map)
	groupConsumers.Delete(consumerID)
	return nil
}

// GetConsumer retrieves a consumer by ID.
func (s *Store) GetConsumer(ctx context.Context, queueName, groupID, consumerID string) (*types.Consumer, error) {
	queueConsumersVal, exists := s.consumers.Load(queueName)
	if !exists {
		return nil, storage.ErrQueueNotFound
	}

	queueConsumers := queueConsumersVal.(*sync.Map)

	groupConsumersVal, exists := queueConsumers.Load(groupID)
	if !exists {
		return nil, storage.ErrConsumerNotFound
	}

	groupConsumers := groupConsumersVal.(*sync.Map)

	consumerVal, exists := groupConsumers.Load(consumerID)
	if !exists {
		return nil, storage.ErrConsumerNotFound
	}

	return consumerVal.(*types.Consumer), nil
}

// ListConsumers lists all consumers in a group.
func (s *Store) ListConsumers(ctx context.Context, queueName, groupID string) ([]*types.Consumer, error) {
	queueConsumersVal, exists := s.consumers.Load(queueName)
	if !exists {
		return nil, storage.ErrQueueNotFound
	}

	queueConsumers := queueConsumersVal.(*sync.Map)

	groupConsumersVal, exists := queueConsumers.Load(groupID)
	if !exists {
		return []*types.Consumer{}, nil
	}

	groupConsumers := groupConsumersVal.(*sync.Map)
	var result []*types.Consumer

	groupConsumers.Range(func(key, value interface{}) bool {
		result = append(result, value.(*types.Consumer))
		return true
	})

	return result, nil
}

// ListGroups lists all group IDs for a queue.
func (s *Store) ListGroups(ctx context.Context, queueName string) ([]string, error) {
	queueConsumersVal, exists := s.consumers.Load(queueName)
	if !exists {
		return nil, storage.ErrQueueNotFound
	}

	queueConsumers := queueConsumersVal.(*sync.Map)
	var result []string

	queueConsumers.Range(func(key, value interface{}) bool {
		result = append(result, key.(string))
		return true
	})

	return result, nil
}

// UpdateHeartbeat updates the heartbeat timestamp for a consumer.
func (s *Store) UpdateHeartbeat(ctx context.Context, queueName, groupID, consumerID string, timestamp time.Time) error {
	consumer, err := s.GetConsumer(ctx, queueName, groupID, consumerID)
	if err != nil {
		return err
	}

	consumer.LastHeartbeat = timestamp
	return nil
}

// Compile-time interface assertions
var (
	_ storage.QueueStore    = (*Store)(nil)
	_ storage.ConsumerStore = (*Store)(nil)
)
