// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

// Package log provides an in-memory implementation of the stream-based storage.
// This implementation uses append-only logs with offset-based access, inspired by
// NATS JetStream.
package log

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/absmach/fluxmq/queue/storage"
	"github.com/absmach/fluxmq/queue/types"
)

// Store implements StreamStore and ConsumerGroupStore using in-memory data structures.
type Store struct {
	streams      sync.Map // map[string]*streamLog
	groups       sync.Map // map[string]map[string]*types.ConsumerGroupState (streamName -> groupID -> state)
	subjectIndex *storage.TopicIndex
	config       Config
}

// streamLog holds the log data for a single stream.
type streamLog struct {
	config   types.QueueConfig
	messages []*types.Message // Append-only message log
	head     uint64           // First valid offset (after truncation)
	tail     uint64           // Next offset to assign (atomic)
	mu       sync.RWMutex
}

// Config defines configuration for the memory log store.
type Config struct {
	InitialCapacity int // Initial slice capacity per stream
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
		config:       cfg,
		subjectIndex: storage.NewTopicIndex(),
	}
}

// CreateQueue creates a new stream with the specified configuration.
func (s *Store) CreateQueue(ctx context.Context, config types.QueueConfig) error {
	if err := config.Validate(); err != nil {
		return err
	}

	if _, exists := s.streams.Load(config.Name); exists {
		return storage.ErrQueueAlreadyExists
	}

	sl := &streamLog{
		config:   config,
		messages: make([]*types.Message, 0, s.config.InitialCapacity),
		head:     0,
		tail:     0,
	}

	s.streams.Store(config.Name, sl)

	// Index subjects for topic routing
	s.subjectIndex.AddQueue(config.Name, config.Topics)

	// Initialize empty groups map for this stream
	s.groups.Store(config.Name, &sync.Map{})

	return nil
}

// GetQueue retrieves stream configuration.
func (s *Store) GetQueue(ctx context.Context, streamName string) (*types.QueueConfig, error) {
	val, exists := s.streams.Load(streamName)
	if !exists {
		return nil, storage.ErrQueueNotFound
	}

	sl := val.(*streamLog)
	configCopy := sl.config
	return &configCopy, nil
}

// DeleteQueue removes a stream and all its data.
func (s *Store) DeleteQueue(ctx context.Context, streamName string) error {
	val, exists := s.streams.Load(streamName)
	if exists {
		sl := val.(*streamLog)
		if sl.config.Reserved {
			return storage.ErrQueueNotFound // Cannot delete reserved streams
		}
	}

	s.streams.Delete(streamName)
	s.groups.Delete(streamName)
	s.subjectIndex.RemoveQueue(streamName)
	return nil
}

// ListQueues returns all stream configurations.
func (s *Store) ListQueues(ctx context.Context) ([]types.QueueConfig, error) {
	var configs []types.QueueConfig

	s.streams.Range(func(key, value interface{}) bool {
		sl := value.(*streamLog)
		configs = append(configs, sl.config)
		return true
	})

	return configs, nil
}

// FindMatchingQueues returns all streams whose subject patterns match the given topic.
func (s *Store) FindMatchingQueues(ctx context.Context, topic string) ([]string, error) {
	return s.subjectIndex.FindMatching(topic), nil
}

// Append adds a message to the end of a stream's log.
func (s *Store) Append(ctx context.Context, streamName string, msg *types.Message) (uint64, error) {
	sl, err := s.getStreamLog(streamName)
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

// AppendBatch adds multiple messages to a stream's log.
func (s *Store) AppendBatch(ctx context.Context, streamName string, msgs []*types.Message) (uint64, error) {
	if len(msgs) == 0 {
		return 0, nil
	}

	sl, err := s.getStreamLog(streamName)
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
func (s *Store) Read(ctx context.Context, streamName string, offset uint64) (*types.Message, error) {
	sl, err := s.getStreamLog(streamName)
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
func (s *Store) ReadBatch(ctx context.Context, streamName string, startOffset uint64, limit int) ([]*types.Message, error) {
	sl, err := s.getStreamLog(streamName)
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

// Head returns the first valid offset in the stream.
func (s *Store) Head(ctx context.Context, streamName string) (uint64, error) {
	sl, err := s.getStreamLog(streamName)
	if err != nil {
		return 0, err
	}

	sl.mu.RLock()
	defer sl.mu.RUnlock()

	return sl.head, nil
}

// Tail returns the next offset that will be assigned.
func (s *Store) Tail(ctx context.Context, streamName string) (uint64, error) {
	sl, err := s.getStreamLog(streamName)
	if err != nil {
		return 0, err
	}

	return atomic.LoadUint64(&sl.tail), nil
}

// Truncate removes all messages with offset < minOffset.
func (s *Store) Truncate(ctx context.Context, streamName string, minOffset uint64) error {
	sl, err := s.getStreamLog(streamName)
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

// Count returns the number of messages in a stream.
func (s *Store) Count(ctx context.Context, streamName string) (uint64, error) {
	sl, err := s.getStreamLog(streamName)
	if err != nil {
		return 0, err
	}

	sl.mu.RLock()
	defer sl.mu.RUnlock()

	return sl.tail - sl.head, nil
}

// getStreamLog retrieves the stream log, returning an error if not found.
func (s *Store) getStreamLog(streamName string) (*streamLog, error) {
	val, exists := s.streams.Load(streamName)
	if !exists {
		return nil, storage.ErrQueueNotFound
	}
	return val.(*streamLog), nil
}

// --- ConsumerGroupStore Implementation ---

// CreateConsumerGroup creates a new consumer group for a stream.
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
func (s *Store) GetConsumerGroup(ctx context.Context, streamName, groupID string) (*types.ConsumerGroupState, error) {
	groupsVal, exists := s.groups.Load(streamName)
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
func (s *Store) DeleteConsumerGroup(ctx context.Context, streamName, groupID string) error {
	groupsVal, exists := s.groups.Load(streamName)
	if !exists {
		return storage.ErrQueueNotFound
	}

	groups := groupsVal.(*sync.Map)
	groups.Delete(groupID)
	return nil
}

// ListConsumerGroups lists all consumer groups for a stream.
func (s *Store) ListConsumerGroups(ctx context.Context, streamName string) ([]*types.ConsumerGroupState, error) {
	groupsVal, exists := s.groups.Load(streamName)
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
func (s *Store) AddPendingEntry(ctx context.Context, streamName, groupID string, entry *types.PendingEntry) error {
	group, err := s.GetConsumerGroup(ctx, streamName, groupID)
	if err != nil {
		return err
	}

	group.AddPending(entry.ConsumerID, entry)
	return nil
}

// RemovePendingEntry removes an entry from a consumer's PEL.
func (s *Store) RemovePendingEntry(ctx context.Context, streamName, groupID, consumerID string, offset uint64) error {
	group, err := s.GetConsumerGroup(ctx, streamName, groupID)
	if err != nil {
		return err
	}

	if !group.RemovePending(consumerID, offset) {
		return storage.ErrPendingEntryNotFound
	}

	return nil
}

// GetPendingEntries retrieves all pending entries for a consumer.
func (s *Store) GetPendingEntries(ctx context.Context, streamName, groupID, consumerID string) ([]*types.PendingEntry, error) {
	group, err := s.GetConsumerGroup(ctx, streamName, groupID)
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
func (s *Store) GetAllPendingEntries(ctx context.Context, streamName, groupID string) ([]*types.PendingEntry, error) {
	group, err := s.GetConsumerGroup(ctx, streamName, groupID)
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
func (s *Store) TransferPendingEntry(ctx context.Context, streamName, groupID string, offset uint64, fromConsumer, toConsumer string) error {
	group, err := s.GetConsumerGroup(ctx, streamName, groupID)
	if err != nil {
		return err
	}

	if !group.TransferPending(offset, fromConsumer, toConsumer) {
		return storage.ErrPendingEntryNotFound
	}

	return nil
}

// UpdateCursor updates the cursor position.
func (s *Store) UpdateCursor(ctx context.Context, streamName, groupID string, cursor uint64) error {
	group, err := s.GetConsumerGroup(ctx, streamName, groupID)
	if err != nil {
		return err
	}

	c := group.GetCursor()
	c.Cursor = cursor
	group.UpdatedAt = time.Now()

	return nil
}

// UpdateCommitted updates the committed offset.
func (s *Store) UpdateCommitted(ctx context.Context, streamName, groupID string, committed uint64) error {
	group, err := s.GetConsumerGroup(ctx, streamName, groupID)
	if err != nil {
		return err
	}

	c := group.GetCursor()
	c.Committed = committed
	group.UpdatedAt = time.Now()

	return nil
}

// RegisterConsumer adds a consumer to a group.
func (s *Store) RegisterConsumer(ctx context.Context, streamName, groupID string, consumer *types.ConsumerInfo) error {
	group, err := s.GetConsumerGroup(ctx, streamName, groupID)
	if err != nil {
		return err
	}

	group.SetConsumer(consumer.ID, consumer)
	return nil
}

// UnregisterConsumer removes a consumer from a group.
func (s *Store) UnregisterConsumer(ctx context.Context, streamName, groupID, consumerID string) error {
	group, err := s.GetConsumerGroup(ctx, streamName, groupID)
	if err != nil {
		return err
	}

	group.DeleteConsumer(consumerID)
	group.DeleteConsumerPEL(consumerID)
	return nil
}

// ListConsumers lists all consumers in a group.
func (s *Store) ListConsumers(ctx context.Context, streamName, groupID string) ([]*types.ConsumerInfo, error) {
	group, err := s.GetConsumerGroup(ctx, streamName, groupID)
	if err != nil {
		return nil, err
	}

	var result []*types.ConsumerInfo
	group.ForEachConsumer(func(_ string, c *types.ConsumerInfo) bool {
		result = append(result, c)
		return true
	})

	return result, nil
}

// Compile-time interface assertions
var (
	_ storage.QueueStore        = (*Store)(nil)
	_ storage.ConsumerGroupStore = (*Store)(nil)
)
