// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package lockfree

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/absmach/fluxmq/queue/storage"
	"github.com/absmach/fluxmq/queue/types"
)

// Store is a lock-free message store using SPSC ring buffers per partition.
// Provides high-throughput, low-latency message queuing without mutex contention.
type Store struct {
	queues sync.Map // map[string]*queueBuffers
	config Config   // Store configuration
}

// queueBuffers holds all partition buffers for a single queue.
type queueBuffers struct {
	config     types.QueueConfig
	partitions []*partitionBuffer
}

// partitionBuffer wraps a ring buffer with atomic sequence counter.
type partitionBuffer struct {
	ring     *RingBuffer
	sequence uint64 // Atomic sequence counter
	_padding [7]uint64
}

// Config defines lock-free store configuration.
type Config struct {
	RingBufferSize uint64 // Size per partition (default: 16384)
}

// DefaultConfig returns default configuration.
func DefaultConfig() Config {
	return Config{
		// 16K messages per partition
		RingBufferSize: 16384,
	}
}

// New creates a new lock-free message store with default configuration.
func New() *Store {
	return &Store{
		config: DefaultConfig(),
	}
}

// NewWithConfig creates a new lock-free message store with custom configuration.
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

	// Check if queue already exists
	if _, exists := s.queues.Load(config.Name); exists {
		return storage.ErrQueueAlreadyExists
	}

	// Create partition buffers with configured ring buffer size
	partitions := make([]*partitionBuffer, config.Partitions)
	for i := 0; i < config.Partitions; i++ {
		partitions[i] = &partitionBuffer{
			ring:     NewRingBuffer(s.config.RingBufferSize),
			sequence: 0,
		}
	}

	qb := &queueBuffers{
		config:     config,
		partitions: partitions,
	}

	s.queues.Store(config.Name, qb)
	return nil
}

// GetQueue retrieves queue configuration.
func (s *Store) GetQueue(ctx context.Context, queueName string) (*types.QueueConfig, error) {
	val, exists := s.queues.Load(queueName)
	if !exists {
		return nil, storage.ErrQueueNotFound
	}

	qb := val.(*queueBuffers)
	return &qb.config, nil
}

// UpdateQueue updates queue configuration.
func (s *Store) UpdateQueue(ctx context.Context, config types.QueueConfig) error {
	if err := config.Validate(); err != nil {
		return err
	}

	val, exists := s.queues.Load(config.Name)
	if !exists {
		return storage.ErrQueueNotFound
	}

	qb := val.(*queueBuffers)
	qb.config = config
	return nil
}

// DeleteQueue deletes a queue.
func (s *Store) DeleteQueue(ctx context.Context, queueName string) error {
	s.queues.Delete(queueName)
	return nil
}

// ListQueues returns all queue configurations.
func (s *Store) ListQueues(ctx context.Context) ([]types.QueueConfig, error) {
	var configs []types.QueueConfig

	s.queues.Range(func(key, value interface{}) bool {
		qb := value.(*queueBuffers)
		configs = append(configs, qb.config)
		return true
	})

	return configs, nil
}

// Enqueue adds a message to a partition's ring buffer (lock-free).
func (s *Store) Enqueue(ctx context.Context, queueName string, msg *types.Message) error {
	val, exists := s.queues.Load(queueName)
	if !exists {
		return storage.ErrQueueNotFound
	}

	qb := val.(*queueBuffers)
	if msg.PartitionID < 0 || msg.PartitionID >= len(qb.partitions) {
		return fmt.Errorf("invalid partition ID: %d", msg.PartitionID)
	}

	partition := qb.partitions[msg.PartitionID]

	// Enqueue to ring buffer (lock-free)
	if !partition.ring.Enqueue(*msg) {
		return fmt.Errorf("partition %d ring buffer full", msg.PartitionID)
	}

	return nil
}

// Dequeue removes the next message from a partition (lock-free).
func (s *Store) Dequeue(ctx context.Context, queueName string, partitionID int) (*types.Message, error) {
	val, exists := s.queues.Load(queueName)
	if !exists {
		return nil, storage.ErrQueueNotFound
	}

	qb := val.(*queueBuffers)
	if partitionID < 0 || partitionID >= len(qb.partitions) {
		return nil, fmt.Errorf("invalid partition ID: %d", partitionID)
	}

	partition := qb.partitions[partitionID]

	// Dequeue from ring buffer (lock-free)
	msg, ok := partition.ring.Dequeue()
	if !ok {
		return nil, nil // No messages available
	}

	return &msg, nil
}

// DequeueBatch removes up to 'limit' messages from a partition (lock-free).
func (s *Store) DequeueBatch(ctx context.Context, queueName string, partitionID int, limit int) ([]*types.Message, error) {
	val, exists := s.queues.Load(queueName)
	if !exists {
		return nil, storage.ErrQueueNotFound
	}

	qb := val.(*queueBuffers)
	if partitionID < 0 || partitionID >= len(qb.partitions) {
		return nil, fmt.Errorf("invalid partition ID: %d", partitionID)
	}

	partition := qb.partitions[partitionID]

	// Batch dequeue from ring buffer (lock-free)
	messages := partition.ring.DequeueBatch(limit)
	if len(messages) == 0 {
		return nil, nil
	}

	// Convert to pointers
	result := make([]*types.Message, len(messages))
	for i := range messages {
		result[i] = &messages[i]
	}

	return result, nil
}

// GetNextSequence atomically increments and returns the next sequence number.
func (s *Store) GetNextSequence(ctx context.Context, queueName string, partitionID int) (uint64, error) {
	val, exists := s.queues.Load(queueName)
	if !exists {
		return 0, storage.ErrQueueNotFound
	}

	qb := val.(*queueBuffers)
	if partitionID < 0 || partitionID >= len(qb.partitions) {
		return 0, fmt.Errorf("invalid partition ID: %d", partitionID)
	}

	partition := qb.partitions[partitionID]

	// Atomic increment (lock-free)
	seq := atomic.AddUint64(&partition.sequence, 1)
	return seq, nil
}

// Note: The following methods are not performance-critical and use simple implementations.
// They're part of the MessageStore interface but not in the hot path.

// UpdateMessage updates a message (not used in lock-free hot path).
func (s *Store) UpdateMessage(ctx context.Context, queueName string, msg *types.Message) error {
	// Lock-free ring buffers don't support in-place updates
	// Messages are consumed on dequeue
	return nil
}

// DeleteMessage deletes a message (not used in lock-free hot path).
func (s *Store) DeleteMessage(ctx context.Context, queueName string, messageID string) error {
	// Lock-free ring buffers don't support deletion
	// Messages are consumed on dequeue
	return nil
}

// GetMessage retrieves a specific message (not supported in lock-free store).
func (s *Store) GetMessage(ctx context.Context, queueName string, messageID string) (*types.Message, error) {
	return nil, fmt.Errorf("GetMessage not supported in lock-free store")
}

// MarkInflight tracks inflight messages (delegated to separate store).
func (s *Store) MarkInflight(ctx context.Context, state *types.DeliveryState) error {
	// Inflight tracking delegated to hybrid store
	return nil
}

// GetInflight returns all inflight messages.
func (s *Store) GetInflight(ctx context.Context, queueName string) ([]*types.DeliveryState, error) {
	return nil, nil
}

// GetInflightMessage retrieves a specific inflight message.
func (s *Store) GetInflightMessage(ctx context.Context, queueName, messageID, groupID string) (*types.DeliveryState, error) {
	return nil, nil
}

// GetInflightForMessage returns all inflight entries for a specific message (across all groups).
func (s *Store) GetInflightForMessage(ctx context.Context, queueName, messageID string) ([]*types.DeliveryState, error) {
	return nil, nil
}

// RemoveInflight removes an inflight tracking entry.
func (s *Store) RemoveInflight(ctx context.Context, queueName, messageID, groupID string) error {
	return nil
}

// EnqueueDLQ adds a message to the dead-letter queue.
func (s *Store) EnqueueDLQ(ctx context.Context, dlqTopic string, msg *types.Message) error {
	// DLQ delegated to hybrid store
	return nil
}

// ListDLQ lists messages in the dead-letter queue.
func (s *Store) ListDLQ(ctx context.Context, dlqTopic string, limit int) ([]*types.Message, error) {
	return nil, nil
}

// DeleteDLQMessage deletes a message from the DLQ.
func (s *Store) DeleteDLQMessage(ctx context.Context, dlqTopic, messageID string) error {
	return nil
}

// ListRetry lists retry messages.
func (s *Store) ListRetry(ctx context.Context, queueName string, partitionID int) ([]*types.Message, error) {
	return nil, nil
}

// UpdateOffset updates partition offset.
func (s *Store) UpdateOffset(ctx context.Context, queueName string, partitionID int, offset uint64) error {
	return nil
}

// GetOffset retrieves partition offset.
func (s *Store) GetOffset(ctx context.Context, queueName string, partitionID int) (uint64, error) {
	return 0, nil
}

// ListQueued lists queued messages.
func (s *Store) ListQueued(ctx context.Context, queueName string, partitionID int, limit int) ([]*types.Message, error) {
	// Could implement by peeking ring buffer without dequeuing
	return nil, nil
}

// RegisterConsumer registers a consumer (delegated to separate store).
func (s *Store) RegisterConsumer(ctx context.Context, consumer *types.Consumer) error {
	return nil
}

// UnregisterConsumer removes a consumer.
func (s *Store) UnregisterConsumer(ctx context.Context, queueName, groupID, consumerID string) error {
	return nil
}

// GetConsumer retrieves consumer information.
func (s *Store) GetConsumer(ctx context.Context, queueName, groupID, consumerID string) (*types.Consumer, error) {
	return nil, storage.ErrConsumerNotFound
}

// ListConsumers lists all consumers in a group.
func (s *Store) ListConsumers(ctx context.Context, queueName, groupID string) ([]*types.Consumer, error) {
	return nil, nil
}

// ListGroups lists all consumer groups.
func (s *Store) ListGroups(ctx context.Context, queueName string) ([]string, error) {
	return nil, nil
}

// UpdateHeartbeat updates consumer heartbeat.
func (s *Store) UpdateHeartbeat(ctx context.Context, queueName, groupID, consumerID string, timestamp any) error {
	return nil
}
