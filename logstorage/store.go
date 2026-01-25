// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package logstorage

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// Store is the main interface for the AOL storage system.
// It manages multiple queues, each with multiple partitions.
// Consumer state uses JetStream-style semantics with sharded PEL.
type Store struct {
	mu sync.RWMutex

	baseDir string
	config  StoreConfig

	// Queue -> Partition -> SegmentManager
	queues map[string]map[uint32]*SegmentManager

	// Queue -> ConsumerManager (JetStream-style with sharded PEL)
	consumers map[string]*ConsumerManager

	closed bool
}

// StoreConfig holds store configuration.
type StoreConfig struct {
	ManagerConfig
	ConsumerStateConfig
	AutoCreate bool // Automatically create queues/partitions on first access
}

// DefaultStoreConfig returns default store configuration.
func DefaultStoreConfig() StoreConfig {
	return StoreConfig{
		ManagerConfig:       DefaultManagerConfig(),
		ConsumerStateConfig: DefaultConsumerStateConfig(),
		AutoCreate:          true,
	}
}

// NewStore creates a new AOL store.
func NewStore(baseDir string, config StoreConfig) (*Store, error) {
	if err := os.MkdirAll(baseDir, 0o755); err != nil {
		return nil, fmt.Errorf("failed to create store directory: %w", err)
	}

	s := &Store{
		baseDir:   baseDir,
		config:    config,
		queues:    make(map[string]map[uint32]*SegmentManager),
		consumers: make(map[string]*ConsumerManager),
	}

	// Load existing queues
	if err := s.loadQueues(); err != nil {
		return nil, fmt.Errorf("failed to load queues: %w", err)
	}

	return s, nil
}

// loadQueues discovers and loads existing queues from disk.
func (s *Store) loadQueues() error {
	queuesDir := filepath.Join(s.baseDir, "queues")
	if _, err := os.Stat(queuesDir); os.IsNotExist(err) {
		return nil
	}

	entries, err := os.ReadDir(queuesDir)
	if err != nil {
		return err
	}

	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}

		queueName := entry.Name()
		if err := s.loadQueue(queueName); err != nil {
			return fmt.Errorf("failed to load queue %s: %w", queueName, err)
		}
	}

	return nil
}

// loadQueue loads a queue and its partitions.
func (s *Store) loadQueue(queueName string) error {
	queueDir := filepath.Join(s.baseDir, "queues", queueName)
	partitionsDir := filepath.Join(queueDir, "partitions")

	entries, err := os.ReadDir(partitionsDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	s.queues[queueName] = make(map[uint32]*SegmentManager)

	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}

		var partitionID uint32
		if _, err := fmt.Sscanf(entry.Name(), "%d", &partitionID); err != nil {
			continue
		}

		partitionDir := filepath.Join(partitionsDir, entry.Name())
		manager, err := NewSegmentManager(partitionDir, s.config.ManagerConfig)
		if err != nil {
			return fmt.Errorf("failed to load partition %d: %w", partitionID, err)
		}

		s.queues[queueName][partitionID] = manager
	}

	// Load consumer manager for this queue
	cm, err := NewConsumerManager(queueDir, s.config.ConsumerStateConfig)
	if err != nil {
		return fmt.Errorf("failed to load consumer manager: %w", err)
	}
	s.consumers[queueName] = cm

	return nil
}

// queueDir returns the directory for a queue.
func (s *Store) queueDir(queueName string) string {
	return filepath.Join(s.baseDir, "queues", queueName)
}

// partitionDir returns the directory for a partition.
func (s *Store) partitionDir(queueName string, partitionID uint32) string {
	return filepath.Join(s.queueDir(queueName), "partitions", fmt.Sprintf("%d", partitionID))
}

// getOrCreatePartition gets or creates a partition's segment manager.
func (s *Store) getOrCreatePartition(queueName string, partitionID uint32) (*SegmentManager, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return nil, ErrSegmentClosed
	}

	partitions, ok := s.queues[queueName]
	if !ok {
		if !s.config.AutoCreate {
			return nil, ErrQueueNotFound
		}
		partitions = make(map[uint32]*SegmentManager)
		s.queues[queueName] = partitions

		// Create consumer manager for new queue
		queueDir := s.queueDir(queueName)
		cm, err := NewConsumerManager(queueDir, s.config.ConsumerStateConfig)
		if err != nil {
			return nil, fmt.Errorf("failed to create consumer manager: %w", err)
		}
		s.consumers[queueName] = cm
	}

	manager, ok := partitions[partitionID]
	if !ok {
		if !s.config.AutoCreate {
			return nil, ErrPartitionNotFound
		}

		partDir := s.partitionDir(queueName, partitionID)
		var err error
		manager, err = NewSegmentManager(partDir, s.config.ManagerConfig)
		if err != nil {
			return nil, fmt.Errorf("failed to create partition: %w", err)
		}
		partitions[partitionID] = manager
	}

	return manager, nil
}

// getPartition gets a partition's segment manager without creating.
func (s *Store) getPartition(queueName string, partitionID uint32) (*SegmentManager, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.closed {
		return nil, ErrSegmentClosed
	}

	partitions, ok := s.queues[queueName]
	if !ok {
		return nil, ErrQueueNotFound
	}

	manager, ok := partitions[partitionID]
	if !ok {
		return nil, ErrPartitionNotFound
	}

	return manager, nil
}

// getConsumerManager returns the consumer manager for a queue.
func (s *Store) getConsumerManager(queueName string) (*ConsumerManager, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	cm, ok := s.consumers[queueName]
	if !ok {
		return nil, ErrQueueNotFound
	}
	return cm, nil
}

// Message Operations

// Append appends a message to a partition.
func (s *Store) Append(queueName string, partitionID uint32, value []byte, key []byte, headers map[string][]byte) (uint64, error) {
	manager, err := s.getOrCreatePartition(queueName, partitionID)
	if err != nil {
		return 0, err
	}

	return manager.AppendMessage(value, key, headers)
}

// AppendBatch appends a batch of messages to a partition.
func (s *Store) AppendBatch(queueName string, partitionID uint32, batch *Batch) (uint64, error) {
	manager, err := s.getOrCreatePartition(queueName, partitionID)
	if err != nil {
		return 0, err
	}

	return manager.Append(batch)
}

// Read reads a message at the given offset.
func (s *Store) Read(queueName string, partitionID uint32, offset uint64) (*Message, error) {
	manager, err := s.getPartition(queueName, partitionID)
	if err != nil {
		return nil, err
	}

	return manager.Read(offset)
}

// ReadBatch reads a batch at the given offset.
func (s *Store) ReadBatch(queueName string, partitionID uint32, offset uint64) (*Batch, error) {
	manager, err := s.getPartition(queueName, partitionID)
	if err != nil {
		return nil, err
	}

	return manager.ReadBatch(offset)
}

// ReadRange reads messages in a range.
func (s *Store) ReadRange(queueName string, partitionID uint32, startOffset, endOffset uint64, maxMessages int) ([]Message, error) {
	manager, err := s.getPartition(queueName, partitionID)
	if err != nil {
		return nil, err
	}

	return manager.ReadRange(startOffset, endOffset, maxMessages)
}

// Head returns the head offset for a partition.
func (s *Store) Head(queueName string, partitionID uint32) (uint64, error) {
	manager, err := s.getPartition(queueName, partitionID)
	if err != nil {
		return 0, err
	}

	return manager.Head(), nil
}

// Tail returns the tail offset for a partition.
func (s *Store) Tail(queueName string, partitionID uint32) (uint64, error) {
	manager, err := s.getPartition(queueName, partitionID)
	if err != nil {
		return 0, err
	}

	return manager.Tail(), nil
}

// Count returns the message count for a partition.
func (s *Store) Count(queueName string, partitionID uint32) (uint64, error) {
	manager, err := s.getPartition(queueName, partitionID)
	if err != nil {
		return 0, err
	}

	return manager.Count(), nil
}

// Truncate removes messages before the given offset.
func (s *Store) Truncate(queueName string, partitionID uint32, beforeOffset uint64) error {
	manager, err := s.getPartition(queueName, partitionID)
	if err != nil {
		return err
	}

	return manager.Truncate(beforeOffset)
}

// Consumer Group Operations (JetStream-style)

// Deliver marks a message as delivered to a consumer.
func (s *Store) Deliver(queueName, groupID string, partitionID uint32, offset uint64, consumerID string) error {
	cm, err := s.getConsumerManager(queueName)
	if err != nil {
		return err
	}

	state, err := cm.GetOrCreate(groupID)
	if err != nil {
		return err
	}

	return state.Deliver(partitionID, offset, consumerID)
}

// DeliverBatch marks multiple messages as delivered.
func (s *Store) DeliverBatch(queueName, groupID string, partitionID uint32, offsets []uint64, consumerID string) error {
	cm, err := s.getConsumerManager(queueName)
	if err != nil {
		return err
	}

	state, err := cm.GetOrCreate(groupID)
	if err != nil {
		return err
	}

	return state.DeliverBatch(partitionID, offsets, consumerID)
}

// Ack acknowledges a message.
func (s *Store) Ack(queueName, groupID string, partitionID uint32, offset uint64) error {
	cm, err := s.getConsumerManager(queueName)
	if err != nil {
		return err
	}

	state := cm.Get(groupID)
	if state == nil {
		return ErrGroupNotFound
	}

	return state.Ack(partitionID, offset)
}

// AckBatch acknowledges multiple messages.
func (s *Store) AckBatch(queueName, groupID string, partitionID uint32, offsets []uint64) error {
	cm, err := s.getConsumerManager(queueName)
	if err != nil {
		return err
	}

	state := cm.Get(groupID)
	if state == nil {
		return ErrGroupNotFound
	}

	return state.AckBatch(partitionID, offsets)
}

// Nack negatively acknowledges a message (will be redelivered).
func (s *Store) Nack(queueName, groupID string, offset uint64) error {
	cm, err := s.getConsumerManager(queueName)
	if err != nil {
		return err
	}

	state := cm.Get(groupID)
	if state == nil {
		return ErrGroupNotFound
	}

	return state.Nack(offset)
}

// Claim transfers a pending message to a new consumer (work stealing).
func (s *Store) Claim(queueName, groupID string, offset uint64, newConsumerID string) error {
	cm, err := s.getConsumerManager(queueName)
	if err != nil {
		return err
	}

	state := cm.Get(groupID)
	if state == nil {
		return ErrGroupNotFound
	}

	return state.Claim(offset, newConsumerID)
}

// ClaimBatch transfers multiple pending messages to a new consumer.
func (s *Store) ClaimBatch(queueName, groupID string, offsets []uint64, newConsumerID string) error {
	cm, err := s.getConsumerManager(queueName)
	if err != nil {
		return err
	}

	state := cm.Get(groupID)
	if state == nil {
		return ErrGroupNotFound
	}

	return state.ClaimBatch(offsets, newConsumerID)
}

// GetPartitionState returns consumer state for a partition.
func (s *Store) GetPartitionState(queueName, groupID string, partitionID uint32) (*PartitionState, error) {
	cm, err := s.getConsumerManager(queueName)
	if err != nil {
		return nil, err
	}

	state := cm.Get(groupID)
	if state == nil {
		return &PartitionState{}, nil
	}

	return state.GetPartitionState(partitionID), nil
}

// GetAckFloor returns the ack floor (highest contiguous acked offset) for a partition.
func (s *Store) GetAckFloor(queueName, groupID string, partitionID uint32) (uint64, error) {
	ps, err := s.GetPartitionState(queueName, groupID, partitionID)
	if err != nil {
		return 0, err
	}
	return ps.AckFloor, nil
}

// GetCursor returns the cursor (next delivery offset) for a partition.
func (s *Store) GetCursor(queueName, groupID string, partitionID uint32) (uint64, error) {
	ps, err := s.GetPartitionState(queueName, groupID, partitionID)
	if err != nil {
		return 0, err
	}
	return ps.Cursor, nil
}

// PEL Operations

// GetPending returns a pending entry.
func (s *Store) GetPending(queueName, groupID string, offset uint64) (*PendingEntry, error) {
	cm, err := s.getConsumerManager(queueName)
	if err != nil {
		return nil, err
	}

	state := cm.Get(groupID)
	if state == nil {
		return nil, ErrGroupNotFound
	}

	entry, ok := state.GetPending(offset)
	if !ok {
		return nil, ErrPELEntryNotFound
	}

	return entry, nil
}

// GetPendingByConsumer returns all pending entries for a consumer.
func (s *Store) GetPendingByConsumer(queueName, groupID, consumerID string) ([]PendingEntry, error) {
	cm, err := s.getConsumerManager(queueName)
	if err != nil {
		return nil, err
	}

	state := cm.Get(groupID)
	if state == nil {
		return nil, nil
	}

	return state.GetPendingByConsumer(consumerID), nil
}

// GetPendingByShard returns all pending entries in a shard (for distributed redelivery).
func (s *Store) GetPendingByShard(queueName, groupID string, shardID int) ([]PendingEntry, error) {
	cm, err := s.getConsumerManager(queueName)
	if err != nil {
		return nil, err
	}

	state := cm.Get(groupID)
	if state == nil {
		return nil, nil
	}

	return state.GetPendingByShard(shardID), nil
}

// PendingCount returns the number of pending entries.
func (s *Store) PendingCount(queueName, groupID string) (int, error) {
	cm, err := s.getConsumerManager(queueName)
	if err != nil {
		return 0, err
	}

	state := cm.Get(groupID)
	if state == nil {
		return 0, nil
	}

	return state.PendingCount(), nil
}

// Redelivery Operations

// GetRedeliveryCandidates returns entries eligible for redelivery from a shard.
func (s *Store) GetRedeliveryCandidates(queueName, groupID string, shardID int, timeout time.Duration, maxCount int) ([]PendingEntry, error) {
	cm, err := s.getConsumerManager(queueName)
	if err != nil {
		return nil, err
	}

	state := cm.Get(groupID)
	if state == nil {
		return nil, nil
	}

	candidates := state.GetRedeliveryCandidates(shardID, timeout, maxCount)
	return candidates.Entries, nil
}

// GetAllRedeliveryCandidates returns all entries eligible for redelivery.
func (s *Store) GetAllRedeliveryCandidates(queueName, groupID string, timeout time.Duration, maxCount int) ([]PendingEntry, error) {
	cm, err := s.getConsumerManager(queueName)
	if err != nil {
		return nil, err
	}

	state := cm.Get(groupID)
	if state == nil {
		return nil, nil
	}

	return state.GetAllRedeliveryCandidates(timeout, maxCount), nil
}

// GetRedeliveryBatches returns batches of messages ready for redelivery across all groups.
func (s *Store) GetRedeliveryBatches(queueName string, timeout time.Duration, maxPerShard int) ([]RedeliveryBatch, error) {
	cm, err := s.getConsumerManager(queueName)
	if err != nil {
		return nil, err
	}

	return cm.GetRedeliveryBatches(timeout, maxPerShard), nil
}

// GetDeadLetterCandidates returns entries that exceeded max delivery count.
func (s *Store) GetDeadLetterCandidates(queueName, groupID string, maxDeliveries int, maxCount int) ([]PendingEntry, error) {
	cm, err := s.getConsumerManager(queueName)
	if err != nil {
		return nil, err
	}

	state := cm.Get(groupID)
	if state == nil {
		return nil, nil
	}

	return state.GetDeadLetterCandidates(maxDeliveries, maxCount), nil
}

// Consumer Group Management

// CreateConsumerGroup creates a new consumer group.
func (s *Store) CreateConsumerGroup(queueName, groupID string) error {
	cm, err := s.getConsumerManager(queueName)
	if err != nil {
		return err
	}

	_, err = cm.GetOrCreate(groupID)
	return err
}

// DeleteConsumerGroup deletes a consumer group.
func (s *Store) DeleteConsumerGroup(queueName, groupID string) error {
	cm, err := s.getConsumerManager(queueName)
	if err != nil {
		return err
	}

	return cm.Delete(groupID)
}

// ConsumerGroupExists checks if a consumer group exists.
func (s *Store) ConsumerGroupExists(queueName, groupID string) bool {
	cm, err := s.getConsumerManager(queueName)
	if err != nil {
		return false
	}

	return cm.Exists(groupID)
}

// ListConsumerGroups returns all consumer groups for a queue.
func (s *Store) ListConsumerGroups(queueName string) []string {
	cm, err := s.getConsumerManager(queueName)
	if err != nil {
		return nil
	}

	return cm.List()
}

// GetConsumerState returns the consumer state for direct access.
func (s *Store) GetConsumerState(queueName, groupID string) (*ConsumerState, error) {
	cm, err := s.getConsumerManager(queueName)
	if err != nil {
		return nil, err
	}

	state := cm.Get(groupID)
	if state == nil {
		return nil, ErrGroupNotFound
	}

	return state, nil
}

// GetConsumerManager returns the consumer manager for a queue.
func (s *Store) GetConsumerManager(queueName string) (*ConsumerManager, error) {
	return s.getConsumerManager(queueName)
}

// NumPELShards returns the number of PEL shards for a consumer group.
func (s *Store) NumPELShards(queueName, groupID string) (int, error) {
	state, err := s.GetConsumerState(queueName, groupID)
	if err != nil {
		return 0, err
	}
	return state.NumShards(), nil
}

// Queue Management

// CreateQueue creates a new queue with the specified number of partitions.
func (s *Store) CreateQueue(queueName string, partitionCount int) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return ErrSegmentClosed
	}

	if _, ok := s.queues[queueName]; ok {
		return ErrAlreadyExists
	}

	partitions := make(map[uint32]*SegmentManager)
	for i := range partitionCount {
		partDir := s.partitionDir(queueName, uint32(i))
		manager, err := NewSegmentManager(partDir, s.config.ManagerConfig)
		if err != nil {
			// Cleanup on failure
			for _, m := range partitions {
				m.Close()
			}
			return fmt.Errorf("failed to create partition %d: %w", i, err)
		}
		partitions[uint32(i)] = manager
	}

	s.queues[queueName] = partitions

	// Create consumer manager
	queueDir := s.queueDir(queueName)
	cm, err := NewConsumerManager(queueDir, s.config.ConsumerStateConfig)
	if err != nil {
		// Cleanup on failure
		for _, m := range partitions {
			m.Close()
		}
		return fmt.Errorf("failed to create consumer manager: %w", err)
	}
	s.consumers[queueName] = cm

	return nil
}

// DeleteQueue deletes a queue and all its data.
func (s *Store) DeleteQueue(queueName string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return ErrSegmentClosed
	}

	partitions, ok := s.queues[queueName]
	if !ok {
		return ErrQueueNotFound
	}

	// Close all partitions
	for _, manager := range partitions {
		manager.Close()
	}

	// Close consumer manager
	if cm, ok := s.consumers[queueName]; ok {
		cm.Close()
	}

	delete(s.queues, queueName)
	delete(s.consumers, queueName)

	// Remove directory
	return os.RemoveAll(s.queueDir(queueName))
}

// ListQueues returns all queue names.
func (s *Store) ListQueues() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	names := make([]string, 0, len(s.queues))
	for name := range s.queues {
		names = append(names, name)
	}
	return names
}

// ListPartitions returns all partition IDs for a queue.
func (s *Store) ListPartitions(queueName string) ([]uint32, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	partitions, ok := s.queues[queueName]
	if !ok {
		return nil, ErrQueueNotFound
	}

	ids := make([]uint32, 0, len(partitions))
	for id := range partitions {
		ids = append(ids, id)
	}
	return ids, nil
}

// Maintenance

// ApplyRetention applies retention policies to all queues.
func (s *Store) ApplyRetention() error {
	s.mu.RLock()
	defer s.mu.RUnlock()

	for _, partitions := range s.queues {
		for _, manager := range partitions {
			if err := manager.ApplyRetention(); err != nil {
				return err
			}
		}
	}

	return nil
}

// Sync flushes all pending writes to disk.
func (s *Store) Sync() error {
	s.mu.RLock()
	defer s.mu.RUnlock()

	for _, partitions := range s.queues {
		for _, manager := range partitions {
			if err := manager.Sync(); err != nil {
				return err
			}
		}
	}

	for _, cm := range s.consumers {
		if err := cm.Sync(); err != nil {
			return err
		}
	}

	return nil
}

// Compact compacts all consumer state.
func (s *Store) Compact() error {
	s.mu.RLock()
	defer s.mu.RUnlock()

	for _, cm := range s.consumers {
		if err := cm.Compact(); err != nil {
			return err
		}
	}

	return nil
}

// Recover attempts to recover all queues.
func (s *Store) Recover() (*RecoveryResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	result := &RecoveryResult{}

	for queueName, partitions := range s.queues {
		for partitionID := range partitions {
			partDir := s.partitionDir(queueName, partitionID)
			partResult, err := RecoverPartition(partDir)
			if err != nil {
				result.Errors = append(result.Errors, fmt.Errorf("queue %s partition %d: %w", queueName, partitionID, err))
				continue
			}

			result.SegmentsRecovered += partResult.SegmentsRecovered
			result.SegmentsTruncated += partResult.SegmentsTruncated
			result.IndexesRebuilt += partResult.IndexesRebuilt
			result.MessagesLost += partResult.MessagesLost
			result.BytesTruncated += partResult.BytesTruncated
			result.Errors = append(result.Errors, partResult.Errors...)
		}
	}

	return result, nil
}

// Stats returns storage statistics.
func (s *Store) Stats() StoreStats {
	s.mu.RLock()
	defer s.mu.RUnlock()

	stats := StoreStats{
		QueueCount:    len(s.queues),
		QueueStats:    make(map[string]QueueStats),
		ConsumerStats: make(map[string]ConsumerManagerStats),
	}

	for queueName, partitions := range s.queues {
		qStats := QueueStats{
			PartitionCount: len(partitions),
			PartitionStats: make(map[uint32]PartitionStats),
		}

		for partitionID, manager := range partitions {
			pStats := PartitionStats{
				Head:         manager.Head(),
				Tail:         manager.Tail(),
				MessageCount: manager.Count(),
				Size:         manager.Size(),
				SegmentCount: manager.SegmentCount(),
			}
			qStats.PartitionStats[partitionID] = pStats
			qStats.TotalMessages += pStats.MessageCount
			qStats.TotalSize += pStats.Size
		}

		stats.QueueStats[queueName] = qStats
		stats.TotalMessages += qStats.TotalMessages
		stats.TotalSize += qStats.TotalSize

		// Add consumer stats
		if cm, ok := s.consumers[queueName]; ok {
			stats.ConsumerStats[queueName] = cm.Stats()
		}
	}

	return stats
}

// Close closes the store.
func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return nil
	}

	s.closed = true

	var lastErr error

	// Close all partition managers
	for _, partitions := range s.queues {
		for _, manager := range partitions {
			if err := manager.Close(); err != nil {
				lastErr = err
			}
		}
	}

	// Close all consumer managers
	for _, cm := range s.consumers {
		if err := cm.Close(); err != nil {
			lastErr = err
		}
	}

	return lastErr
}

// StoreStats contains store-wide statistics.
type StoreStats struct {
	QueueCount    int
	TotalMessages uint64
	TotalSize     int64
	QueueStats    map[string]QueueStats
	ConsumerStats map[string]ConsumerManagerStats
}

// QueueStats contains queue statistics.
type QueueStats struct {
	PartitionCount int
	TotalMessages  uint64
	TotalSize      int64
	PartitionStats map[uint32]PartitionStats
}

// PartitionStats contains partition statistics.
type PartitionStats struct {
	Head         uint64
	Tail         uint64
	MessageCount uint64
	Size         int64
	SegmentCount int
}

// Legacy compatibility methods (map to new JetStream-style API)

// SetCursor sets the cursor for a consumer group (creates delivery if needed).
func (s *Store) SetCursor(queueName, groupID string, partitionID uint32, cursor uint64) error {
	// In JetStream model, cursor is advanced by Deliver operations
	// This is a legacy compatibility method
	return nil
}

// CommitOffset commits an offset for a consumer group.
// In JetStream model, this maps to Ack.
func (s *Store) CommitOffset(queueName, groupID string, partitionID uint32, offset uint64) error {
	return s.Ack(queueName, groupID, partitionID, offset)
}

// GetCommittedOffset returns the committed offset for a consumer group.
// In JetStream model, this is the AckFloor.
func (s *Store) GetCommittedOffset(queueName, groupID string, partitionID uint32) (uint64, error) {
	return s.GetAckFloor(queueName, groupID, partitionID)
}

// AddPending adds a pending entry (legacy - use Deliver instead).
func (s *Store) AddPending(queueName, groupID string, entry PELEntry) error {
	return s.Deliver(queueName, groupID, entry.PartitionID, entry.Offset, entry.ConsumerID)
}

// AckPending acknowledges a pending entry (legacy - use Ack instead).
func (s *Store) AckPending(queueName, groupID string, offset uint64) error {
	// Need partition ID - get from PEL
	entry, err := s.GetPending(queueName, groupID, offset)
	if err != nil {
		return err
	}
	return s.Ack(queueName, groupID, entry.PartitionID, offset)
}

// ClaimPending claims a pending entry (legacy - use Claim instead).
func (s *Store) ClaimPending(queueName, groupID string, offset uint64, newConsumerID string) error {
	return s.Claim(queueName, groupID, offset, newConsumerID)
}

// GetStealableEntries returns entries that can be stolen (legacy - use GetRedeliveryCandidates).
func (s *Store) GetStealableEntries(queueName, groupID string, timeout time.Duration, excludeConsumer string) ([]PendingEntry, error) {
	entries, err := s.GetAllRedeliveryCandidates(queueName, groupID, timeout, 1000)
	if err != nil {
		return nil, err
	}

	// Filter out excluded consumer
	result := make([]PendingEntry, 0, len(entries))
	for _, e := range entries {
		if e.ConsumerID != excludeConsumer {
			result = append(result, e)
		}
	}
	return result, nil
}

// GetAllPending returns all pending entries (legacy format).
func (s *Store) GetAllPending(queueName, groupID string) (map[string][]PELEntry, error) {
	state, err := s.GetConsumerState(queueName, groupID)
	if err != nil {
		return nil, err
	}

	// Convert PendingEntry to PELEntry format
	result := make(map[string][]PELEntry)
	for shardID := range state.NumShards() {
		entries := state.GetPendingByShard(shardID)
		for _, e := range entries {
			pelEntry := PELEntry{
				Offset:        e.Offset,
				PartitionID:   e.PartitionID,
				ConsumerID:    e.ConsumerID,
				ClaimedAt:     e.DeliveredAt,
				DeliveryCount: e.DeliveryCount,
			}
			result[e.ConsumerID] = append(result[e.ConsumerID], pelEntry)
		}
	}

	return result, nil
}

// GetAllCursors returns cursor state for all partitions (legacy format).
func (s *Store) GetAllCursors(queueName, groupID string) (map[uint32]PartitionCursor, error) {
	state, err := s.GetConsumerState(queueName, groupID)
	if err != nil {
		return nil, err
	}

	result := make(map[uint32]PartitionCursor)

	// Get all partitions for this queue
	partitions, err := s.ListPartitions(queueName)
	if err != nil {
		return nil, err
	}

	for _, partID := range partitions {
		ps := state.GetPartitionState(partID)
		result[partID] = PartitionCursor{
			Cursor:    ps.Cursor,
			Committed: ps.AckFloor,
			UpdatedAt: ps.UpdatedAt,
		}
	}

	return result, nil
}
