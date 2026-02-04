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
// It manages multiple queues, each with a single log (no partitions).
// Consumer state uses JetStream-style semantics with sharded PEL.
type Store struct {
	mu sync.RWMutex

	baseDir string
	config  StoreConfig

	// Queue -> SegmentManager (single log per queue, no partitions)
	queues map[string]*SegmentManager

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
		queues:    make(map[string]*SegmentManager),
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

// loadQueue loads a queue's segment manager.
func (s *Store) loadQueue(queueName string) error {
	queueDir := filepath.Join(s.baseDir, "queues", queueName)
	segmentsDir := filepath.Join(queueDir, "segments")

	// Check if segments directory exists
	if _, err := os.Stat(segmentsDir); os.IsNotExist(err) {
		return nil
	}

	manager, err := NewSegmentManager(segmentsDir, s.config.ManagerConfig)
	if err != nil {
		return fmt.Errorf("failed to load queue segments: %w", err)
	}

	s.queues[queueName] = manager

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

// segmentsDir returns the segments directory for a queue.
func (s *Store) segmentsDir(queueName string) string {
	return filepath.Join(s.queueDir(queueName), "segments")
}

// getOrCreateQueue gets or creates a queue's segment manager.
func (s *Store) getOrCreateQueue(queueName string) (*SegmentManager, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return nil, ErrSegmentClosed
	}

	manager, ok := s.queues[queueName]
	if !ok {
		if !s.config.AutoCreate {
			return nil, ErrQueueNotFound
		}

		// Create consumer manager for new queue
		queueDir := s.queueDir(queueName)
		cm, err := NewConsumerManager(queueDir, s.config.ConsumerStateConfig)
		if err != nil {
			return nil, fmt.Errorf("failed to create consumer manager: %w", err)
		}
		s.consumers[queueName] = cm

		segDir := s.segmentsDir(queueName)
		manager, err = NewSegmentManager(segDir, s.config.ManagerConfig)
		if err != nil {
			return nil, fmt.Errorf("failed to create queue: %w", err)
		}
		s.queues[queueName] = manager
	}

	return manager, nil
}

// getQueue gets a queue's segment manager without creating.
func (s *Store) getQueue(queueName string) (*SegmentManager, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.closed {
		return nil, ErrSegmentClosed
	}

	manager, ok := s.queues[queueName]
	if !ok {
		return nil, ErrQueueNotFound
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

// Append appends a message to a queue.
func (s *Store) Append(queueName string, value []byte, key []byte, headers map[string][]byte) (uint64, error) {
	manager, err := s.getOrCreateQueue(queueName)
	if err != nil {
		return 0, err
	}

	return manager.AppendMessage(value, key, headers)
}

// AppendBatch appends a batch of messages to a queue.
func (s *Store) AppendBatch(queueName string, batch *Batch) (uint64, error) {
	manager, err := s.getOrCreateQueue(queueName)
	if err != nil {
		return 0, err
	}

	return manager.Append(batch)
}

// Read reads a message at the given offset.
func (s *Store) Read(queueName string, offset uint64) (*Message, error) {
	manager, err := s.getQueue(queueName)
	if err != nil {
		return nil, err
	}

	return manager.Read(offset)
}

// ReadBatch reads a batch at the given offset.
func (s *Store) ReadBatch(queueName string, offset uint64) (*Batch, error) {
	manager, err := s.getQueue(queueName)
	if err != nil {
		return nil, err
	}

	return manager.ReadBatch(offset)
}

// ReadRange reads messages in a range.
func (s *Store) ReadRange(queueName string, startOffset, endOffset uint64, maxMessages int) ([]Message, error) {
	manager, err := s.getQueue(queueName)
	if err != nil {
		return nil, err
	}

	return manager.ReadRange(startOffset, endOffset, maxMessages)
}

// LookupByTime finds the offset for the given timestamp.
func (s *Store) LookupByTime(queueName string, ts time.Time) (uint64, error) {
	manager, err := s.getQueue(queueName)
	if err != nil {
		return 0, err
	}

	return manager.LookupByTime(ts)
}

// RetentionOffsetBySize returns the offset to keep when enforcing size retention.
func (s *Store) RetentionOffsetBySize(queueName string, retentionBytes int64) (uint64, error) {
	manager, err := s.getQueue(queueName)
	if err != nil {
		return 0, err
	}

	return manager.RetentionOffsetBySize(retentionBytes)
}

// Head returns the head offset for a queue.
func (s *Store) Head(queueName string) (uint64, error) {
	manager, err := s.getQueue(queueName)
	if err != nil {
		return 0, err
	}

	return manager.Head(), nil
}

// Tail returns the tail offset for a queue.
func (s *Store) Tail(queueName string) (uint64, error) {
	manager, err := s.getQueue(queueName)
	if err != nil {
		return 0, err
	}

	return manager.Tail(), nil
}

// Count returns the message count for a queue.
func (s *Store) Count(queueName string) (uint64, error) {
	manager, err := s.getQueue(queueName)
	if err != nil {
		return 0, err
	}

	return manager.Count(), nil
}

// Truncate removes messages before the given offset.
func (s *Store) Truncate(queueName string, beforeOffset uint64) error {
	manager, err := s.getQueue(queueName)
	if err != nil {
		return err
	}

	return manager.Truncate(beforeOffset)
}

// Consumer Group Operations

// Deliver marks a message as delivered to a consumer.
func (s *Store) Deliver(queueName, groupID string, offset uint64, consumerID string) error {
	cm, err := s.getConsumerManager(queueName)
	if err != nil {
		return err
	}

	state, err := cm.GetOrCreate(groupID)
	if err != nil {
		return err
	}

	return state.Deliver(offset, consumerID)
}

// DeliverBatch marks multiple messages as delivered.
func (s *Store) DeliverBatch(queueName, groupID string, offsets []uint64, consumerID string) error {
	cm, err := s.getConsumerManager(queueName)
	if err != nil {
		return err
	}

	state, err := cm.GetOrCreate(groupID)
	if err != nil {
		return err
	}

	return state.DeliverBatch(offsets, consumerID)
}

// Ack acknowledges a message.
func (s *Store) Ack(queueName, groupID string, offset uint64) error {
	cm, err := s.getConsumerManager(queueName)
	if err != nil {
		return err
	}

	state := cm.Get(groupID)
	if state == nil {
		return ErrGroupNotFound
	}

	return state.Ack(offset)
}

// AckBatch acknowledges multiple messages.
func (s *Store) AckBatch(queueName, groupID string, offsets []uint64) error {
	cm, err := s.getConsumerManager(queueName)
	if err != nil {
		return err
	}

	state := cm.Get(groupID)
	if state == nil {
		return ErrGroupNotFound
	}

	return state.AckBatch(offsets)
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

// GetGroupState returns consumer group state.
func (s *Store) GetGroupState(queueName, groupID string) (*GroupState, error) {
	cm, err := s.getConsumerManager(queueName)
	if err != nil {
		return nil, err
	}

	state := cm.Get(groupID)
	if state == nil {
		return &GroupState{}, nil
	}

	return state.GetGroupState(), nil
}

// GetAckFloor returns the ack floor (highest contiguous acked offset).
func (s *Store) GetAckFloor(queueName, groupID string) (uint64, error) {
	gs, err := s.GetGroupState(queueName, groupID)
	if err != nil {
		return 0, err
	}
	return gs.AckFloor, nil
}

// GetCursor returns the cursor (next delivery offset).
func (s *Store) GetCursor(queueName, groupID string) (uint64, error) {
	gs, err := s.GetGroupState(queueName, groupID)
	if err != nil {
		return 0, err
	}
	return gs.Cursor, nil
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

// CreateQueue creates a new queue.
func (s *Store) CreateQueue(queueName string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return ErrSegmentClosed
	}

	if _, ok := s.queues[queueName]; ok {
		return ErrAlreadyExists
	}

	segDir := s.segmentsDir(queueName)
	manager, err := NewSegmentManager(segDir, s.config.ManagerConfig)
	if err != nil {
		return fmt.Errorf("failed to create queue: %w", err)
	}

	s.queues[queueName] = manager

	// Create consumer manager
	queueDir := s.queueDir(queueName)
	cm, err := NewConsumerManager(queueDir, s.config.ConsumerStateConfig)
	if err != nil {
		manager.Close()
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

	manager, ok := s.queues[queueName]
	if !ok {
		return ErrQueueNotFound
	}

	// Close queue
	manager.Close()

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

// QueueExists checks if a queue exists.
func (s *Store) QueueExists(queueName string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	_, ok := s.queues[queueName]
	return ok
}

// Maintenance

// ApplyRetention applies retention policies to all queues.
func (s *Store) ApplyRetention() error {
	s.mu.RLock()
	defer s.mu.RUnlock()

	for _, manager := range s.queues {
		if err := manager.ApplyRetention(); err != nil {
			return err
		}
	}

	return nil
}

// Sync flushes all pending writes to disk.
func (s *Store) Sync() error {
	s.mu.RLock()
	defer s.mu.RUnlock()

	for _, manager := range s.queues {
		if err := manager.Sync(); err != nil {
			return err
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

	for queueName, manager := range s.queues {
		segDir := s.segmentsDir(queueName)
		queueResult, err := RecoverSegments(segDir)
		if err != nil {
			result.Errors = append(result.Errors, fmt.Errorf("queue %s: %w", queueName, err))
			continue
		}

		// Reload the manager after recovery
		newManager, err := NewSegmentManager(segDir, s.config.ManagerConfig)
		if err != nil {
			result.Errors = append(result.Errors, fmt.Errorf("queue %s reload: %w", queueName, err))
			continue
		}
		manager.Close()
		s.queues[queueName] = newManager

		result.SegmentsRecovered += queueResult.SegmentsRecovered
		result.SegmentsTruncated += queueResult.SegmentsTruncated
		result.IndexesRebuilt += queueResult.IndexesRebuilt
		result.MessagesLost += queueResult.MessagesLost
		result.BytesTruncated += queueResult.BytesTruncated
		result.Errors = append(result.Errors, queueResult.Errors...)
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

	for queueName, manager := range s.queues {
		qStats := QueueStats{
			Head:         manager.Head(),
			Tail:         manager.Tail(),
			MessageCount: manager.Count(),
			Size:         manager.Size(),
			SegmentCount: manager.SegmentCount(),
		}

		stats.QueueStats[queueName] = qStats
		stats.TotalMessages += qStats.MessageCount
		stats.TotalSize += qStats.Size

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

	// Close all queue managers
	for _, manager := range s.queues {
		if err := manager.Close(); err != nil {
			lastErr = err
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
	Head         uint64
	Tail         uint64
	MessageCount uint64
	Size         int64
	SegmentCount int
}

// Legacy compatibility methods (map to new JetStream-style API)

// SetCursor sets the cursor for a consumer group (creates delivery if needed).
func (s *Store) SetCursor(queueName, groupID string, cursor uint64) error {
	// In JetStream model, cursor is advanced by Deliver operations
	// This is a legacy compatibility method
	return nil
}

// CommitOffset commits an offset for a consumer group.
// In JetStream model, this maps to Ack.
func (s *Store) CommitOffset(queueName, groupID string, offset uint64) error {
	return s.Ack(queueName, groupID, offset)
}

// GetCommittedOffset returns the committed offset for a consumer group.
// In JetStream model, this is the AckFloor.
func (s *Store) GetCommittedOffset(queueName, groupID string) (uint64, error) {
	return s.GetAckFloor(queueName, groupID)
}

// AddPending adds a pending entry (legacy - use Deliver instead).
func (s *Store) AddPending(queueName, groupID string, entry PELEntry) error {
	return s.Deliver(queueName, groupID, entry.Offset, entry.ConsumerID)
}

// AckPending acknowledges a pending entry (legacy - use Ack instead).
func (s *Store) AckPending(queueName, groupID string, offset uint64) error {
	return s.Ack(queueName, groupID, offset)
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
				ConsumerID:    e.ConsumerID,
				ClaimedAt:     e.DeliveredAt,
				DeliveryCount: e.DeliveryCount,
			}
			result[e.ConsumerID] = append(result[e.ConsumerID], pelEntry)
		}
	}

	return result, nil
}

// GetCursorState returns cursor state for the queue (legacy format).
func (s *Store) GetCursorState(queueName, groupID string) (*Cursor, error) {
	gs, err := s.GetGroupState(queueName, groupID)
	if err != nil {
		return nil, err
	}

	return &Cursor{
		Cursor:    gs.Cursor,
		Committed: gs.AckFloor,
		UpdatedAt: gs.UpdatedAt,
	}, nil
}
