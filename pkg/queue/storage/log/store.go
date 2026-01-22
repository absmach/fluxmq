// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package log

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// Store is the main interface for the AOL storage system.
// It manages multiple queues, each with multiple partitions.
type Store struct {
	mu sync.RWMutex

	baseDir string
	config  StoreConfig

	// Queue -> Partition -> SegmentManager
	queues map[string]map[uint32]*SegmentManager

	// Consumer group state per queue
	cursors map[string]*ConsumerGroupCursors
	pels    map[string]*ConsumerGroupPELs

	closed bool
}

// StoreConfig holds store configuration.
type StoreConfig struct {
	ManagerConfig
	AutoCreate bool // Automatically create queues/partitions on first access
}

// DefaultStoreConfig returns default store configuration.
func DefaultStoreConfig() StoreConfig {
	return StoreConfig{
		ManagerConfig: DefaultManagerConfig(),
		AutoCreate:    true,
	}
}

// NewStore creates a new AOL store.
func NewStore(baseDir string, config StoreConfig) (*Store, error) {
	if err := os.MkdirAll(baseDir, 0o755); err != nil {
		return nil, fmt.Errorf("failed to create store directory: %w", err)
	}

	s := &Store{
		baseDir: baseDir,
		config:  config,
		queues:  make(map[string]map[uint32]*SegmentManager),
		cursors: make(map[string]*ConsumerGroupCursors),
		pels:    make(map[string]*ConsumerGroupPELs),
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

	// Load consumer state
	consumersDir := filepath.Join(queueDir, "consumers")
	s.cursors[queueName], _ = NewConsumerGroupCursors(filepath.Join(consumersDir, "cursors"))
	s.pels[queueName], _ = NewConsumerGroupPELs(filepath.Join(consumersDir, "pels"))

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

		// Create consumer state directories
		consumersDir := filepath.Join(s.queueDir(queueName), "consumers")
		s.cursors[queueName], _ = NewConsumerGroupCursors(filepath.Join(consumersDir, "cursors"))
		s.pels[queueName], _ = NewConsumerGroupPELs(filepath.Join(consumersDir, "pels"))
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

// Consumer Group Operations

// GetCursor gets the cursor for a consumer group in a partition.
func (s *Store) GetCursor(queueName, groupID string, partitionID uint32) (*PartitionCursor, error) {
	s.mu.RLock()
	cgc, ok := s.cursors[queueName]
	s.mu.RUnlock()

	if !ok {
		return nil, ErrQueueNotFound
	}

	cs, err := cgc.GetOrCreate(groupID)
	if err != nil {
		return nil, err
	}

	return cs.GetCursor(partitionID), nil
}

// SetCursor sets the cursor for a consumer group in a partition.
func (s *Store) SetCursor(queueName, groupID string, partitionID uint32, cursor uint64) error {
	s.mu.RLock()
	cgc, ok := s.cursors[queueName]
	s.mu.RUnlock()

	if !ok {
		return ErrQueueNotFound
	}

	cs, err := cgc.GetOrCreate(groupID)
	if err != nil {
		return err
	}

	cs.SetCursor(partitionID, cursor)
	return nil
}

// CommitOffset commits an offset for a consumer group.
func (s *Store) CommitOffset(queueName, groupID string, partitionID uint32, offset uint64) error {
	s.mu.RLock()
	cgc, ok := s.cursors[queueName]
	s.mu.RUnlock()

	if !ok {
		return ErrQueueNotFound
	}

	cs, err := cgc.GetOrCreate(groupID)
	if err != nil {
		return err
	}

	cs.Commit(partitionID, offset)
	return nil
}

// GetCommittedOffset returns the committed offset for a consumer group.
func (s *Store) GetCommittedOffset(queueName, groupID string, partitionID uint32) (uint64, error) {
	s.mu.RLock()
	cgc, ok := s.cursors[queueName]
	s.mu.RUnlock()

	if !ok {
		return 0, ErrQueueNotFound
	}

	cs, err := cgc.GetOrCreate(groupID)
	if err != nil {
		return 0, err
	}

	return cs.GetCommitted(partitionID), nil
}

// PEL Operations

// AddPending adds a pending entry to the PEL.
func (s *Store) AddPending(queueName, groupID string, entry PELEntry) error {
	s.mu.RLock()
	cgp, ok := s.pels[queueName]
	s.mu.RUnlock()

	if !ok {
		return ErrQueueNotFound
	}

	pel, err := cgp.GetOrCreate(groupID)
	if err != nil {
		return err
	}

	return pel.Add(entry)
}

// AckPending acknowledges a pending entry.
func (s *Store) AckPending(queueName, groupID string, offset uint64) error {
	s.mu.RLock()
	cgp, ok := s.pels[queueName]
	s.mu.RUnlock()

	if !ok {
		return ErrQueueNotFound
	}

	pel, err := cgp.GetOrCreate(groupID)
	if err != nil {
		return err
	}

	return pel.Ack(offset)
}

// ClaimPending claims a pending entry for another consumer.
func (s *Store) ClaimPending(queueName, groupID string, offset uint64, newConsumerID string) error {
	s.mu.RLock()
	cgp, ok := s.pels[queueName]
	s.mu.RUnlock()

	if !ok {
		return ErrQueueNotFound
	}

	pel, err := cgp.GetOrCreate(groupID)
	if err != nil {
		return err
	}

	return pel.Claim(offset, newConsumerID)
}

// GetPending returns a pending entry.
func (s *Store) GetPending(queueName, groupID string, offset uint64) (*PELEntry, error) {
	s.mu.RLock()
	cgp, ok := s.pels[queueName]
	s.mu.RUnlock()

	if !ok {
		return nil, ErrQueueNotFound
	}

	pel := cgp.Get(groupID)
	if pel == nil {
		return nil, ErrGroupNotFound
	}

	entry, ok := pel.Get(offset)
	if !ok {
		return nil, ErrPELEntryNotFound
	}

	return entry, nil
}

// GetPendingByConsumer returns all pending entries for a consumer.
func (s *Store) GetPendingByConsumer(queueName, groupID, consumerID string) ([]PELEntry, error) {
	s.mu.RLock()
	cgp, ok := s.pels[queueName]
	s.mu.RUnlock()

	if !ok {
		return nil, ErrQueueNotFound
	}

	pel := cgp.Get(groupID)
	if pel == nil {
		return nil, ErrGroupNotFound
	}

	return pel.GetByConsumer(consumerID), nil
}

// GetStealableEntries returns entries that can be stolen.
func (s *Store) GetStealableEntries(queueName, groupID string, timeout time.Duration, excludeConsumer string) ([]PELEntry, error) {
	s.mu.RLock()
	cgp, ok := s.pels[queueName]
	s.mu.RUnlock()

	if !ok {
		return nil, ErrQueueNotFound
	}

	pel := cgp.Get(groupID)
	if pel == nil {
		return nil, ErrGroupNotFound
	}

	return pel.GetStealable(timeout, excludeConsumer), nil
}

// PendingCount returns the number of pending entries.
func (s *Store) PendingCount(queueName, groupID string) (int, error) {
	s.mu.RLock()
	cgp, ok := s.pels[queueName]
	s.mu.RUnlock()

	if !ok {
		return 0, ErrQueueNotFound
	}

	pel := cgp.Get(groupID)
	if pel == nil {
		return 0, nil
	}

	return pel.Count(), nil
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
	for i := 0; i < partitionCount; i++ {
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

	// Create consumer state directories
	consumersDir := filepath.Join(s.queueDir(queueName), "consumers")
	s.cursors[queueName], _ = NewConsumerGroupCursors(filepath.Join(consumersDir, "cursors"))
	s.pels[queueName], _ = NewConsumerGroupPELs(filepath.Join(consumersDir, "pels"))

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

	// Close consumer state
	if cgc, ok := s.cursors[queueName]; ok {
		cgc.Close()
	}
	if cgp, ok := s.pels[queueName]; ok {
		cgp.Close()
	}

	delete(s.queues, queueName)
	delete(s.cursors, queueName)
	delete(s.pels, queueName)

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

	for _, cgc := range s.cursors {
		if err := cgc.SaveAll(); err != nil {
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
		QueueCount: len(s.queues),
		QueueStats: make(map[string]QueueStats),
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

	// Close consumer state
	for _, cgc := range s.cursors {
		if err := cgc.Close(); err != nil {
			lastErr = err
		}
	}

	for _, cgp := range s.pels {
		if err := cgp.Close(); err != nil {
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
