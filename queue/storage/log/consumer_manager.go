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

// ConsumerManager manages consumer groups for a queue.
// It provides JetStream-style consumer semantics with sharded PEL.
type ConsumerManager struct {
	mu sync.RWMutex

	dir    string
	config ConsumerStateConfig
	groups map[string]*ConsumerState

	// Redelivery
	redeliveryTicker *time.Ticker
	closeCh          chan struct{}
	closed           bool
}

// NewConsumerManager creates a new consumer manager for a queue.
func NewConsumerManager(baseDir string, config ConsumerStateConfig) (*ConsumerManager, error) {
	dir := filepath.Join(baseDir, "consumers")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("failed to create consumers directory: %w", err)
	}

	cm := &ConsumerManager{
		dir:     dir,
		config:  config,
		groups:  make(map[string]*ConsumerState),
		closeCh: make(chan struct{}),
	}

	// Load existing consumer groups
	if err := cm.loadGroups(); err != nil {
		return nil, fmt.Errorf("failed to load consumer groups: %w", err)
	}

	return cm, nil
}

// loadGroups discovers and loads existing consumer groups.
func (cm *ConsumerManager) loadGroups() error {
	entries, err := os.ReadDir(cm.dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}

		groupID := entry.Name()
		state, err := NewConsumerState(cm.dir, groupID, cm.config)
		if err != nil {
			return fmt.Errorf("failed to load consumer group %s: %w", groupID, err)
		}
		cm.groups[groupID] = state
	}

	return nil
}

// GetOrCreate returns an existing consumer group or creates a new one.
func (cm *ConsumerManager) GetOrCreate(groupID string) (*ConsumerState, error) {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	if cm.closed {
		return nil, ErrSegmentClosed
	}

	if state, ok := cm.groups[groupID]; ok {
		return state, nil
	}

	state, err := NewConsumerState(cm.dir, groupID, cm.config)
	if err != nil {
		return nil, err
	}

	cm.groups[groupID] = state
	return state, nil
}

// Get returns a consumer group if it exists.
func (cm *ConsumerManager) Get(groupID string) *ConsumerState {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	return cm.groups[groupID]
}

// Delete removes a consumer group.
func (cm *ConsumerManager) Delete(groupID string) error {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	state, ok := cm.groups[groupID]
	if !ok {
		return nil
	}

	state.Close()
	delete(cm.groups, groupID)

	// Remove directory
	groupDir := filepath.Join(cm.dir, groupID)
	return os.RemoveAll(groupDir)
}

// List returns all consumer group IDs.
func (cm *ConsumerManager) List() []string {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	groups := make([]string, 0, len(cm.groups))
	for id := range cm.groups {
		groups = append(groups, id)
	}
	return groups
}

// Exists checks if a consumer group exists.
func (cm *ConsumerManager) Exists(groupID string) bool {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	_, ok := cm.groups[groupID]
	return ok
}

// Sync flushes all consumer groups.
func (cm *ConsumerManager) Sync() error {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	var lastErr error
	for _, state := range cm.groups {
		if err := state.Sync(); err != nil {
			lastErr = err
		}
	}
	return lastErr
}

// Compact compacts all consumer groups.
func (cm *ConsumerManager) Compact() error {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	var lastErr error
	for _, state := range cm.groups {
		if err := state.Compact(); err != nil {
			lastErr = err
		}
	}
	return lastErr
}

// Close closes all consumer groups.
func (cm *ConsumerManager) Close() error {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	if cm.closed {
		return nil
	}
	cm.closed = true

	close(cm.closeCh)

	if cm.redeliveryTicker != nil {
		cm.redeliveryTicker.Stop()
	}

	var lastErr error
	for _, state := range cm.groups {
		if err := state.Close(); err != nil {
			lastErr = err
		}
	}
	return lastErr
}

// Stats returns statistics for all consumer groups.
func (cm *ConsumerManager) Stats() ConsumerManagerStats {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	stats := ConsumerManagerStats{
		GroupCount:  len(cm.groups),
		GroupStats:  make(map[string]ConsumerStateStats),
	}

	for groupID, state := range cm.groups {
		groupStats := state.Stats()
		stats.GroupStats[groupID] = groupStats
		stats.TotalPending += groupStats.TotalPending
	}

	return stats
}

// ConsumerManagerStats contains statistics about the consumer manager.
type ConsumerManagerStats struct {
	GroupCount   int
	TotalPending int
	GroupStats   map[string]ConsumerStateStats
}

// Batched Redelivery Support

// RedeliveryBatch represents a batch of messages to redeliver.
type RedeliveryBatch struct {
	GroupID   string
	ShardID   int
	Entries   []PendingEntry
	Timestamp time.Time
}

// GetRedeliveryBatches returns batches of messages ready for redelivery.
// This is designed for distributed redelivery - each shard can be processed independently.
func (cm *ConsumerManager) GetRedeliveryBatches(timeout time.Duration, maxPerShard int) []RedeliveryBatch {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	var batches []RedeliveryBatch
	now := time.Now()

	for groupID, state := range cm.groups {
		numShards := state.NumShards()
		for shardID := range numShards {
			candidates := state.GetRedeliveryCandidates(shardID, timeout, maxPerShard)
			if len(candidates.Entries) > 0 {
				batches = append(batches, RedeliveryBatch{
					GroupID:   groupID,
					ShardID:   shardID,
					Entries:   candidates.Entries,
					Timestamp: now,
				})
			}
		}
	}

	return batches
}

// GetRedeliveryBatchForShard returns a redelivery batch for a specific shard.
// This allows a node to own specific shards for redelivery.
func (cm *ConsumerManager) GetRedeliveryBatchForShard(groupID string, shardID int, timeout time.Duration, maxCount int) *RedeliveryBatch {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	state, ok := cm.groups[groupID]
	if !ok {
		return nil
	}

	candidates := state.GetRedeliveryCandidates(shardID, timeout, maxCount)
	if len(candidates.Entries) == 0 {
		return nil
	}

	return &RedeliveryBatch{
		GroupID:   groupID,
		ShardID:   shardID,
		Entries:   candidates.Entries,
		Timestamp: time.Now(),
	}
}

// ClaimBatch claims a batch of messages for a new consumer.
// Returns the offsets that were successfully claimed.
func (cm *ConsumerManager) ClaimBatch(groupID string, offsets []uint64, newConsumerID string) ([]uint64, error) {
	state := cm.Get(groupID)
	if state == nil {
		return nil, ErrGroupNotFound
	}

	if err := state.ClaimBatch(offsets, newConsumerID); err != nil {
		return nil, err
	}

	return offsets, nil
}

// AckBatch acknowledges a batch of messages.
func (cm *ConsumerManager) AckBatch(groupID string, partitionID uint32, offsets []uint64) error {
	state := cm.Get(groupID)
	if state == nil {
		return ErrGroupNotFound
	}

	return state.AckBatch(partitionID, offsets)
}

// GetDeadLetterBatches returns batches of messages that exceeded max delivery count.
func (cm *ConsumerManager) GetDeadLetterBatches(maxDeliveries int, maxPerGroup int) []RedeliveryBatch {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	var batches []RedeliveryBatch
	now := time.Now()

	for groupID, state := range cm.groups {
		candidates := state.GetDeadLetterCandidates(maxDeliveries, maxPerGroup)
		if len(candidates) > 0 {
			batches = append(batches, RedeliveryBatch{
				GroupID:   groupID,
				ShardID:   -1, // All shards
				Entries:   candidates,
				Timestamp: now,
			})
		}
	}

	return batches
}

// Shard Assignment for Distributed Redelivery

// ShardAssignment describes which shards a node is responsible for.
type ShardAssignment struct {
	NodeID    string
	GroupID   string
	ShardIDs  []int
	AssignedAt time.Time
}

// GetShardAssignments returns shard assignments for a consumer group.
// This can be used to distribute redelivery across nodes.
func (cm *ConsumerManager) GetShardAssignments(groupID string, nodeIDs []string) []ShardAssignment {
	state := cm.Get(groupID)
	if state == nil {
		return nil
	}

	numShards := state.NumShards()
	numNodes := len(nodeIDs)
	if numNodes == 0 {
		return nil
	}

	assignments := make([]ShardAssignment, numNodes)
	now := time.Now()

	for i, nodeID := range nodeIDs {
		assignments[i] = ShardAssignment{
			NodeID:     nodeID,
			GroupID:    groupID,
			ShardIDs:   make([]int, 0),
			AssignedAt: now,
		}
	}

	// Round-robin assignment
	for shardID := range numShards {
		nodeIdx := shardID % numNodes
		assignments[nodeIdx].ShardIDs = append(assignments[nodeIdx].ShardIDs, shardID)
	}

	return assignments
}

// ForEachGroup iterates over all consumer groups.
func (cm *ConsumerManager) ForEachGroup(fn func(groupID string, state *ConsumerState) error) error {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	for groupID, state := range cm.groups {
		if err := fn(groupID, state); err != nil {
			return err
		}
	}
	return nil
}
