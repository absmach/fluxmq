// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package log

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// ConsumerState implements JetStream-style consumer state with:
// - Monotonic AckFloor (highest contiguous acked offset)
// - Sharded PEL for distributed redelivery
// - Batched operations for performance
type ConsumerState struct {
	mu sync.RWMutex

	dir     string
	groupID string
	config  ConsumerStateConfig

	// Per-partition state
	partitions map[uint32]*PartitionState

	// Sharded PEL for scalable redelivery
	pelShards []*PELShard

	// Persistence
	dirty       bool
	lastSync    time.Time
	opLog       *os.File
	opLogPath   string
	opCount     int
	snapVersion uint64
}

// ConsumerStateConfig holds consumer state configuration.
type ConsumerStateConfig struct {
	// PEL sharding
	NumPELShards int // Number of PEL shards (default: 8)

	// Batching
	MaxBatchSize     int           // Max entries per batch operation
	BatchFlushDelay  time.Duration // Max delay before flushing batch

	// Compaction
	CompactThreshold int // Operations before compaction (default: 10000)

	// Redelivery
	RedeliveryTimeout   time.Duration // Time before message can be redelivered
	MaxRedeliveryCount  int           // Max redeliveries before DLQ
	RedeliveryBatchSize int           // Messages per redelivery batch
}

// DefaultConsumerStateConfig returns default configuration.
func DefaultConsumerStateConfig() ConsumerStateConfig {
	return ConsumerStateConfig{
		NumPELShards:        8,
		MaxBatchSize:        1000,
		BatchFlushDelay:     10 * time.Millisecond,
		CompactThreshold:    10000,
		RedeliveryTimeout:   30 * time.Second,
		MaxRedeliveryCount:  5,
		RedeliveryBatchSize: 100,
	}
}

// PartitionState tracks consumer progress in a single partition.
type PartitionState struct {
	// JetStream-style monotonic progress
	AckFloor  uint64 // Highest contiguous acked offset (everything <= is done)
	Cursor    uint64 // Next offset to deliver
	Delivered uint64 // Total messages delivered

	// Timestamps
	CreatedAt int64
	UpdatedAt int64
}

// PELShard is a shard of the Pending Entry List.
// Sharding allows parallel redelivery across nodes.
type PELShard struct {
	mu sync.RWMutex

	shardID int
	entries map[uint64]*PendingEntry // offset -> entry

	// Index by consumer for efficient consumer-based operations
	byConsumer map[string]map[uint64]struct{} // consumerID -> set of offsets

	// Sorted offsets for efficient range queries
	dirty bool
}

// PendingEntry represents a delivered but unacknowledged message.
type PendingEntry struct {
	Offset        uint64 `json:"o"`
	PartitionID   uint32 `json:"p"`
	ConsumerID    string `json:"c"`
	DeliveredAt   int64  `json:"d"`
	DeliveryCount uint16 `json:"n"`
	LastAttempt   int64  `json:"l"`
}

// ShardKey returns the shard assignment for an offset.
func (cs *ConsumerState) ShardKey(offset uint64) int {
	return int(offset % uint64(cs.config.NumPELShards))
}

// ConsumerShardKey returns shard based on consumer ID (for consumer-affinity sharding).
func ConsumerShardKey(consumerID string, numShards int) int {
	h := fnv.New32a()
	h.Write([]byte(consumerID))
	return int(h.Sum32() % uint32(numShards))
}

// NewConsumerState creates or opens consumer state for a group.
func NewConsumerState(baseDir, groupID string, config ConsumerStateConfig) (*ConsumerState, error) {
	dir := filepath.Join(baseDir, groupID)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("failed to create consumer state directory: %w", err)
	}

	cs := &ConsumerState{
		dir:        dir,
		groupID:    groupID,
		config:     config,
		partitions: make(map[uint32]*PartitionState),
		pelShards:  make([]*PELShard, config.NumPELShards),
		lastSync:   time.Now(),
	}

	// Initialize PEL shards
	for i := 0; i < config.NumPELShards; i++ {
		cs.pelShards[i] = &PELShard{
			shardID:    i,
			entries:    make(map[uint64]*PendingEntry),
			byConsumer: make(map[string]map[uint64]struct{}),
		}
	}

	// Load existing state
	if err := cs.load(); err != nil {
		return nil, fmt.Errorf("failed to load consumer state: %w", err)
	}

	// Open operation log for incremental persistence
	cs.opLogPath = filepath.Join(dir, "ops.log")
	var err error
	cs.opLog, err = os.OpenFile(cs.opLogPath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0o644)
	if err != nil {
		return nil, fmt.Errorf("failed to open operation log: %w", err)
	}

	// Replay operation log
	if err := cs.replayOpLog(); err != nil {
		cs.opLog.Close()
		return nil, fmt.Errorf("failed to replay operation log: %w", err)
	}

	return cs, nil
}

// State snapshot file paths
func (cs *ConsumerState) statePath() string {
	return filepath.Join(cs.dir, "state.json")
}

func (cs *ConsumerState) pelPath(shardID int) string {
	return filepath.Join(cs.dir, fmt.Sprintf("pel_%d.json", shardID))
}

// StateSnapshot is the serialized consumer state.
type StateSnapshot struct {
	Version    uint64                     `json:"version"`
	GroupID    string                     `json:"group_id"`
	Partitions map[uint32]*PartitionState `json:"partitions"`
	SavedAt    int64                      `json:"saved_at"`
}

// PELSnapshot is the serialized PEL shard.
type PELSnapshot struct {
	Version uint64                    `json:"version"`
	ShardID int                       `json:"shard_id"`
	Entries map[uint64]*PendingEntry  `json:"entries"`
	SavedAt int64                     `json:"saved_at"`
}

// load loads state from snapshots.
func (cs *ConsumerState) load() error {
	// Load partition state
	data, err := os.ReadFile(cs.statePath())
	if err == nil {
		var snap StateSnapshot
		if err := json.Unmarshal(data, &snap); err == nil {
			cs.partitions = snap.Partitions
			cs.snapVersion = snap.Version
			if cs.partitions == nil {
				cs.partitions = make(map[uint32]*PartitionState)
			}
		}
	}

	// Load PEL shards
	for i := 0; i < cs.config.NumPELShards; i++ {
		data, err := os.ReadFile(cs.pelPath(i))
		if err != nil {
			continue
		}

		var snap PELSnapshot
		if err := json.Unmarshal(data, &snap); err != nil {
			continue
		}

		shard := cs.pelShards[i]
		shard.entries = snap.Entries
		if shard.entries == nil {
			shard.entries = make(map[uint64]*PendingEntry)
		}

		// Rebuild consumer index
		shard.byConsumer = make(map[string]map[uint64]struct{})
		for offset, entry := range shard.entries {
			if shard.byConsumer[entry.ConsumerID] == nil {
				shard.byConsumer[entry.ConsumerID] = make(map[uint64]struct{})
			}
			shard.byConsumer[entry.ConsumerID][offset] = struct{}{}
		}
	}

	return nil
}

// Operation types for the operation log.
type OpType uint8

const (
	OpDeliver OpType = iota + 1
	OpAck
	OpAckBatch
	OpNack
	OpClaim
	OpClaimBatch
	OpAdvanceFloor
)

// Operation represents an operation in the log.
type Operation struct {
	Type        OpType   `json:"t"`
	PartitionID uint32   `json:"p,omitempty"`
	Offset      uint64   `json:"o,omitempty"`
	Offsets     []uint64 `json:"os,omitempty"` // For batch operations
	ConsumerID  string   `json:"c,omitempty"`
	Timestamp   int64    `json:"ts"`
}

// replayOpLog replays the operation log on top of loaded snapshots.
func (cs *ConsumerState) replayOpLog() error {
	info, err := cs.opLog.Stat()
	if err != nil {
		return err
	}

	if info.Size() == 0 {
		return nil
	}

	data := make([]byte, info.Size())
	if _, err := cs.opLog.ReadAt(data, 0); err != nil {
		return err
	}

	// Parse newline-delimited JSON operations
	decoder := json.NewDecoder(newByteReader(data))
	for decoder.More() {
		var op Operation
		if err := decoder.Decode(&op); err != nil {
			break // Stop at first malformed entry
		}
		cs.applyOp(&op)
		cs.opCount++
	}

	return nil
}

// applyOp applies an operation to in-memory state.
func (cs *ConsumerState) applyOp(op *Operation) {
	switch op.Type {
	case OpDeliver:
		cs.applyDeliver(op.PartitionID, op.Offset, op.ConsumerID, op.Timestamp)
	case OpAck:
		cs.applyAck(op.PartitionID, op.Offset)
	case OpAckBatch:
		for _, offset := range op.Offsets {
			cs.applyAck(op.PartitionID, offset)
		}
	case OpNack:
		cs.applyNack(op.Offset, op.Timestamp)
	case OpClaim:
		cs.applyClaim(op.Offset, op.ConsumerID, op.Timestamp)
	case OpClaimBatch:
		for _, offset := range op.Offsets {
			cs.applyClaim(offset, op.ConsumerID, op.Timestamp)
		}
	case OpAdvanceFloor:
		cs.applyAdvanceFloor(op.PartitionID, op.Offset)
	}
}

// applyDeliver applies a deliver operation.
func (cs *ConsumerState) applyDeliver(partitionID uint32, offset uint64, consumerID string, ts int64) {
	// Update partition state
	ps := cs.getOrCreatePartition(partitionID)
	ps.Delivered++
	if offset >= ps.Cursor {
		ps.Cursor = offset + 1
	}
	ps.UpdatedAt = ts

	// Add to PEL
	entry := &PendingEntry{
		Offset:        offset,
		PartitionID:   partitionID,
		ConsumerID:    consumerID,
		DeliveredAt:   ts,
		DeliveryCount: 1,
		LastAttempt:   ts,
	}

	shard := cs.pelShards[cs.ShardKey(offset)]
	shard.entries[offset] = entry
	if shard.byConsumer[consumerID] == nil {
		shard.byConsumer[consumerID] = make(map[uint64]struct{})
	}
	shard.byConsumer[consumerID][offset] = struct{}{}
	shard.dirty = true
}

// applyAck applies an ack operation.
func (cs *ConsumerState) applyAck(partitionID uint32, offset uint64) {
	shard := cs.pelShards[cs.ShardKey(offset)]

	entry, ok := shard.entries[offset]
	if !ok {
		return
	}

	// Remove from consumer index
	if offsets, ok := shard.byConsumer[entry.ConsumerID]; ok {
		delete(offsets, offset)
		if len(offsets) == 0 {
			delete(shard.byConsumer, entry.ConsumerID)
		}
	}

	delete(shard.entries, offset)
	shard.dirty = true

	// Try to advance AckFloor
	cs.tryAdvanceAckFloor(partitionID)
}

// applyNack applies a nack operation (increment delivery count, update timestamp).
func (cs *ConsumerState) applyNack(offset uint64, ts int64) {
	shard := cs.pelShards[cs.ShardKey(offset)]

	entry, ok := shard.entries[offset]
	if !ok {
		return
	}

	entry.DeliveryCount++
	entry.LastAttempt = ts
	shard.dirty = true
}

// applyClaim applies a claim operation (work stealing).
func (cs *ConsumerState) applyClaim(offset uint64, newConsumerID string, ts int64) {
	shard := cs.pelShards[cs.ShardKey(offset)]

	entry, ok := shard.entries[offset]
	if !ok {
		return
	}

	oldConsumerID := entry.ConsumerID

	// Remove from old consumer index
	if offsets, ok := shard.byConsumer[oldConsumerID]; ok {
		delete(offsets, offset)
		if len(offsets) == 0 {
			delete(shard.byConsumer, oldConsumerID)
		}
	}

	// Update entry
	entry.ConsumerID = newConsumerID
	entry.DeliveryCount++
	entry.LastAttempt = ts

	// Add to new consumer index
	if shard.byConsumer[newConsumerID] == nil {
		shard.byConsumer[newConsumerID] = make(map[uint64]struct{})
	}
	shard.byConsumer[newConsumerID][offset] = struct{}{}
	shard.dirty = true
}

// applyAdvanceFloor applies an ack floor advancement.
func (cs *ConsumerState) applyAdvanceFloor(partitionID uint32, floor uint64) {
	ps := cs.getOrCreatePartition(partitionID)
	if floor > ps.AckFloor {
		ps.AckFloor = floor
		ps.UpdatedAt = time.Now().UnixMilli()
	}
}

// tryAdvanceAckFloor tries to advance the AckFloor if gaps are filled.
func (cs *ConsumerState) tryAdvanceAckFloor(partitionID uint32) {
	ps, ok := cs.partitions[partitionID]
	if !ok {
		return
	}

	// Check if we can advance AckFloor
	// This is O(gap_size) but gaps should be small in practice
	newFloor := ps.AckFloor
	for offset := ps.AckFloor + 1; offset < ps.Cursor; offset++ {
		shard := cs.pelShards[cs.ShardKey(offset)]
		if _, pending := shard.entries[offset]; pending {
			break // Found a gap
		}
		newFloor = offset
	}

	if newFloor > ps.AckFloor {
		ps.AckFloor = newFloor
		ps.UpdatedAt = time.Now().UnixMilli()
		cs.dirty = true
	}
}

// getOrCreatePartition gets or creates partition state.
func (cs *ConsumerState) getOrCreatePartition(partitionID uint32) *PartitionState {
	ps, ok := cs.partitions[partitionID]
	if !ok {
		now := time.Now().UnixMilli()
		ps = &PartitionState{
			CreatedAt: now,
			UpdatedAt: now,
		}
		cs.partitions[partitionID] = ps
	}
	return ps
}

// writeOp writes an operation to the log.
func (cs *ConsumerState) writeOp(op *Operation) error {
	data, err := json.Marshal(op)
	if err != nil {
		return err
	}
	data = append(data, '\n')

	if _, err := cs.opLog.Write(data); err != nil {
		return err
	}

	cs.opCount++
	cs.dirty = true

	// Check if compaction needed
	if cs.opCount >= cs.config.CompactThreshold {
		go cs.Compact()
	}

	return nil
}

// Public API

// Deliver marks a message as delivered to a consumer.
func (cs *ConsumerState) Deliver(partitionID uint32, offset uint64, consumerID string) error {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	ts := time.Now().UnixMilli()
	cs.applyDeliver(partitionID, offset, consumerID, ts)

	return cs.writeOp(&Operation{
		Type:        OpDeliver,
		PartitionID: partitionID,
		Offset:      offset,
		ConsumerID:  consumerID,
		Timestamp:   ts,
	})
}

// DeliverBatch marks multiple messages as delivered.
func (cs *ConsumerState) DeliverBatch(partitionID uint32, offsets []uint64, consumerID string) error {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	ts := time.Now().UnixMilli()
	for _, offset := range offsets {
		cs.applyDeliver(partitionID, offset, consumerID, ts)
	}

	// Write as individual ops for now (could optimize to batch op)
	for _, offset := range offsets {
		if err := cs.writeOp(&Operation{
			Type:        OpDeliver,
			PartitionID: partitionID,
			Offset:      offset,
			ConsumerID:  consumerID,
			Timestamp:   ts,
		}); err != nil {
			return err
		}
	}

	return nil
}

// Ack acknowledges a message.
func (cs *ConsumerState) Ack(partitionID uint32, offset uint64) error {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	cs.applyAck(partitionID, offset)

	return cs.writeOp(&Operation{
		Type:        OpAck,
		PartitionID: partitionID,
		Offset:      offset,
		Timestamp:   time.Now().UnixMilli(),
	})
}

// AckBatch acknowledges multiple messages.
func (cs *ConsumerState) AckBatch(partitionID uint32, offsets []uint64) error {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	for _, offset := range offsets {
		cs.applyAck(partitionID, offset)
	}

	return cs.writeOp(&Operation{
		Type:        OpAckBatch,
		PartitionID: partitionID,
		Offsets:     offsets,
		Timestamp:   time.Now().UnixMilli(),
	})
}

// Nack negatively acknowledges a message (will be redelivered).
func (cs *ConsumerState) Nack(offset uint64) error {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	ts := time.Now().UnixMilli()
	cs.applyNack(offset, ts)

	return cs.writeOp(&Operation{
		Type:      OpNack,
		Offset:    offset,
		Timestamp: ts,
	})
}

// Claim transfers a pending message to a new consumer (work stealing).
func (cs *ConsumerState) Claim(offset uint64, newConsumerID string) error {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	ts := time.Now().UnixMilli()
	cs.applyClaim(offset, newConsumerID, ts)

	return cs.writeOp(&Operation{
		Type:       OpClaim,
		Offset:     offset,
		ConsumerID: newConsumerID,
		Timestamp:  ts,
	})
}

// ClaimBatch transfers multiple pending messages to a new consumer.
func (cs *ConsumerState) ClaimBatch(offsets []uint64, newConsumerID string) error {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	ts := time.Now().UnixMilli()
	for _, offset := range offsets {
		cs.applyClaim(offset, newConsumerID, ts)
	}

	return cs.writeOp(&Operation{
		Type:       OpClaimBatch,
		Offsets:    offsets,
		ConsumerID: newConsumerID,
		Timestamp:  ts,
	})
}

// GetPartitionState returns the state for a partition.
func (cs *ConsumerState) GetPartitionState(partitionID uint32) *PartitionState {
	cs.mu.RLock()
	defer cs.mu.RUnlock()

	ps, ok := cs.partitions[partitionID]
	if !ok {
		return &PartitionState{}
	}

	// Return a copy
	return &PartitionState{
		AckFloor:  ps.AckFloor,
		Cursor:    ps.Cursor,
		Delivered: ps.Delivered,
		CreatedAt: ps.CreatedAt,
		UpdatedAt: ps.UpdatedAt,
	}
}

// GetPending returns a pending entry.
func (cs *ConsumerState) GetPending(offset uint64) (*PendingEntry, bool) {
	cs.mu.RLock()
	defer cs.mu.RUnlock()

	shard := cs.pelShards[cs.ShardKey(offset)]
	shard.mu.RLock()
	defer shard.mu.RUnlock()

	entry, ok := shard.entries[offset]
	if !ok {
		return nil, false
	}

	// Return a copy
	return &PendingEntry{
		Offset:        entry.Offset,
		PartitionID:   entry.PartitionID,
		ConsumerID:    entry.ConsumerID,
		DeliveredAt:   entry.DeliveredAt,
		DeliveryCount: entry.DeliveryCount,
		LastAttempt:   entry.LastAttempt,
	}, true
}

// GetPendingByConsumer returns all pending entries for a consumer.
func (cs *ConsumerState) GetPendingByConsumer(consumerID string) []PendingEntry {
	cs.mu.RLock()
	defer cs.mu.RUnlock()

	var result []PendingEntry

	for _, shard := range cs.pelShards {
		shard.mu.RLock()
		if offsets, ok := shard.byConsumer[consumerID]; ok {
			for offset := range offsets {
				if entry, ok := shard.entries[offset]; ok {
					result = append(result, *entry)
				}
			}
		}
		shard.mu.RUnlock()
	}

	return result
}

// GetPendingByShard returns all pending entries in a shard.
func (cs *ConsumerState) GetPendingByShard(shardID int) []PendingEntry {
	cs.mu.RLock()
	defer cs.mu.RUnlock()

	if shardID < 0 || shardID >= len(cs.pelShards) {
		return nil
	}

	shard := cs.pelShards[shardID]
	shard.mu.RLock()
	defer shard.mu.RUnlock()

	result := make([]PendingEntry, 0, len(shard.entries))
	for _, entry := range shard.entries {
		result = append(result, *entry)
	}

	return result
}

// RedeliveryCandidates holds candidates for redelivery from a shard.
type RedeliveryCandidates struct {
	ShardID int
	Entries []PendingEntry
}

// GetRedeliveryCandidates returns entries eligible for redelivery.
// This is designed to be called per-shard for distributed redelivery.
func (cs *ConsumerState) GetRedeliveryCandidates(shardID int, timeout time.Duration, maxCount int) RedeliveryCandidates {
	cs.mu.RLock()
	defer cs.mu.RUnlock()

	result := RedeliveryCandidates{ShardID: shardID}

	if shardID < 0 || shardID >= len(cs.pelShards) {
		return result
	}

	shard := cs.pelShards[shardID]
	shard.mu.RLock()
	defer shard.mu.RUnlock()

	cutoff := time.Now().Add(-timeout).UnixMilli()
	result.Entries = make([]PendingEntry, 0, maxCount)

	for _, entry := range shard.entries {
		if entry.LastAttempt < cutoff {
			result.Entries = append(result.Entries, *entry)
			if len(result.Entries) >= maxCount {
				break
			}
		}
	}

	return result
}

// GetAllRedeliveryCandidates returns candidates from all shards (for single-node).
func (cs *ConsumerState) GetAllRedeliveryCandidates(timeout time.Duration, maxCount int) []PendingEntry {
	cs.mu.RLock()
	defer cs.mu.RUnlock()

	cutoff := time.Now().Add(-timeout).UnixMilli()
	result := make([]PendingEntry, 0, maxCount)

	for _, shard := range cs.pelShards {
		shard.mu.RLock()
		for _, entry := range shard.entries {
			if entry.LastAttempt < cutoff {
				result = append(result, *entry)
				if len(result) >= maxCount {
					shard.mu.RUnlock()
					return result
				}
			}
		}
		shard.mu.RUnlock()
	}

	return result
}

// GetDeadLetterCandidates returns entries that exceeded max delivery count.
func (cs *ConsumerState) GetDeadLetterCandidates(maxDeliveries int, maxCount int) []PendingEntry {
	cs.mu.RLock()
	defer cs.mu.RUnlock()

	result := make([]PendingEntry, 0, maxCount)

	for _, shard := range cs.pelShards {
		shard.mu.RLock()
		for _, entry := range shard.entries {
			if int(entry.DeliveryCount) >= maxDeliveries {
				result = append(result, *entry)
				if len(result) >= maxCount {
					shard.mu.RUnlock()
					return result
				}
			}
		}
		shard.mu.RUnlock()
	}

	return result
}

// PendingCount returns total pending count.
func (cs *ConsumerState) PendingCount() int {
	cs.mu.RLock()
	defer cs.mu.RUnlock()

	count := 0
	for _, shard := range cs.pelShards {
		shard.mu.RLock()
		count += len(shard.entries)
		shard.mu.RUnlock()
	}
	return count
}

// PendingCountByShard returns pending count per shard.
func (cs *ConsumerState) PendingCountByShard() map[int]int {
	cs.mu.RLock()
	defer cs.mu.RUnlock()

	result := make(map[int]int, len(cs.pelShards))
	for i, shard := range cs.pelShards {
		shard.mu.RLock()
		result[i] = len(shard.entries)
		shard.mu.RUnlock()
	}
	return result
}

// NumShards returns the number of PEL shards.
func (cs *ConsumerState) NumShards() int {
	return len(cs.pelShards)
}

// Compact creates a new snapshot and truncates the operation log.
func (cs *ConsumerState) Compact() error {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	return cs.compactLocked()
}

func (cs *ConsumerState) compactLocked() error {
	cs.snapVersion++
	now := time.Now().UnixMilli()

	// Save partition state
	stateSnap := StateSnapshot{
		Version:    cs.snapVersion,
		GroupID:    cs.groupID,
		Partitions: cs.partitions,
		SavedAt:    now,
	}

	if err := writeJSONAtomic(cs.statePath(), stateSnap); err != nil {
		return fmt.Errorf("failed to save state snapshot: %w", err)
	}

	// Save PEL shards
	for i, shard := range cs.pelShards {
		shard.mu.Lock()
		pelSnap := PELSnapshot{
			Version: cs.snapVersion,
			ShardID: i,
			Entries: shard.entries,
			SavedAt: now,
		}
		if err := writeJSONAtomic(cs.pelPath(i), pelSnap); err != nil {
			shard.mu.Unlock()
			return fmt.Errorf("failed to save PEL shard %d: %w", i, err)
		}
		shard.dirty = false
		shard.mu.Unlock()
	}

	// Truncate operation log
	if err := cs.opLog.Truncate(0); err != nil {
		return fmt.Errorf("failed to truncate operation log: %w", err)
	}
	if _, err := cs.opLog.Seek(0, 0); err != nil {
		return fmt.Errorf("failed to seek operation log: %w", err)
	}

	cs.opCount = 0
	cs.dirty = false
	cs.lastSync = time.Now()

	return nil
}

// Sync flushes pending writes.
func (cs *ConsumerState) Sync() error {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	return cs.opLog.Sync()
}

// Close closes the consumer state.
func (cs *ConsumerState) Close() error {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	if cs.dirty {
		cs.compactLocked()
	}

	return cs.opLog.Close()
}

// Stats returns consumer state statistics.
func (cs *ConsumerState) Stats() ConsumerStateStats {
	cs.mu.RLock()
	defer cs.mu.RUnlock()

	stats := ConsumerStateStats{
		GroupID:        cs.groupID,
		NumPartitions:  len(cs.partitions),
		NumShards:      len(cs.pelShards),
		OpCount:        cs.opCount,
		SnapshotVersion: cs.snapVersion,
		ShardStats:     make([]ShardStats, len(cs.pelShards)),
	}

	for i, shard := range cs.pelShards {
		shard.mu.RLock()
		stats.TotalPending += len(shard.entries)
		stats.ShardStats[i] = ShardStats{
			ShardID:      i,
			PendingCount: len(shard.entries),
			ConsumerCount: len(shard.byConsumer),
		}
		shard.mu.RUnlock()
	}

	return stats
}

// ConsumerStateStats contains statistics about consumer state.
type ConsumerStateStats struct {
	GroupID         string
	NumPartitions   int
	NumShards       int
	TotalPending    int
	OpCount         int
	SnapshotVersion uint64
	ShardStats      []ShardStats
}

// ShardStats contains statistics about a PEL shard.
type ShardStats struct {
	ShardID       int
	PendingCount  int
	ConsumerCount int
}

// Helper functions

func writeJSONAtomic(path string, v any) error {
	data, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}

	tempPath := path + TempExtension
	if err := os.WriteFile(tempPath, data, 0o644); err != nil {
		return err
	}

	if err := os.Rename(tempPath, path); err != nil {
		os.Remove(tempPath)
		return err
	}

	return nil
}

// byteReader wraps a byte slice for json.Decoder.
type byteReader struct {
	data []byte
	pos  int
}

func newByteReader(data []byte) *byteReader {
	return &byteReader{data: data}
}

func (r *byteReader) Read(p []byte) (n int, err error) {
	if r.pos >= len(r.data) {
		return 0, fmt.Errorf("EOF")
	}
	n = copy(p, r.data[r.pos:])
	r.pos += n
	return n, nil
}
