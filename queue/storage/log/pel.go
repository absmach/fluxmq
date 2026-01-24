// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package log

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// PEL (Pending Entry List) tracks messages that have been delivered but not yet acknowledged.
// Inspired by Redis Streams XPENDING.
type PEL struct {
	mu sync.RWMutex

	dir     string
	groupID string

	// Consumer -> list of pending entries
	entries map[string][]PELEntry

	// Index by offset for fast lookup
	byOffset map[uint64]*PELEntry

	// Operation log for durability
	logFile *os.File
	logPath string
	opCount int
	dirty   bool

	// Configuration
	compactThreshold int
}

// PELState is the serialized state of a PEL.
type PELState struct {
	Version uint8                 `json:"version"`
	GroupID string                `json:"group_id"`
	Entries map[string][]PELEntry `json:"entries"`
	SavedAt int64                 `json:"saved_at"`
}

// NewPEL creates or opens a PEL for a consumer group.
func NewPEL(dir, groupID string) (*PEL, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("failed to create PEL directory: %w", err)
	}

	pel := &PEL{
		dir:              dir,
		groupID:          groupID,
		entries:          make(map[string][]PELEntry),
		byOffset:         make(map[uint64]*PELEntry),
		compactThreshold: DefaultPELCompactThreshold,
	}

	// Load existing state
	if err := pel.load(); err != nil && !os.IsNotExist(err) {
		return nil, fmt.Errorf("failed to load PEL state: %w", err)
	}

	// Open operation log for append
	pel.logPath = filepath.Join(dir, groupID+PELExtension+".log")
	var err error
	pel.logFile, err = os.OpenFile(pel.logPath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0o644)
	if err != nil {
		return nil, fmt.Errorf("failed to open PEL log: %w", err)
	}

	// Replay log on top of snapshot
	if err := pel.replayLog(); err != nil {
		pel.logFile.Close()
		return nil, fmt.Errorf("failed to replay PEL log: %w", err)
	}

	return pel, nil
}

// statePath returns the path to the PEL state snapshot file.
func (p *PEL) statePath() string {
	return filepath.Join(p.dir, p.groupID+PELExtension)
}

// load loads PEL state from the snapshot file.
func (p *PEL) load() error {
	data, err := os.ReadFile(p.statePath())
	if err != nil {
		return err
	}

	var state PELState
	if err := json.Unmarshal(data, &state); err != nil {
		return fmt.Errorf("failed to unmarshal PEL state: %w", err)
	}

	if state.Version > PELVersion {
		return fmt.Errorf("unsupported PEL version: %d", state.Version)
	}

	p.entries = state.Entries
	if p.entries == nil {
		p.entries = make(map[string][]PELEntry)
	}

	// Rebuild offset index
	p.byOffset = make(map[uint64]*PELEntry)
	for consumerID, entries := range p.entries {
		for i := range entries {
			entries[i].ConsumerID = consumerID
			p.byOffset[entries[i].Offset] = &entries[i]
		}
	}

	return nil
}

// replayLog replays the operation log on top of the loaded state.
func (p *PEL) replayLog() error {
	info, err := p.logFile.Stat()
	if err != nil {
		return err
	}

	if info.Size() == 0 {
		return nil
	}

	// Read entire log
	data := make([]byte, info.Size())
	_, err = p.logFile.ReadAt(data, 0)
	if err != nil {
		return err
	}

	// Parse and apply operations
	r := NewBufferReader(data)
	for r.Remaining() > 0 {
		op, err := r.ReadUint8()
		if err != nil {
			break
		}

		switch PELOperation(op) {
		case PELOpAdd:
			entry, err := p.readEntry(r)
			if err != nil {
				break
			}
			p.addEntryInternal(entry)

		case PELOpAck:
			offset, err := r.ReadUint64()
			if err != nil {
				break
			}
			p.removeEntryInternal(offset)

		case PELOpClaim:
			offset, err := r.ReadUint64()
			if err != nil {
				break
			}
			newConsumer, err := r.ReadBytes()
			if err != nil {
				break
			}
			p.claimEntryInternal(offset, string(newConsumer))

		case PELOpExpire:
			offset, err := r.ReadUint64()
			if err != nil {
				break
			}
			p.incrementDeliveryInternal(offset)
		}

		p.opCount++
	}

	return nil
}

// readEntry reads a PEL entry from a buffer.
func (p *PEL) readEntry(r *BufferReader) (PELEntry, error) {
	offset, err := r.ReadUint64()
	if err != nil {
		return PELEntry{}, err
	}

	partitionID, err := r.ReadUint32()
	if err != nil {
		return PELEntry{}, err
	}

	consumerID, err := r.ReadBytes()
	if err != nil {
		return PELEntry{}, err
	}

	claimedAt, err := r.ReadUint64()
	if err != nil {
		return PELEntry{}, err
	}

	deliveryCount, err := r.ReadUint16()
	if err != nil {
		return PELEntry{}, err
	}

	return PELEntry{
		Offset:        offset,
		PartitionID:   partitionID,
		ConsumerID:    string(consumerID),
		ClaimedAt:     int64(claimedAt),
		DeliveryCount: deliveryCount,
	}, nil
}

// writeEntry writes a PEL entry to the log.
func (p *PEL) writeEntry(op PELOperation, entry PELEntry) error {
	w := NewBufferWriter(64)
	w.WriteUint8(uint8(op))
	w.WriteUint64(entry.Offset)
	w.WriteUint32(entry.PartitionID)
	w.WriteBytes([]byte(entry.ConsumerID))
	w.WriteUint64(uint64(entry.ClaimedAt))
	w.WriteUint16(entry.DeliveryCount)

	_, err := p.logFile.Write(w.Bytes())
	if err != nil {
		return err
	}

	p.opCount++
	return nil
}

// writeOffsetOp writes a simple offset-based operation to the log.
func (p *PEL) writeOffsetOp(op PELOperation, offset uint64, extra []byte) error {
	w := NewBufferWriter(32)
	w.WriteUint8(uint8(op))
	w.WriteUint64(offset)
	if extra != nil {
		w.WriteBytes(extra)
	}

	_, err := p.logFile.Write(w.Bytes())
	if err != nil {
		return err
	}

	p.opCount++
	return nil
}

// Add adds a new entry to the PEL.
func (p *PEL) Add(entry PELEntry) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Write to log first
	if err := p.writeEntry(PELOpAdd, entry); err != nil {
		return err
	}

	p.addEntryInternal(entry)
	p.dirty = true

	// Check if compaction is needed
	if p.opCount >= p.compactThreshold {
		go p.Compact()
	}

	return nil
}

// addEntryInternal adds an entry without logging.
func (p *PEL) addEntryInternal(entry PELEntry) {
	entries := p.entries[entry.ConsumerID]
	entries = append(entries, entry)
	p.entries[entry.ConsumerID] = entries
	p.byOffset[entry.Offset] = &p.entries[entry.ConsumerID][len(entries)-1]
}

// Ack removes an entry from the PEL.
func (p *PEL) Ack(offset uint64) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if _, ok := p.byOffset[offset]; !ok {
		return ErrPELEntryNotFound
	}

	// Write to log first
	if err := p.writeOffsetOp(PELOpAck, offset, nil); err != nil {
		return err
	}

	p.removeEntryInternal(offset)
	p.dirty = true

	return nil
}

// removeEntryInternal removes an entry without logging.
func (p *PEL) removeEntryInternal(offset uint64) {
	entry, ok := p.byOffset[offset]
	if !ok {
		return
	}

	consumerID := entry.ConsumerID
	entries := p.entries[consumerID]

	// Find and remove
	for i, e := range entries {
		if e.Offset == offset {
			entries = append(entries[:i], entries[i+1:]...)
			break
		}
	}

	if len(entries) == 0 {
		delete(p.entries, consumerID)
	} else {
		p.entries[consumerID] = entries
	}

	delete(p.byOffset, offset)
}

// Claim transfers an entry to a new consumer (work stealing).
func (p *PEL) Claim(offset uint64, newConsumerID string) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if _, ok := p.byOffset[offset]; !ok {
		return ErrPELEntryNotFound
	}

	// Write to log first
	if err := p.writeOffsetOp(PELOpClaim, offset, []byte(newConsumerID)); err != nil {
		return err
	}

	p.claimEntryInternal(offset, newConsumerID)
	p.dirty = true

	return nil
}

// claimEntryInternal transfers an entry without logging.
func (p *PEL) claimEntryInternal(offset uint64, newConsumerID string) {
	entry, ok := p.byOffset[offset]
	if !ok {
		return
	}

	oldConsumerID := entry.ConsumerID

	// Remove from old consumer
	entries := p.entries[oldConsumerID]
	for i, e := range entries {
		if e.Offset == offset {
			entries = append(entries[:i], entries[i+1:]...)
			break
		}
	}
	if len(entries) == 0 {
		delete(p.entries, oldConsumerID)
	} else {
		p.entries[oldConsumerID] = entries
	}

	// Add to new consumer
	entry.ConsumerID = newConsumerID
	entry.ClaimedAt = time.Now().UnixMilli()
	entry.DeliveryCount++

	newEntries := p.entries[newConsumerID]
	newEntries = append(newEntries, *entry)
	p.entries[newConsumerID] = newEntries
	p.byOffset[offset] = &p.entries[newConsumerID][len(newEntries)-1]
}

// IncrementDelivery increments the delivery count for an entry.
func (p *PEL) IncrementDelivery(offset uint64) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if _, ok := p.byOffset[offset]; !ok {
		return ErrPELEntryNotFound
	}

	// Write to log first
	if err := p.writeOffsetOp(PELOpExpire, offset, nil); err != nil {
		return err
	}

	p.incrementDeliveryInternal(offset)
	p.dirty = true

	return nil
}

// incrementDeliveryInternal increments delivery count without logging.
func (p *PEL) incrementDeliveryInternal(offset uint64) {
	if entry, ok := p.byOffset[offset]; ok {
		entry.DeliveryCount++
		entry.ClaimedAt = time.Now().UnixMilli()
	}
}

// Get returns the entry for an offset.
func (p *PEL) Get(offset uint64) (*PELEntry, bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	entry, ok := p.byOffset[offset]
	if !ok {
		return nil, false
	}

	// Return a copy
	entryCopy := *entry
	return &entryCopy, true
}

// GetByConsumer returns all entries for a consumer.
func (p *PEL) GetByConsumer(consumerID string) []PELEntry {
	p.mu.RLock()
	defer p.mu.RUnlock()

	entries := p.entries[consumerID]
	result := make([]PELEntry, len(entries))
	copy(result, entries)
	return result
}

// GetStealable returns entries that can be stolen (older than timeout).
func (p *PEL) GetStealable(timeout time.Duration, excludeConsumer string) []PELEntry {
	p.mu.RLock()
	defer p.mu.RUnlock()

	cutoff := time.Now().Add(-timeout).UnixMilli()
	var result []PELEntry

	for consumerID, entries := range p.entries {
		if consumerID == excludeConsumer {
			continue
		}

		for _, entry := range entries {
			if entry.ClaimedAt < cutoff {
				result = append(result, entry)
			}
		}
	}

	return result
}

// Count returns the total number of pending entries.
func (p *PEL) Count() int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return len(p.byOffset)
}

// CountByConsumer returns the number of pending entries per consumer.
func (p *PEL) CountByConsumer() map[string]int {
	p.mu.RLock()
	defer p.mu.RUnlock()

	result := make(map[string]int, len(p.entries))
	for consumerID, entries := range p.entries {
		result[consumerID] = len(entries)
	}
	return result
}

// GetAll returns all entries organized by consumer.
func (p *PEL) GetAll() map[string][]PELEntry {
	p.mu.RLock()
	defer p.mu.RUnlock()

	result := make(map[string][]PELEntry, len(p.entries))
	for consumerID, entries := range p.entries {
		entriesCopy := make([]PELEntry, len(entries))
		copy(entriesCopy, entries)
		result[consumerID] = entriesCopy
	}
	return result
}

// MinOffset returns the minimum pending offset.
func (p *PEL) MinOffset() uint64 {
	p.mu.RLock()
	defer p.mu.RUnlock()

	var min uint64 = ^uint64(0)
	for offset := range p.byOffset {
		if offset < min {
			min = offset
		}
	}

	if min == ^uint64(0) {
		return 0
	}
	return min
}

// Compact creates a new snapshot and truncates the log.
func (p *PEL) Compact() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	return p.compactUnlocked()
}

func (p *PEL) compactUnlocked() error {
	// Save snapshot
	state := PELState{
		Version: PELVersion,
		GroupID: p.groupID,
		Entries: p.entries,
		SavedAt: time.Now().UnixMilli(),
	}

	data, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal PEL state: %w", err)
	}

	tempPath := p.statePath() + TempExtension
	if err := os.WriteFile(tempPath, data, 0o644); err != nil {
		return fmt.Errorf("failed to write PEL snapshot: %w", err)
	}

	if err := os.Rename(tempPath, p.statePath()); err != nil {
		os.Remove(tempPath)
		return fmt.Errorf("failed to rename PEL snapshot: %w", err)
	}

	// Truncate log
	if err := p.logFile.Truncate(0); err != nil {
		return fmt.Errorf("failed to truncate PEL log: %w", err)
	}

	if _, err := p.logFile.Seek(0, 0); err != nil {
		return fmt.Errorf("failed to seek PEL log: %w", err)
	}

	p.opCount = 0
	p.dirty = false

	return nil
}

// Sync flushes the log to disk.
func (p *PEL) Sync() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	return p.logFile.Sync()
}

// Close closes the PEL.
func (p *PEL) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.dirty {
		p.compactUnlocked()
	}

	return p.logFile.Close()
}

// ConsumerGroupPELs manages PELs for multiple consumer groups.
type ConsumerGroupPELs struct {
	mu sync.RWMutex

	dir  string
	pels map[string]*PEL
}

// NewConsumerGroupPELs creates a new consumer group PEL manager.
func NewConsumerGroupPELs(dir string) (*ConsumerGroupPELs, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}

	return &ConsumerGroupPELs{
		dir:  dir,
		pels: make(map[string]*PEL),
	}, nil
}

// GetOrCreate gets or creates a PEL for a consumer group.
func (cgp *ConsumerGroupPELs) GetOrCreate(groupID string) (*PEL, error) {
	cgp.mu.Lock()
	defer cgp.mu.Unlock()

	if pel, ok := cgp.pels[groupID]; ok {
		return pel, nil
	}

	pel, err := NewPEL(cgp.dir, groupID)
	if err != nil {
		return nil, err
	}

	cgp.pels[groupID] = pel
	return pel, nil
}

// Get returns the PEL for a consumer group if it exists.
func (cgp *ConsumerGroupPELs) Get(groupID string) *PEL {
	cgp.mu.RLock()
	defer cgp.mu.RUnlock()
	return cgp.pels[groupID]
}

// Delete removes a consumer group's PEL.
func (cgp *ConsumerGroupPELs) Delete(groupID string) error {
	cgp.mu.Lock()
	defer cgp.mu.Unlock()

	if pel, ok := cgp.pels[groupID]; ok {
		pel.Close()
		delete(cgp.pels, groupID)
	}

	// Remove files
	if err := os.Remove(filepath.Join(cgp.dir, groupID+PELExtension)); err != nil {
		return err
	}

	return os.Remove(filepath.Join(cgp.dir, groupID+PELExtension+".log"))
}

// Close closes all PELs.
func (cgp *ConsumerGroupPELs) Close() error {
	cgp.mu.Lock()
	defer cgp.mu.Unlock()

	var lastErr error
	for _, pel := range cgp.pels {
		if err := pel.Close(); err != nil {
			lastErr = err
		}
	}
	cgp.pels = make(map[string]*PEL)
	return lastErr
}
