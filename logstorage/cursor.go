// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package logstorage

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// CursorStore manages consumer cursors for a consumer group.
type CursorStore struct {
	mu sync.RWMutex

	dir     string
	groupID string

	// Per-partition cursors
	cursors map[uint32]*PartitionCursor

	dirty    bool
	lastSave time.Time
}

// CursorStoreState is the serialized state of a cursor store.
type CursorStoreState struct {
	Version uint8                       `json:"version"`
	GroupID string                      `json:"group_id"`
	Cursors map[uint32]*PartitionCursor `json:"cursors"`
	SavedAt int64                       `json:"saved_at"`
}

// NewCursorStore creates or opens a cursor store for a consumer group.
func NewCursorStore(dir, groupID string) (*CursorStore, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("failed to create cursor directory: %w", err)
	}

	cs := &CursorStore{
		dir:     dir,
		groupID: groupID,
		cursors: make(map[uint32]*PartitionCursor),
	}

	// Try to load existing state
	if err := cs.load(); err != nil && !os.IsNotExist(err) {
		return nil, fmt.Errorf("failed to load cursor state: %w", err)
	}

	return cs, nil
}

// cursorPath returns the path to the cursor file.
func (cs *CursorStore) cursorPath() string {
	return filepath.Join(cs.dir, cs.groupID+CursorExtension)
}

// load loads cursor state from disk.
func (cs *CursorStore) load() error {
	data, err := os.ReadFile(cs.cursorPath())
	if err != nil {
		return err
	}

	var state CursorStoreState
	if err := json.Unmarshal(data, &state); err != nil {
		return fmt.Errorf("failed to unmarshal cursor state: %w", err)
	}

	if state.Version > CursorVersion {
		return fmt.Errorf("unsupported cursor version: %d", state.Version)
	}

	cs.cursors = state.Cursors
	if cs.cursors == nil {
		cs.cursors = make(map[uint32]*PartitionCursor)
	}

	return nil
}

// Save persists cursor state to disk.
func (cs *CursorStore) Save() error {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	return cs.saveUnlocked()
}

func (cs *CursorStore) saveUnlocked() error {
	state := CursorStoreState{
		Version: CursorVersion,
		GroupID: cs.groupID,
		Cursors: cs.cursors,
		SavedAt: time.Now().UnixMilli(),
	}

	data, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal cursor state: %w", err)
	}

	tempPath := cs.cursorPath() + TempExtension
	if err := os.WriteFile(tempPath, data, 0o644); err != nil {
		return fmt.Errorf("failed to write cursor file: %w", err)
	}

	if err := os.Rename(tempPath, cs.cursorPath()); err != nil {
		os.Remove(tempPath)
		return fmt.Errorf("failed to rename cursor file: %w", err)
	}

	cs.dirty = false
	cs.lastSave = time.Now()

	return nil
}

// GetCursor returns the cursor for a partition.
func (cs *CursorStore) GetCursor(partitionID uint32) *PartitionCursor {
	cs.mu.RLock()
	defer cs.mu.RUnlock()

	cursor, ok := cs.cursors[partitionID]
	if !ok {
		return nil
	}

	// Return a copy
	return &PartitionCursor{
		Cursor:    cursor.Cursor,
		Committed: cursor.Committed,
		UpdatedAt: cursor.UpdatedAt,
	}
}

// SetCursor sets the cursor for a partition.
func (cs *CursorStore) SetCursor(partitionID uint32, cursor uint64) {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	c, ok := cs.cursors[partitionID]
	if !ok {
		c = &PartitionCursor{}
		cs.cursors[partitionID] = c
	}

	c.Cursor = cursor
	c.UpdatedAt = time.Now().UnixMilli()
	cs.dirty = true
}

// AdvanceCursor advances the cursor for a partition if the new value is greater.
func (cs *CursorStore) AdvanceCursor(partitionID uint32, cursor uint64) bool {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	c, ok := cs.cursors[partitionID]
	if !ok {
		c = &PartitionCursor{}
		cs.cursors[partitionID] = c
	}

	if cursor > c.Cursor {
		c.Cursor = cursor
		c.UpdatedAt = time.Now().UnixMilli()
		cs.dirty = true
		return true
	}

	return false
}

// Commit commits the cursor for a partition.
func (cs *CursorStore) Commit(partitionID uint32, offset uint64) {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	c, ok := cs.cursors[partitionID]
	if !ok {
		c = &PartitionCursor{Cursor: offset}
		cs.cursors[partitionID] = c
	}

	if offset > c.Committed {
		c.Committed = offset
		c.UpdatedAt = time.Now().UnixMilli()
		cs.dirty = true
	}
}

// GetCommitted returns the committed offset for a partition.
func (cs *CursorStore) GetCommitted(partitionID uint32) uint64 {
	cs.mu.RLock()
	defer cs.mu.RUnlock()

	if c, ok := cs.cursors[partitionID]; ok {
		return c.Committed
	}
	return 0
}

// GetAllCursors returns all partition cursors.
func (cs *CursorStore) GetAllCursors() map[uint32]PartitionCursor {
	cs.mu.RLock()
	defer cs.mu.RUnlock()

	result := make(map[uint32]PartitionCursor, len(cs.cursors))
	for id, c := range cs.cursors {
		result[id] = *c
	}
	return result
}

// MinCommitted returns the minimum committed offset across all partitions.
func (cs *CursorStore) MinCommitted() uint64 {
	cs.mu.RLock()
	defer cs.mu.RUnlock()

	var min uint64 = ^uint64(0)
	for _, c := range cs.cursors {
		if c.Committed < min {
			min = c.Committed
		}
	}

	if min == ^uint64(0) {
		return 0
	}
	return min
}

// ResetPartition resets the cursor for a partition to a specific offset.
func (cs *CursorStore) ResetPartition(partitionID uint32, offset uint64) {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	cs.cursors[partitionID] = &PartitionCursor{
		Cursor:    offset,
		Committed: offset,
		UpdatedAt: time.Now().UnixMilli(),
	}
	cs.dirty = true
}

// DeletePartition removes cursor state for a partition.
func (cs *CursorStore) DeletePartition(partitionID uint32) {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	delete(cs.cursors, partitionID)
	cs.dirty = true
}

// IsDirty returns whether the cursor state has unsaved changes.
func (cs *CursorStore) IsDirty() bool {
	cs.mu.RLock()
	defer cs.mu.RUnlock()
	return cs.dirty
}

// Close saves and closes the cursor store.
func (cs *CursorStore) Close() error {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	if cs.dirty {
		return cs.saveUnlocked()
	}
	return nil
}

// ConsumerGroupCursors manages cursors for multiple consumer groups.
type ConsumerGroupCursors struct {
	mu sync.RWMutex

	dir    string
	groups map[string]*CursorStore
}

// NewConsumerGroupCursors creates a new consumer group cursor manager.
func NewConsumerGroupCursors(dir string) (*ConsumerGroupCursors, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}

	return &ConsumerGroupCursors{
		dir:    dir,
		groups: make(map[string]*CursorStore),
	}, nil
}

// GetOrCreate gets or creates a cursor store for a consumer group.
func (cgc *ConsumerGroupCursors) GetOrCreate(groupID string) (*CursorStore, error) {
	cgc.mu.Lock()
	defer cgc.mu.Unlock()

	if cs, ok := cgc.groups[groupID]; ok {
		return cs, nil
	}

	cs, err := NewCursorStore(cgc.dir, groupID)
	if err != nil {
		return nil, err
	}

	cgc.groups[groupID] = cs
	return cs, nil
}

// Get returns the cursor store for a consumer group if it exists.
func (cgc *ConsumerGroupCursors) Get(groupID string) *CursorStore {
	cgc.mu.RLock()
	defer cgc.mu.RUnlock()
	return cgc.groups[groupID]
}

// Delete removes a consumer group's cursor state.
func (cgc *ConsumerGroupCursors) Delete(groupID string) error {
	cgc.mu.Lock()
	defer cgc.mu.Unlock()

	if cs, ok := cgc.groups[groupID]; ok {
		cs.Close()
		delete(cgc.groups, groupID)
	}

	// Remove the cursor file
	path := filepath.Join(cgc.dir, groupID+CursorExtension)
	return os.Remove(path)
}

// List returns all consumer group IDs.
func (cgc *ConsumerGroupCursors) List() []string {
	cgc.mu.RLock()
	defer cgc.mu.RUnlock()

	groups := make([]string, 0, len(cgc.groups))
	for id := range cgc.groups {
		groups = append(groups, id)
	}
	return groups
}

// SaveAll saves all cursor stores.
func (cgc *ConsumerGroupCursors) SaveAll() error {
	cgc.mu.RLock()
	defer cgc.mu.RUnlock()

	var lastErr error
	for _, cs := range cgc.groups {
		if err := cs.Save(); err != nil {
			lastErr = err
		}
	}
	return lastErr
}

// Close closes all cursor stores.
func (cgc *ConsumerGroupCursors) Close() error {
	cgc.mu.Lock()
	defer cgc.mu.Unlock()

	var lastErr error
	for _, cs := range cgc.groups {
		if err := cs.Close(); err != nil {
			lastErr = err
		}
	}
	cgc.groups = make(map[string]*CursorStore)
	return lastErr
}
