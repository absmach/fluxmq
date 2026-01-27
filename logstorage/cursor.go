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

// CursorStore manages consumer cursor for a consumer group.
// In the new model without partitions, there's a single cursor per consumer group.
type CursorStore struct {
	mu sync.RWMutex

	dir     string
	groupID string

	cursor *Cursor

	dirty    bool
	lastSave time.Time
}

// CursorStoreState is the serialized state of a cursor store.
type CursorStoreState struct {
	Version uint8   `json:"version"`
	GroupID string  `json:"group_id"`
	Cursor  *Cursor `json:"cursor"`
	SavedAt int64   `json:"saved_at"`
}

// NewCursorStore creates or opens a cursor store for a consumer group.
func NewCursorStore(dir, groupID string) (*CursorStore, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("failed to create cursor directory: %w", err)
	}

	cs := &CursorStore{
		dir:     dir,
		groupID: groupID,
		cursor:  &Cursor{},
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

	if state.Cursor != nil {
		cs.cursor = state.Cursor
	} else {
		cs.cursor = &Cursor{}
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
		Cursor:  cs.cursor,
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

// GetCursor returns the cursor.
func (cs *CursorStore) GetCursor() *Cursor {
	cs.mu.RLock()
	defer cs.mu.RUnlock()

	// Return a copy
	return &Cursor{
		Cursor:    cs.cursor.Cursor,
		Committed: cs.cursor.Committed,
		UpdatedAt: cs.cursor.UpdatedAt,
	}
}

// SetCursor sets the cursor position.
func (cs *CursorStore) SetCursor(cursor uint64) {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	cs.cursor.Cursor = cursor
	cs.cursor.UpdatedAt = time.Now().UnixMilli()
	cs.dirty = true
}

// AdvanceCursor advances the cursor if the new value is greater.
func (cs *CursorStore) AdvanceCursor(cursor uint64) bool {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	if cursor > cs.cursor.Cursor {
		cs.cursor.Cursor = cursor
		cs.cursor.UpdatedAt = time.Now().UnixMilli()
		cs.dirty = true
		return true
	}

	return false
}

// Commit commits the cursor to a specific offset.
func (cs *CursorStore) Commit(offset uint64) {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	if offset > cs.cursor.Committed {
		cs.cursor.Committed = offset
		cs.cursor.UpdatedAt = time.Now().UnixMilli()
		cs.dirty = true
	}
}

// GetCommitted returns the committed offset.
func (cs *CursorStore) GetCommitted() uint64 {
	cs.mu.RLock()
	defer cs.mu.RUnlock()

	return cs.cursor.Committed
}

// Reset resets the cursor to a specific offset.
func (cs *CursorStore) Reset(offset uint64) {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	cs.cursor = &Cursor{
		Cursor:    offset,
		Committed: offset,
		UpdatedAt: time.Now().UnixMilli(),
	}
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
