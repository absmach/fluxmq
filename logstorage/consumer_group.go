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

	"github.com/absmach/fluxmq/queue/types"
)

// ConsumerGroupStateStore manages consumer group state persistence.
type ConsumerGroupStateStore struct {
	mu sync.RWMutex

	dir    string
	groups map[string]map[string]*types.ConsumerGroupState // queueName -> groupID -> state
	dirty  map[string]bool                                 // groupKey -> dirty flag
}

const consumerGroupVersion uint8 = 2

// NewConsumerGroupStateStore creates or opens a consumer group state store.
func NewConsumerGroupStateStore(baseDir string) (*ConsumerGroupStateStore, error) {
	dir := filepath.Join(baseDir, "groups")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("failed to create groups directory: %w", err)
	}

	store := &ConsumerGroupStateStore{
		dir:    dir,
		groups: make(map[string]map[string]*types.ConsumerGroupState),
		dirty:  make(map[string]bool),
	}

	if err := store.loadAll(); err != nil {
		return nil, fmt.Errorf("failed to load consumer groups: %w", err)
	}

	return store, nil
}

// loadAll loads all consumer group states from disk.
func (s *ConsumerGroupStateStore) loadAll() error {
	err := filepath.Walk(s.dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return nil
		}

		if info.IsDir() || filepath.Ext(path) != ".json" {
			return nil
		}

		data, err := os.ReadFile(path)
		if err != nil {
			return nil
		}

		var wrapper struct {
			Version uint8                     `json:"version"`
			State   *types.ConsumerGroupState `json:"state"`
			SavedAt int64                     `json:"saved_at"`
		}

		if err := json.Unmarshal(data, &wrapper); err != nil {
			return nil
		}

		if wrapper.State == nil {
			return nil
		}

		state := wrapper.State

		// Ensure maps are initialized
		if state.Cursor == nil {
			state.Cursor = &types.QueueCursor{}
		}
		if state.PEL == nil {
			state.PEL = make(map[string][]*types.PendingEntry)
		}
		if state.Consumers == nil {
			state.Consumers = make(map[string]*types.ConsumerInfo)
		}

		// Add to memory map
		groups, ok := s.groups[state.QueueName]
		if !ok {
			groups = make(map[string]*types.ConsumerGroupState)
			s.groups[state.QueueName] = groups
		}
		groups[state.ID] = state

		return nil
	})

	if err != nil && !os.IsNotExist(err) {
		return err
	}

	return nil
}

// groupPath returns the path to a group's state file.
func (s *ConsumerGroupStateStore) groupPath(queueName, groupID string) string {
	return filepath.Join(s.dir, queueName, groupID+".json")
}

// groupKey returns a unique key for a group.
func groupKey(queueName, groupID string) string {
	return queueName + "/" + groupID
}

// loadGroup loads a consumer group state from disk.
func (s *ConsumerGroupStateStore) loadGroup(queueName, groupID string) (*types.ConsumerGroupState, error) {
	data, err := os.ReadFile(s.groupPath(queueName, groupID))
	if err != nil {
		return nil, err
	}

	var wrapper struct {
		Version uint8                     `json:"version"`
		State   *types.ConsumerGroupState `json:"state"`
		SavedAt int64                     `json:"saved_at"`
	}

	if err := json.Unmarshal(data, &wrapper); err != nil {
		return nil, fmt.Errorf("failed to unmarshal consumer group state: %w", err)
	}

	if wrapper.Version > consumerGroupVersion {
		return nil, fmt.Errorf("unsupported consumer group version: %d", wrapper.Version)
	}

	state := wrapper.State

	// Ensure maps are initialized
	if state.Cursor == nil {
		state.Cursor = &types.QueueCursor{}
	}
	if state.PEL == nil {
		state.PEL = make(map[string][]*types.PendingEntry)
	}
	if state.Consumers == nil {
		state.Consumers = make(map[string]*types.ConsumerInfo)
	}

	return state, nil
}

// Save persists a consumer group state.
func (s *ConsumerGroupStateStore) Save(state *types.ConsumerGroupState) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	groups, ok := s.groups[state.QueueName]
	if !ok {
		groups = make(map[string]*types.ConsumerGroupState)
		s.groups[state.QueueName] = groups
	}

	groups[state.ID] = state
	s.dirty[groupKey(state.QueueName, state.ID)] = true

	return s.saveGroup(state)
}

// saveGroup saves a single group to disk (must hold lock).
func (s *ConsumerGroupStateStore) saveGroup(state *types.ConsumerGroupState) error {
	path := s.groupPath(state.QueueName, state.ID)

	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("failed to create consumer group directory: %w", err)
	}

	wrapper := struct {
		Version uint8                     `json:"version"`
		State   *types.ConsumerGroupState `json:"state"`
		SavedAt int64                     `json:"saved_at"`
	}{
		Version: consumerGroupVersion,
		State:   state,
		SavedAt: time.Now().UnixMilli(),
	}

	data, err := json.MarshalIndent(wrapper, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal consumer group state: %w", err)
	}

	tempPath := path + TempExtension

	if err := os.WriteFile(tempPath, data, 0o644); err != nil {
		return fmt.Errorf("failed to write consumer group file: %w", err)
	}

	if err := os.Rename(tempPath, path); err != nil {
		os.Remove(tempPath)
		return fmt.Errorf("failed to rename consumer group file: %w", err)
	}

	delete(s.dirty, groupKey(state.QueueName, state.ID))

	return nil
}

// Get retrieves a consumer group state.
func (s *ConsumerGroupStateStore) Get(queueName, groupID string) (*types.ConsumerGroupState, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	groups, ok := s.groups[queueName]
	if !ok {
		return nil, ErrGroupNotFound
	}

	state, ok := groups[groupID]
	if !ok {
		return nil, ErrGroupNotFound
	}

	return state, nil
}

// Delete removes a consumer group state.
func (s *ConsumerGroupStateStore) Delete(queueName, groupID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	groups, ok := s.groups[queueName]
	if ok {
		delete(groups, groupID)
		if len(groups) == 0 {
			delete(s.groups, queueName)
		}
	}

	delete(s.dirty, groupKey(queueName, groupID))

	path := s.groupPath(queueName, groupID)
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return err
	}

	// Clean up empty queue directory
	dir := filepath.Join(s.dir, queueName)
	entries, _ := os.ReadDir(dir)
	if len(entries) == 0 {
		os.Remove(dir)
	}

	return nil
}

// List returns all consumer groups for a queue.
func (s *ConsumerGroupStateStore) List(queueName string) ([]*types.ConsumerGroupState, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	groups, ok := s.groups[queueName]
	if !ok {
		return []*types.ConsumerGroupState{}, nil
	}

	result := make([]*types.ConsumerGroupState, 0, len(groups))
	for _, state := range groups {
		result = append(result, state)
	}

	return result, nil
}

// ListAll returns all consumer groups across all queues.
func (s *ConsumerGroupStateStore) ListAll() ([]*types.ConsumerGroupState, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var result []*types.ConsumerGroupState
	for _, groups := range s.groups {
		for _, state := range groups {
			result = append(result, state)
		}
	}

	return result, nil
}

// Sync saves all dirty states to disk.
func (s *ConsumerGroupStateStore) Sync() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	var lastErr error
	for key := range s.dirty {
		for queueName, group := range s.groups {
			for groupID, state := range group {
				if groupKey(queueName, groupID) == key {
					if err := s.saveGroup(state); err != nil {
						lastErr = err
					}
				}
			}
		}
	}

	return lastErr
}

// Close closes the store.
func (s *ConsumerGroupStateStore) Close() error {
	return s.Sync()
}

// Exists checks if a consumer group exists.
func (s *ConsumerGroupStateStore) Exists(queueName, groupID string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	groups, ok := s.groups[queueName]
	if !ok {
		return false
	}

	_, ok = groups[groupID]
	return ok
}

// CreateIfNotExists creates a consumer group if it doesn't exist.
func (s *ConsumerGroupStateStore) CreateIfNotExists(state *types.ConsumerGroupState) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	groups, ok := s.groups[state.QueueName]
	if !ok {
		groups = make(map[string]*types.ConsumerGroupState)
		s.groups[state.QueueName] = groups
	}

	if _, exists := groups[state.ID]; exists {
		return nil
	}

	groups[state.ID] = state
	s.dirty[groupKey(state.QueueName, state.ID)] = true

	return s.saveGroup(state)
}

// UpdateCursor updates just the cursor for a queue.
func (s *ConsumerGroupStateStore) UpdateCursor(queueName, groupID string, cursor, committed uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	groups, ok := s.groups[queueName]
	if !ok {
		return ErrGroupNotFound
	}

	state, ok := groups[groupID]
	if !ok {
		return ErrGroupNotFound
	}

	c := state.GetCursor()
	c.Cursor = cursor
	c.Committed = committed
	state.UpdatedAt = time.Now()

	s.dirty[groupKey(queueName, groupID)] = true

	return nil
}

// GetCursor retrieves cursor state for a queue.
func (s *ConsumerGroupStateStore) GetCursor(queueName, groupID string) (*types.QueueCursor, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	groups, ok := s.groups[queueName]
	if !ok {
		return nil, ErrGroupNotFound
	}

	state, ok := groups[groupID]
	if !ok {
		return nil, ErrGroupNotFound
	}

	return state.GetCursor(), nil
}
