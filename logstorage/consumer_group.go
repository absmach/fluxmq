// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package logstorage

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/absmach/fluxmq/internal/keylock"
	"github.com/absmach/fluxmq/queue/types"
)

// groupRef names one consumer group.
//
// It is a struct rather than a joined string because queue names contain
// slashes: "$dlq/tasks" with group "workers" and "$dlq" with group
// "tasks/workers" would otherwise collide on the same key.
type groupRef struct {
	queueName string
	groupID   string
}

// ConsumerGroupStateStore manages consumer group state persistence.
//
// Two locks, deliberately: mu guards the maps below and is never held across
// disk I/O, while writeLocks serialises writers of one group's file, which
// share a temp path. Scoping them this way keeps one group's file write off
// every other group's path.
type ConsumerGroupStateStore struct {
	mu sync.RWMutex

	dir    string
	groups map[string]map[string]*types.ConsumerGroup // queueName -> groupID -> state
	dirty  map[groupRef]bool

	writeLocks keylock.Sharded
}

const consumerGroupVersion uint8 = 2

type consumerGroupWrapper struct {
	Version uint8           `json:"version"`
	State   json.RawMessage `json:"state"`
	SavedAt int64           `json:"saved_at"`
}

// NewConsumerGroupStateStore creates or opens a consumer group state store.
func NewConsumerGroupStateStore(baseDir string) (*ConsumerGroupStateStore, error) {
	dir := filepath.Join(baseDir, "groups")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("failed to create groups directory: %w", err)
	}

	store := &ConsumerGroupStateStore{
		dir:    dir,
		groups: make(map[string]map[string]*types.ConsumerGroup),
		dirty:  make(map[groupRef]bool),
	}

	if err := store.loadAll(); err != nil {
		return nil, fmt.Errorf("failed to load consumer groups: %w", err)
	}

	return store, nil
}

func decodeConsumerGroupState(data []byte) (*types.ConsumerGroup, bool, error) {
	var wrapper consumerGroupWrapper
	if err := json.Unmarshal(data, &wrapper); err != nil {
		return nil, false, err
	}
	if wrapper.Version > consumerGroupVersion {
		return nil, false, fmt.Errorf("unsupported consumer group version: %d", wrapper.Version)
	}

	rawState := bytes.TrimSpace(wrapper.State)
	if len(rawState) == 0 || bytes.Equal(rawState, []byte("null")) {
		return nil, false, nil
	}

	var state types.ConsumerGroup
	if err := json.Unmarshal(rawState, &state); err != nil {
		return nil, false, err
	}

	hasAutoCommit := false
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(rawState, &fields); err == nil {
		if _, ok := fields["AutoCommit"]; ok {
			hasAutoCommit = true
		} else if _, ok := fields["autoCommit"]; ok {
			hasAutoCommit = true
		} else if _, ok := fields["auto_commit"]; ok {
			hasAutoCommit = true
		}
	}

	return &state, hasAutoCommit, nil
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
		state, hasAutoCommit, err := decodeConsumerGroupState(data)
		if err != nil {
			return nil
		}
		if state == nil {
			return nil
		}

		// Ensure maps are initialized
		if state.Cursor == nil {
			state.Cursor = &types.QueueCursor{}
		}
		if state.Mode == "" {
			state.Mode = types.GroupModeQueue
		}
		if !hasAutoCommit {
			state.AutoCommit = true
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
			groups = make(map[string]*types.ConsumerGroup)
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

// encodePathComponent makes one name safe as a single path element.
//
// Queue names contain slashes: "$dlq/tasks" is what the dead-letter path
// creates. Joining them straight into a path made ("$dlq/tasks", "workers") and
// ("$dlq", "tasks/workers") the same file, so one group's state overwrote the
// other's. Percent-encoding the separator keeps each name in one element and
// leaves names without one — the overwhelming majority — byte for byte as they
// were. The dot cases are handled so a name can never resolve to a parent
// directory.
func encodePathComponent(name string) string {
	switch name {
	case ".":
		return "%2E"
	case "..":
		return "%2E%2E"
	}
	if !strings.ContainsAny(name, `%/\`) {
		return name
	}

	var encoded strings.Builder
	encoded.Grow(len(name) + 8)
	for i := 0; i < len(name); i++ {
		switch c := name[i]; c {
		case '%', '/', '\\':
			encoded.WriteString("%")
			encoded.WriteString(strings.ToUpper(strconv.FormatUint(uint64(c), 16)))
		default:
			encoded.WriteByte(c)
		}
	}
	return encoded.String()
}

// contained returns path only when it stays inside the store directory.
//
// Group IDs come from clients and queue names from configuration, and neither
// carries a containment contract: a group named ".." joined into a path
// resolves outside the store, and Delete would then remove a file belonging to
// something else entirely. Canonical paths are encoded and cannot escape, but
// they are checked here too rather than trusted, so containment holds no matter
// which path construction a caller used.
func (s *ConsumerGroupStateStore) contained(path string) (string, bool) {
	relative, err := filepath.Rel(s.dir, path)
	if err != nil {
		return "", false
	}
	if relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
		return "", false
	}
	return path, true
}

// groupPath returns the path to a group's state file.
func (s *ConsumerGroupStateStore) groupPath(queueName, groupID string) string {
	return filepath.Join(s.dir, encodePathComponent(queueName), encodePathComponent(groupID)+".json")
}

// legacyGroupPath is where a group's file lived before names were encoded.
//
// Loading walks the directory and takes identity from file contents, so old
// files are still read wherever they sit. Delete has to know about them anyway:
// a stale file left behind would be loaded again on the next start and
// resurrect a group that was removed.
func (s *ConsumerGroupStateStore) legacyGroupPath(queueName, groupID string) string {
	return filepath.Join(s.dir, queueName, groupID+".json")
}

// Save persists a consumer group state.
func (s *ConsumerGroupStateStore) Save(state *types.ConsumerGroup) error {
	ref := groupRef{queueName: state.QueueName, groupID: state.ID}

	s.mu.Lock()
	groups, ok := s.groups[ref.queueName]
	if !ok {
		groups = make(map[string]*types.ConsumerGroup)
		s.groups[ref.queueName] = groups
	}
	groups[ref.groupID] = state
	s.dirty[ref] = true
	s.mu.Unlock()

	return s.flush(ref, state)
}

// flush writes one group to disk outside the map lock.
//
// The dirty flag is cleared before the write rather than after: a mutation
// arriving mid-write must leave the group dirty for the next Sync, and clearing
// afterwards would discard it. A failed write marks it dirty again so the state
// is not silently dropped.
func (s *ConsumerGroupStateStore) flush(ref groupRef, state *types.ConsumerGroup) error {
	writeLock := s.writeLocks.KeyPair(ref.queueName, ref.groupID)
	writeLock.Lock()
	defer writeLock.Unlock()

	s.mu.Lock()
	delete(s.dirty, ref)
	s.mu.Unlock()

	if err := s.writeGroup(ref, state); err != nil {
		s.mu.Lock()
		s.dirty[ref] = true
		s.mu.Unlock()
		return err
	}

	return nil
}

// writeGroup encodes and replaces one group's file. Callers hold the group's
// write lock; the map lock must not be held.
func (s *ConsumerGroupStateStore) writeGroup(ref groupRef, state *types.ConsumerGroup) error {
	path, ok := s.contained(s.groupPath(ref.queueName, ref.groupID))
	if !ok {
		return fmt.Errorf("consumer group %q/%q resolves outside the group directory", ref.queueName, ref.groupID)
	}

	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("failed to create consumer group directory: %w", err)
	}

	wrapper := struct {
		Version uint8                `json:"version"`
		State   *types.ConsumerGroup `json:"state"`
		SavedAt int64                `json:"saved_at"`
	}{
		Version: consumerGroupVersion,
		State:   state,
		SavedAt: time.Now().UnixMilli(),
	}

	// Compact rather than indented: this file is rewritten on every flush and
	// carries the whole pending entry list, so the indentation is write
	// amplification on a hot path, not readability anyone relies on.
	data, err := json.Marshal(wrapper)
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

	return nil
}

// Get retrieves a consumer group state.
func (s *ConsumerGroupStateStore) Get(queueName, groupID string) (*types.ConsumerGroup, error) {
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
	groups, ok := s.groups[queueName]
	if ok {
		delete(groups, groupID)
		if len(groups) == 0 {
			delete(s.groups, queueName)
		}
	}

	delete(s.dirty, groupRef{queueName: queueName, groupID: groupID})
	s.mu.Unlock()

	writeLock := s.writeLocks.KeyPair(queueName, groupID)
	writeLock.Lock()
	defer writeLock.Unlock()

	for _, candidate := range []string{s.groupPath(queueName, groupID), s.legacyGroupPath(queueName, groupID)} {
		path, ok := s.contained(candidate)
		if !ok {
			continue
		}
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			return err
		}
	}

	// Clean up empty queue directories, canonical and legacy alike.
	for _, candidate := range []string{
		filepath.Join(s.dir, encodePathComponent(queueName)),
		filepath.Join(s.dir, queueName),
	} {
		dir, ok := s.contained(candidate)
		if !ok || dir == s.dir {
			continue
		}
		entries, err := os.ReadDir(dir)
		if err == nil && len(entries) == 0 {
			os.Remove(dir)
		}
	}

	return nil
}

// List returns all consumer groups for a queue.
func (s *ConsumerGroupStateStore) List(queueName string) ([]*types.ConsumerGroup, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	groups, ok := s.groups[queueName]
	if !ok {
		return []*types.ConsumerGroup{}, nil
	}

	result := make([]*types.ConsumerGroup, 0, len(groups))
	for _, state := range groups {
		result = append(result, state)
	}

	return result, nil
}

// ListAll returns all consumer groups across all queues.
func (s *ConsumerGroupStateStore) ListAll() ([]*types.ConsumerGroup, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var result []*types.ConsumerGroup
	for _, groups := range s.groups {
		for _, state := range groups {
			result = append(result, state)
		}
	}

	return result, nil
}

// Sync saves all dirty states to disk.
//
// The dirty set names the queue and group directly, so resolving it is a map
// lookup rather than a scan over every group in the process. The writes happen
// after the map lock is released.
func (s *ConsumerGroupStateStore) Sync() error {
	s.mu.RLock()
	pending := make(map[groupRef]*types.ConsumerGroup, len(s.dirty))
	for ref := range s.dirty {
		groups, ok := s.groups[ref.queueName]
		if !ok {
			continue
		}
		if state, ok := groups[ref.groupID]; ok {
			pending[ref] = state
		}
	}
	s.mu.RUnlock()

	var errs []error
	for ref, state := range pending {
		if err := s.flush(ref, state); err != nil {
			errs = append(errs, err)
		}
	}

	return errors.Join(errs...)
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
func (s *ConsumerGroupStateStore) CreateIfNotExists(state *types.ConsumerGroup) error {
	ref := groupRef{queueName: state.QueueName, groupID: state.ID}

	s.mu.Lock()
	groups, ok := s.groups[ref.queueName]
	if !ok {
		groups = make(map[string]*types.ConsumerGroup)
		s.groups[ref.queueName] = groups
	}
	if _, exists := groups[ref.groupID]; exists {
		s.mu.Unlock()
		return nil
	}
	groups[ref.groupID] = state
	s.dirty[ref] = true
	s.mu.Unlock()

	return s.flush(ref, state)
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

	// Through the group's own lock: GetCursor hands out the live pointer, and
	// writing it here would race everything reading the group.
	state.SetCursor(cursor, committed)

	s.dirty[groupRef{queueName: queueName, groupID: groupID}] = true

	return nil
}

// GetCursor retrieves cursor state for a queue.
//
// The cursor is returned by value: handing out the group's live pointer let
// callers write group state without the group's lock.
func (s *ConsumerGroupStateStore) GetCursor(queueName, groupID string) (types.QueueCursor, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	groups, ok := s.groups[queueName]
	if !ok {
		return types.QueueCursor{}, ErrGroupNotFound
	}

	state, ok := groups[groupID]
	if !ok {
		return types.QueueCursor{}, ErrGroupNotFound
	}

	return state.CursorView(), nil
}
