// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package logstorage

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/absmach/fluxmq/queue/types"
)

// QueueConfigStore manages queue configuration persistence.
type QueueConfigStore struct {
	mu sync.RWMutex

	dir     string
	configs map[string]*types.QueueConfig
	dirty   bool
}

// QueueConfigState is the serialized state of all queue configs.
type QueueConfigState struct {
	Version uint8               `json:"version"`
	Configs []types.QueueConfig `json:"configs"`
	SavedAt int64               `json:"saved_at"`
}

const (
	queueConfigVersion uint8 = 2
	queueConfigFile          = "queues.json"
)

// NewQueueConfigStore creates or opens a queue config store.
func NewQueueConfigStore(baseDir string) (*QueueConfigStore, error) {
	dir := filepath.Join(baseDir, "config")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("failed to create config directory: %w", err)
	}

	store := &QueueConfigStore{
		dir:     dir,
		configs: make(map[string]*types.QueueConfig),
	}

	if err := store.load(); err != nil && !os.IsNotExist(err) {
		return nil, fmt.Errorf("failed to load queue configs: %w", err)
	}

	return store, nil
}

// configPath returns the path to the config file.
func (s *QueueConfigStore) configPath() string {
	return filepath.Join(s.dir, queueConfigFile)
}

// load loads queue configs from disk.
func (s *QueueConfigStore) load() error {
	data, err := os.ReadFile(s.configPath())
	if err != nil {
		return err
	}

	var state QueueConfigState
	if err := json.Unmarshal(data, &state); err != nil {
		return fmt.Errorf("failed to unmarshal queue config state: %w", err)
	}

	if state.Version > queueConfigVersion {
		return fmt.Errorf("unsupported queue config version: %d", state.Version)
	}

	s.configs = make(map[string]*types.QueueConfig)
	for i := range state.Configs {
		s.configs[state.Configs[i].Name] = &state.Configs[i]
	}

	return nil
}

// Save persists a queue config.
func (s *QueueConfigStore) Save(config types.QueueConfig) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.configs[config.Name] = &config
	s.dirty = true

	return s.saveUnlocked()
}

// Get retrieves a queue config.
func (s *QueueConfigStore) Get(queueName string) (*types.QueueConfig, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	config, ok := s.configs[queueName]
	if !ok {
		return nil, ErrQueueNotFound
	}

	return config, nil
}

// Delete removes a queue config.
func (s *QueueConfigStore) Delete(queueName string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.configs, queueName)
	s.dirty = true

	return s.saveUnlocked()
}

// List returns all queue configs.
func (s *QueueConfigStore) List() ([]types.QueueConfig, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	configs := make([]types.QueueConfig, 0, len(s.configs))
	for _, config := range s.configs {
		configs = append(configs, *config)
	}

	return configs, nil
}

// Sync saves any dirty state to disk.
func (s *QueueConfigStore) Sync() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.dirty {
		return nil
	}

	return s.saveUnlocked()
}

// saveUnlocked saves configs to disk (must hold lock).
func (s *QueueConfigStore) saveUnlocked() error {
	configs := make([]types.QueueConfig, 0, len(s.configs))
	for _, config := range s.configs {
		configs = append(configs, *config)
	}

	state := QueueConfigState{
		Version: queueConfigVersion,
		Configs: configs,
	}

	data, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal queue config state: %w", err)
	}

	tempPath := s.configPath() + TempExtension
	if err := os.WriteFile(tempPath, data, 0o644); err != nil {
		return fmt.Errorf("failed to write queue config file: %w", err)
	}

	if err := os.Rename(tempPath, s.configPath()); err != nil {
		os.Remove(tempPath)
		return fmt.Errorf("failed to rename queue config file: %w", err)
	}

	s.dirty = false

	return nil
}

// Close closes the store.
func (s *QueueConfigStore) Close() error {
	return s.Sync()
}
