// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package logstorage

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/absmach/fluxmq/queue/types"
)

// QueueConfigStore manages queue configuration persistence.
//
// Each queue's configuration is its own file below config/queues, named by
// encodePathComponent, so creating, updating or deleting one queue writes one
// small file however many queues exist. Every change is durable when it
// returns: the file is synced, renamed into place, and its directory synced.
// It is safe for concurrent use.
type QueueConfigStore struct {
	mu sync.RWMutex

	dir     string
	configs map[string]*types.QueueConfig
}

// queueConfigRecord is the on-disk form of one queue's configuration.
type queueConfigRecord struct {
	Version uint8             `json:"version"`
	Config  types.QueueConfig `json:"config"`
}

// QueueConfigState is the legacy catalog that held every queue's
// configuration in one file. It is read once, to migrate it.
type QueueConfigState struct {
	Version uint8               `json:"version"`
	Configs []types.QueueConfig `json:"configs"`
	SavedAt int64               `json:"saved_at"`
}

const (
	queueConfigVersion uint8 = 2
	queueConfigsDir          = "queues"
	queueConfigExt           = ".json"
	legacyCatalogFile        = "queues.json"
)

// NewQueueConfigStore creates or opens a queue config store.
func NewQueueConfigStore(baseDir string) (*QueueConfigStore, error) {
	dir := filepath.Join(baseDir, "config", queueConfigsDir)
	if err := MkdirAllSynced(dir, 0o755); err != nil {
		return nil, fmt.Errorf("failed to create config directory: %w", err)
	}

	store := &QueueConfigStore{
		dir:     dir,
		configs: make(map[string]*types.QueueConfig),
	}
	if err := store.migrateLegacyCatalog(); err != nil {
		return nil, fmt.Errorf("failed to migrate queue config catalog: %w", err)
	}
	if err := store.load(); err != nil {
		return nil, fmt.Errorf("failed to load queue configs: %w", err)
	}

	return store, nil
}

func (s *QueueConfigStore) recordPath(queueName string) string {
	return filepath.Join(s.dir, encodePathComponent(queueName)+queueConfigExt)
}

// load reads every queue configuration file.
func (s *QueueConfigStore) load() error {
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		return err
	}

	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, queueConfigExt) {
			continue
		}
		data, err := os.ReadFile(filepath.Join(s.dir, name))
		if err != nil {
			return err
		}
		var record queueConfigRecord
		if err := json.Unmarshal(data, &record); err != nil {
			return fmt.Errorf("queue config %s: %w", name, err)
		}
		if record.Version > queueConfigVersion {
			return fmt.Errorf("queue config %s: unsupported version %d", name, record.Version)
		}
		// The file must be the one this name is stored under, so a stray or
		// renamed file cannot shadow another queue.
		if encodePathComponent(record.Config.Name)+queueConfigExt != name {
			return fmt.Errorf("queue config %s holds queue %q", name, record.Config.Name)
		}
		config := record.Config
		s.configs[config.Name] = &config
	}
	return nil
}

// migrateLegacyCatalog splits the single-file catalog into per-queue files.
// The catalog is removed only after every file it held is durable, so an
// interrupted migration repeats from the catalog on the next open.
func (s *QueueConfigStore) migrateLegacyCatalog() error {
	legacyPath := filepath.Join(filepath.Dir(s.dir), legacyCatalogFile)
	data, err := os.ReadFile(legacyPath)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
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
	for _, config := range state.Configs {
		if err := s.writeRecord(config); err != nil {
			return err
		}
	}
	if err := os.Remove(legacyPath); err != nil {
		return err
	}
	return SyncDir(filepath.Dir(s.dir))
}

// writeRecord durably replaces one queue's configuration file.
func (s *QueueConfigStore) writeRecord(config types.QueueConfig) error {
	data, err := json.MarshalIndent(queueConfigRecord{Version: queueConfigVersion, Config: config}, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal queue config %q: %w", config.Name, err)
	}

	// Queue metadata must reach disk before the queue it describes can be used
	// to acknowledge a publisher: an atomic rename makes the replacement
	// all-or-nothing, but not durable. Sync the replacement, then the directory
	// that now names it.
	path := s.recordPath(config.Name)
	tempPath := path + TempExtension
	if err := WriteFileSynced(tempPath, data, 0o644); err != nil {
		return fmt.Errorf("failed to write queue config %q: %w", config.Name, err)
	}
	if err := os.Rename(tempPath, path); err != nil {
		os.Remove(tempPath)
		return fmt.Errorf("failed to rename queue config %q: %w", config.Name, err)
	}
	if err := SyncDir(s.dir); err != nil {
		return fmt.Errorf("failed to sync queue config directory: %w", err)
	}
	return nil
}

// Save persists a queue config. The stored configuration changes only once it
// is durable.
func (s *QueueConfigStore) Save(config types.QueueConfig) error {
	if err := validateStorageName(config.Name); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	stored := config.Clone()
	if err := s.writeRecord(stored); err != nil {
		return err
	}
	s.configs[config.Name] = &stored
	return nil
}

// Get retrieves a queue config.
func (s *QueueConfigStore) Get(queueName string) (*types.QueueConfig, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	stored, ok := s.configs[queueName]
	if !ok {
		return nil, ErrQueueNotFound
	}

	config := stored.Clone()
	return &config, nil
}

// Delete removes a queue config.
func (s *QueueConfigStore) Delete(queueName string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.configs[queueName]; !ok {
		return nil
	}
	if err := os.Remove(s.recordPath(queueName)); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("failed to remove queue config %q: %w", queueName, err)
	}
	if err := SyncDir(s.dir); err != nil {
		return fmt.Errorf("failed to sync queue config directory: %w", err)
	}
	delete(s.configs, queueName)
	return nil
}

// List returns all queue configs.
func (s *QueueConfigStore) List() ([]types.QueueConfig, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	configs := make([]types.QueueConfig, 0, len(s.configs))
	for _, config := range s.configs {
		configs = append(configs, config.Clone())
	}

	return configs, nil
}

// Sync is retained for callers of the earlier buffered catalog. Every change
// is already durable when it returns, so there is nothing left to write.
func (s *QueueConfigStore) Sync() error {
	return nil
}

// Close closes the store.
func (s *QueueConfigStore) Close() error {
	return nil
}
