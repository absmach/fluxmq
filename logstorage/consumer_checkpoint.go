// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package logstorage

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
)

// A consumer group's state is a checkpoint plus the operations logged since.
//
// The checkpoint holds the group state and every pending entry in one file,
// so replacing it is a single atomic rename; it is synced, renamed into place,
// and its directory synced before the operation log is cut back. Each
// checkpoint carries a generation, and the operation log starts with a header
// naming the generation its operations follow. Recovery replays the log only
// when the two match: a crash after a checkpoint is published but before the
// log is reset leaves a log that the checkpoint already contains, and
// replaying it again would count deliveries twice.
//
// Groups written before checkpoints existed keep state.json and one
// pel_<shard>.json per shard, and an operation log without a header
// (generation zero). They load as before; their first compaction writes a
// checkpoint and then removes them.

const (
	consumerCheckpointName    = "checkpoint.json"
	consumerCheckpointVersion = 1
)

type consumerCheckpoint struct {
	Version    uint8           `json:"version"`
	Generation uint64          `json:"generation"`
	GroupID    string          `json:"group_id"`
	State      *GroupState     `json:"state"`
	Pending    []*PendingEntry `json:"pending"`
	SavedAt    int64           `json:"saved_at"`
}

// opLogHeader is the first line of an operation log written after a
// checkpoint.
type opLogHeader struct {
	Generation uint64 `json:"generation"`
}

var opLogHeaderPrefix = []byte(`{"generation":`)

func (cs *ConsumerState) checkpointPath() string {
	return filepath.Join(cs.dir, consumerCheckpointName)
}

// loadCheckpoint loads the checkpoint if there is one, and reports whether it
// did. A checkpoint that exists but cannot be read is an error: skipping it
// would replay the operation log over empty state and lose every pending
// entry it held.
func (cs *ConsumerState) loadCheckpoint() (bool, error) {
	data, err := os.ReadFile(cs.checkpointPath())
	if errors.Is(err, fs.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("read consumer checkpoint: %w", err)
	}

	var cp consumerCheckpoint
	if err := json.Unmarshal(data, &cp); err != nil {
		return false, fmt.Errorf("decode consumer checkpoint %s: %w", cs.checkpointPath(), err)
	}
	if cp.Version == 0 || cp.Version > consumerCheckpointVersion {
		return false, fmt.Errorf("consumer checkpoint %s: unsupported version %d", cs.checkpointPath(), cp.Version)
	}

	if cp.State != nil {
		cs.state = cp.State
	}
	cs.generation = cp.Generation
	for _, entry := range cp.Pending {
		if entry != nil {
			cs.pelShards[cs.ShardKey(entry.Offset)].add(entry)
		}
	}
	return true, nil
}

// writeCheckpointLocked durably replaces the checkpoint with the current
// state under the next generation. Caller must hold cs.mu.
func (cs *ConsumerState) writeCheckpointLocked(savedAt int64) error {
	cp := consumerCheckpoint{
		Version:    consumerCheckpointVersion,
		Generation: cs.generation + 1,
		GroupID:    cs.groupID,
		State:      cs.state,
		SavedAt:    savedAt,
	}
	for _, shard := range cs.pelShards {
		shard.mu.RLock()
		for _, entry := range shard.entries {
			cp.Pending = append(cp.Pending, entry)
		}
		shard.mu.RUnlock()
	}

	data, err := json.Marshal(cp)
	if err != nil {
		return fmt.Errorf("encode consumer checkpoint: %w", err)
	}
	path := cs.checkpointPath()
	tempPath := path + TempExtension
	if err := WriteFileSynced(tempPath, data, 0o644); err != nil {
		return fmt.Errorf("write consumer checkpoint: %w", err)
	}
	if err := os.Rename(tempPath, path); err != nil {
		_ = os.Remove(tempPath)
		return fmt.Errorf("publish consumer checkpoint: %w", err)
	}
	if err := SyncDir(cs.dir); err != nil {
		return fmt.Errorf("sync consumer checkpoint directory: %w", err)
	}
	cs.generation = cp.Generation
	return nil
}

// resetOpLogLocked replaces the operation log with one that holds only a
// header for the current generation. The replacement is written beside the
// log, synced, and renamed over it, so a crash leaves either the old log or
// the new one, never an empty or half-written header that later operations
// would be appended to. Caller must hold cs.mu.
func (cs *ConsumerState) resetOpLogLocked() error {
	header, err := json.Marshal(opLogHeader{Generation: cs.generation})
	if err != nil {
		return err
	}
	tempPath := cs.opLogPath + TempExtension
	if err := WriteFileSynced(tempPath, append(header, '\n'), 0o644); err != nil {
		return fmt.Errorf("write operation log header: %w", err)
	}
	if err := os.Rename(tempPath, cs.opLogPath); err != nil {
		_ = os.Remove(tempPath)
		return fmt.Errorf("replace operation log: %w", err)
	}
	if err := SyncDir(cs.dir); err != nil {
		return fmt.Errorf("sync operation log directory: %w", err)
	}
	reopened, err := os.OpenFile(cs.opLogPath, os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return fmt.Errorf("reopen operation log: %w", err)
	}
	_ = cs.opLog.Close()
	cs.opLog = reopened
	return nil
}

// removeLegacySnapshots deletes the pre-checkpoint snapshot files once a
// checkpoint has replaced them.
func (cs *ConsumerState) removeLegacySnapshots() error {
	paths := []string{cs.statePath()}
	for i := range cs.config.NumPELShards {
		paths = append(paths, cs.pelPath(i))
	}
	for _, path := range paths {
		if err := os.Remove(path); err != nil && !errors.Is(err, fs.ErrNotExist) {
			return err
		}
	}
	return SyncDir(cs.dir)
}

// splitOpLogHeader returns the generation an operation log's operations
// follow, and the operations. A log without a header predates checkpoints
// and follows generation zero. A header that cannot be read is an error: read
// as no header, it would make the log look older than any checkpoint and
// every operation in it would be skipped.
func splitOpLogHeader(data []byte) (uint64, []byte, error) {
	if !bytes.HasPrefix(data, opLogHeaderPrefix) {
		return 0, data, nil
	}
	line, rest, complete := bytes.Cut(data, []byte{'\n'})
	var header opLogHeader
	if !complete || json.Unmarshal(line, &header) != nil {
		return 0, nil, fmt.Errorf("operation log header %q is unreadable", line)
	}
	return header.Generation, rest, nil
}

// add puts a pending entry into the shard and its consumer index.
func (s *PELShard) add(entry *PendingEntry) {
	s.entries[entry.Offset] = entry
	if s.byConsumer[entry.ConsumerID] == nil {
		s.byConsumer[entry.ConsumerID] = make(map[uint64]struct{})
	}
	s.byConsumer[entry.ConsumerID][entry.Offset] = struct{}{}
}
