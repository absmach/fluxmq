// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package logstorage

import (
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const checkpointGroup = "checkpoint-group"

func openConsumerState(t *testing.T, dir string) *ConsumerState {
	t.Helper()
	config := DefaultConsumerStateConfig()
	config.CompactThreshold = 1 << 30 // compactions only when a test asks
	cs, err := NewConsumerState(dir, checkpointGroup, config)
	require.NoError(t, err)
	return cs
}

// crash drops the state as a killed process would: the operation log's file
// is closed, nothing else is flushed or compacted.
func crash(t *testing.T, cs *ConsumerState) {
	t.Helper()
	require.NoError(t, cs.opLog.Close())
}

func pendingOffsets(cs *ConsumerState) map[uint64]uint16 {
	out := make(map[uint64]uint16)
	for shard := range cs.NumShards() {
		for _, entry := range cs.GetPendingByShard(shard) {
			out[entry.Offset] = entry.DeliveryCount
		}
	}
	return out
}

// Every point at which compaction can be interrupted recovers the state the
// group had: nothing lost, and no logged operation applied twice.
func TestCompactionRecoversAtEveryCrashPoint(t *testing.T) {
	tests := []struct {
		name  string
		steps func(t *testing.T, cs *ConsumerState)
	}{
		{"before_checkpoint", func(*testing.T, *ConsumerState) {}},
		{"after_checkpoint_before_log_reset", func(t *testing.T, cs *ConsumerState) {
			cs.mu.Lock()
			defer cs.mu.Unlock()
			require.NoError(t, cs.writeCheckpointLocked(time.Now().UnixMilli()))
		}},
		{"after_log_reset", func(t *testing.T, cs *ConsumerState) {
			require.NoError(t, cs.Compact())
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			cs := openConsumerState(t, dir)
			require.NoError(t, cs.DeliverBatch([]uint64{1, 2, 3}, "c"))
			require.NoError(t, cs.Ack(1))
			want := pendingOffsets(cs)
			wantDelivered := cs.GetGroupState().Delivered

			tt.steps(t, cs)
			crash(t, cs)

			recovered := openConsumerState(t, dir)
			assert.Equal(t, want, pendingOffsets(recovered), "pending entries or their delivery counts changed")
			assert.Equal(t, uint64(4), recovered.GetGroupState().Cursor)
			assert.Equal(t, wantDelivered, recovered.GetGroupState().Delivered, "logged operations were applied twice")

			// What is logged after recovery must survive the next crash too:
			// a log the checkpoint already covered is not appended to.
			require.NoError(t, recovered.Deliver(4, "c"))
			crash(t, recovered)
			again := openConsumerState(t, dir)
			defer again.Close()
			assert.Contains(t, pendingOffsets(again), uint64(4))
			assert.Len(t, pendingOffsets(again), 3)
		})
	}
}

// A checkpoint that exists but cannot be read must stop the group from
// opening: replaying the log over empty state would drop every pending entry
// the checkpoint held.
func TestUnreadableCheckpointFailsOpen(t *testing.T) {
	dir := t.TempDir()
	cs := openConsumerState(t, dir)
	require.NoError(t, cs.Deliver(1, "c"))
	require.NoError(t, cs.Compact())
	require.NoError(t, cs.Close())

	path := filepath.Join(dir, checkpointGroup, consumerCheckpointName)
	require.NoError(t, os.WriteFile(path, []byte(`{"version":1,"pending":[`), 0o644))

	_, err := NewConsumerState(dir, checkpointGroup, DefaultConsumerStateConfig())
	require.Error(t, err)
}

// A group written before checkpoints existed loads from its snapshot files
// and headerless log, and its first compaction replaces them.
func TestLegacySnapshotsMigrateToCheckpoint(t *testing.T) {
	dir := t.TempDir()
	groupDir := filepath.Join(dir, encodePathComponent(checkpointGroup))
	require.NoError(t, os.MkdirAll(groupDir, 0o755))

	writeJSON := func(name string, v any) {
		data, err := json.Marshal(v)
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(filepath.Join(groupDir, name), data, 0o644))
	}
	writeJSON("state.json", StateSnapshot{Version: 7, GroupID: checkpointGroup, State: &GroupState{Cursor: 11, Delivered: 11}})
	writeJSON("pel_2.json", PELSnapshot{Version: 7, ShardID: 2, Entries: map[uint64]*PendingEntry{
		10: {Offset: 10, ConsumerID: "c", DeliveryCount: 2},
	}})
	op, err := json.Marshal(Operation{Type: OpDeliver, Offset: 11, ConsumerID: "c", Timestamp: 1})
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(groupDir, consumerOpLogName), append(op, '\n'), 0o644))

	cs := openConsumerState(t, dir)
	assert.Equal(t, map[uint64]uint16{10: 2, 11: 1}, pendingOffsets(cs))
	require.NoError(t, cs.Compact())

	for _, legacy := range []string{"state.json", "pel_2.json"} {
		_, err := os.Stat(filepath.Join(groupDir, legacy))
		assert.ErrorIs(t, err, os.ErrNotExist, "%s was not removed", legacy)
	}
	crash(t, cs)

	reopened := openConsumerState(t, dir)
	defer reopened.Close()
	assert.Equal(t, map[uint64]uint16{10: 2, 11: 1}, pendingOffsets(reopened))
	assert.Equal(t, uint64(12), reopened.GetGroupState().Cursor)
}

// Operations past the compaction threshold schedule at most one compaction
// between them; each used to start a goroutine that waited for the lock.
func TestCompactionIsScheduledOnce(t *testing.T) {
	config := DefaultConsumerStateConfig()
	config.CompactThreshold = 1
	cs, err := NewConsumerState(t.TempDir(), checkpointGroup, config)
	require.NoError(t, err)
	t.Cleanup(func() { _ = cs.Close() })

	before := runtime.NumGoroutine()
	cs.mu.Lock()
	for i := range uint64(200) {
		require.NoError(t, cs.writeOp(&Operation{Type: OpSetCursor, Offset: i, Timestamp: 1}))
	}
	waiting := runtime.NumGoroutine() - before
	cs.mu.Unlock()

	assert.LessOrEqual(t, waiting, 1, "compactions queued behind the lock")
}

// A log that follows a checkpoint but has lost its header, emptied or gone,
// must not take later operations without one: they would read as older than
// the checkpoint and be skipped at the next recovery.
func TestOpLogWithoutHeaderAfterCheckpointKeepsLaterOperations(t *testing.T) {
	tests := []struct {
		name   string
		damage func(path string) error
	}{
		{"emptied", func(path string) error { return os.Truncate(path, 0) }},
		{"removed", os.Remove},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			cs := openConsumerState(t, dir)
			require.NoError(t, cs.Deliver(1, "c"))
			require.NoError(t, cs.Compact())
			crash(t, cs)
			require.NoError(t, tt.damage(filepath.Join(dir, checkpointGroup, consumerOpLogName)))

			recovered := openConsumerState(t, dir)
			require.NoError(t, recovered.Deliver(2, "c"))
			crash(t, recovered)

			again := openConsumerState(t, dir)
			defer again.Close()
			assert.Equal(t, map[uint64]uint16{1: 1, 2: 1}, pendingOffsets(again))
		})
	}
}

// A header that cannot be read fails the open rather than reading as a log
// older than the checkpoint, which would skip everything in it.
func TestUnreadableOpLogHeaderFailsOpen(t *testing.T) {
	for name, content := range map[string]string{
		"torn":      `{"generation":1`,
		"malformed": "{\"generation\":x}\n",
	} {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			cs := openConsumerState(t, dir)
			require.NoError(t, cs.Deliver(1, "c"))
			require.NoError(t, cs.Compact())
			crash(t, cs)
			require.NoError(t, os.WriteFile(filepath.Join(dir, checkpointGroup, consumerOpLogName), []byte(content), 0o644))

			_, err := NewConsumerState(dir, checkpointGroup, DefaultConsumerStateConfig())
			require.Error(t, err)
		})
	}
}

func TestCheckpointWithoutVersionFailsOpen(t *testing.T) {
	dir := t.TempDir()
	cs := openConsumerState(t, dir)
	require.NoError(t, cs.Close())
	require.NoError(t, os.WriteFile(filepath.Join(dir, checkpointGroup, consumerCheckpointName),
		[]byte(`{"generation":1,"pending":[]}`), 0o644))

	_, err := NewConsumerState(dir, checkpointGroup, DefaultConsumerStateConfig())
	require.Error(t, err)
}

// A compaction that was waiting for the lock when the state closed does
// nothing: the log it would reset is already closed.
func TestCompactAfterCloseDoesNothing(t *testing.T) {
	dir := t.TempDir()
	cs := openConsumerState(t, dir)
	require.NoError(t, cs.Deliver(1, "c"))
	require.NoError(t, cs.Close())

	before, err := os.ReadFile(filepath.Join(dir, checkpointGroup, consumerCheckpointName))
	require.NoError(t, err)
	require.NoError(t, cs.Compact())
	after, err := os.ReadFile(filepath.Join(dir, checkpointGroup, consumerCheckpointName))
	require.NoError(t, err)
	assert.Equal(t, before, after)
}
