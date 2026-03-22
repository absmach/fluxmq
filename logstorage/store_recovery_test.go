// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package logstorage

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewStore_RecoverOnStartup_CorruptedSegment(t *testing.T) {
	dir := t.TempDir()

	// Phase 1: create a store and write some data.
	cfg := DefaultStoreConfig()
	store, err := NewStore(dir, cfg)
	require.NoError(t, err)

	require.NoError(t, store.CreateQueue("q1"))
	_, err = store.Append("q1", []byte("hello"), nil, map[string][]byte{"_topic": []byte("t")})
	require.NoError(t, err)
	_, err = store.Append("q1", []byte("world"), nil, map[string][]byte{"_topic": []byte("t")})
	require.NoError(t, err)
	require.NoError(t, store.Close())

	// Phase 2: corrupt the segment by appending garbage.
	segDir := filepath.Join(dir, "queues", "q1", "segments")
	segFiles, _ := filepath.Glob(filepath.Join(segDir, "*"+SegmentExtension))
	require.NotEmpty(t, segFiles)

	f, err := os.OpenFile(segFiles[0], os.O_WRONLY|os.O_APPEND, 0o644)
	require.NoError(t, err)
	_, err = f.WriteString("CORRUPTION")
	require.NoError(t, err)
	require.NoError(t, f.Close())

	// Phase 3: open with recovery enabled — should truncate corruption.
	var logMessages []string
	recoverCfg := DefaultStoreConfig()
	recoverCfg.RecoverOnStartup = true
	recoverCfg.RecoveryLogger = func(msg string, args ...any) {
		logMessages = append(logMessages, msg)
	}

	store2, err := NewStore(dir, recoverCfg)
	require.NoError(t, err)
	defer store2.Close()

	// Valid messages should still be readable.
	msg, err := store2.Read("q1", 0)
	require.NoError(t, err)
	assert.Equal(t, []byte("hello"), msg.Value)

	msg, err = store2.Read("q1", 1)
	require.NoError(t, err)
	assert.Equal(t, []byte("world"), msg.Value)
}

func TestNewStore_RecoverOnStartup_CleanSegments(t *testing.T) {
	dir := t.TempDir()

	// Create a store and write data normally.
	cfg := DefaultStoreConfig()
	store, err := NewStore(dir, cfg)
	require.NoError(t, err)

	require.NoError(t, store.CreateQueue("q1"))
	_, err = store.Append("q1", []byte("ok"), nil, map[string][]byte{"_topic": []byte("t")})
	require.NoError(t, err)
	require.NoError(t, store.Close())

	// Open with recovery — nothing to repair.
	recoverCfg := DefaultStoreConfig()
	recoverCfg.RecoverOnStartup = true

	store2, err := NewStore(dir, recoverCfg)
	require.NoError(t, err)
	defer store2.Close()

	msg, err := store2.Read("q1", 0)
	require.NoError(t, err)
	assert.Equal(t, []byte("ok"), msg.Value)
}

func TestNewStore_RecoverOnStartup_NoQueuesDir(t *testing.T) {
	dir := t.TempDir()

	// Recovery on an empty directory should not fail.
	cfg := DefaultStoreConfig()
	cfg.RecoverOnStartup = true

	store, err := NewStore(dir, cfg)
	require.NoError(t, err)
	defer store.Close()
}
