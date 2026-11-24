// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package logstorage

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/absmach/fluxmq/queue/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const testQueueName = "test-queue"

func TestQueueConfigStore_NewAndClose(t *testing.T) {
	dir := t.TempDir()

	store, err := NewQueueConfigStore(dir)
	require.NoError(t, err)
	require.NotNil(t, store)

	err = store.Close()
	assert.NoError(t, err)
}

func TestQueueConfigStore_SaveAndGet(t *testing.T) {
	dir := t.TempDir()
	store, err := NewQueueConfigStore(dir)
	require.NoError(t, err)
	defer store.Close()

	config := types.QueueConfig{
		Name: testQueueName,
	}

	err = store.Save(config)
	require.NoError(t, err)

	retrieved, err := store.Get(testQueueName)
	require.NoError(t, err)
	assert.Equal(t, config.Name, retrieved.Name)

	_, err = store.Get("non-existent")
	assert.ErrorIs(t, err, ErrQueueNotFound)
}

func TestQueueConfigStore_Delete(t *testing.T) {
	dir := t.TempDir()
	store, err := NewQueueConfigStore(dir)
	require.NoError(t, err)
	defer store.Close()

	config := types.QueueConfig{
		Name: testQueueName,
	}

	err = store.Save(config)
	require.NoError(t, err)

	err = store.Delete(testQueueName)
	require.NoError(t, err)

	_, err = store.Get(testQueueName)
	assert.ErrorIs(t, err, ErrQueueNotFound)
}

func TestQueueConfigStore_List(t *testing.T) {
	dir := t.TempDir()
	store, err := NewQueueConfigStore(dir)
	require.NoError(t, err)
	defer store.Close()

	configs, err := store.List()
	require.NoError(t, err)
	assert.Len(t, configs, 0)

	for i := 0; i < 3; i++ {
		err = store.Save(types.QueueConfig{
			Name: string(rune('a'+i)) + "-queue",
		})
		require.NoError(t, err)
	}

	configs, err = store.List()
	require.NoError(t, err)
	assert.Len(t, configs, 3)
}

func TestQueueConfigStore_Sync(t *testing.T) {
	dir := t.TempDir()
	store, err := NewQueueConfigStore(dir)
	require.NoError(t, err)
	defer store.Close()

	err = store.Sync()
	assert.NoError(t, err)

	err = store.Save(types.QueueConfig{
		Name: testQueueName,
	})
	require.NoError(t, err)

	err = store.Sync()
	assert.NoError(t, err)
}

func TestQueueConfigStore_Persistence(t *testing.T) {
	dir := t.TempDir()

	store, err := NewQueueConfigStore(dir)
	require.NoError(t, err)

	config1 := types.QueueConfig{
		Name: "queue-1",
	}
	config2 := types.QueueConfig{
		Name: "queue-2",
	}

	err = store.Save(config1)
	require.NoError(t, err)
	err = store.Save(config2)
	require.NoError(t, err)

	err = store.Close()
	require.NoError(t, err)

	store2, err := NewQueueConfigStore(dir)
	require.NoError(t, err)
	defer store2.Close()

	retrieved1, err := store2.Get("queue-1")
	require.NoError(t, err)
	assert.Equal(t, config1.Name, retrieved1.Name)

	retrieved2, err := store2.Get("queue-2")
	require.NoError(t, err)
	assert.Equal(t, config2.Name, retrieved2.Name)
}

func TestQueueConfigStore_Update(t *testing.T) {
	dir := t.TempDir()
	store, err := NewQueueConfigStore(dir)
	require.NoError(t, err)
	defer store.Close()

	config := types.QueueConfig{
		Name: testQueueName,
	}

	err = store.Save(config)
	require.NoError(t, err)

	err = store.Save(config)
	require.NoError(t, err)

	configs, err := store.List()
	require.NoError(t, err)
	assert.Len(t, configs, 1)
}

func writeLegacyCatalog(t *testing.T, baseDir string, configs ...types.QueueConfig) {
	t.Helper()
	data, err := json.Marshal(QueueConfigState{Version: queueConfigVersion, Configs: configs})
	require.NoError(t, err)
	require.NoError(t, os.MkdirAll(filepath.Join(baseDir, "config"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(baseDir, "config", legacyCatalogFile), data, 0o600))
}

func TestQueueConfigStoreMigratesLegacyCatalog(t *testing.T) {
	configs := []types.QueueConfig{
		types.DefaultQueueConfig("billing", "billing/#"),
		types.DefaultQueueConfig("$dlq/billing", "$dlq/billing/#"),
		types.DefaultQueueConfig("50%", "pct/#"),
	}
	names := []string{"billing", "$dlq/billing", "50%"}

	assertMigrated := func(t *testing.T, dir string) {
		t.Helper()
		for range 2 { // and again after reopening the migrated store
			store, err := NewQueueConfigStore(dir)
			require.NoError(t, err)
			listed, err := store.List()
			require.NoError(t, err)
			got := make([]string, 0, len(listed))
			for _, cfg := range listed {
				got = append(got, cfg.Name)
			}
			assert.ElementsMatch(t, names, got)
			require.NoError(t, store.Close())
		}
		assert.NoFileExists(t, filepath.Join(dir, "config", legacyCatalogFile))
	}

	t.Run("full", func(t *testing.T) {
		dir := t.TempDir()
		writeLegacyCatalog(t, dir, configs...)
		assertMigrated(t, dir)
	})

	t.Run("resumes after an interrupted split", func(t *testing.T) {
		dir := t.TempDir()
		// Some records were written, the catalog was not yet removed.
		store, err := NewQueueConfigStore(dir)
		require.NoError(t, err)
		require.NoError(t, store.Save(configs[0]))
		writeLegacyCatalog(t, dir, configs...)
		assertMigrated(t, dir)
	})
}

func TestQueueConfigStoreWritesOneFilePerChange(t *testing.T) {
	dir := t.TempDir()
	store, err := NewQueueConfigStore(dir)
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, store.Close()) })

	for _, name := range []string{"a", "b", "a/b"} {
		require.NoError(t, store.Save(types.DefaultQueueConfig(name, "t/#")))
	}
	entries, err := os.ReadDir(filepath.Join(dir, "config", queueConfigsDir))
	require.NoError(t, err)
	assert.Len(t, entries, 3, "one file per queue, slashes included")

	untouched := filepath.Join(dir, "config", queueConfigsDir, "b"+queueConfigExt)
	before, err := os.ReadFile(untouched)
	require.NoError(t, err)
	statBefore, err := os.Stat(untouched)
	require.NoError(t, err)

	updated := types.DefaultQueueConfig("a", "t/#")
	updated.MaxDepth = 7
	require.NoError(t, store.Save(updated))
	require.NoError(t, store.Delete("a/b"))

	after, err := os.ReadFile(untouched)
	require.NoError(t, err)
	statAfter, err := os.Stat(untouched)
	require.NoError(t, err)
	assert.Equal(t, before, after)
	assert.Equal(t, statBefore.ModTime(), statAfter.ModTime(), "changing one queue must not rewrite another")
	assert.NoFileExists(t, filepath.Join(dir, "config", queueConfigsDir, encodePathComponent("a/b")+queueConfigExt))
}

func TestQueueConfigStoreFailedWriteKeepsPreviousConfig(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("permissions do not stop root")
	}
	dir := t.TempDir()
	store, err := NewQueueConfigStore(dir)
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, store.Close()) })

	original := types.DefaultQueueConfig("q", "t/#")
	require.NoError(t, store.Save(original))

	recordsDir := filepath.Join(dir, "config", queueConfigsDir)
	require.NoError(t, os.Chmod(recordsDir, 0o500))
	t.Cleanup(func() { assert.NoError(t, os.Chmod(recordsDir, 0o755)) })

	changed := original.Clone()
	changed.MaxDepth = 7
	require.Error(t, store.Save(changed))

	stored, err := store.Get("q")
	require.NoError(t, err)
	assert.Equal(t, original.MaxDepth, stored.MaxDepth, "an unwritten change must not be served")
}

func TestQueueConfigStoreRejectsMisnamedRecord(t *testing.T) {
	dir := t.TempDir()
	store, err := NewQueueConfigStore(dir)
	require.NoError(t, err)
	require.NoError(t, store.Save(types.DefaultQueueConfig("real", "t/#")))
	require.NoError(t, store.Close())

	recordsDir := filepath.Join(dir, "config", queueConfigsDir)
	require.NoError(t, os.Rename(filepath.Join(recordsDir, "real"+queueConfigExt), filepath.Join(recordsDir, "other"+queueConfigExt)))

	_, err = NewQueueConfigStore(dir)
	assert.Error(t, err, "a file must hold the queue it is named for")
}
