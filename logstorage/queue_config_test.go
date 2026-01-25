// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package logstorage

import (
	"testing"

	"github.com/absmach/fluxmq/queue/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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
		Name:       "test-queue",
		Partitions: 4,
	}

	err = store.Save(config)
	require.NoError(t, err)

	retrieved, err := store.Get("test-queue")
	require.NoError(t, err)
	assert.Equal(t, config.Name, retrieved.Name)
	assert.Equal(t, config.Partitions, retrieved.Partitions)

	_, err = store.Get("non-existent")
	assert.ErrorIs(t, err, ErrQueueNotFound)
}

func TestQueueConfigStore_Delete(t *testing.T) {
	dir := t.TempDir()
	store, err := NewQueueConfigStore(dir)
	require.NoError(t, err)
	defer store.Close()

	config := types.QueueConfig{
		Name:       "test-queue",
		Partitions: 4,
	}

	err = store.Save(config)
	require.NoError(t, err)

	err = store.Delete("test-queue")
	require.NoError(t, err)

	_, err = store.Get("test-queue")
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
			Name:       string(rune('a' + i)) + "-queue",
			Partitions: i + 1,
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
		Name:       "test-queue",
		Partitions: 4,
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
		Name:       "queue-1",
		Partitions: 4,
	}
	config2 := types.QueueConfig{
		Name:       "queue-2",
		Partitions: 8,
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
	assert.Equal(t, config1.Partitions, retrieved1.Partitions)

	retrieved2, err := store2.Get("queue-2")
	require.NoError(t, err)
	assert.Equal(t, config2.Name, retrieved2.Name)
	assert.Equal(t, config2.Partitions, retrieved2.Partitions)
}

func TestQueueConfigStore_Update(t *testing.T) {
	dir := t.TempDir()
	store, err := NewQueueConfigStore(dir)
	require.NoError(t, err)
	defer store.Close()

	config := types.QueueConfig{
		Name:       "test-queue",
		Partitions: 4,
	}

	err = store.Save(config)
	require.NoError(t, err)

	config.Partitions = 8
	err = store.Save(config)
	require.NoError(t, err)

	retrieved, err := store.Get("test-queue")
	require.NoError(t, err)
	assert.Equal(t, 8, retrieved.Partitions)

	configs, err := store.List()
	require.NoError(t, err)
	assert.Len(t, configs, 1)
}
