// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package logstorage

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/absmach/fluxmq/queue/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConsumerGroupStateStore_SaveGetAndExists(t *testing.T) {
	dir := t.TempDir()
	store, err := NewConsumerGroupStateStore(dir)
	require.NoError(t, err)

	state := types.NewConsumerGroupState("queue-a", "group-1", "topic/#")
	err = store.Save(state)
	require.NoError(t, err)

	assert.True(t, store.Exists("queue-a", "group-1"))

	got, err := store.Get("queue-a", "group-1")
	require.NoError(t, err)
	assert.Equal(t, "group-1", got.ID)
	assert.Equal(t, "queue-a", got.QueueName)
	assert.NotNil(t, got.Cursor)
	assert.NotNil(t, got.PEL)
	assert.NotNil(t, got.Consumers)

	_, err = store.Get("queue-a", "missing")
	assert.ErrorIs(t, err, ErrGroupNotFound)
}

func TestConsumerGroupStateStore_ListAndListAll(t *testing.T) {
	dir := t.TempDir()
	store, err := NewConsumerGroupStateStore(dir)
	require.NoError(t, err)

	require.NoError(t, store.Save(types.NewConsumerGroupState("queue-a", "group-1", "a/#")))
	require.NoError(t, store.Save(types.NewConsumerGroupState("queue-a", "group-2", "b/#")))
	require.NoError(t, store.Save(types.NewConsumerGroupState("queue-b", "group-3", "c/#")))

	listA, err := store.List("queue-a")
	require.NoError(t, err)
	assert.Len(t, listA, 2)

	seenA := make(map[string]bool)
	for _, g := range listA {
		seenA[g.ID] = true
	}
	assert.True(t, seenA["group-1"])
	assert.True(t, seenA["group-2"])

	all, err := store.ListAll()
	require.NoError(t, err)
	assert.Len(t, all, 3)

	seenAll := make(map[string]bool)
	for _, g := range all {
		seenAll[g.QueueName+"/"+g.ID] = true
	}
	assert.True(t, seenAll["queue-a/group-1"])
	assert.True(t, seenAll["queue-a/group-2"])
	assert.True(t, seenAll["queue-b/group-3"])
}

func TestConsumerGroupStateStore_CreateIfNotExists(t *testing.T) {
	dir := t.TempDir()
	store, err := NewConsumerGroupStateStore(dir)
	require.NoError(t, err)

	original := types.NewConsumerGroupState("queue-a", "group-1", "a/#")
	original.Cursor.Cursor = 5
	require.NoError(t, store.Save(original))

	// Should not overwrite existing
	other := types.NewConsumerGroupState("queue-a", "group-1", "b/#")
	other.Cursor.Cursor = 10
	require.NoError(t, store.CreateIfNotExists(other))

	got, err := store.Get("queue-a", "group-1")
	require.NoError(t, err)
	assert.Equal(t, "a/#", got.Pattern)
	assert.Equal(t, uint64(5), got.Cursor.Cursor)

	// Should create new when missing
	newState := types.NewConsumerGroupState("queue-a", "group-2", "c/#")
	require.NoError(t, store.CreateIfNotExists(newState))
	assert.True(t, store.Exists("queue-a", "group-2"))
}

func TestConsumerGroupStateStore_UpdateCursorAndPersistence(t *testing.T) {
	dir := t.TempDir()
	store, err := NewConsumerGroupStateStore(dir)
	require.NoError(t, err)

	state := types.NewConsumerGroupState("queue-a", "group-1", "a/#")
	require.NoError(t, store.Save(state))

	require.NoError(t, store.UpdateCursor("queue-a", "group-1", 10, 7))

	cursor, err := store.GetCursor("queue-a", "group-1")
	require.NoError(t, err)
	assert.Equal(t, uint64(10), cursor.Cursor)
	assert.Equal(t, uint64(7), cursor.Committed)

	require.NoError(t, store.Sync())
	require.NoError(t, store.Close())

	store2, err := NewConsumerGroupStateStore(dir)
	require.NoError(t, err)
	defer store2.Close()

	cursor2, err := store2.GetCursor("queue-a", "group-1")
	require.NoError(t, err)
	assert.Equal(t, uint64(10), cursor2.Cursor)
	assert.Equal(t, uint64(7), cursor2.Committed)
}

func TestConsumerGroupStateStore_Delete(t *testing.T) {
	dir := t.TempDir()
	store, err := NewConsumerGroupStateStore(dir)
	require.NoError(t, err)

	state := types.NewConsumerGroupState("queue-a", "group-1", "a/#")
	require.NoError(t, store.Save(state))

	path := store.groupPath("queue-a", "group-1")
	_, err = os.Stat(path)
	require.NoError(t, err)

	require.NoError(t, store.Delete("queue-a", "group-1"))

	_, err = store.Get("queue-a", "group-1")
	assert.ErrorIs(t, err, ErrGroupNotFound)

	_, err = os.Stat(path)
	assert.True(t, os.IsNotExist(err))

	queueDir := filepath.Join(store.dir, "queue-a")
	_, err = os.Stat(queueDir)
	assert.True(t, os.IsNotExist(err))
}
