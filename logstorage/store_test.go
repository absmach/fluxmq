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

func TestStore_AutoCreateAppendRead(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultStoreConfig()

	store, err := NewStore(dir, cfg)
	require.NoError(t, err)
	defer store.Close()

	offset, err := store.Append("q1", []byte("v1"), nil, nil)
	require.NoError(t, err)
	assert.Equal(t, uint64(0), offset)

	msg, err := store.Read("q1", 0)
	require.NoError(t, err)
	assert.Equal(t, []byte("v1"), msg.Value)

	head, err := store.Head("q1")
	require.NoError(t, err)
	assert.Equal(t, uint64(0), head)

	tail, err := store.Tail("q1")
	require.NoError(t, err)
	assert.Equal(t, uint64(1), tail)

	count, err := store.Count("q1")
	require.NoError(t, err)
	assert.Equal(t, uint64(1), count)

	assert.True(t, store.QueueExists("q1"))
	assert.Contains(t, store.ListQueues(), "q1")

	err = store.CreateConsumerGroup("q1", "g1")
	require.NoError(t, err)
	assert.True(t, store.ConsumerGroupExists("q1", "g1"))
	assert.Contains(t, store.ListConsumerGroups("q1"), "g1")
}

func TestStore_NoAutoCreate(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultStoreConfig()
	cfg.AutoCreate = false

	store, err := NewStore(dir, cfg)
	require.NoError(t, err)
	defer store.Close()

	_, err = store.Append("missing", []byte("v1"), nil, nil)
	assert.ErrorIs(t, err, ErrQueueNotFound)

	err = store.CreateQueue("q1")
	require.NoError(t, err)

	_, err = store.Append("q1", []byte("v1"), nil, nil)
	require.NoError(t, err)
}

func TestStore_ReadBatch(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultStoreConfig()

	store, err := NewStore(dir, cfg)
	require.NoError(t, err)
	defer store.Close()

	batch1 := NewBatch(0)
	batch1.Append([]byte("a"), nil, nil)
	batch1.Append([]byte("b"), nil, nil)
	batch1.Append([]byte("c"), nil, nil)
	_, err = store.AppendBatch("q1", batch1)
	require.NoError(t, err)

	batch2 := NewBatch(0)
	batch2.Append([]byte("d"), nil, nil)
	_, err = store.AppendBatch("q1", batch2)
	require.NoError(t, err)

	readBatch, err := store.ReadBatch("q1", 1)
	require.NoError(t, err)
	assert.Equal(t, uint64(0), readBatch.BaseOffset)

	msg, err := readBatch.GetMessage(1)
	require.NoError(t, err)
	assert.Equal(t, []byte("b"), msg.Value)

	readBatch2, err := store.ReadBatch("q1", 3)
	require.NoError(t, err)
	assert.Equal(t, uint64(3), readBatch2.BaseOffset)

	_, err = store.ReadBatch("q1", 100)
	assert.ErrorIs(t, err, ErrOffsetOutOfRange)
}

func TestStore_Persistence(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultStoreConfig()

	store, err := NewStore(dir, cfg)
	require.NoError(t, err)

	_, err = store.Append("q1", []byte("v1"), nil, nil)
	require.NoError(t, err)
	require.NoError(t, store.Sync())
	require.NoError(t, store.Close())

	store2, err := NewStore(dir, cfg)
	require.NoError(t, err)
	defer store2.Close()

	msg, err := store2.Read("q1", 0)
	require.NoError(t, err)
	assert.Equal(t, []byte("v1"), msg.Value)
}

func TestStore_DeleteQueue(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultStoreConfig()

	store, err := NewStore(dir, cfg)
	require.NoError(t, err)
	defer store.Close()

	err = store.CreateQueue("q1")
	require.NoError(t, err)

	_, err = store.Append("q1", []byte("v1"), nil, nil)
	require.NoError(t, err)

	queueDir := filepath.Join(dir, "queues", "q1")
	_, err = os.Stat(queueDir)
	require.NoError(t, err)

	err = store.DeleteQueue("q1")
	require.NoError(t, err)

	assert.False(t, store.QueueExists("q1"))
	assert.NotContains(t, store.ListQueues(), "q1")

	_, err = store.Read("q1", 0)
	assert.ErrorIs(t, err, ErrQueueNotFound)

	_, err = os.Stat(queueDir)
	assert.True(t, os.IsNotExist(err))
}
