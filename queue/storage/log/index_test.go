// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package log

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIndex_CreateAndClose(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.idx")

	idx, err := CreateIndex(path, 0, 4096)
	require.NoError(t, err)
	require.NotNil(t, idx)

	assert.Equal(t, 0, idx.EntryCount())

	err = idx.Close()
	assert.NoError(t, err)

	_, err = os.Stat(path)
	assert.NoError(t, err)
}

func TestIndex_OpenExisting(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.idx")

	idx, err := CreateIndex(path, 100, 4096)
	require.NoError(t, err)

	err = idx.Append(0, 0)
	require.NoError(t, err)
	err = idx.Append(10, 1000)
	require.NoError(t, err)
	err = idx.Append(20, 2000)
	require.NoError(t, err)

	err = idx.Close()
	require.NoError(t, err)

	idx2, err := OpenIndex(path, 100, false)
	require.NoError(t, err)
	defer idx2.Close()

	assert.Equal(t, 3, idx2.EntryCount())

	entries := idx2.Entries()
	assert.Len(t, entries, 3)
	assert.Equal(t, uint32(0), entries[0].RelativeOffset)
	assert.Equal(t, uint32(0), entries[0].FilePosition)
	assert.Equal(t, uint32(20), entries[2].RelativeOffset)
	assert.Equal(t, uint32(2000), entries[2].FilePosition)
}

func TestIndex_OpenReadonly(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.idx")

	idx, err := CreateIndex(path, 0, 4096)
	require.NoError(t, err)
	err = idx.Append(0, 0)
	require.NoError(t, err)
	err = idx.Close()
	require.NoError(t, err)

	idx2, err := OpenIndex(path, 0, true)
	require.NoError(t, err)
	defer idx2.Close()

	err = idx2.Append(10, 1000)
	assert.Error(t, err)
}

func TestIndex_Lookup(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.idx")

	idx, err := CreateIndex(path, 1000, 4096)
	require.NoError(t, err)
	defer idx.Close()

	err = idx.Append(0, 0)
	require.NoError(t, err)
	err = idx.Append(100, 10000)
	require.NoError(t, err)
	err = idx.Append(200, 20000)
	require.NoError(t, err)
	err = idx.Append(300, 30000)
	require.NoError(t, err)

	pos, err := idx.Lookup(1000)
	require.NoError(t, err)
	assert.Equal(t, int64(0), pos)

	pos, err = idx.Lookup(1050)
	require.NoError(t, err)
	assert.Equal(t, int64(0), pos)

	pos, err = idx.Lookup(1100)
	require.NoError(t, err)
	assert.Equal(t, int64(10000), pos)

	pos, err = idx.Lookup(1250)
	require.NoError(t, err)
	assert.Equal(t, int64(20000), pos)

	pos, err = idx.Lookup(1350)
	require.NoError(t, err)
	assert.Equal(t, int64(30000), pos)
}

func TestIndex_LookupEntry(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.idx")

	idx, err := CreateIndex(path, 0, 4096)
	require.NoError(t, err)
	defer idx.Close()

	entry, found := idx.LookupEntry(50)
	assert.False(t, found)

	err = idx.Append(0, 0)
	require.NoError(t, err)
	err = idx.Append(100, 10000)
	require.NoError(t, err)

	entry, found = idx.LookupEntry(50)
	assert.True(t, found)
	assert.Equal(t, uint32(0), entry.RelativeOffset)

	entry, found = idx.LookupEntry(100)
	assert.True(t, found)
	assert.Equal(t, uint32(100), entry.RelativeOffset)

	entry, found = idx.LookupEntry(150)
	assert.True(t, found)
	assert.Equal(t, uint32(100), entry.RelativeOffset)
}

func TestIndex_ShouldIndex(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.idx")

	idx, err := CreateIndex(path, 0, 1000)
	require.NoError(t, err)
	defer idx.Close()

	assert.False(t, idx.ShouldIndex(100))
	assert.False(t, idx.ShouldIndex(100))
	assert.False(t, idx.ShouldIndex(100))
	assert.False(t, idx.ShouldIndex(100))
	assert.False(t, idx.ShouldIndex(100))
	assert.False(t, idx.ShouldIndex(100))
	assert.False(t, idx.ShouldIndex(100))
	assert.False(t, idx.ShouldIndex(100))
	assert.False(t, idx.ShouldIndex(100))
	assert.True(t, idx.ShouldIndex(100))
	assert.False(t, idx.ShouldIndex(500))
	assert.True(t, idx.ShouldIndex(600))
}

func TestIndex_Truncate(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.idx")

	idx, err := CreateIndex(path, 0, 4096)
	require.NoError(t, err)
	defer idx.Close()

	for i := uint32(0); i < 10; i++ {
		err = idx.Append(i*100, i*10000)
		require.NoError(t, err)
	}
	assert.Equal(t, 10, idx.EntryCount())

	err = idx.Truncate(500)
	require.NoError(t, err)
	assert.Equal(t, 5, idx.EntryCount())

	entries := idx.Entries()
	assert.Equal(t, uint32(400), entries[4].RelativeOffset)
}

func TestIndex_Sync(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.idx")

	idx, err := CreateIndex(path, 0, 4096)
	require.NoError(t, err)
	defer idx.Close()

	err = idx.Append(0, 0)
	require.NoError(t, err)

	err = idx.Sync()
	assert.NoError(t, err)
}

func TestIndex_InvalidBaseOffset(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.idx")

	idx, err := CreateIndex(path, 100, 4096)
	require.NoError(t, err)
	err = idx.Close()
	require.NoError(t, err)

	_, err = OpenIndex(path, 200, false)
	assert.Error(t, err)
}

func TestIndex_EmptyLookup(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.idx")

	idx, err := CreateIndex(path, 0, 4096)
	require.NoError(t, err)
	defer idx.Close()

	pos, err := idx.Lookup(100)
	require.NoError(t, err)
	assert.Equal(t, int64(0), pos)
}
