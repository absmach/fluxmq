// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package log

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCursorStore_NewAndClose(t *testing.T) {
	dir := t.TempDir()

	cs, err := NewCursorStore(dir, "test-group")
	require.NoError(t, err)
	require.NotNil(t, cs)

	cursor := cs.GetCursor(0)
	assert.Nil(t, cursor)

	err = cs.Close()
	assert.NoError(t, err)
}

func TestCursorStore_SetAndGetCursor(t *testing.T) {
	dir := t.TempDir()
	cs, err := NewCursorStore(dir, "test-group")
	require.NoError(t, err)
	defer cs.Close()

	cs.SetCursor(0, 100)

	cursor := cs.GetCursor(0)
	require.NotNil(t, cursor)
	assert.Equal(t, uint64(100), cursor.Cursor)

	cursor = cs.GetCursor(1)
	assert.Nil(t, cursor)
}

func TestCursorStore_AdvanceCursor(t *testing.T) {
	dir := t.TempDir()
	cs, err := NewCursorStore(dir, "test-group")
	require.NoError(t, err)
	defer cs.Close()

	advanced := cs.AdvanceCursor(0, 100)
	assert.True(t, advanced)

	cursor := cs.GetCursor(0)
	assert.Equal(t, uint64(100), cursor.Cursor)

	advanced = cs.AdvanceCursor(0, 50)
	assert.False(t, advanced)

	cursor = cs.GetCursor(0)
	assert.Equal(t, uint64(100), cursor.Cursor)

	advanced = cs.AdvanceCursor(0, 200)
	assert.True(t, advanced)

	cursor = cs.GetCursor(0)
	assert.Equal(t, uint64(200), cursor.Cursor)
}

func TestCursorStore_Commit(t *testing.T) {
	dir := t.TempDir()
	cs, err := NewCursorStore(dir, "test-group")
	require.NoError(t, err)
	defer cs.Close()

	cs.Commit(0, 100)

	committed := cs.GetCommitted(0)
	assert.Equal(t, uint64(100), committed)

	cs.Commit(0, 50)
	committed = cs.GetCommitted(0)
	assert.Equal(t, uint64(100), committed)

	cs.Commit(0, 200)
	committed = cs.GetCommitted(0)
	assert.Equal(t, uint64(200), committed)
}

func TestCursorStore_GetAllCursors(t *testing.T) {
	dir := t.TempDir()
	cs, err := NewCursorStore(dir, "test-group")
	require.NoError(t, err)
	defer cs.Close()

	cs.SetCursor(0, 100)
	cs.SetCursor(1, 200)
	cs.SetCursor(2, 300)

	all := cs.GetAllCursors()
	assert.Len(t, all, 3)
	assert.Equal(t, uint64(100), all[0].Cursor)
	assert.Equal(t, uint64(200), all[1].Cursor)
	assert.Equal(t, uint64(300), all[2].Cursor)
}

func TestCursorStore_MinCommitted(t *testing.T) {
	dir := t.TempDir()
	cs, err := NewCursorStore(dir, "test-group")
	require.NoError(t, err)
	defer cs.Close()

	assert.Equal(t, uint64(0), cs.MinCommitted())

	cs.Commit(0, 100)
	cs.Commit(1, 50)
	cs.Commit(2, 200)

	assert.Equal(t, uint64(50), cs.MinCommitted())
}

func TestCursorStore_ResetPartition(t *testing.T) {
	dir := t.TempDir()
	cs, err := NewCursorStore(dir, "test-group")
	require.NoError(t, err)
	defer cs.Close()

	cs.SetCursor(0, 100)
	cs.Commit(0, 50)

	cs.ResetPartition(0, 200)

	cursor := cs.GetCursor(0)
	assert.Equal(t, uint64(200), cursor.Cursor)
	assert.Equal(t, uint64(200), cursor.Committed)
}

func TestCursorStore_DeletePartition(t *testing.T) {
	dir := t.TempDir()
	cs, err := NewCursorStore(dir, "test-group")
	require.NoError(t, err)
	defer cs.Close()

	cs.SetCursor(0, 100)
	cs.SetCursor(1, 200)

	cs.DeletePartition(0)

	assert.Nil(t, cs.GetCursor(0))
	assert.NotNil(t, cs.GetCursor(1))
}

func TestCursorStore_IsDirty(t *testing.T) {
	dir := t.TempDir()
	cs, err := NewCursorStore(dir, "test-group")
	require.NoError(t, err)
	defer cs.Close()

	assert.False(t, cs.IsDirty())

	cs.SetCursor(0, 100)
	assert.True(t, cs.IsDirty())

	err = cs.Save()
	require.NoError(t, err)
	assert.False(t, cs.IsDirty())
}

func TestCursorStore_Persistence(t *testing.T) {
	dir := t.TempDir()

	cs, err := NewCursorStore(dir, "test-group")
	require.NoError(t, err)

	cs.SetCursor(0, 100)
	cs.SetCursor(1, 200)
	cs.Commit(0, 50)

	err = cs.Save()
	require.NoError(t, err)
	err = cs.Close()
	require.NoError(t, err)

	cs2, err := NewCursorStore(dir, "test-group")
	require.NoError(t, err)
	defer cs2.Close()

	cursor0 := cs2.GetCursor(0)
	require.NotNil(t, cursor0)
	assert.Equal(t, uint64(100), cursor0.Cursor)
	assert.Equal(t, uint64(50), cursor0.Committed)

	cursor1 := cs2.GetCursor(1)
	require.NotNil(t, cursor1)
	assert.Equal(t, uint64(200), cursor1.Cursor)
}

func TestConsumerGroupCursors_NewAndClose(t *testing.T) {
	dir := t.TempDir()

	cgc, err := NewConsumerGroupCursors(dir)
	require.NoError(t, err)
	require.NotNil(t, cgc)

	err = cgc.Close()
	assert.NoError(t, err)
}

func TestConsumerGroupCursors_GetOrCreate(t *testing.T) {
	dir := t.TempDir()
	cgc, err := NewConsumerGroupCursors(dir)
	require.NoError(t, err)
	defer cgc.Close()

	cs1, err := cgc.GetOrCreate("group-1")
	require.NoError(t, err)
	require.NotNil(t, cs1)

	cs2, err := cgc.GetOrCreate("group-1")
	require.NoError(t, err)
	assert.Equal(t, cs1, cs2)

	cs3, err := cgc.GetOrCreate("group-2")
	require.NoError(t, err)
	assert.NotEqual(t, cs1, cs3)
}

func TestConsumerGroupCursors_Get(t *testing.T) {
	dir := t.TempDir()
	cgc, err := NewConsumerGroupCursors(dir)
	require.NoError(t, err)
	defer cgc.Close()

	cs := cgc.Get("group-1")
	assert.Nil(t, cs)

	_, err = cgc.GetOrCreate("group-1")
	require.NoError(t, err)

	cs = cgc.Get("group-1")
	assert.NotNil(t, cs)
}

func TestConsumerGroupCursors_Delete(t *testing.T) {
	dir := t.TempDir()
	cgc, err := NewConsumerGroupCursors(dir)
	require.NoError(t, err)
	defer cgc.Close()

	cs, err := cgc.GetOrCreate("group-1")
	require.NoError(t, err)

	// Set and save a cursor so the file exists
	cs.SetCursor(0, 100)
	err = cs.Save()
	require.NoError(t, err)

	err = cgc.Delete("group-1")
	require.NoError(t, err)

	cs = cgc.Get("group-1")
	assert.Nil(t, cs)
}

func TestConsumerGroupCursors_List(t *testing.T) {
	dir := t.TempDir()
	cgc, err := NewConsumerGroupCursors(dir)
	require.NoError(t, err)
	defer cgc.Close()

	assert.Len(t, cgc.List(), 0)

	_, err = cgc.GetOrCreate("group-a")
	require.NoError(t, err)
	_, err = cgc.GetOrCreate("group-b")
	require.NoError(t, err)
	_, err = cgc.GetOrCreate("group-c")
	require.NoError(t, err)

	groups := cgc.List()
	assert.Len(t, groups, 3)
	assert.Contains(t, groups, "group-a")
	assert.Contains(t, groups, "group-b")
	assert.Contains(t, groups, "group-c")
}

func TestConsumerGroupCursors_SaveAll(t *testing.T) {
	dir := t.TempDir()
	cgc, err := NewConsumerGroupCursors(dir)
	require.NoError(t, err)
	defer cgc.Close()

	cs1, err := cgc.GetOrCreate("group-1")
	require.NoError(t, err)
	cs1.SetCursor(0, 100)

	cs2, err := cgc.GetOrCreate("group-2")
	require.NoError(t, err)
	cs2.SetCursor(0, 200)

	err = cgc.SaveAll()
	assert.NoError(t, err)

	assert.False(t, cs1.IsDirty())
	assert.False(t, cs2.IsDirty())
}
