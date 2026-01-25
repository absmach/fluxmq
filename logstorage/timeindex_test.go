// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package logstorage

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTimeIndex_CreateAndClose(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.tix")

	tidx, err := CreateTimeIndex(path, 0)
	require.NoError(t, err)
	require.NotNil(t, tidx)

	assert.Equal(t, 0, tidx.EntryCount())
	assert.True(t, tidx.MinTimestamp().IsZero())
	assert.True(t, tidx.MaxTimestamp().IsZero())

	err = tidx.Close()
	assert.NoError(t, err)
}

func TestTimeIndex_OpenExisting(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.tix")

	tidx, err := CreateTimeIndex(path, 100)
	require.NoError(t, err)
	tidx.SetMinInterval(0)

	ts1 := time.Now().UnixMilli()
	err = tidx.Append(ts1, 0)
	require.NoError(t, err)

	ts2 := ts1 + 1000
	err = tidx.Append(ts2, 100)
	require.NoError(t, err)

	ts3 := ts2 + 1000
	err = tidx.Append(ts3, 200)
	require.NoError(t, err)

	err = tidx.Close()
	require.NoError(t, err)

	tidx2, err := OpenTimeIndex(path, 100, false)
	require.NoError(t, err)
	defer tidx2.Close()

	assert.Equal(t, 3, tidx2.EntryCount())

	entries := tidx2.Entries()
	assert.Len(t, entries, 3)
	assert.Equal(t, ts1, entries[0].Timestamp)
	assert.Equal(t, uint32(0), entries[0].RelativeOffset)
}

func TestTimeIndex_OpenReadonly(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.tix")

	tidx, err := CreateTimeIndex(path, 0)
	require.NoError(t, err)
	tidx.SetMinInterval(0)
	err = tidx.Append(time.Now().UnixMilli(), 0)
	require.NoError(t, err)
	err = tidx.Close()
	require.NoError(t, err)

	tidx2, err := OpenTimeIndex(path, 0, true)
	require.NoError(t, err)
	defer tidx2.Close()

	err = tidx2.Append(time.Now().UnixMilli(), 100)
	assert.Error(t, err)
}

func TestTimeIndex_Lookup(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.tix")

	tidx, err := CreateTimeIndex(path, 1000)
	require.NoError(t, err)
	defer tidx.Close()
	tidx.SetMinInterval(0)

	baseTS := int64(1000000)
	err = tidx.Append(baseTS, 0)
	require.NoError(t, err)
	err = tidx.Append(baseTS+1000, 100)
	require.NoError(t, err)
	err = tidx.Append(baseTS+2000, 200)
	require.NoError(t, err)
	err = tidx.Append(baseTS+3000, 300)
	require.NoError(t, err)

	offset, err := tidx.Lookup(baseTS)
	require.NoError(t, err)
	assert.Equal(t, uint64(1000), offset)

	offset, err = tidx.Lookup(baseTS + 500)
	require.NoError(t, err)
	assert.Equal(t, uint64(1000), offset)

	offset, err = tidx.Lookup(baseTS + 1500)
	require.NoError(t, err)
	assert.Equal(t, uint64(1100), offset)

	offset, err = tidx.Lookup(baseTS - 1000)
	require.NoError(t, err)
	assert.Equal(t, uint64(1000), offset)

	offset, err = tidx.Lookup(baseTS + 10000)
	require.NoError(t, err)
	assert.Equal(t, uint64(1300), offset)
}

func TestTimeIndex_LookupBefore(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.tix")

	tidx, err := CreateTimeIndex(path, 0)
	require.NoError(t, err)
	defer tidx.Close()
	tidx.SetMinInterval(0)

	baseTS := int64(1000000)
	err = tidx.Append(baseTS, 0)
	require.NoError(t, err)
	err = tidx.Append(baseTS+1000, 100)
	require.NoError(t, err)
	err = tidx.Append(baseTS+2000, 200)
	require.NoError(t, err)

	offset, err := tidx.LookupBefore(baseTS)
	require.NoError(t, err)
	assert.Equal(t, uint64(0), offset)

	offset, err = tidx.LookupBefore(baseTS + 1500)
	require.NoError(t, err)
	assert.Equal(t, uint64(100), offset)

	offset, err = tidx.LookupBefore(baseTS + 3000)
	require.NoError(t, err)
	assert.Equal(t, uint64(200), offset)
}

func TestTimeIndex_MinMaxTimestamp(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.tix")

	tidx, err := CreateTimeIndex(path, 0)
	require.NoError(t, err)
	defer tidx.Close()
	tidx.SetMinInterval(0)

	assert.True(t, tidx.MinTimestamp().IsZero())
	assert.True(t, tidx.MaxTimestamp().IsZero())

	ts1 := int64(1000000)
	err = tidx.Append(ts1, 0)
	require.NoError(t, err)

	assert.Equal(t, ts1, tidx.MinTimestamp().UnixMilli())
	assert.Equal(t, ts1, tidx.MaxTimestamp().UnixMilli())

	ts2 := ts1 + 5000
	err = tidx.Append(ts2, 100)
	require.NoError(t, err)

	assert.Equal(t, ts1, tidx.MinTimestamp().UnixMilli())
	assert.Equal(t, ts2, tidx.MaxTimestamp().UnixMilli())
}

func TestTimeIndex_MinIntervalFiltering(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.tix")

	tidx, err := CreateTimeIndex(path, 0)
	require.NoError(t, err)
	defer tidx.Close()

	tidx.SetMinInterval(1000 * time.Millisecond)

	baseTS := int64(1000000)

	err = tidx.Append(baseTS, 0)
	require.NoError(t, err)
	assert.Equal(t, 1, tidx.EntryCount())

	err = tidx.Append(baseTS+500, 50)
	require.NoError(t, err)
	assert.Equal(t, 1, tidx.EntryCount())

	err = tidx.Append(baseTS+1001, 100)
	require.NoError(t, err)
	assert.Equal(t, 2, tidx.EntryCount())
}

func TestTimeIndex_Truncate(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.tix")

	tidx, err := CreateTimeIndex(path, 0)
	require.NoError(t, err)
	defer tidx.Close()
	tidx.SetMinInterval(0)

	baseTS := int64(1000000)
	for i := 0; i < 10; i++ {
		err = tidx.Append(baseTS+int64(i*1000), uint32(i*100))
		require.NoError(t, err)
	}
	assert.Equal(t, 10, tidx.EntryCount())

	err = tidx.Truncate(500)
	require.NoError(t, err)
	assert.Equal(t, 5, tidx.EntryCount())

	entries := tidx.Entries()
	assert.Equal(t, uint32(400), entries[4].RelativeOffset)
}

func TestTimeIndex_Sync(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.tix")

	tidx, err := CreateTimeIndex(path, 0)
	require.NoError(t, err)
	defer tidx.Close()

	tidx.SetMinInterval(0)
	err = tidx.Append(time.Now().UnixMilli(), 0)
	require.NoError(t, err)

	err = tidx.Sync()
	assert.NoError(t, err)
}

func TestTimeIndex_EmptyLookup(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.tix")

	tidx, err := CreateTimeIndex(path, 100)
	require.NoError(t, err)
	defer tidx.Close()

	offset, err := tidx.Lookup(1000000)
	require.NoError(t, err)
	assert.Equal(t, uint64(100), offset)
}

func TestTimeIndex_InvalidBaseOffset(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.tix")

	tidx, err := CreateTimeIndex(path, 100)
	require.NoError(t, err)
	err = tidx.Close()
	require.NoError(t, err)

	_, err = OpenTimeIndex(path, 200, false)
	assert.Error(t, err)
}
