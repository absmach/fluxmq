// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package logstorage

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestManager(t *testing.T, cfg ManagerConfig) *SegmentManager {
	t.Helper()
	mgr, err := NewSegmentManager(t.TempDir(), cfg)
	require.NoError(t, err)
	return mgr
}

func newTimestampBatch(ts int64, value []byte) *Batch {
	b := NewBatch(0)
	b.BaseTimestamp = ts
	b.MaxTimestamp = ts
	b.Flags = BatchFlagHasTimestamp
	b.Records = []Record{{
		OffsetDelta:    0,
		TimestampDelta: 0,
		Value:          value,
	}}
	return b
}

func TestSegmentManager_RotateAndRead(t *testing.T) {
	cfg := DefaultManagerConfig()
	cfg.MaxSegmentSize = 1
	cfg.MaxSegmentAge = 0
	cfg.SyncInterval = 0
	cfg.Compression = CompressionNone

	mgr := newTestManager(t, cfg)
	defer mgr.Close()

	_, err := mgr.AppendMessage([]byte("a"), nil, nil)
	require.NoError(t, err)
	_, err = mgr.AppendMessage([]byte("b"), nil, nil)
	require.NoError(t, err)

	assert.Equal(t, 2, mgr.SegmentCount())
	assert.Equal(t, uint64(0), mgr.Head())
	assert.Equal(t, uint64(2), mgr.Tail())

	segs := mgr.Segments()
	require.Len(t, segs, 2)
	assert.Equal(t, uint64(0), segs[0].BaseOffset)
	assert.Equal(t, uint64(1), segs[1].BaseOffset)

	msg0, err := mgr.Read(0)
	require.NoError(t, err)
	assert.Equal(t, []byte("a"), msg0.Value)

	msg1, err := mgr.Read(1)
	require.NoError(t, err)
	assert.Equal(t, []byte("b"), msg1.Value)

	rangeMsgs, err := mgr.ReadRange(0, 2, 10)
	require.NoError(t, err)
	require.Len(t, rangeMsgs, 2)
	assert.Equal(t, []byte("a"), rangeMsgs[0].Value)
	assert.Equal(t, []byte("b"), rangeMsgs[1].Value)
}

func TestSegmentManager_Truncate(t *testing.T) {
	cfg := DefaultManagerConfig()
	cfg.MaxSegmentSize = 1
	cfg.MaxSegmentAge = 0
	cfg.SyncInterval = 0
	cfg.Compression = CompressionNone

	mgr := newTestManager(t, cfg)
	defer mgr.Close()

	_, err := mgr.AppendMessage([]byte("a"), nil, nil)
	require.NoError(t, err)
	_, err = mgr.AppendMessage([]byte("b"), nil, nil)
	require.NoError(t, err)
	_, err = mgr.AppendMessage([]byte("c"), nil, nil)
	require.NoError(t, err)

	assert.Equal(t, 3, mgr.SegmentCount())

	err = mgr.Truncate(2)
	require.NoError(t, err)

	assert.Equal(t, 1, mgr.SegmentCount())
	assert.Equal(t, uint64(2), mgr.Head())
	assert.Equal(t, uint64(3), mgr.Tail())

	_, err = mgr.Read(1)
	assert.ErrorIs(t, err, ErrOffsetOutOfRange)

	msg, err := mgr.Read(2)
	require.NoError(t, err)
	assert.Equal(t, []byte("c"), msg.Value)
}

func TestSegmentManager_ApplyRetentionSize(t *testing.T) {
	cfg := DefaultManagerConfig()
	cfg.MaxSegmentSize = 1
	cfg.MaxSegmentAge = 0
	cfg.SyncInterval = 0
	cfg.RetentionBytes = 1
	cfg.Compression = CompressionNone

	mgr := newTestManager(t, cfg)
	defer mgr.Close()

	_, err := mgr.AppendMessage([]byte("a"), nil, nil)
	require.NoError(t, err)
	_, err = mgr.AppendMessage([]byte("b"), nil, nil)
	require.NoError(t, err)
	_, err = mgr.AppendMessage([]byte("c"), nil, nil)
	require.NoError(t, err)

	assert.GreaterOrEqual(t, mgr.SegmentCount(), 2)

	err = mgr.ApplyRetention()
	require.NoError(t, err)

	assert.Equal(t, 1, mgr.SegmentCount())
	assert.Equal(t, uint64(2), mgr.Head())
}

func TestSegmentManager_LookupByTime(t *testing.T) {
	cfg := DefaultManagerConfig()
	cfg.MaxSegmentSize = 1
	cfg.MaxSegmentAge = 0
	cfg.SyncInterval = 0
	cfg.Compression = CompressionNone

	mgr := newTestManager(t, cfg)
	defer mgr.Close()

	baseTS := int64(1_000_000)
	batch1 := newTimestampBatch(baseTS, []byte("a"))
	_, err := mgr.Append(batch1)
	require.NoError(t, err)

	batch2 := newTimestampBatch(baseTS+2000, []byte("b"))
	_, err = mgr.Append(batch2)
	require.NoError(t, err)

	off, err := mgr.LookupByTime(time.UnixMilli(baseTS + 1500))
	require.NoError(t, err)
	assert.Equal(t, uint64(0), off)

	off, err = mgr.LookupByTime(time.UnixMilli(baseTS + 2500))
	require.NoError(t, err)
	assert.Equal(t, uint64(1), off)
}

func TestSegmentManager_IndexIntervalRespected(t *testing.T) {
	cfg := DefaultManagerConfig()
	cfg.IndexInterval = 1 << 30
	cfg.MaxSegmentSize = DefaultMaxSegmentSize
	cfg.MaxSegmentAge = 0
	cfg.SyncInterval = 0
	cfg.Compression = CompressionNone

	mgr := newTestManager(t, cfg)
	defer mgr.Close()

	for i := 0; i < 5; i++ {
		batch := NewBatch(0)
		batch.Append([]byte("msg"), nil, nil)
		_, err := mgr.Append(batch)
		require.NoError(t, err)
	}

	seg := mgr.activeSegment
	require.NotNil(t, seg)
	require.NotNil(t, seg.index)
	assert.Equal(t, 1, seg.index.EntryCount())
}

func TestSegmentManager_TimeIndexMinInterval(t *testing.T) {
	cfg := DefaultManagerConfig()
	cfg.TimeIndexMinInterval = 10 * time.Second
	cfg.MaxSegmentSize = DefaultMaxSegmentSize
	cfg.MaxSegmentAge = 0
	cfg.SyncInterval = 0
	cfg.Compression = CompressionNone

	mgr := newTestManager(t, cfg)
	defer mgr.Close()

	baseTS := int64(1_000_000)
	batch1 := newTimestampBatch(baseTS, []byte("a"))
	_, err := mgr.Append(batch1)
	require.NoError(t, err)

	batch2 := newTimestampBatch(baseTS+1000, []byte("b"))
	_, err = mgr.Append(batch2)
	require.NoError(t, err)

	seg := mgr.activeSegment
	require.NotNil(t, seg)
	require.NotNil(t, seg.timeIndex)
	assert.Equal(t, 1, seg.timeIndex.EntryCount())
}

func TestSegmentManager_RebuildMissingIndexes(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultManagerConfig()
	cfg.MaxSegmentSize = DefaultMaxSegmentSize
	cfg.MaxSegmentAge = 0
	cfg.SyncInterval = 0
	cfg.Compression = CompressionNone

	mgr, err := NewSegmentManager(dir, cfg)
	require.NoError(t, err)

	batch := newTimestampBatch(time.Now().UnixMilli(), []byte("msg"))
	_, err = mgr.Append(batch)
	require.NoError(t, err)
	require.NoError(t, mgr.Close())

	require.NoError(t, os.Remove(filepath.Join(dir, FormatIndexName(0))))
	require.NoError(t, os.Remove(filepath.Join(dir, FormatTimeIndexName(0))))

	mgr2, err := NewSegmentManager(dir, cfg)
	require.NoError(t, err)
	defer mgr2.Close()

	require.NotEmpty(t, mgr2.segments)
	seg := mgr2.segments[0]
	require.NotNil(t, seg.index)
	require.NotNil(t, seg.timeIndex)
	assert.Greater(t, seg.index.EntryCount(), 0)
	assert.Greater(t, seg.timeIndex.EntryCount(), 0)
}
