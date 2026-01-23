// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package log

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSegment_CreateAndClose(t *testing.T) {
	dir := t.TempDir()
	config := DefaultSegmentConfig()

	seg, err := CreateSegment(dir, 0, config)
	require.NoError(t, err)
	require.NotNil(t, seg)

	assert.Equal(t, uint64(0), seg.BaseOffset())
	assert.Equal(t, uint64(0), seg.NextOffset())
	assert.Equal(t, int64(0), seg.Size())

	err = seg.Close()
	assert.NoError(t, err)
}

func TestSegment_AppendAndRead(t *testing.T) {
	dir := t.TempDir()
	config := DefaultSegmentConfig()

	seg, err := CreateSegment(dir, 0, config)
	require.NoError(t, err)
	defer seg.Close()

	batch := NewBatch(0)
	batch.Append([]byte("hello"), nil, nil)
	batch.Append([]byte("world"), nil, nil)

	offset, err := seg.Append(batch)
	require.NoError(t, err)
	assert.Equal(t, uint64(0), offset)
	assert.Equal(t, uint64(2), seg.NextOffset())
	assert.True(t, seg.Size() > 0)

	msg, err := seg.Read(0)
	require.NoError(t, err)
	assert.Equal(t, uint64(0), msg.Offset)
	assert.Equal(t, []byte("hello"), msg.Value)

	msg, err = seg.Read(1)
	require.NoError(t, err)
	assert.Equal(t, uint64(1), msg.Offset)
	assert.Equal(t, []byte("world"), msg.Value)
}

func TestSegment_ReadOutOfRange(t *testing.T) {
	dir := t.TempDir()
	config := DefaultSegmentConfig()

	seg, err := CreateSegment(dir, 100, config)
	require.NoError(t, err)
	defer seg.Close()

	batch := NewBatch(100)
	batch.Append([]byte("test"), nil, nil)
	_, err = seg.Append(batch)
	require.NoError(t, err)

	_, err = seg.Read(99)
	assert.ErrorIs(t, err, ErrOffsetOutOfRange)

	_, err = seg.Read(101)
	assert.ErrorIs(t, err, ErrOffsetOutOfRange)
}

func TestSegment_ReadBatch(t *testing.T) {
	dir := t.TempDir()
	config := DefaultSegmentConfig()

	seg, err := CreateSegment(dir, 0, config)
	require.NoError(t, err)
	defer seg.Close()

	batch := NewBatch(0)
	batch.Append([]byte("msg1"), nil, nil)
	batch.Append([]byte("msg2"), nil, nil)
	batch.Append([]byte("msg3"), nil, nil)

	_, err = seg.Append(batch)
	require.NoError(t, err)

	readBatch, err := seg.ReadBatch(0)
	require.NoError(t, err)
	assert.Equal(t, 3, readBatch.Count())
	assert.Equal(t, uint64(0), readBatch.BaseOffset)
}

func TestSegment_ReadRange(t *testing.T) {
	dir := t.TempDir()
	config := DefaultSegmentConfig()

	seg, err := CreateSegment(dir, 0, config)
	require.NoError(t, err)
	defer seg.Close()

	batch1 := NewBatch(0)
	batch1.Append([]byte("msg0"), nil, nil)
	batch1.Append([]byte("msg1"), nil, nil)
	_, err = seg.Append(batch1)
	require.NoError(t, err)

	batch2 := NewBatch(2)
	batch2.Append([]byte("msg2"), nil, nil)
	batch2.Append([]byte("msg3"), nil, nil)
	_, err = seg.Append(batch2)
	require.NoError(t, err)

	messages, err := seg.ReadRange(0, 4, 10)
	require.NoError(t, err)
	assert.Len(t, messages, 4)

	messages, err = seg.ReadRange(1, 3, 10)
	require.NoError(t, err)
	assert.Len(t, messages, 2)
	assert.Equal(t, uint64(1), messages[0].Offset)
	assert.Equal(t, uint64(2), messages[1].Offset)

	messages, err = seg.ReadRange(0, 10, 2)
	require.NoError(t, err)
	assert.Len(t, messages, 2)
}

func TestSegment_OpenExisting(t *testing.T) {
	dir := t.TempDir()
	config := DefaultSegmentConfig()

	seg, err := CreateSegment(dir, 100, config)
	require.NoError(t, err)

	batch := NewBatch(100)
	batch.Append([]byte("test1"), nil, nil)
	batch.Append([]byte("test2"), nil, nil)
	_, err = seg.Append(batch)
	require.NoError(t, err)

	err = seg.Close()
	require.NoError(t, err)

	seg2, err := OpenSegment(dir, 100, false)
	require.NoError(t, err)
	defer seg2.Close()

	assert.Equal(t, uint64(100), seg2.BaseOffset())
	assert.Equal(t, uint64(102), seg2.NextOffset())

	msg, err := seg2.Read(100)
	require.NoError(t, err)
	assert.Equal(t, []byte("test1"), msg.Value)
}

func TestSegment_Readonly(t *testing.T) {
	dir := t.TempDir()
	config := DefaultSegmentConfig()

	seg, err := CreateSegment(dir, 0, config)
	require.NoError(t, err)

	batch := NewBatch(0)
	batch.Append([]byte("test"), nil, nil)
	_, err = seg.Append(batch)
	require.NoError(t, err)

	err = seg.Close()
	require.NoError(t, err)

	seg2, err := OpenSegment(dir, 0, true)
	require.NoError(t, err)
	defer seg2.Close()

	newBatch := NewBatch(1)
	newBatch.Append([]byte("new"), nil, nil)
	_, err = seg2.Append(newBatch)
	assert.Error(t, err)

	msg, err := seg2.Read(0)
	require.NoError(t, err)
	assert.Equal(t, []byte("test"), msg.Value)
}

func TestSegment_Info(t *testing.T) {
	dir := t.TempDir()
	config := DefaultSegmentConfig()

	seg, err := CreateSegment(dir, 100, config)
	require.NoError(t, err)
	defer seg.Close()

	batch := NewBatch(100)
	for i := 0; i < 10; i++ {
		batch.Append([]byte("message"), nil, nil)
	}
	_, err = seg.Append(batch)
	require.NoError(t, err)

	info := seg.Info()
	assert.Equal(t, uint64(100), info.BaseOffset)
	assert.Equal(t, uint64(110), info.NextOffset)
	assert.True(t, info.Size > 0)
	assert.Equal(t, uint64(10), info.MessageCount)
	assert.False(t, info.CreatedAt.IsZero())
	assert.False(t, info.ModifiedAt.IsZero())
}

func TestSegment_IsFull(t *testing.T) {
	dir := t.TempDir()
	config := DefaultSegmentConfig()

	seg, err := CreateSegment(dir, 0, config)
	require.NoError(t, err)
	defer seg.Close()

	assert.False(t, seg.IsFull(1024*1024))

	batch := NewBatch(0)
	batch.Append(make([]byte, 1000), nil, nil)
	_, err = seg.Append(batch)
	require.NoError(t, err)

	assert.True(t, seg.IsFull(100))
	assert.False(t, seg.IsFull(1024*1024))
}

func TestSegment_IsExpired(t *testing.T) {
	dir := t.TempDir()
	config := DefaultSegmentConfig()

	seg, err := CreateSegment(dir, 0, config)
	require.NoError(t, err)
	defer seg.Close()

	assert.False(t, seg.IsExpired(0))
	assert.False(t, seg.IsExpired(time.Hour))
	assert.True(t, seg.IsExpired(1*time.Nanosecond))
}

func TestSegment_Age(t *testing.T) {
	dir := t.TempDir()
	config := DefaultSegmentConfig()

	seg, err := CreateSegment(dir, 0, config)
	require.NoError(t, err)
	defer seg.Close()

	age := seg.Age()
	assert.True(t, age >= 0)
	assert.True(t, age < time.Second)
}

func TestSegment_SetReadonly(t *testing.T) {
	dir := t.TempDir()
	config := DefaultSegmentConfig()

	seg, err := CreateSegment(dir, 0, config)
	require.NoError(t, err)
	defer seg.Close()

	batch := NewBatch(0)
	batch.Append([]byte("test"), nil, nil)
	_, err = seg.Append(batch)
	require.NoError(t, err)

	seg.SetReadonly()

	newBatch := NewBatch(1)
	newBatch.Append([]byte("new"), nil, nil)
	_, err = seg.Append(newBatch)
	assert.Error(t, err)
}

func TestSegment_Sync(t *testing.T) {
	dir := t.TempDir()
	config := DefaultSegmentConfig()

	seg, err := CreateSegment(dir, 0, config)
	require.NoError(t, err)
	defer seg.Close()

	batch := NewBatch(0)
	batch.Append([]byte("test"), nil, nil)
	_, err = seg.Append(batch)
	require.NoError(t, err)

	err = seg.Sync()
	assert.NoError(t, err)
}

func TestSegment_Delete(t *testing.T) {
	dir := t.TempDir()
	config := DefaultSegmentConfig()

	seg, err := CreateSegment(dir, 0, config)
	require.NoError(t, err)

	batch := NewBatch(0)
	batch.Append([]byte("test"), nil, nil)
	_, err = seg.Append(batch)
	require.NoError(t, err)

	err = seg.Delete()
	assert.NoError(t, err)
}

func TestSegment_MultipleBatches(t *testing.T) {
	dir := t.TempDir()
	config := DefaultSegmentConfig()

	seg, err := CreateSegment(dir, 0, config)
	require.NoError(t, err)
	defer seg.Close()

	for i := 0; i < 10; i++ {
		batch := NewBatch(uint64(i * 5))
		for j := 0; j < 5; j++ {
			batch.Append([]byte("message"), nil, nil)
		}
		_, err = seg.Append(batch)
		require.NoError(t, err)
	}

	assert.Equal(t, uint64(50), seg.NextOffset())

	info := seg.Info()
	assert.Equal(t, uint64(50), info.MessageCount)

	for i := 0; i < 50; i++ {
		msg, err := seg.Read(uint64(i))
		require.NoError(t, err, "failed to read offset %d", i)
		assert.Equal(t, uint64(i), msg.Offset)
	}
}

func TestSegment_RebuildIndex(t *testing.T) {
	dir := t.TempDir()
	config := DefaultSegmentConfig()
	config.IndexInterval = 100

	seg, err := CreateSegment(dir, 0, config)
	require.NoError(t, err)
	defer seg.Close()

	for i := 0; i < 10; i++ {
		batch := NewBatch(uint64(i * 10))
		for j := 0; j < 10; j++ {
			batch.Append([]byte("message"), nil, nil)
		}
		_, err = seg.Append(batch)
		require.NoError(t, err)
	}

	err = seg.RebuildIndex(100)
	require.NoError(t, err)

	for i := 0; i < 100; i++ {
		msg, err := seg.Read(uint64(i))
		require.NoError(t, err)
		assert.Equal(t, uint64(i), msg.Offset)
	}
}

func TestSegment_Closed(t *testing.T) {
	dir := t.TempDir()
	config := DefaultSegmentConfig()

	seg, err := CreateSegment(dir, 0, config)
	require.NoError(t, err)

	err = seg.Close()
	require.NoError(t, err)

	batch := NewBatch(0)
	batch.Append([]byte("test"), nil, nil)
	_, err = seg.Append(batch)
	assert.ErrorIs(t, err, ErrSegmentClosed)

	_, err = seg.Read(0)
	assert.ErrorIs(t, err, ErrSegmentClosed)

	_, err = seg.ReadBatch(0)
	assert.ErrorIs(t, err, ErrSegmentClosed)

	_, err = seg.ReadRange(0, 10, 10)
	assert.ErrorIs(t, err, ErrSegmentClosed)
}

func TestFormatParseSegmentName(t *testing.T) {
	testCases := []uint64{0, 1, 100, 1000000, ^uint64(0) / 2}

	for _, offset := range testCases {
		name := FormatSegmentName(offset)
		parsed, err := ParseSegmentName(name)
		require.NoError(t, err)
		assert.Equal(t, offset, parsed)
	}
}

func TestFormatIndexName(t *testing.T) {
	name := FormatIndexName(12345)
	assert.Contains(t, name, "12345")
	assert.Contains(t, name, IndexExtension)
}

func TestFormatTimeIndexName(t *testing.T) {
	name := FormatTimeIndexName(12345)
	assert.Contains(t, name, "12345")
	assert.Contains(t, name, TimeIndexExtension)
}
