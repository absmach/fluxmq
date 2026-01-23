// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package log

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewBatch(t *testing.T) {
	batch := NewBatch(100)

	assert.Equal(t, uint64(100), batch.BaseOffset)
	assert.True(t, batch.BaseTimestamp > 0)
	assert.Equal(t, int64(0), batch.MaxTimestamp)
	assert.True(t, batch.Flags.HasTimestamp())
	assert.Equal(t, CompressionNone, batch.Compression)
	assert.Len(t, batch.Records, 0)
}

func TestBatch_Append(t *testing.T) {
	batch := NewBatch(100)

	batch.Append([]byte("hello"), nil, nil)
	assert.Equal(t, 1, batch.Count())

	batch.Append([]byte("world"), []byte("key1"), nil)
	assert.Equal(t, 2, batch.Count())
	assert.True(t, batch.Flags.HasKeys())

	headers := map[string][]byte{"h1": []byte("v1")}
	batch.Append([]byte("test"), nil, headers)
	assert.Equal(t, 3, batch.Count())
	assert.True(t, batch.Flags.HasHeaders())
}

func TestBatch_Offsets(t *testing.T) {
	batch := NewBatch(100)

	assert.Equal(t, uint64(100), batch.LastOffset())
	assert.Equal(t, uint64(100), batch.NextOffset())

	batch.Append([]byte("msg1"), nil, nil)
	assert.Equal(t, uint64(100), batch.LastOffset())
	assert.Equal(t, uint64(101), batch.NextOffset())

	batch.Append([]byte("msg2"), nil, nil)
	assert.Equal(t, uint64(101), batch.LastOffset())
	assert.Equal(t, uint64(102), batch.NextOffset())

	batch.Append([]byte("msg3"), nil, nil)
	assert.Equal(t, uint64(102), batch.LastOffset())
	assert.Equal(t, uint64(103), batch.NextOffset())
}

func TestBatch_EncodeDecodeEmpty(t *testing.T) {
	batch := NewBatch(100)

	_, err := batch.Encode()
	assert.ErrorIs(t, err, ErrEmptyBatch)
}

func TestBatch_EncodeDecode(t *testing.T) {
	batch := NewBatch(100)
	batch.Append([]byte("hello"), nil, nil)
	batch.Append([]byte("world"), []byte("key1"), nil)
	batch.Append([]byte("test"), nil, map[string][]byte{"h1": []byte("v1")})

	data, err := batch.Encode()
	require.NoError(t, err)
	assert.True(t, len(data) > BatchHeaderSize)

	decoded, err := DecodeBatch(data)
	require.NoError(t, err)

	assert.Equal(t, batch.BaseOffset, decoded.BaseOffset)
	assert.Equal(t, batch.Count(), decoded.Count())
	assert.Equal(t, batch.Flags, decoded.Flags)

	assert.Equal(t, []byte("hello"), decoded.Records[0].Value)
	assert.Equal(t, []byte("world"), decoded.Records[1].Value)
	assert.Equal(t, []byte("key1"), decoded.Records[1].Key)
	assert.Equal(t, []byte("test"), decoded.Records[2].Value)
	assert.Equal(t, []byte("v1"), decoded.Records[2].Headers["h1"])
}

func TestBatch_EncodeDecodeWithS2Compression(t *testing.T) {
	batch := NewBatch(100)
	batch.Compression = CompressionS2

	for i := 0; i < 100; i++ {
		batch.Append([]byte("this is a longer message that should compress well when repeated many times"), nil, nil)
	}

	data, err := batch.Encode()
	require.NoError(t, err)

	decoded, err := DecodeBatch(data)
	require.NoError(t, err)

	assert.Equal(t, batch.Count(), decoded.Count())
	assert.Equal(t, batch.BaseOffset, decoded.BaseOffset)

	for i, rec := range decoded.Records {
		assert.Equal(t, batch.Records[i].Value, rec.Value)
	}
}

func TestBatch_EncodeDecodeWithZstdCompression(t *testing.T) {
	batch := NewBatch(100)
	batch.Compression = CompressionZstd

	for i := 0; i < 100; i++ {
		batch.Append([]byte("this is a longer message that should compress well when repeated many times"), nil, nil)
	}

	data, err := batch.Encode()
	require.NoError(t, err)

	decoded, err := DecodeBatch(data)
	require.NoError(t, err)

	assert.Equal(t, batch.Count(), decoded.Count())
}

func TestBatch_ToMessages(t *testing.T) {
	batch := NewBatch(100)
	batch.Append([]byte("msg1"), []byte("k1"), nil)
	batch.Append([]byte("msg2"), nil, nil)
	batch.Append([]byte("msg3"), []byte("k3"), map[string][]byte{"h": []byte("v")})

	messages := batch.ToMessages()
	assert.Len(t, messages, 3)

	assert.Equal(t, uint64(100), messages[0].Offset)
	assert.Equal(t, []byte("msg1"), messages[0].Value)
	assert.Equal(t, []byte("k1"), messages[0].Key)

	assert.Equal(t, uint64(101), messages[1].Offset)
	assert.Equal(t, []byte("msg2"), messages[1].Value)

	assert.Equal(t, uint64(102), messages[2].Offset)
	assert.Equal(t, []byte("msg3"), messages[2].Value)
	assert.Equal(t, []byte("k3"), messages[2].Key)
	assert.Equal(t, []byte("v"), messages[2].Headers["h"])
}

func TestBatch_GetMessage(t *testing.T) {
	batch := NewBatch(100)
	batch.Append([]byte("msg1"), nil, nil)
	batch.Append([]byte("msg2"), nil, nil)
	batch.Append([]byte("msg3"), nil, nil)

	msg, err := batch.GetMessage(100)
	require.NoError(t, err)
	assert.Equal(t, uint64(100), msg.Offset)
	assert.Equal(t, []byte("msg1"), msg.Value)

	msg, err = batch.GetMessage(102)
	require.NoError(t, err)
	assert.Equal(t, uint64(102), msg.Offset)
	assert.Equal(t, []byte("msg3"), msg.Value)

	_, err = batch.GetMessage(99)
	assert.ErrorIs(t, err, ErrOffsetOutOfRange)

	_, err = batch.GetMessage(103)
	assert.ErrorIs(t, err, ErrOffsetOutOfRange)
}

func TestBatch_Size(t *testing.T) {
	batch := NewBatch(100)
	batch.Append([]byte("hello"), nil, nil)
	batch.Append([]byte("world"), []byte("key"), nil)

	size := batch.Size()
	assert.True(t, size > BatchHeaderSize)

	data, err := batch.Encode()
	require.NoError(t, err)

	assert.True(t, size >= len(data)-BatchHeaderSize)
}

func TestBatch_NilValues(t *testing.T) {
	batch := NewBatch(100)
	batch.Append(nil, nil, nil)
	batch.Append([]byte{}, []byte{}, nil)

	data, err := batch.Encode()
	require.NoError(t, err)

	decoded, err := DecodeBatch(data)
	require.NoError(t, err)

	assert.Equal(t, 2, decoded.Count())
}

func TestDecodeBatch_InvalidData(t *testing.T) {
	_, err := DecodeBatch([]byte{})
	assert.ErrorIs(t, err, ErrInvalidBatch)

	_, err = DecodeBatch(make([]byte, BatchHeaderSize-1))
	assert.ErrorIs(t, err, ErrInvalidBatch)

	invalidMagic := make([]byte, BatchHeaderSize+10)
	PutUint32(invalidMagic[0:4], 0x12345678)
	_, err = DecodeBatch(invalidMagic)
	assert.ErrorIs(t, err, ErrInvalidMagic)
}

func TestDecodeBatch_CRCMismatch(t *testing.T) {
	batch := NewBatch(100)
	batch.Append([]byte("test"), nil, nil)

	data, err := batch.Encode()
	require.NoError(t, err)

	data[10] ^= 0xFF

	_, err = DecodeBatch(data)
	assert.ErrorIs(t, err, ErrCRCMismatch)
}

func TestBatch_Timestamps(t *testing.T) {
	batch := NewBatch(100)

	time.Sleep(10 * time.Millisecond)
	batch.Append([]byte("msg1"), nil, nil)

	time.Sleep(10 * time.Millisecond)
	batch.Append([]byte("msg2"), nil, nil)

	assert.True(t, batch.MaxTimestamp >= batch.BaseTimestamp)

	messages := batch.ToMessages()
	assert.True(t, messages[0].Timestamp.UnixMilli() >= batch.BaseTimestamp)
	assert.True(t, messages[1].Timestamp.UnixMilli() >= messages[0].Timestamp.UnixMilli())
}

func TestCompress_Decompress(t *testing.T) {
	data := []byte("this is test data that should compress well when repeated multiple times")

	s2Compressed, err := compress(data, CompressionS2)
	require.NoError(t, err)

	s2Decompressed, err := decompress(s2Compressed, CompressionS2)
	require.NoError(t, err)
	assert.Equal(t, data, s2Decompressed)

	zstdCompressed, err := compress(data, CompressionZstd)
	require.NoError(t, err)

	zstdDecompressed, err := decompress(zstdCompressed, CompressionZstd)
	require.NoError(t, err)
	assert.Equal(t, data, zstdDecompressed)

	noneCompressed, err := compress(data, CompressionNone)
	require.NoError(t, err)
	assert.Equal(t, data, noneCompressed)
}
