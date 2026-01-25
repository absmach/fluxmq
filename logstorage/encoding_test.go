// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package logstorage

import (
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestChecksum(t *testing.T) {
	data := []byte("hello world")
	crc1 := Checksum(data)
	crc2 := Checksum(data)
	assert.Equal(t, crc1, crc2)

	differentData := []byte("hello world!")
	crc3 := Checksum(differentData)
	assert.NotEqual(t, crc1, crc3)
}

func TestVarintEncodeDecode(t *testing.T) {
	tests := []int64{0, 1, -1, 127, -128, 255, -256, 1000000, -1000000}

	for _, v := range tests {
		buf := make([]byte, 16)
		n := PutVarint(buf, v)
		assert.True(t, n > 0)

		decoded, m := Varint(buf)
		assert.Equal(t, n, m)
		assert.Equal(t, v, decoded)
	}
}

func TestUvarintEncodeDecode(t *testing.T) {
	tests := []uint64{0, 1, 127, 128, 255, 256, 1000000, ^uint64(0)}

	for _, v := range tests {
		buf := make([]byte, 16)
		n := PutUvarint(buf, v)
		assert.True(t, n > 0)

		decoded, m := Uvarint(buf)
		assert.Equal(t, n, m)
		assert.Equal(t, v, decoded)
	}
}

func TestVarintSize(t *testing.T) {
	tests := []struct {
		value    int64
		expected int
	}{
		{0, 1},
		{1, 1},
		{-1, 1},
		{63, 1},
		{64, 2},
		{-64, 1},
		{-65, 2},
	}

	for _, tc := range tests {
		assert.Equal(t, tc.expected, VarintSize(tc.value), "for value %d", tc.value)
	}
}

func TestUvarintSize(t *testing.T) {
	tests := []struct {
		value    uint64
		expected int
	}{
		{0, 1},
		{1, 1},
		{127, 1},
		{128, 2},
		{16383, 2},
		{16384, 3},
	}

	for _, tc := range tests {
		assert.Equal(t, tc.expected, UvarintSize(tc.value), "for value %d", tc.value)
	}
}

func TestPutGetUint16(t *testing.T) {
	buf := make([]byte, 2)
	values := []uint16{0, 1, 255, 256, 65535}

	for _, v := range values {
		PutUint16(buf, v)
		assert.Equal(t, v, GetUint16(buf))
	}
}

func TestPutGetUint32(t *testing.T) {
	buf := make([]byte, 4)
	values := []uint32{0, 1, 255, 65535, 0xFFFFFFFF}

	for _, v := range values {
		PutUint32(buf, v)
		assert.Equal(t, v, GetUint32(buf))
	}
}

func TestPutGetUint64(t *testing.T) {
	buf := make([]byte, 8)
	values := []uint64{0, 1, 255, 65535, 0xFFFFFFFF, ^uint64(0)}

	for _, v := range values {
		PutUint64(buf, v)
		assert.Equal(t, v, GetUint64(buf))
	}
}

func TestBufferWriter(t *testing.T) {
	w := NewBufferWriter(16)
	assert.Equal(t, 0, w.Len())

	w.WriteUint8(0x42)
	assert.Equal(t, 1, w.Len())

	w.WriteUint16(0x1234)
	assert.Equal(t, 3, w.Len())

	w.WriteUint32(0xDEADBEEF)
	assert.Equal(t, 7, w.Len())

	w.WriteUint64(0x123456789ABCDEF0)
	assert.Equal(t, 15, w.Len())

	bytes := w.Bytes()
	assert.Len(t, bytes, 15)

	w.Reset()
	assert.Equal(t, 0, w.Len())
}

func TestBufferWriter_Varint(t *testing.T) {
	w := NewBufferWriter(32)

	w.WriteVarint(-1)
	w.WriteUvarint(1000000)

	bytes := w.Bytes()
	assert.True(t, len(bytes) > 0)
}

func TestBufferWriter_Bytes(t *testing.T) {
	w := NewBufferWriter(32)

	data := []byte("hello world")
	w.WriteBytes(data)

	w.WriteRawBytes([]byte{1, 2, 3})

	bytes := w.Bytes()
	assert.True(t, len(bytes) > len(data))
}

func TestBufferWriter_Grow(t *testing.T) {
	w := NewBufferWriter(4)

	for i := 0; i < 100; i++ {
		w.WriteUint8(byte(i))
	}

	assert.Equal(t, 100, w.Len())
	bytes := w.Bytes()
	assert.Len(t, bytes, 100)

	for i := 0; i < 100; i++ {
		assert.Equal(t, byte(i), bytes[i])
	}
}

func TestBufferReader(t *testing.T) {
	data := []byte{0x42, 0x34, 0x12, 0xEF, 0xBE, 0xAD, 0xDE}
	r := NewBufferReader(data)

	assert.Equal(t, 7, r.Remaining())
	assert.Equal(t, 0, r.Position())

	v8, err := r.ReadUint8()
	require.NoError(t, err)
	assert.Equal(t, uint8(0x42), v8)
	assert.Equal(t, 1, r.Position())

	v16, err := r.ReadUint16()
	require.NoError(t, err)
	assert.Equal(t, uint16(0x1234), v16)

	v32, err := r.ReadUint32()
	require.NoError(t, err)
	assert.Equal(t, uint32(0xDEADBEEF), v32)
}

func TestBufferReader_Varint(t *testing.T) {
	w := NewBufferWriter(32)
	w.WriteVarint(-12345)
	w.WriteUvarint(67890)

	r := NewBufferReader(w.Bytes())

	v1, err := r.ReadVarint()
	require.NoError(t, err)
	assert.Equal(t, int64(-12345), v1)

	v2, err := r.ReadUvarint()
	require.NoError(t, err)
	assert.Equal(t, uint64(67890), v2)
}

func TestBufferReader_Bytes(t *testing.T) {
	w := NewBufferWriter(64)
	original := []byte("hello world")
	w.WriteBytes(original)

	r := NewBufferReader(w.Bytes())

	data, err := r.ReadBytes()
	require.NoError(t, err)
	assert.Equal(t, original, data)
}

func TestBufferReader_RawBytes(t *testing.T) {
	data := []byte{1, 2, 3, 4, 5}
	r := NewBufferReader(data)

	raw, err := r.ReadRawBytes(3)
	require.NoError(t, err)
	assert.Equal(t, []byte{1, 2, 3}, raw)
	assert.Equal(t, 2, r.Remaining())
}

func TestBufferReader_Skip(t *testing.T) {
	data := []byte{1, 2, 3, 4, 5}
	r := NewBufferReader(data)

	err := r.Skip(3)
	require.NoError(t, err)
	assert.Equal(t, 2, r.Remaining())

	err = r.Skip(10)
	assert.ErrorIs(t, err, io.ErrUnexpectedEOF)
}

func TestBufferReader_EOF(t *testing.T) {
	r := NewBufferReader([]byte{})

	_, err := r.ReadUint8()
	assert.ErrorIs(t, err, io.ErrUnexpectedEOF)

	_, err = r.ReadUint16()
	assert.ErrorIs(t, err, io.ErrUnexpectedEOF)

	_, err = r.ReadUint32()
	assert.ErrorIs(t, err, io.ErrUnexpectedEOF)

	_, err = r.ReadUint64()
	assert.ErrorIs(t, err, io.ErrUnexpectedEOF)

	_, err = r.ReadVarint()
	assert.ErrorIs(t, err, io.ErrUnexpectedEOF)

	_, err = r.ReadUvarint()
	assert.ErrorIs(t, err, io.ErrUnexpectedEOF)
}
