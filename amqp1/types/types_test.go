// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package types

import (
	"bytes"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func roundTrip(t *testing.T, write func(w *bytes.Buffer) error, expected any) {
	t.Helper()
	var buf bytes.Buffer
	require.NoError(t, write(&buf))
	got, err := ReadType(&buf)
	require.NoError(t, err)
	assert.Equal(t, expected, got)
}

func TestNull(t *testing.T) {
	roundTrip(t, func(w *bytes.Buffer) error { return WriteNull(w) }, nil)
}

func TestBool(t *testing.T) {
	roundTrip(t, func(w *bytes.Buffer) error { return WriteBool(w, true) }, true)
	roundTrip(t, func(w *bytes.Buffer) error { return WriteBool(w, false) }, false)
}

func TestUbyte(t *testing.T) {
	roundTrip(t, func(w *bytes.Buffer) error { return WriteUbyte(w, 0) }, uint8(0))
	roundTrip(t, func(w *bytes.Buffer) error { return WriteUbyte(w, 255) }, uint8(255))
}

func TestUshort(t *testing.T) {
	roundTrip(t, func(w *bytes.Buffer) error { return WriteUshort(w, 0) }, uint16(0))
	roundTrip(t, func(w *bytes.Buffer) error { return WriteUshort(w, 65535) }, uint16(65535))
}

func TestUint(t *testing.T) {
	// uint0 encoding
	roundTrip(t, func(w *bytes.Buffer) error { return WriteUint(w, 0) }, uint32(0))
	// smalluint encoding
	roundTrip(t, func(w *bytes.Buffer) error { return WriteUint(w, 200) }, uint32(200))
	// full uint encoding
	roundTrip(t, func(w *bytes.Buffer) error { return WriteUint(w, 70000) }, uint32(70000))
}

func TestUlong(t *testing.T) {
	roundTrip(t, func(w *bytes.Buffer) error { return WriteUlong(w, 0) }, uint64(0))
	roundTrip(t, func(w *bytes.Buffer) error { return WriteUlong(w, 200) }, uint64(200))
	roundTrip(t, func(w *bytes.Buffer) error { return WriteUlong(w, 1<<40) }, uint64(1<<40))
}

func TestByte(t *testing.T) {
	roundTrip(t, func(w *bytes.Buffer) error { return WriteByte(w, -128) }, int8(-128))
	roundTrip(t, func(w *bytes.Buffer) error { return WriteByte(w, 127) }, int8(127))
}

func TestShort(t *testing.T) {
	roundTrip(t, func(w *bytes.Buffer) error { return WriteShort(w, -32768) }, int16(-32768))
	roundTrip(t, func(w *bytes.Buffer) error { return WriteShort(w, 32767) }, int16(32767))
}

func TestInt(t *testing.T) {
	// smallint encoding
	roundTrip(t, func(w *bytes.Buffer) error { return WriteInt(w, 42) }, int32(42))
	roundTrip(t, func(w *bytes.Buffer) error { return WriteInt(w, -100) }, int32(-100))
	// full int encoding
	roundTrip(t, func(w *bytes.Buffer) error { return WriteInt(w, 70000) }, int32(70000))
}

func TestLong(t *testing.T) {
	roundTrip(t, func(w *bytes.Buffer) error { return WriteLong(w, 0) }, int64(0))
	roundTrip(t, func(w *bytes.Buffer) error { return WriteLong(w, -50) }, int64(-50))
	roundTrip(t, func(w *bytes.Buffer) error { return WriteLong(w, 1<<40) }, int64(1<<40))
}

func TestFloat(t *testing.T) {
	roundTrip(t, func(w *bytes.Buffer) error { return WriteFloat(w, 3.14) }, float32(3.14))
	roundTrip(t, func(w *bytes.Buffer) error { return WriteFloat(w, 0) }, float32(0))
}

func TestDouble(t *testing.T) {
	roundTrip(t, func(w *bytes.Buffer) error { return WriteDouble(w, math.Pi) }, math.Pi)
}

func TestTimestamp(t *testing.T) {
	ts := TimestampFromMillis(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC).UnixMilli())
	roundTrip(t, func(w *bytes.Buffer) error { return WriteTimestamp(w, ts) }, ts)
}

func TestUUID(t *testing.T) {
	u := UUID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	roundTrip(t, func(w *bytes.Buffer) error { return WriteUUID(w, u) }, u)
}

func TestBinary(t *testing.T) {
	// Short binary
	short := []byte("hello")
	roundTrip(t, func(w *bytes.Buffer) error { return WriteBinary(w, short) }, short)

	// Long binary (>255 bytes)
	long := make([]byte, 300)
	for i := range long {
		long[i] = byte(i % 256)
	}
	roundTrip(t, func(w *bytes.Buffer) error { return WriteBinary(w, long) }, long)
}

func TestString(t *testing.T) {
	roundTrip(t, func(w *bytes.Buffer) error { return WriteString(w, "hello") }, "hello")
	roundTrip(t, func(w *bytes.Buffer) error { return WriteString(w, "") }, "")
}

func TestSymbol(t *testing.T) {
	roundTrip(t, func(w *bytes.Buffer) error { return WriteSymbol(w, "amqp") }, Symbol("amqp"))
}

func TestList(t *testing.T) {
	// Empty list
	var buf bytes.Buffer
	require.NoError(t, WriteList(&buf, nil, 0))
	got, err := ReadType(&buf)
	require.NoError(t, err)
	assert.Equal(t, []any{}, got)

	// Non-empty list
	buf.Reset()
	var fields bytes.Buffer
	require.NoError(t, WriteString(&fields, "hello"))
	require.NoError(t, WriteUint(&fields, 42))
	require.NoError(t, WriteNull(&fields))
	require.NoError(t, WriteList(&buf, fields.Bytes(), 3))
	got, err = ReadType(&buf)
	require.NoError(t, err)
	list := got.([]any)
	assert.Len(t, list, 3)
	assert.Equal(t, "hello", list[0])
	assert.Equal(t, uint32(42), list[1])
	assert.Nil(t, list[2])
}

func TestMap(t *testing.T) {
	var buf, pairs bytes.Buffer
	require.NoError(t, WriteSymbol(&pairs, "key1"))
	require.NoError(t, WriteString(&pairs, "value1"))
	require.NoError(t, WriteSymbol(&pairs, "key2"))
	require.NoError(t, WriteUint(&pairs, 100))
	require.NoError(t, WriteMap(&buf, pairs.Bytes(), 2))

	got, err := ReadType(&buf)
	require.NoError(t, err)
	m := got.(map[any]any)
	assert.Len(t, m, 2)
	assert.Equal(t, "value1", m[Symbol("key1")])
	assert.Equal(t, uint32(100), m[Symbol("key2")])
}

func TestDescribed(t *testing.T) {
	var buf bytes.Buffer
	require.NoError(t, WriteDescriptor(&buf, 0x10))
	// Write a list as the value
	var fields bytes.Buffer
	require.NoError(t, WriteString(&fields, "container-1"))
	require.NoError(t, WriteList(&buf, fields.Bytes(), 1))

	got, err := ReadType(&buf)
	require.NoError(t, err)
	desc := got.(*Described)
	assert.Equal(t, uint64(0x10), desc.Descriptor)
	list := desc.Value.([]any)
	assert.Equal(t, "container-1", list[0])
}

func TestWriteAny(t *testing.T) {
	cases := []struct {
		name string
		val  any
	}{
		{"nil", nil},
		{"bool", true},
		{"uint8", uint8(42)},
		{"uint16", uint16(1000)},
		{"uint32", uint32(50000)},
		{"uint64", uint64(1 << 40)},
		{"int8", int8(-1)},
		{"int16", int16(-1000)},
		{"int32", int32(-50000)},
		{"int64", int64(-1 << 40)},
		{"float32", float32(1.5)},
		{"float64", float64(2.5)},
		{"string", "test"},
		{"symbol", Symbol("sym")},
		{"binary", []byte{1, 2, 3}},
		{"uuid", UUID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			require.NoError(t, WriteAny(&buf, tc.val))
			got, err := ReadType(&buf)
			require.NoError(t, err)
			assert.Equal(t, tc.val, got)
		})
	}
}
