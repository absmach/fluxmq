// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package logstorage

import (
	"encoding/binary"
	"hash/crc32"
	"io"
)

// CRC32 table for Castagnoli polynomial (same as used by Kafka).
var crcTable = crc32.MakeTable(crc32.Castagnoli)

// Checksum computes CRC32-C checksum.
func Checksum(data []byte) uint32 {
	return crc32.Checksum(data, crcTable)
}

// Varint encoding/decoding utilities.

// PutVarint encodes a signed integer as a varint and returns bytes written.
func PutVarint(buf []byte, v int64) int {
	return binary.PutVarint(buf, v)
}

// PutUvarint encodes an unsigned integer as a varint and returns bytes written.
func PutUvarint(buf []byte, v uint64) int {
	return binary.PutUvarint(buf, v)
}

// Varint decodes a signed varint from buf and returns value and bytes read.
func Varint(buf []byte) (int64, int) {
	return binary.Varint(buf)
}

// Uvarint decodes an unsigned varint from buf and returns value and bytes read.
func Uvarint(buf []byte) (uint64, int) {
	return binary.Uvarint(buf)
}

// VarintSize returns the number of bytes needed to encode v as a signed varint.
func VarintSize(v int64) int {
	ux := uint64(v) << 1
	if v < 0 {
		ux = ^ux
	}
	return UvarintSize(ux)
}

// UvarintSize returns the number of bytes needed to encode v as an unsigned varint.
func UvarintSize(v uint64) int {
	size := 1
	for v >= 0x80 {
		v >>= 7
		size++
	}
	return size
}

// Binary encoding utilities for fixed-size integers.

// PutUint16 writes a uint16 in little-endian format.
func PutUint16(buf []byte, v uint16) {
	binary.LittleEndian.PutUint16(buf, v)
}

// PutUint32 writes a uint32 in little-endian format.
func PutUint32(buf []byte, v uint32) {
	binary.LittleEndian.PutUint32(buf, v)
}

// PutUint64 writes a uint64 in little-endian format.
func PutUint64(buf []byte, v uint64) {
	binary.LittleEndian.PutUint64(buf, v)
}

// GetUint16 reads a uint16 in little-endian format.
func GetUint16(buf []byte) uint16 {
	return binary.LittleEndian.Uint16(buf)
}

// GetUint32 reads a uint32 in little-endian format.
func GetUint32(buf []byte) uint32 {
	return binary.LittleEndian.Uint32(buf)
}

// GetUint64 reads a uint64 in little-endian format.
func GetUint64(buf []byte) uint64 {
	return binary.LittleEndian.Uint64(buf)
}

// BufferWriter provides buffered writing with position tracking.
type BufferWriter struct {
	buf []byte
	pos int
}

// NewBufferWriter creates a new buffer writer with given capacity.
func NewBufferWriter(capacity int) *BufferWriter {
	return &BufferWriter{
		buf: make([]byte, capacity),
		pos: 0,
	}
}

// Reset resets the buffer for reuse.
func (w *BufferWriter) Reset() {
	w.pos = 0
}

// Bytes returns the written bytes.
func (w *BufferWriter) Bytes() []byte {
	return w.buf[:w.pos]
}

// Len returns the number of bytes written.
func (w *BufferWriter) Len() int {
	return w.pos
}

// Grow ensures the buffer has at least n additional bytes of capacity.
func (w *BufferWriter) Grow(n int) {
	if w.pos+n > len(w.buf) {
		newBuf := make([]byte, 2*(w.pos+n))
		copy(newBuf, w.buf[:w.pos])
		w.buf = newBuf
	}
}

// WriteUint8 writes a single byte.
func (w *BufferWriter) WriteUint8(v uint8) {
	w.Grow(1)
	w.buf[w.pos] = v
	w.pos++
}

// WriteUint16 writes a uint16 in little-endian format.
func (w *BufferWriter) WriteUint16(v uint16) {
	w.Grow(2)
	PutUint16(w.buf[w.pos:], v)
	w.pos += 2
}

// WriteUint32 writes a uint32 in little-endian format.
func (w *BufferWriter) WriteUint32(v uint32) {
	w.Grow(4)
	PutUint32(w.buf[w.pos:], v)
	w.pos += 4
}

// WriteUint64 writes a uint64 in little-endian format.
func (w *BufferWriter) WriteUint64(v uint64) {
	w.Grow(8)
	PutUint64(w.buf[w.pos:], v)
	w.pos += 8
}

// WriteVarint writes a signed varint.
func (w *BufferWriter) WriteVarint(v int64) {
	w.Grow(binary.MaxVarintLen64)
	n := PutVarint(w.buf[w.pos:], v)
	w.pos += n
}

// WriteUvarint writes an unsigned varint.
func (w *BufferWriter) WriteUvarint(v uint64) {
	w.Grow(binary.MaxVarintLen64)
	n := PutUvarint(w.buf[w.pos:], v)
	w.pos += n
}

// WriteBytes writes a length-prefixed byte slice.
func (w *BufferWriter) WriteBytes(data []byte) {
	w.WriteUvarint(uint64(len(data)))
	w.Grow(len(data))
	copy(w.buf[w.pos:], data)
	w.pos += len(data)
}

// WriteRawBytes writes raw bytes without length prefix.
func (w *BufferWriter) WriteRawBytes(data []byte) {
	w.Grow(len(data))
	copy(w.buf[w.pos:], data)
	w.pos += len(data)
}

// BufferReader provides buffered reading with position tracking.
type BufferReader struct {
	buf []byte
	pos int
}

// NewBufferReader creates a new buffer reader.
func NewBufferReader(data []byte) *BufferReader {
	return &BufferReader{
		buf: data,
		pos: 0,
	}
}

// Remaining returns the number of unread bytes.
func (r *BufferReader) Remaining() int {
	return len(r.buf) - r.pos
}

// Position returns the current read position.
func (r *BufferReader) Position() int {
	return r.pos
}

// ReadUint8 reads a single byte.
func (r *BufferReader) ReadUint8() (uint8, error) {
	if r.Remaining() < 1 {
		return 0, io.ErrUnexpectedEOF
	}
	v := r.buf[r.pos]
	r.pos++
	return v, nil
}

// ReadUint16 reads a uint16 in little-endian format.
func (r *BufferReader) ReadUint16() (uint16, error) {
	if r.Remaining() < 2 {
		return 0, io.ErrUnexpectedEOF
	}
	v := GetUint16(r.buf[r.pos:])
	r.pos += 2
	return v, nil
}

// ReadUint32 reads a uint32 in little-endian format.
func (r *BufferReader) ReadUint32() (uint32, error) {
	if r.Remaining() < 4 {
		return 0, io.ErrUnexpectedEOF
	}
	v := GetUint32(r.buf[r.pos:])
	r.pos += 4
	return v, nil
}

// ReadUint64 reads a uint64 in little-endian format.
func (r *BufferReader) ReadUint64() (uint64, error) {
	if r.Remaining() < 8 {
		return 0, io.ErrUnexpectedEOF
	}
	v := GetUint64(r.buf[r.pos:])
	r.pos += 8
	return v, nil
}

// ReadVarint reads a signed varint.
func (r *BufferReader) ReadVarint() (int64, error) {
	v, n := Varint(r.buf[r.pos:])
	if n <= 0 {
		return 0, io.ErrUnexpectedEOF
	}
	r.pos += n
	return v, nil
}

// ReadUvarint reads an unsigned varint.
func (r *BufferReader) ReadUvarint() (uint64, error) {
	v, n := Uvarint(r.buf[r.pos:])
	if n <= 0 {
		return 0, io.ErrUnexpectedEOF
	}
	r.pos += n
	return v, nil
}

// ReadBytes reads a length-prefixed byte slice.
func (r *BufferReader) ReadBytes() ([]byte, error) {
	length, err := r.ReadUvarint()
	if err != nil {
		return nil, err
	}
	if r.Remaining() < int(length) {
		return nil, io.ErrUnexpectedEOF
	}
	data := make([]byte, length)
	copy(data, r.buf[r.pos:r.pos+int(length)])
	r.pos += int(length)
	return data, nil
}

// ReadRawBytes reads n raw bytes.
func (r *BufferReader) ReadRawBytes(n int) ([]byte, error) {
	if r.Remaining() < n {
		return nil, io.ErrUnexpectedEOF
	}
	data := make([]byte, n)
	copy(data, r.buf[r.pos:r.pos+n])
	r.pos += n
	return data, nil
}

// Skip skips n bytes.
func (r *BufferReader) Skip(n int) error {
	if r.Remaining() < n {
		return io.ErrUnexpectedEOF
	}
	r.pos += n
	return nil
}
