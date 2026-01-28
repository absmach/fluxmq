// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package codec

import (
	"encoding/binary"
	"errors"
)

// Errors for zero-copy decoding.
var (
	ErrBufferTooShort = errors.New("buffer too short")
	ErrMalformedVBI   = errors.New("malformed variable byte integer")
	ErrStringTooLong  = errors.New("string exceeds buffer")
)

// ZeroCopyReader provides zero-copy reading from a byte slice.
// It maintains an offset into the slice and provides methods to read
// various MQTT data types without allocation.
type ZeroCopyReader struct {
	data   []byte
	offset int
}

// NewZeroCopyReader creates a new reader from a byte slice.
func NewZeroCopyReader(data []byte) *ZeroCopyReader {
	return &ZeroCopyReader{data: data, offset: 0}
}

// Reset resets the reader to use a new byte slice.
func (r *ZeroCopyReader) Reset(data []byte) {
	r.data = data
	r.offset = 0
}

// Remaining returns the number of bytes remaining to be read.
func (r *ZeroCopyReader) Remaining() int {
	return len(r.data) - r.offset
}

// Offset returns the current read position.
func (r *ZeroCopyReader) Offset() int {
	return r.offset
}

// ReadByte reads a single byte.
func (r *ZeroCopyReader) ReadByte() (byte, error) {
	if r.offset >= len(r.data) {
		return 0, ErrBufferTooShort
	}
	b := r.data[r.offset]
	r.offset++
	return b, nil
}

// ReadUint16 reads a big-endian uint16.
func (r *ZeroCopyReader) ReadUint16() (uint16, error) {
	if r.offset+2 > len(r.data) {
		return 0, ErrBufferTooShort
	}
	v := binary.BigEndian.Uint16(r.data[r.offset:])
	r.offset += 2
	return v, nil
}

// ReadUint32 reads a big-endian uint32.
func (r *ZeroCopyReader) ReadUint32() (uint32, error) {
	if r.offset+4 > len(r.data) {
		return 0, ErrBufferTooShort
	}
	v := binary.BigEndian.Uint32(r.data[r.offset:])
	r.offset += 4
	return v, nil
}

// ReadVBI reads a Variable Byte Integer (1-4 bytes).
func (r *ZeroCopyReader) ReadVBI() (int, error) {
	var vbi uint32
	var multiplier uint32
	for i := 0; i < 4; i++ {
		if r.offset >= len(r.data) {
			return 0, ErrBufferTooShort
		}
		b := r.data[r.offset]
		r.offset++
		vbi |= uint32(b&0x7F) << multiplier
		if (b & 0x80) == 0 {
			return int(vbi), nil
		}
		multiplier += 7
	}
	return 0, ErrMalformedVBI
}

// ReadBytes reads a length-prefixed byte slice (2-byte length prefix).
// Returns a slice pointing into the original data (zero-copy).
func (r *ZeroCopyReader) ReadBytes() ([]byte, error) {
	length, err := r.ReadUint16()
	if err != nil {
		return nil, err
	}
	if r.offset+int(length) > len(r.data) {
		return nil, ErrStringTooLong
	}
	b := r.data[r.offset : r.offset+int(length)]
	r.offset += int(length)
	return b, nil
}

// ReadBytesNoCopy is an alias for ReadBytes, emphasizing zero-copy semantics.
// The returned slice points into the original data buffer.
// IMPORTANT: The returned slice is only valid as long as the original data is not modified.
func (r *ZeroCopyReader) ReadBytesNoCopy() ([]byte, error) {
	return r.ReadBytes()
}

// ReadString reads a length-prefixed UTF-8 string.
// Note: This does allocate because Go strings are immutable.
// For zero-copy string handling, use ReadBytes and convert only when needed.
func (r *ZeroCopyReader) ReadString() (string, error) {
	b, err := r.ReadBytes()
	if err != nil {
		return "", err
	}
	return string(b), nil
}

// ReadStringNoCopy reads a length-prefixed string as a byte slice (zero-copy).
// Convert to string only when needed to avoid allocation.
func (r *ZeroCopyReader) ReadStringNoCopy() ([]byte, error) {
	return r.ReadBytes()
}

// ReadN reads exactly n bytes and returns a slice pointing into the original data.
func (r *ZeroCopyReader) ReadN(n int) ([]byte, error) {
	if r.offset+n > len(r.data) {
		return nil, ErrBufferTooShort
	}
	b := r.data[r.offset : r.offset+n]
	r.offset += n
	return b, nil
}

// ReadRemaining returns all remaining bytes as a slice (zero-copy).
func (r *ZeroCopyReader) ReadRemaining() []byte {
	b := r.data[r.offset:]
	r.offset = len(r.data)
	return b
}

// Skip advances the offset by n bytes.
func (r *ZeroCopyReader) Skip(n int) error {
	if r.offset+n > len(r.data) {
		return ErrBufferTooShort
	}
	r.offset += n
	return nil
}

// Peek returns the next n bytes without advancing the offset.
func (r *ZeroCopyReader) Peek(n int) ([]byte, error) {
	if r.offset+n > len(r.data) {
		return nil, ErrBufferTooShort
	}
	return r.data[r.offset : r.offset+n], nil
}

// PeekByte returns the next byte without advancing the offset.
func (r *ZeroCopyReader) PeekByte() (byte, error) {
	if r.offset >= len(r.data) {
		return 0, ErrBufferTooShort
	}
	return r.data[r.offset], nil
}

// Read implements io.Reader interface.
// This allows ZeroCopyReader to be used with functions expecting io.Reader.
func (r *ZeroCopyReader) Read(p []byte) (n int, err error) {
	if r.offset >= len(r.data) {
		return 0, ErrBufferTooShort
	}
	n = copy(p, r.data[r.offset:])
	r.offset += n
	return n, nil
}
