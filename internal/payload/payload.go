// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

// Package payload centralizes the zero-copy payload ownership rules shared by
// the message types in storage/ and queue/types/. Both carry a legacy Payload
// byte slice alongside a pooled PayloadBuf; keeping the buffer lifetime logic
// in one place stops the two copies from drifting.
package payload

import (
	core "github.com/absmach/fluxmq/mqtt"
)

// Get returns the effective payload, preferring the zero-copy buffer over the
// legacy slice. The returned slice is only valid while buf is retained.
func Get(legacy []byte, buf *core.RefCountedBuffer) []byte {
	if buf != nil {
		return buf.Bytes()
	}
	return legacy
}

// Stable returns a payload slice that stays valid after buf is released:
// buffer-backed payloads are copied out of the pooled buffer, while a plain
// payload is returned as-is without allocating.
func Stable(legacy []byte, buf *core.RefCountedBuffer) []byte {
	if buf != nil {
		return append([]byte(nil), buf.Bytes()...)
	}
	return legacy
}

// FromBuffer transfers ownership of one reference to newBuf, releasing any
// previous buffer. It returns the (legacy, buf) field values to assign back:
// the legacy slice is cleared so Get resolves to the buffer.
func FromBuffer(prev, newBuf *core.RefCountedBuffer) ([]byte, *core.RefCountedBuffer) {
	if prev != nil {
		prev.Release()
	}
	return nil, newBuf
}

// FromBytes releases any previous buffer and creates a pooled buffer holding a
// copy of data (nil for empty input). It returns the (legacy, buf) field values
// to assign back.
func FromBytes(prev *core.RefCountedBuffer, data []byte) ([]byte, *core.RefCountedBuffer) {
	if prev != nil {
		prev.Release()
	}
	if len(data) > 0 {
		return nil, core.GetBufferWithData(data)
	}
	return nil, nil
}

// ReleaseBuffer releases buf and returns nil to assign back. Callers clear the
// legacy slice separately when their contract requires it, since the two
// message types differ on whether ReleasePayload also drops the legacy bytes.
func ReleaseBuffer(buf *core.RefCountedBuffer) *core.RefCountedBuffer {
	if buf != nil {
		buf.Release()
	}
	return nil
}

// Retain increments the buffer reference count for sharing across goroutines.
func Retain(buf *core.RefCountedBuffer) {
	if buf != nil {
		buf.Retain()
	}
}
