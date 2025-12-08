package pool

import (
	"sync"
)

// Buffer size classes for different packet sizes
const (
	SmallBufferSize  = 256    // Small packets (PINGREQ, PUBACK, etc.)
	MediumBufferSize = 4096   // Medium packets (typical PUBLISH)
	LargeBufferSize  = 65536  // Large packets (bulk data)
)

var (
	smallBufferPool = sync.Pool{
		New: func() any {
			b := make([]byte, 0, SmallBufferSize)
			return &b
		},
	}

	mediumBufferPool = sync.Pool{
		New: func() any {
			b := make([]byte, 0, MediumBufferSize)
			return &b
		},
	}

	largeBufferPool = sync.Pool{
		New: func() any {
			b := make([]byte, 0, LargeBufferSize)
			return &b
		},
	}
)

// AcquireSmallBuffer gets a small buffer (256 bytes capacity) from the pool.
// Suitable for: PINGREQ, PINGRESP, PUBACK, PUBREC, PUBREL, PUBCOMP.
func AcquireSmallBuffer() *[]byte {
	return smallBufferPool.Get().(*[]byte)
}

// ReleaseSmallBuffer returns a small buffer to the pool.
func ReleaseSmallBuffer(buf *[]byte) {
	if buf == nil {
		return
	}
	*buf = (*buf)[:0]
	smallBufferPool.Put(buf)
}

// AcquireMediumBuffer gets a medium buffer (4KB capacity) from the pool.
// Suitable for: typical PUBLISH, SUBSCRIBE, CONNECT messages.
func AcquireMediumBuffer() *[]byte {
	return mediumBufferPool.Get().(*[]byte)
}

// ReleaseMediumBuffer returns a medium buffer to the pool.
func ReleaseMediumBuffer(buf *[]byte) {
	if buf == nil {
		return
	}
	*buf = (*buf)[:0]
	mediumBufferPool.Put(buf)
}

// AcquireLargeBuffer gets a large buffer (64KB capacity) from the pool.
// Suitable for: large PUBLISH payloads.
func AcquireLargeBuffer() *[]byte {
	return largeBufferPool.Get().(*[]byte)
}

// ReleaseLargeBuffer returns a large buffer to the pool.
func ReleaseLargeBuffer(buf *[]byte) {
	if buf == nil {
		return
	}
	*buf = (*buf)[:0]
	largeBufferPool.Put(buf)
}

// AcquireBuffer gets a buffer of appropriate size from the pool.
// sizeHint is the expected size needed; the returned buffer may have more capacity.
func AcquireBuffer(sizeHint int) *[]byte {
	switch {
	case sizeHint <= SmallBufferSize:
		return AcquireSmallBuffer()
	case sizeHint <= MediumBufferSize:
		return AcquireMediumBuffer()
	default:
		return AcquireLargeBuffer()
	}
}

// ReleaseBuffer returns a buffer to the appropriate pool based on its capacity.
func ReleaseBuffer(buf *[]byte) {
	if buf == nil {
		return
	}
	cap := cap(*buf)
	*buf = (*buf)[:0]

	switch {
	case cap <= SmallBufferSize:
		smallBufferPool.Put(buf)
	case cap <= MediumBufferSize:
		mediumBufferPool.Put(buf)
	default:
		largeBufferPool.Put(buf)
	}
}

// ByteSlice is a wrapper around []byte that tracks its pool origin.
// This allows for automatic release back to the correct pool.
type ByteSlice struct {
	Data []byte
	pool *sync.Pool
}

// Release returns the buffer to its origin pool.
func (b *ByteSlice) Release() {
	if b.pool != nil {
		b.Data = b.Data[:0]
		b.pool.Put(&b.Data)
	}
}

// AcquireByteSlice gets a ByteSlice with automatic pool tracking.
func AcquireByteSlice(sizeHint int) *ByteSlice {
	var pool *sync.Pool
	var buf *[]byte

	switch {
	case sizeHint <= SmallBufferSize:
		pool = &smallBufferPool
		buf = smallBufferPool.Get().(*[]byte)
	case sizeHint <= MediumBufferSize:
		pool = &mediumBufferPool
		buf = mediumBufferPool.Get().(*[]byte)
	default:
		pool = &largeBufferPool
		buf = largeBufferPool.Get().(*[]byte)
	}

	return &ByteSlice{
		Data: *buf,
		pool: pool,
	}
}
