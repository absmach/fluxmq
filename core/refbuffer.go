// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package core

import (
	"sync/atomic"
)

// RefCountedBuffer is a reference-counted byte buffer that eliminates
// unnecessary copying of message payloads. When a buffer is created,
// its reference count is 1. Retain() increments the count, and Release()
// decrements it. When the count reaches 0, the buffer is returned to the pool.
//
// This is critical for MQTT message routing where the same payload
// may be delivered to multiple subscribers. Instead of copying the payload
// for each subscriber, we retain a reference and share the same underlying bytes.
type RefCountedBuffer struct {
	data     []byte
	refCount atomic.Int32
	pool     *BufferPool
}

// NewRefCountedBuffer creates a new buffer with the given data.
// The buffer starts with a reference count of 1.
func NewRefCountedBuffer(data []byte, pool *BufferPool) *RefCountedBuffer {
	buf := &RefCountedBuffer{
		data: data,
		pool: pool,
	}
	buf.refCount.Store(1)
	return buf
}

// Bytes returns the underlying byte slice.
// The slice must not be modified after the buffer is shared.
func (r *RefCountedBuffer) Bytes() []byte {
	if r == nil {
		return nil
	}
	return r.data
}

// Len returns the length of the buffer.
func (r *RefCountedBuffer) Len() int {
	if r == nil {
		return 0
	}
	return len(r.data)
}

// Retain increments the reference count.
// Must be called before sharing the buffer with another goroutine.
func (r *RefCountedBuffer) Retain() {
	if r == nil {
		return
	}
	r.refCount.Add(1)
}

// Release decrements the reference count.
// When the count reaches 0, the buffer is returned to the pool.
// Must be called by every holder of the buffer when done.
func (r *RefCountedBuffer) Release() {
	if r == nil {
		return
	}

	newCount := r.refCount.Add(-1)
	if newCount == 0 {
		// Last reference released, return to pool
		if r.pool != nil {
			r.pool.Put(r)
		}
	} else if newCount < 0 {
		// Should never happen - indicates a bug
		panic("RefCountedBuffer: negative reference count")
	}
}

// RefCount returns the current reference count (for testing/debugging).
func (r *RefCountedBuffer) RefCount() int32 {
	if r == nil {
		return 0
	}
	return r.refCount.Load()
}

// BufferPool manages a pool of reusable RefCountedBuffers.
// Buffers are organized into size classes to reduce allocation overhead.
type BufferPool struct {
	// Size-based pools
	small  chan *RefCountedBuffer // <1KB
	medium chan *RefCountedBuffer // 1KB-64KB
	large  chan *RefCountedBuffer // 64KB-1MB

	// Pool configuration
	smallCap  int
	mediumCap int
	largeCap  int

	// Statistics (for monitoring)
	stats BufferPoolStats
}

// BufferPoolStats tracks pool performance metrics.
type BufferPoolStats struct {
	SmallHits   atomic.Uint64
	MediumHits  atomic.Uint64
	LargeHits   atomic.Uint64
	SmallMisses atomic.Uint64
	MediumMisses atomic.Uint64
	LargeMisses atomic.Uint64
}

// NewBufferPool creates a new buffer pool with default capacity.
func NewBufferPool() *BufferPool {
	return NewBufferPoolWithCapacity(1000, 500, 100)
}

// NewBufferPoolWithCapacity creates a new buffer pool with custom capacity for each size class.
func NewBufferPoolWithCapacity(smallCap, mediumCap, largeCap int) *BufferPool {
	return &BufferPool{
		small:     make(chan *RefCountedBuffer, smallCap),
		medium:    make(chan *RefCountedBuffer, mediumCap),
		large:     make(chan *RefCountedBuffer, largeCap),
		smallCap:  smallCap,
		mediumCap: mediumCap,
		largeCap:  largeCap,
	}
}

// Get retrieves a buffer of at least the requested size from the pool.
// If no buffer is available, allocates a new one.
func (p *BufferPool) Get(size int) *RefCountedBuffer {
	var pool chan *RefCountedBuffer
	var bufSize int
	var hits, misses *atomic.Uint64

	// Determine which pool to use based on size
	switch {
	case size <= 1024:
		pool = p.small
		bufSize = 1024
		hits = &p.stats.SmallHits
		misses = &p.stats.SmallMisses
	case size <= 65536:
		pool = p.medium
		bufSize = 65536
		hits = &p.stats.MediumHits
		misses = &p.stats.MediumMisses
	case size <= 1048576:
		pool = p.large
		bufSize = 1048576
		hits = &p.stats.LargeHits
		misses = &p.stats.LargeMisses
	default:
		// Very large buffers are not pooled
		misses = &p.stats.LargeMisses
		misses.Add(1)
		return NewRefCountedBuffer(make([]byte, size), p)
	}

	// Try to get from pool
	select {
	case buf := <-pool:
		// Reuse buffer from pool
		hits.Add(1)
		buf.data = buf.data[:size] // Resize to requested length
		buf.refCount.Store(1)      // Reset reference count
		return buf
	default:
		// Pool empty, allocate new
		misses.Add(1)
		return NewRefCountedBuffer(make([]byte, size, bufSize), p)
	}
}

// GetWithData creates a new buffer containing a copy of the provided data.
// This is a convenience method for cases where you need to copy data into a pooled buffer.
func (p *BufferPool) GetWithData(data []byte) *RefCountedBuffer {
	buf := p.Get(len(data))
	copy(buf.data, data)
	return buf
}

// Put returns a buffer to the pool for reuse.
// Called automatically by RefCountedBuffer.Release() when refcount reaches 0.
func (p *BufferPool) Put(buf *RefCountedBuffer) {
	if buf == nil {
		return
	}

	// Determine which pool based on capacity
	var pool chan *RefCountedBuffer
	cap := cap(buf.data)

	switch {
	case cap <= 1024:
		pool = p.small
	case cap <= 65536:
		pool = p.medium
	case cap <= 1048576:
		pool = p.large
	default:
		// Very large buffers are not pooled, just GC them
		return
	}

	// Try to return to pool (non-blocking)
	select {
	case pool <- buf:
		// Successfully returned to pool
	default:
		// Pool full, let GC handle it
		// This is fine - it just means we have enough buffers in circulation
	}
}

// Stats returns current pool statistics.
func (p *BufferPool) Stats() BufferPoolStats {
	return p.stats
}

// Clear empties all pools. Useful for testing.
func (p *BufferPool) Clear() {
	// Drain all pools
	for {
		select {
		case <-p.small:
		case <-p.medium:
		case <-p.large:
		default:
			return
		}
	}
}

// DefaultBufferPool is a global buffer pool instance.
// Can be used for simple cases where a single global pool is sufficient.
var DefaultBufferPool = NewBufferPool()

// GetBuffer is a convenience function that uses the default buffer pool.
func GetBuffer(size int) *RefCountedBuffer {
	return DefaultBufferPool.Get(size)
}

// GetBufferWithData is a convenience function that uses the default buffer pool.
func GetBufferWithData(data []byte) *RefCountedBuffer {
	return DefaultBufferPool.GetWithData(data)
}
