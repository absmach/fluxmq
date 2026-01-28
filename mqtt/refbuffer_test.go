// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package mqtt

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRefCountedBuffer_Basic(t *testing.T) {
	pool := NewBufferPool()
	data := []byte("hello world")

	buf := pool.GetWithData(data)
	require.NotNil(t, buf)
	assert.Equal(t, data, buf.Bytes())
	assert.Equal(t, len(data), buf.Len())
	assert.Equal(t, int32(1), buf.RefCount())

	buf.Release()
}

func TestRefCountedBuffer_RetainRelease(t *testing.T) {
	pool := NewBufferPool()
	buf := pool.Get(100)

	// Initial count is 1
	assert.Equal(t, int32(1), buf.RefCount())

	// Retain twice
	buf.Retain()
	assert.Equal(t, int32(2), buf.RefCount())

	buf.Retain()
	assert.Equal(t, int32(3), buf.RefCount())

	// Release back to 1
	buf.Release()
	assert.Equal(t, int32(2), buf.RefCount())

	buf.Release()
	assert.Equal(t, int32(1), buf.RefCount())

	// Final release returns to pool
	buf.Release()
	// Buffer is now back in pool
}

func TestRefCountedBuffer_PoolReuse(t *testing.T) {
	pool := NewBufferPool()

	// Get a small buffer
	buf1 := pool.Get(512)
	ptr1 := &buf1.data[0]
	buf1.Release()

	// Get another buffer of same size class
	buf2 := pool.Get(512)
	ptr2 := &buf2.data[0]

	// Should be the same buffer (reused from pool)
	assert.Equal(t, ptr1, ptr2, "Buffer should be reused from pool")

	buf2.Release()
}

func TestRefCountedBuffer_SizeClasses(t *testing.T) {
	pool := NewBufferPool()

	testCases := []struct {
		name        string
		size        int
		expectedCap int
	}{
		{"small", 512, 1024},
		{"medium", 32768, 65536},
		{"large", 500000, 1048576},
		{"exact_small", 1024, 1024},
		{"exact_medium", 65536, 65536},
		{"exact_large", 1048576, 1048576},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			buf := pool.Get(tc.size)
			assert.Equal(t, tc.size, len(buf.Bytes()))
			assert.Equal(t, tc.expectedCap, cap(buf.Bytes()))
			buf.Release()
		})
	}
}

func TestRefCountedBuffer_VeryLarge(t *testing.T) {
	pool := NewBufferPool()

	// Very large buffer (>1MB) should not be pooled
	buf := pool.Get(2 * 1024 * 1024)
	assert.Equal(t, 2*1024*1024, len(buf.Bytes()))
	buf.Release()

	// Verify it's not in any pool (stats should show miss)
	stats := pool.Stats()
	assert.Greater(t, stats.LargeMisses, uint64(0))
}

func TestRefCountedBuffer_NilSafety(t *testing.T) {
	var buf *RefCountedBuffer

	// All operations on nil buffer should be safe
	assert.Nil(t, buf.Bytes())
	assert.Equal(t, 0, buf.Len())
	assert.Equal(t, int32(0), buf.RefCount())
	buf.Retain()  // Should not panic
	buf.Release() // Should not panic
}

func TestRefCountedBuffer_ConcurrentAccess(t *testing.T) {
	pool := NewBufferPool()
	buf := pool.Get(1024)

	const numGoroutines = 100
	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	// Multiple goroutines retain the buffer
	for i := 0; i < numGoroutines; i++ {
		go func() {
			defer wg.Done()
			buf.Retain()
		}()
	}

	wg.Wait()

	// Count should be 1 (initial) + 100 (retains) = 101
	assert.Equal(t, int32(101), buf.RefCount())

	// Release all
	for i := 0; i < numGoroutines; i++ {
		buf.Release()
	}

	assert.Equal(t, int32(1), buf.RefCount())
	buf.Release() // Final release
}

func TestRefCountedBuffer_ConcurrentGetPut(t *testing.T) {
	pool := NewBufferPool()

	const numGoroutines = 100
	const iterations = 1000

	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				buf := pool.Get(512)
				// Simulate some work
				buf.data[0] = byte(j)
				buf.Release()
			}
		}()
	}

	wg.Wait()

	// Verify pool has buffers
	stats := pool.Stats()
	assert.Greater(t, stats.SmallHits, uint64(0), "Should have pool hits")
}

func TestBufferPool_Stats(t *testing.T) {
	pool := NewBufferPool()

	// First get is a miss
	buf1 := pool.Get(512)
	stats := pool.Stats()
	assert.Equal(t, uint64(0), stats.SmallHits)
	assert.Equal(t, uint64(1), stats.SmallMisses)

	buf1.Release()

	// Second get is a hit
	buf2 := pool.Get(512)
	stats = pool.Stats()
	assert.Equal(t, uint64(1), stats.SmallHits)
	assert.Equal(t, uint64(1), stats.SmallMisses)

	buf2.Release()
}

func TestBufferPool_PoolFull(t *testing.T) {
	// Create a pool with capacity 1
	pool := NewBufferPoolWithCapacity(1, 1, 1)

	// Get and release buffer 1 (miss, then goes to pool)
	buf1 := pool.Get(512)
	buf1.Release()

	// Get and release buffer 2 (hit - reuses buf1 from pool, then goes back to pool)
	buf2 := pool.Get(512)
	buf2.Release()

	// Get buffer 3 (hit - reuses buf1 from pool again)
	buf3 := pool.Get(512)
	stats := pool.Stats()
	// Should have 2 hits: buf2 and buf3 both got buf1 from pool
	assert.Equal(t, uint64(2), stats.SmallHits)
	assert.Equal(t, uint64(1), stats.SmallMisses)

	buf3.Release()
}

func TestBufferPool_Clear(t *testing.T) {
	pool := NewBufferPool()

	// Add some buffers to pool
	for i := 0; i < 10; i++ {
		buf := pool.Get(512)
		buf.Release()
	}

	// Clear the pool
	pool.Clear()

	// Next get should be a miss
	stats1 := pool.Stats()
	buf := pool.Get(512)
	stats2 := pool.Stats()

	assert.Equal(t, stats1.SmallMisses+1, stats2.SmallMisses)
	buf.Release()
}

func TestRefCountedBuffer_PanicOnNegativeCount(t *testing.T) {
	pool := NewBufferPool()
	buf := pool.Get(100)

	// Release once (count goes to 0, buffer returned to pool)
	buf.Release()

	// Releasing again should panic (negative count)
	assert.Panics(t, func() {
		buf.Release()
	})
}

func TestDefaultBufferPool(t *testing.T) {
	buf := GetBuffer(1024)
	assert.NotNil(t, buf)
	assert.Equal(t, 1024, len(buf.Bytes()))
	buf.Release()

	data := []byte("test data")
	buf2 := GetBufferWithData(data)
	assert.Equal(t, data, buf2.Bytes())
	buf2.Release()
}

// Benchmark: Measure overhead of ref counting

func BenchmarkRefCountedBuffer_RetainRelease(b *testing.B) {
	pool := NewBufferPool()
	buf := pool.Get(1024)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf.Retain()
		buf.Release()
	}

	buf.Release()
}

func BenchmarkRefCountedBuffer_GetRelease(b *testing.B) {
	pool := NewBufferPool()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf := pool.Get(1024)
		buf.Release()
	}
}

func BenchmarkRefCountedBuffer_GetRelease_Parallel(b *testing.B) {
	pool := NewBufferPool()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			buf := pool.Get(1024)
			buf.Release()
		}
	})
}

// Compare against plain allocation

func BenchmarkPlainAllocation_1KB(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_ = make([]byte, 1024)
	}
}

func BenchmarkRefCountedBuffer_1KB(b *testing.B) {
	pool := NewBufferPool()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf := pool.Get(1024)
		buf.Release()
	}
}

func BenchmarkPlainAllocation_64KB(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_ = make([]byte, 65536)
	}
}

func BenchmarkRefCountedBuffer_64KB(b *testing.B) {
	pool := NewBufferPool()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf := pool.Get(65536)
		buf.Release()
	}
}
