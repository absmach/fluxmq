// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package payload

import (
	"testing"
)

// The pool had no benchmarks. Its open questions — whether the fixed-size
// channels are worth their retained memory, whether a hit is meaningfully
// cheaper than an allocation, and how it behaves under fan-out — were being
// argued without numbers, so these establish them.
//
// Size classes are 1 KB / 64 KB / 1 MB (Pool.Get), with channel capacities
// 1000 / 500 / 100 (NewPool), so a full pool retains roughly 133 MB that the
// collector cannot reclaim.
//
// What these first measured, which is worth keeping in view: a hit is excellent
// — about 31ns and zero allocations, flat across every size class — but a miss
// is worse than having no pool at all. Pool.Get rounds a miss up to the whole
// size class, so a 256 KB request allocates 1 MB, and PoolMiss runs roughly
// 5x slower than PoolUnpooled for the same bytes. The pool is therefore a bet
// on hit rate: it pays well while the class has a spare buffer and costs
// several times its own weight in the burst that empties one.

var benchSizes = []struct {
	name string
	size int
}{
	{"small_512B", 512},
	{"medium_8KB", 8 << 10},
	{"large_256KB", 256 << 10},
}

// BenchmarkPoolHit measures the reuse path: the buffer is returned before the
// next acquisition, so the pool always has one to hand back.
func BenchmarkPoolHit(b *testing.B) {
	for _, size := range benchSizes {
		b.Run(size.name, func(b *testing.B) {
			pool := NewPool()
			// Prime the class so the first iteration is a hit like the rest.
			pool.get(size.size).Release()

			b.ReportAllocs()
			for b.Loop() {
				pool.get(size.size).Release()
			}

			stats := pool.Stats()
			if stats.SmallMisses+stats.MediumMisses+stats.LargeMisses > 1 {
				b.Fatalf("expected hits, got misses: %+v", stats)
			}
		})
	}
}

// BenchmarkPoolMiss measures the path taken when the class is empty, which is
// what a burst wider than the channel capacity degrades to.
//
// The pool has zero capacity, so every Release is dropped and every Get misses.
// An earlier version held buffers in a slice to force misses; it measured the
// collector's response to holding a thousand large buffers instead, and for the
// small class it stopped missing after the first drain returned them.
func BenchmarkPoolMiss(b *testing.B) {
	for _, size := range benchSizes {
		b.Run(size.name, func(b *testing.B) {
			pool := NewPoolWithCapacity(0, 0, 0)

			b.ReportAllocs()
			for b.Loop() {
				pool.get(size.size).Release()
			}

			if stats := pool.Stats(); stats.SmallHits+stats.MediumHits+stats.LargeHits != 0 {
				b.Fatalf("expected only misses, got hits: %+v", stats)
			}
		})
	}
}

// BenchmarkPoolUnpooled is the comparison the pool has to beat: a plain
// allocation of the same size, with no pool behind it.
func BenchmarkPoolUnpooled(b *testing.B) {
	for _, size := range benchSizes {
		b.Run(size.name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				NewBuffer(make([]byte, size.size), nil).Release()
			}
		})
	}
}

// BenchmarkPoolParallel measures the channel under fan-out. A single channel
// per size class is a contention point that a per-P cache such as sync.Pool
// would not have.
func BenchmarkPoolParallel(b *testing.B) {
	for _, size := range benchSizes {
		b.Run(size.name, func(b *testing.B) {
			pool := NewPool()

			b.ReportAllocs()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					pool.get(size.size).Release()
				}
			})
		})
	}
}

// BenchmarkFromBytes covers the only path production actually reaches: Get is
// exported but has no production caller, so every real acquisition copies.
func BenchmarkFromBytes(b *testing.B) {
	for _, size := range benchSizes {
		b.Run(size.name, func(b *testing.B) {
			pool := NewPool()
			data := make([]byte, size.size)

			b.ReportAllocs()
			b.SetBytes(int64(size.size))
			for b.Loop() {
				pool.FromBytes(data).Release()
			}
		})
	}
}
