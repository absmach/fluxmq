// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package payload

import (
	"bytes"
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBufferReferenceLifetime(t *testing.T) {
	pool := NewPool()
	buf := pool.FromBytes([]byte("payload"))
	buf.Retain()

	buf.Release()
	if got := string(buf.Bytes()); got != "payload" {
		t.Fatalf("payload after first release = %q", got)
	}
	buf.Release()

	// sync.Pool promises reuse, not identity: which buffer comes back is the
	// runtime's decision, and it may drop the lot at any GC. What is
	// observable, and what matters, is that the release reached the pool
	// instead of dropping the buffer on the floor.
	reused := pool.get(len("payload"))
	defer reused.Release()
	if pool.Stats().SmallHits == 0 {
		t.Fatal("released buffer was not returned to its size-class pool")
	}
}

func TestFromBytesCopiesInput(t *testing.T) {
	input := []byte("payload")
	buf := FromBytes(input)
	defer buf.Release()

	input[0] = 'X'
	if !bytes.Equal(buf.Bytes(), []byte("payload")) {
		t.Fatalf("buffer aliases caller input: %q", buf.Bytes())
	}
}

// The reason the size classes are sync.Pools rather than buffered channels: a
// channel holds a strong reference to everything in it, so a burst of large
// payloads pinned memory for the life of the process, invisible to GC and to
// any memory-pressure signal. A sync.Pool is drained.
//
// This asserts the property directly. Fill a class, collect twice — once to
// move the pool's live entries to its victim cache and once to drop them — and
// the next acquisition has to allocate again.
func TestPoolDoesNotPinBuffersAcrossGC(t *testing.T) {
	pool := NewPool()

	for range 64 {
		pool.put(pool.get(512))
	}
	require.NotZero(t, pool.Stats().SmallHits, "the pool must serve a warm acquisition from itself")

	runtime.GC()
	runtime.GC()

	before := pool.Stats().SmallMisses
	pool.get(512).Release()
	require.Greater(t, pool.Stats().SmallMisses, before,
		"a collected pool must allocate again rather than hand back a pinned buffer")
}

// An oversized buffer is served by a plain allocation and never pooled, so one
// outsized payload cannot set the capacity every later caller inherits.
func TestOversizedBufferIsNeverPooled(t *testing.T) {
	pool := NewPool()

	buf := pool.get(largeClass + 1)
	require.Equal(t, largeClass+1, cap(buf.data))
	buf.Release()

	next := pool.get(largeClass + 1)
	defer next.Release()
	require.NotSame(t, buf, next, "an oversized buffer must not be retained")
}

func TestPoolRejectsBuffersOutsideExactSizeClasses(t *testing.T) {
	pool := NewPool()
	invalid := newBuffer(make([]byte, 1, 1), pool)
	invalid.Release()

	before := pool.Stats().SmallMisses
	buf := pool.get(512)
	defer buf.Release()
	require.Equal(t, smallClass, cap(buf.data))
	require.Greater(t, pool.Stats().SmallMisses, before,
		"an arbitrary-capacity buffer must not enter a size-class pool")
}
