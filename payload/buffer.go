// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

// Package payload owns the broker's immutable, reference-counted message
// payloads. It is deliberately protocol-neutral: payload lifetime is a broker
// concern, not an MQTT implementation detail.
// Package payload provides the broker's reference-counted message buffer.
//
// A nil payload and a zero-length payload are the same state: FromBytes(nil)
// and FromBytes([]byte{}) both return nil, so Envelope.Payload == nil covers
// both. This is deliberate rather than an oversight — MQTT's zero-length
// retained publish, which clears a retained message, works correctly under it,
// and nothing in the broker needs to tell "no payload" from "empty payload".
// A protocol that ever needs a real absent-payload state has to add one rather
// than assume it is already there.
package payload

import (
	"log/slog"
	"runtime/debug"
	"sync"
	"sync/atomic"
)

var doubleReleaseCount atomic.Uint64

// DoubleReleaseCount returns the number of double releases observed since
// process start. A non-zero value identifies a broker ownership bug.
func DoubleReleaseCount() uint64 { return doubleReleaseCount.Load() }

// Buffer is an immutable, reference-counted byte buffer. New buffers own one
// reference. Every holder must retain before sharing and release when done.
type Buffer struct {
	data []byte
	refs atomic.Int32
	pool *Pool
}

// NewBuffer takes ownership of data. Callers must not modify data afterwards.
func NewBuffer(data []byte, pool *Pool) *Buffer {
	buf := &Buffer{data: data, pool: pool}
	buf.refs.Store(1)
	return buf
}

// Bytes returns an immutable view valid while the caller owns a reference.
func (b *Buffer) Bytes() []byte {
	if b == nil {
		return nil
	}
	return b.data
}

// Len returns the payload length.
func (b *Buffer) Len() int {
	if b == nil {
		return 0
	}
	return len(b.data)
}

// Retain adds one reference before the buffer is shared.
func (b *Buffer) Retain() {
	if b != nil {
		b.refs.Add(1)
	}
}

// Release drops one reference and returns an unreferenced buffer to its pool.
func (b *Buffer) Release() {
	if b == nil {
		return
	}

	refs := b.refs.Add(-1)
	if refs == 0 {
		if b.pool != nil {
			b.pool.put(b)
		}
		return
	}
	if refs < 0 {
		count := doubleReleaseCount.Add(1)
		b.refs.Store(0)
		// One ownership bug inside a fan-out loop reaches here once per
		// message, and a stack capture is expensive enough to change the
		// behaviour being diagnosed. The first few carry the stack that
		// identifies the caller; DoubleReleaseCount is the durable signal for
		// the rest.
		if count > loggedDoubleReleases {
			return
		}
		slog.Error("payload.Buffer: negative reference count",
			slog.Int("count", int(refs)),
			slog.Uint64("double_releases", count),
			slog.String("stack", string(debug.Stack())))
	}
}

// loggedDoubleReleases bounds how many negative reference counts capture a
// stack. The first ones say where the bug is; the counter says how bad it is.
const loggedDoubleReleases = 5

// RefCount returns the current reference count for diagnostics and tests.
func (b *Buffer) RefCount() int32 {
	if b == nil {
		return 0
	}
	return b.refs.Load()
}

// PoolSnapshot is a point-in-time view of pool hit and miss counters.
type PoolSnapshot struct {
	SmallHits    uint64
	MediumHits   uint64
	LargeHits    uint64
	SmallMisses  uint64
	MediumMisses uint64
	LargeMisses  uint64
}

// poolStats counts acquisitions and the subset of them that had to allocate.
// Hits are derived rather than counted: a hit is an acquisition that did not
// reach the class's New, and incrementing a separate counter for it would have
// to observe that from outside, which cannot be done without a race.
type poolStats struct {
	smallGets    atomic.Uint64
	mediumGets   atomic.Uint64
	largeGets    atomic.Uint64
	smallMisses  atomic.Uint64
	mediumMisses atomic.Uint64
	largeMisses  atomic.Uint64
}

// Pool reuses payload buffers in three size classes.
//
// Each class is a sync.Pool, which the runtime drains on GC. The buffered
// channels this replaces held strong references to everything in them, so a
// burst of large payloads pinned memory for the life of the process — the
// defaults allowed roughly 133 MB — invisible to GC and to any memory-pressure
// signal. sync.Pool also keeps a per-P cache, so acquiring does not contend on
// one channel per size class across every publisher.
type Pool struct {
	small  sync.Pool
	medium sync.Pool
	large  sync.Pool
	stats  poolStats
}

// NewPool creates a pool sized for normal broker traffic.
// NewPool creates a pool over the three size classes.
//
// There is no capacity to configure: how much a sync.Pool retains is the
// runtime's decision, revised at every GC. The constructor that took three
// capacities is gone with the channels — those numbers set a retention ceiling
// nobody tuned and GC could not lower.
func NewPool() *Pool {
	p := &Pool{}
	p.small.New = func() any {
		p.stats.smallMisses.Add(1)
		return NewBuffer(make([]byte, 0, smallClass), p)
	}
	p.medium.New = func() any {
		p.stats.mediumMisses.Add(1)
		return NewBuffer(make([]byte, 0, mediumClass), p)
	}
	p.large.New = func() any {
		p.stats.largeMisses.Add(1)
		return NewBuffer(make([]byte, 0, largeClass), p)
	}
	return p
}

// Size classes. A buffer is pooled in the smallest class that fits it, and a
// request larger than the largest class is served by a plain allocation that is
// never pooled.
const (
	smallClass  = 1024
	mediumClass = 65536
	largeClass  = 1048576
)

// get returns a buffer of size bytes with one owned reference.
//
// It is unexported because a pool hit reslices the previous owner's buffer and
// does not clear it: a caller that asks for n bytes and fills fewer hands the
// tail of a previous message to whoever reads it next. The only caller,
// FromBytes, copies over the full length immediately. Anything that needs this
// from outside the package wants a variant that zeroes or takes the fill, not
// this contract with a warning attached.
func (p *Pool) get(size int) *Buffer {
	var class *sync.Pool
	var gets *atomic.Uint64

	switch {
	case size <= smallClass:
		class, gets = &p.small, &p.stats.smallGets
	case size <= mediumClass:
		class, gets = &p.medium, &p.stats.mediumGets
	case size <= largeClass:
		class, gets = &p.large, &p.stats.largeGets
	default:
		// Larger than any class: allocate exactly, and do not pool it. Handing
		// it to the large class would let one oversized payload set the
		// capacity every later caller inherits.
		p.stats.largeGets.Add(1)
		p.stats.largeMisses.Add(1)
		return NewBuffer(make([]byte, size), nil)
	}

	gets.Add(1)
	buf := class.Get().(*Buffer)
	buf.data = buf.data[:size]
	buf.refs.Store(1)
	return buf
}

// FromBytes returns a pooled buffer containing a copy of data.
func (p *Pool) FromBytes(data []byte) *Buffer {
	if len(data) == 0 {
		return nil
	}
	buf := p.get(len(data))
	copy(buf.data, data)
	return buf
}

func (p *Pool) put(buf *Buffer) {
	if buf == nil {
		return
	}

	switch capacity := cap(buf.data); {
	case capacity <= smallClass:
		p.small.Put(buf)
	case capacity <= mediumClass:
		p.medium.Put(buf)
	case capacity <= largeClass:
		p.large.Put(buf)
	}
}

// Stats returns a consistent snapshot of pool counters.
func (p *Pool) Stats() PoolSnapshot {
	// Hits are gets that did not have to allocate. Both terms are read
	// independently, so a snapshot taken during a burst can show a hit count one
	// or two behind; it is a counter for operators, not an invariant.
	return PoolSnapshot{
		SmallHits:    hits(p.stats.smallGets.Load(), p.stats.smallMisses.Load()),
		MediumHits:   hits(p.stats.mediumGets.Load(), p.stats.mediumMisses.Load()),
		LargeHits:    hits(p.stats.largeGets.Load(), p.stats.largeMisses.Load()),
		SmallMisses:  p.stats.smallMisses.Load(),
		MediumMisses: p.stats.mediumMisses.Load(),
		LargeMisses:  p.stats.largeMisses.Load(),
	}
}

// hits derives the number of acquisitions served from the pool.
func hits(gets, misses uint64) uint64 {
	if gets < misses {
		return 0
	}
	return gets - misses
}

// DefaultPool is shared by envelopes constructed from byte slices.
var DefaultPool = NewPool()

// FromBytes returns a DefaultPool buffer containing a copy of data.
func FromBytes(data []byte) *Buffer { return DefaultPool.FromBytes(data) }
