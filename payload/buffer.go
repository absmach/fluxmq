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

type poolStats struct {
	smallHits    atomic.Uint64
	mediumHits   atomic.Uint64
	largeHits    atomic.Uint64
	smallMisses  atomic.Uint64
	mediumMisses atomic.Uint64
	largeMisses  atomic.Uint64
}

// Pool reuses payload buffers in three size classes.
type Pool struct {
	small  chan *Buffer
	medium chan *Buffer
	large  chan *Buffer
	stats  poolStats
}

// NewPool creates a pool sized for normal broker traffic.
func NewPool() *Pool { return NewPoolWithCapacity(1000, 500, 100) }

// NewPoolWithCapacity creates a pool with explicit size-class capacities.
func NewPoolWithCapacity(small, medium, large int) *Pool {
	return &Pool{
		small:  make(chan *Buffer, small),
		medium: make(chan *Buffer, medium),
		large:  make(chan *Buffer, large),
	}
}

// get returns a buffer of size bytes with one owned reference.
//
// It is unexported because a pool hit reslices the previous owner's buffer and
// does not clear it: a caller that asks for n bytes and fills fewer hands the
// tail of a previous message to whoever reads it next. The only caller,
// FromBytes, copies over the full length immediately. Anything that needs this
// from outside the package wants a variant that zeroes or takes the fill, not
// this contract with a warning attached.
func (p *Pool) get(size int) *Buffer {
	var class chan *Buffer
	var capacity int
	var hits, misses *atomic.Uint64

	switch {
	case size <= 1024:
		class, capacity = p.small, 1024
		hits, misses = &p.stats.smallHits, &p.stats.smallMisses
	case size <= 65536:
		class, capacity = p.medium, 65536
		hits, misses = &p.stats.mediumHits, &p.stats.mediumMisses
	case size <= 1048576:
		class, capacity = p.large, 1048576
		hits, misses = &p.stats.largeHits, &p.stats.largeMisses
	default:
		p.stats.largeMisses.Add(1)
		return NewBuffer(make([]byte, size), p)
	}

	select {
	case buf := <-class:
		hits.Add(1)
		buf.data = buf.data[:size]
		buf.refs.Store(1)
		return buf
	default:
		misses.Add(1)
		return NewBuffer(make([]byte, size, capacity), p)
	}
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

	var class chan *Buffer
	switch capacity := cap(buf.data); {
	case capacity <= 1024:
		class = p.small
	case capacity <= 65536:
		class = p.medium
	case capacity <= 1048576:
		class = p.large
	default:
		return
	}

	select {
	case class <- buf:
	default:
	}
}

// Stats returns a consistent snapshot of pool counters.
func (p *Pool) Stats() PoolSnapshot {
	return PoolSnapshot{
		SmallHits:    p.stats.smallHits.Load(),
		MediumHits:   p.stats.mediumHits.Load(),
		LargeHits:    p.stats.largeHits.Load(),
		SmallMisses:  p.stats.smallMisses.Load(),
		MediumMisses: p.stats.mediumMisses.Load(),
		LargeMisses:  p.stats.largeMisses.Load(),
	}
}

// Clear empties every size class.
func (p *Pool) Clear() {
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

// DefaultPool is shared by envelopes constructed from byte slices.
var DefaultPool = NewPool()

// FromBytes returns a DefaultPool buffer containing a copy of data.
func FromBytes(data []byte) *Buffer { return DefaultPool.FromBytes(data) }
