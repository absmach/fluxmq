// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

// Package keylock provides per-key mutual exclusion for state that is naturally
// partitioned — one session, one consumer group — where a single lock over the
// whole collection would serialise unrelated work.
package keylock

import (
	"hash/maphash"
	"sync"
)

// DefaultShards is the shard count used by Sharded. It is a fixed number rather
// than one mutex per key so memory stays bounded regardless of how many keys the
// caller uses, at the cost of occasional false sharing between two keys that
// hash together.
const DefaultShards = 128

// Sharded provides per-key locking across a fixed set of mutexes. The zero value
// is ready to use and safe for concurrent use.
//
// Two different keys may still contend if they hash to the same shard, so this
// bounds contention rather than eliminating it. It is not reentrant: a key held
// by a goroutine must not be locked again by that goroutine, and two keys that
// share a shard behave as one lock for that purpose.
type Sharded struct {
	seed   maphash.Seed
	seeded sync.Once
	shards [DefaultShards]sync.Mutex
}

// Guard is one key's resolved shard. Resolving it once keeps the hash off the
// unlock path, which otherwise pays for it a second time on every operation.
type Guard struct {
	shard *sync.Mutex
}

// Key resolves the guard for a single-part key.
func (s *Sharded) Key(key string) Guard {
	return Guard{shard: &s.shards[maphash.String(s.hashSeed(), key)%DefaultShards]}
}

// KeyPair resolves the guard for a two-part key.
//
// The parts are hashed separately and mixed rather than joined into a new
// string, so no allocation is needed and ("ab", "c") cannot collide with
// ("a", "bc").
func (s *Sharded) KeyPair(first, second string) Guard {
	return Guard{shard: &s.shards[pairHash(s.hashSeed(), first, second)%DefaultShards]}
}

// prime64 separates the two halves of a pair key so their hashes do not cancel.
const prime64 = 1099511628211

// hashSeed lazily seeds the table so the zero value stays usable.
func (s *Sharded) hashSeed() maphash.Seed {
	s.seeded.Do(func() { s.seed = maphash.MakeSeed() })
	return s.seed
}

// Lock acquires the guarded shard.
func (g Guard) Lock() { g.shard.Lock() }

// Unlock releases the guarded shard.
func (g Guard) Unlock() { g.shard.Unlock() }

// pairHash exposes the pair mixing for tests, which assert that the two parts
// are separated rather than concatenated.
func pairHash(seed maphash.Seed, first, second string) uint64 {
	return maphash.String(seed, first)*prime64 ^ maphash.String(seed, second)
}
