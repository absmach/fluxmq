// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package keylock

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The same key must resolve to the same shard every time, or the lock guards
// nothing.
func TestSameKeyResolvesToOneShard(t *testing.T) {
	var locks Sharded

	assert.Equal(t, locks.Key("a").shard, locks.Key("a").shard)
	assert.Equal(t, locks.KeyPair("q", "g").shard, locks.KeyPair("q", "g").shard)
}

// A pair key must not be formed by concatenation: ("ab", "c") and ("a", "bc")
// are different groups and must not be forced onto one lock by the key scheme.
// They may still share a shard by chance, so this asserts the hashes differ
// rather than the shards.
func TestPairKeyPartsAreSeparated(t *testing.T) {
	var locks Sharded
	seed := locks.hashSeed()

	first := pairHash(seed, "ab", "c")
	second := pairHash(seed, "a", "bc")
	assert.NotEqual(t, first, second, "pair parts must be separated, not concatenated")
}

// Independent keys must be lockable at the same time. A single lock over the
// whole table would deadlock this.
func TestDifferentKeysDoNotSerialise(t *testing.T) {
	var locks Sharded

	// Find two keys on different shards; with 128 shards this is immediate.
	var a, b string
	for i := range 100 {
		candidate := string(rune('a' + i%26))
		if a == "" {
			a = candidate
			continue
		}
		if locks.Key(candidate).shard != locks.Key(a).shard {
			b = candidate
			break
		}
	}
	require.NotEmpty(t, b, "expected two keys on different shards")

	held := locks.Key(a)
	held.Lock()
	defer held.Unlock()

	done := make(chan struct{})
	go func() {
		other := locks.Key(b)
		other.Lock()
		other.Unlock()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("locking an unrelated key blocked on a held one")
	}
}

// The same key must exclude concurrent holders.
func TestSameKeyExcludes(t *testing.T) {
	var locks Sharded
	var counter int
	var wg sync.WaitGroup

	for range 50 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			guard := locks.KeyPair("queue", "group")
			guard.Lock()
			defer guard.Unlock()
			counter++
		}()
	}
	wg.Wait()

	assert.Equal(t, 50, counter, "the counter is only safe if the key excluded")
}

// The zero value must be usable without construction.
func TestZeroValueIsUsable(t *testing.T) {
	var locks Sharded
	assert.NotPanics(t, func() {
		guard := locks.Key("k")
		guard.Lock()
		guard.Unlock()
	})
}
