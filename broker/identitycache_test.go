// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package broker

import (
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestIdentityCache_StoreLoad(t *testing.T) {
	c := newIdentityCache(10, 0)
	c.Store("c1", "ext1")
	got, ok := c.Load("c1")
	assert.True(t, ok)
	assert.Equal(t, "ext1", got)
}

func TestIdentityCache_LoadMiss(t *testing.T) {
	c := newIdentityCache(10, 0)
	got, ok := c.Load("missing")
	assert.False(t, ok)
	assert.Equal(t, "", got)
}

func TestIdentityCache_DeleteRemoves(t *testing.T) {
	c := newIdentityCache(10, 0)
	c.Store("c1", "ext1")
	c.Delete("c1")
	_, ok := c.Load("c1")
	assert.False(t, ok)
	assert.Equal(t, 0, c.Len())
}

func TestIdentityCache_OverwriteSameKey(t *testing.T) {
	c := newIdentityCache(10, 0)
	c.Store("c1", "ext1")
	c.Store("c1", "ext2")
	got, _ := c.Load("c1")
	assert.Equal(t, "ext2", got)
	assert.Equal(t, 1, c.Len())
}

func TestIdentityCache_EvictsLRUWhenAtCapacity(t *testing.T) {
	c := newIdentityCache(3, 0)
	c.Store("c1", "ext1")
	c.Store("c2", "ext2")
	c.Store("c3", "ext3")
	// Touch c1 so c2 becomes LRU.
	_, _ = c.Load("c1")
	c.Store("c4", "ext4")

	_, ok := c.Load("c2")
	assert.False(t, ok, "c2 should be evicted as LRU")
	for _, id := range []string{"c1", "c3", "c4"} {
		_, ok := c.Load(id)
		assert.True(t, ok, "%s should remain", id)
	}
	assert.Equal(t, 3, c.Len())
}

func TestIdentityCache_TTLExpiresOnLoad(t *testing.T) {
	now := time.Unix(0, 0)
	c := newIdentityCache(10, 100*time.Millisecond)
	c.clock = func() time.Time { return now }

	c.Store("c1", "ext1")
	now = now.Add(50 * time.Millisecond)
	if _, ok := c.Load("c1"); !ok {
		t.Fatal("entry should still be valid before TTL")
	}

	now = now.Add(200 * time.Millisecond)
	_, ok := c.Load("c1")
	assert.False(t, ok, "entry should be evicted after TTL")
	assert.Equal(t, 0, c.Len(), "expired entry should be removed from index")
}

func TestIdentityCache_ZeroTTLNoExpiry(t *testing.T) {
	c := newIdentityCache(10, 0)
	c.Store("c1", "ext1")
	// No clock advance — TTL=0 means entries live forever.
	got, ok := c.Load("c1")
	assert.True(t, ok)
	assert.Equal(t, "ext1", got)
}

func TestIdentityCache_ZeroCapacityNoEviction(t *testing.T) {
	c := newIdentityCache(0, 0)
	for i := range 1000 {
		c.Store(strconv.Itoa(i), strconv.Itoa(i))
	}
	assert.Equal(t, 1000, c.Len())
}

func TestIdentityCache_ConcurrentSafe(t *testing.T) {
	c := newIdentityCache(100, 0)
	var wg sync.WaitGroup
	for i := range 10 {
		wg.Add(1)
		go func(base int) {
			defer wg.Done()
			for j := range 100 {
				id := strconv.Itoa(base*100 + j)
				c.Store(id, id)
				_, _ = c.Load(id)
				if j%5 == 0 {
					c.Delete(id)
				}
			}
		}(i)
	}
	wg.Wait()
	// Size must respect cap.
	assert.LessOrEqual(t, c.Len(), 100)
}
