// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package broker

import (
	"container/list"
	"sync"
	"time"
)

// identityCache is a fixed-capacity TTL+LRU cache mapping protocol client IDs
// to external identities. It exists to bound the memory footprint of the
// AuthEngine: without it, a sync.Map of identities grew unbounded whenever
// Forget was missed (abrupt disconnects, session takeover races).
//
// Implementation: classic doubly-linked-list + map. Reads promote to MRU and
// drop entries whose TTL has elapsed; writes evict the LRU when the cap is
// reached. A single mutex is fine here — every read is a write (list reorder)
// so sync.RWMutex would not help.
//
// Zero values: capacity <= 0 disables size eviction (effectively unbounded),
// ttl <= 0 disables TTL eviction (entries live until evicted or deleted).
type identityCache struct {
	mu       sync.Mutex
	capacity int
	ttl      time.Duration
	clock    func() time.Time
	entries  map[string]*list.Element
	order    *list.List
}

type identityEntry struct {
	clientID string
	external string
	expires  time.Time // zero means no expiry
}

func newIdentityCache(capacity int, ttl time.Duration) *identityCache {
	return &identityCache{
		capacity: capacity,
		ttl:      ttl,
		clock:    time.Now,
		entries:  make(map[string]*list.Element),
		order:    list.New(),
	}
}

// Store inserts or updates the mapping for clientID. If the cache is at
// capacity, the least-recently-used entry is evicted.
func (c *identityCache) Store(clientID, external string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if elem, ok := c.entries[clientID]; ok {
		entry := elem.Value.(*identityEntry)
		entry.external = external
		entry.expires = c.expiry()
		c.order.MoveToFront(elem)
		return
	}

	entry := &identityEntry{clientID: clientID, external: external, expires: c.expiry()}
	elem := c.order.PushFront(entry)
	c.entries[clientID] = elem

	if c.capacity > 0 && c.order.Len() > c.capacity {
		c.evictOldest()
	}
}

// Load returns the cached external identity. Returns ("", false) when the
// entry is absent or its TTL has elapsed.
func (c *identityCache) Load(clientID string) (string, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	elem, ok := c.entries[clientID]
	if !ok {
		return "", false
	}
	entry := elem.Value.(*identityEntry)
	if !entry.expires.IsZero() && c.clock().After(entry.expires) {
		c.removeElement(elem)
		return "", false
	}
	c.order.MoveToFront(elem)
	return entry.external, true
}

// Delete removes the mapping if present. No-op when absent.
func (c *identityCache) Delete(clientID string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if elem, ok := c.entries[clientID]; ok {
		c.removeElement(elem)
	}
}

// Len returns the current number of cached entries.
func (c *identityCache) Len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.order.Len()
}

func (c *identityCache) expiry() time.Time {
	if c.ttl <= 0 {
		return time.Time{}
	}
	return c.clock().Add(c.ttl)
}

func (c *identityCache) evictOldest() {
	elem := c.order.Back()
	if elem != nil {
		c.removeElement(elem)
	}
}

func (c *identityCache) removeElement(elem *list.Element) {
	entry := elem.Value.(*identityEntry)
	c.order.Remove(elem)
	delete(c.entries, entry.clientID)
}
