// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package authatom

import (
	"sync"
	"time"
)

type ttlCache[V any] struct {
	mu      sync.Mutex
	ttl     time.Duration
	clock   func() time.Time
	entries map[string]ttlEntry[V]
}

type ttlEntry[V any] struct {
	value   V
	expires time.Time
}

func newTTLCache[V any](ttl time.Duration) *ttlCache[V] {
	if ttl <= 0 {
		return nil
	}
	return &ttlCache[V]{
		ttl:     ttl,
		clock:   time.Now,
		entries: make(map[string]ttlEntry[V]),
	}
}

func (c *ttlCache[V]) Get(key string) (V, bool) {
	var zero V
	if c == nil {
		return zero, false
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	entry, ok := c.entries[key]
	if !ok {
		return zero, false
	}
	if c.clock().After(entry.expires) {
		delete(c.entries, key)
		return zero, false
	}
	return entry.value, true
}

func (c *ttlCache[V]) Set(key string, value V) {
	if c == nil {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	c.entries[key] = ttlEntry[V]{
		value:   value,
		expires: c.clock().Add(c.ttl),
	}
}
