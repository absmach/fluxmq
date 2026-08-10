// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package pki

import (
	"container/list"
	"sync"
	"time"

	corebroker "github.com/absmach/fluxmq/broker"
)

type resolutionCache struct {
	mu       sync.Mutex
	capacity int
	ttl      time.Duration
	clock    func() time.Time
	entries  map[string]*list.Element
	order    *list.List
}

type resolutionEntry struct {
	fingerprint string
	identity    corebroker.CertificateIdentity
	expiresAt   time.Time
}

func newResolutionCache(capacity int, ttl time.Duration) *resolutionCache {
	return &resolutionCache{
		capacity: capacity,
		ttl:      ttl,
		clock:    time.Now,
		entries:  make(map[string]*list.Element),
		order:    list.New(),
	}
}

func (c *resolutionCache) get(fingerprint string) (corebroker.CertificateIdentity, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	elem, ok := c.entries[fingerprint]
	if !ok {
		return corebroker.CertificateIdentity{}, false
	}
	entry := elem.Value.(*resolutionEntry)
	if !entry.expiresAt.After(c.clock()) {
		c.remove(elem)
		return corebroker.CertificateIdentity{}, false
	}
	c.order.MoveToFront(elem)
	return entry.identity, true
}

func (c *resolutionCache) put(identity corebroker.CertificateIdentity) (evicted bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	expiresAt := c.clock().Add(c.ttl)
	if identity.ExpiresAt.Before(expiresAt) {
		expiresAt = identity.ExpiresAt
	}
	if elem, ok := c.entries[identity.Fingerprint]; ok {
		entry := elem.Value.(*resolutionEntry)
		entry.identity = identity
		entry.expiresAt = expiresAt
		c.order.MoveToFront(elem)
		return false
	}

	entry := &resolutionEntry{
		fingerprint: identity.Fingerprint,
		identity:    identity,
		expiresAt:   expiresAt,
	}
	c.entries[identity.Fingerprint] = c.order.PushFront(entry)
	if c.capacity > 0 && c.order.Len() > c.capacity {
		c.remove(c.order.Back())
		return true
	}
	return false
}

func (c *resolutionCache) invalidate(match func(corebroker.CertificateIdentity) bool) int {
	c.mu.Lock()
	defer c.mu.Unlock()

	removed := 0
	for elem := c.order.Back(); elem != nil; {
		previous := elem.Prev()
		if match(elem.Value.(*resolutionEntry).identity) {
			c.remove(elem)
			removed++
		}
		elem = previous
	}
	return removed
}

func (c *resolutionCache) len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.order.Len()
}

func (c *resolutionCache) remove(elem *list.Element) {
	if elem == nil {
		return
	}
	entry := elem.Value.(*resolutionEntry)
	delete(c.entries, entry.fingerprint)
	c.order.Remove(elem)
}
