// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package session

import "sync"

var _ Cache = (*MapCache)(nil)

// Cache is an in-memory cache for active sessions.
// This abstraction allows different cache implementations (map, LRU, distributed, etc.)
// while keeping the Manager logic clean.
type Cache interface {
	// Get retrieves a session by client ID.
	// Returns nil if the session is not in the cache.
	Get(clientID string) *Session

	// Set stores a session in the cache.
	Set(clientID string, session *Session)

	// Delete removes a session from the cache.
	// Returns true if the session was present, false otherwise.
	Delete(clientID string) bool

	// ForEach iterates over all sessions in the cache.
	// The iteration order is not guaranteed.
	ForEach(fn func(*Session))

	// Count returns the total number of sessions in the cache.
	Count() int

	// ConnectedCount returns the number of connected sessions.
	ConnectedCount() int
}

// MapCache is a simple map-based cache implementation.
// It uses a sync.RWMutex for concurrent access.
type MapCache struct {
	mu       sync.RWMutex
	sessions map[string]*Session
}

// NewMapCache creates a new map-based session cache.
func NewMapCache() *MapCache {
	return &MapCache{
		sessions: make(map[string]*Session),
	}
}

// Get retrieves a session by client ID.
func (c *MapCache) Get(clientID string) *Session {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.sessions[clientID]
}

// Set stores a session in the cache.
func (c *MapCache) Set(clientID string, session *Session) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.sessions[clientID] = session
}

// Delete removes a session from the cache.
func (c *MapCache) Delete(clientID string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, exists := c.sessions[clientID]; exists {
		delete(c.sessions, clientID)
		return true
	}
	return false
}

// ForEach iterates over all sessions in the cache.
func (c *MapCache) ForEach(fn func(*Session)) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	for _, s := range c.sessions {
		fn(s)
	}
}

// Count returns the total number of sessions.
func (c *MapCache) Count() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.sessions)
}

// ConnectedCount returns the number of connected sessions.
func (c *MapCache) ConnectedCount() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	count := 0
	for _, s := range c.sessions {
		if s.IsConnected() {
			count++
		}
	}
	return count
}
