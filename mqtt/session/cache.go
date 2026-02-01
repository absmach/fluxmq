// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package session

import (
	"hash/fnv"
	"sync"
	"sync/atomic"
)

var _ Cache = (*ShardedCache)(nil)

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

const numShards = 64

type cacheShard struct {
	mu       sync.RWMutex
	sessions map[string]*Session
}

// ShardedCache splits sessions across multiple shards to reduce lock contention.
// Each shard has its own RWMutex so concurrent operations on different clients
// don't block each other.
type ShardedCache struct {
	shards [numShards]cacheShard
	count  atomic.Int64
}

// NewShardedCache creates a new sharded session cache.
func NewShardedCache() *ShardedCache {
	c := &ShardedCache{}
	for i := range c.shards {
		c.shards[i].sessions = make(map[string]*Session)
	}
	return c
}

func (c *ShardedCache) shard(key string) *cacheShard {
	h := fnv.New32a()
	h.Write([]byte(key))
	return &c.shards[h.Sum32()%numShards]
}

// Get retrieves a session by client ID.
func (c *ShardedCache) Get(clientID string) *Session {
	s := c.shard(clientID)
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.sessions[clientID]
}

// Set stores a session in the cache.
func (c *ShardedCache) Set(clientID string, session *Session) {
	s := c.shard(clientID)
	s.mu.Lock()
	if _, exists := s.sessions[clientID]; !exists {
		c.count.Add(1)
	}
	s.sessions[clientID] = session
	s.mu.Unlock()
}

// Delete removes a session from the cache.
func (c *ShardedCache) Delete(clientID string) bool {
	s := c.shard(clientID)
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.sessions[clientID]; exists {
		delete(s.sessions, clientID)
		c.count.Add(-1)
		return true
	}
	return false
}

// ForEach iterates over all sessions in the cache.
func (c *ShardedCache) ForEach(fn func(*Session)) {
	for i := range c.shards {
		s := &c.shards[i]
		s.mu.RLock()
		for _, sess := range s.sessions {
			fn(sess)
		}
		s.mu.RUnlock()
	}
}

// Count returns the total number of sessions.
func (c *ShardedCache) Count() int {
	return int(c.count.Load())
}

// ConnectedCount returns the number of connected sessions.
func (c *ShardedCache) ConnectedCount() int {
	count := 0
	for i := range c.shards {
		s := &c.shards[i]
		s.mu.RLock()
		for _, sess := range s.sessions {
			if sess.IsConnected() {
				count++
			}
		}
		s.mu.RUnlock()
	}
	return count
}
