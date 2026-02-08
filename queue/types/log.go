// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package types

import (
	"strings"
	"sync"
	"time"
)

// PendingEntry represents a message that has been delivered but not yet acknowledged.
// This is part of the PEL (Pending Entry List) for work stealing support.
type PendingEntry struct {
	Offset        uint64    // Message offset in the queue log
	ConsumerID    string    // Consumer that claimed this entry
	ClaimedAt     time.Time // When the entry was claimed
	DeliveryCount int       // Number of times this message has been delivered
}

// QueueCursor tracks consumption state for a queue within a consumer group.
type QueueCursor struct {
	Cursor    uint64 // Next offset to deliver (read position)
	Committed uint64 // Oldest unacknowledged offset (safe truncation point)
}

// ConsumerInfo represents a consumer within a consumer group.
type ConsumerInfo struct {
	ID            string    // Consumer identifier (usually client ID)
	ClientID      string    // MQTT client ID
	ProxyNodeID   string    // Cluster node handling this consumer
	RegisteredAt  time.Time // When the consumer joined the group
	LastHeartbeat time.Time // Last activity timestamp
}

// ConsumerGroup represents the complete state of a consumer group.
// This includes cursor, PEL, and consumer membership.
// All map access is protected by an internal mutex for thread safety.
type ConsumerGroup struct {
	mu sync.RWMutex `json:"-"`

	// Identity
	ID        string // Group identifier
	QueueName string // Queue this group consumes from
	Pattern   string // Subscription pattern (e.g., "sensors/#")
	Mode      ConsumerGroupMode

	// AutoCommit controls whether stream groups automatically commit offsets
	// as messages are delivered. Default is true for backwards compatibility.
	AutoCommit bool

	// Queue cursor state (single cursor per queue, no partitions)
	Cursor *QueueCursor

	// Pending Entry List (PEL) - messages delivered but not acked
	// Organized by consumer for efficient work stealing
	PEL map[string][]*PendingEntry // ConsumerID -> pending entries

	// Consumer membership
	Consumers map[string]*ConsumerInfo // ConsumerID -> consumer info

	// Timestamps
	CreatedAt time.Time
	UpdatedAt time.Time
}

// ConsumerGroupMode defines how a consumer group is tracked.
type ConsumerGroupMode string

const (
	GroupModeQueue  ConsumerGroupMode = "queue"
	GroupModeStream ConsumerGroupMode = "stream"
)

// NewConsumerGroupState creates a new consumer group state.
func NewConsumerGroupState(queueName, groupID, pattern string) *ConsumerGroup {
	now := time.Now()
	return &ConsumerGroup{
		ID:         groupID,
		QueueName:  queueName,
		Pattern:    pattern,
		Mode:       GroupModeQueue,
		AutoCommit: true,
		Cursor: &QueueCursor{
			Cursor:    0,
			Committed: 0,
		},
		PEL:       make(map[string][]*PendingEntry),
		Consumers: make(map[string]*ConsumerInfo),
		CreatedAt: now,
		UpdatedAt: now,
	}
}

// GetCursor returns the queue cursor, creating if needed.
func (g *ConsumerGroup) GetCursor() *QueueCursor {
	g.mu.Lock()
	defer g.mu.Unlock()

	if g.Cursor == nil {
		g.Cursor = &QueueCursor{
			Cursor:    0,
			Committed: 0,
		}
	}
	return g.Cursor
}

// ReplacePEL atomically replaces the entire PEL map.
func (g *ConsumerGroup) ReplacePEL(pel map[string][]*PendingEntry) {
	g.mu.Lock()
	defer g.mu.Unlock()

	g.PEL = pel
	g.UpdatedAt = time.Now()
}

// AddPending adds a pending entry for a consumer.
func (g *ConsumerGroup) AddPending(consumerID string, entry *PendingEntry) {
	g.mu.Lock()
	defer g.mu.Unlock()

	g.PEL[consumerID] = append(g.PEL[consumerID], entry)
	g.UpdatedAt = time.Now()
}

// RemovePending removes a pending entry for a consumer by offset.
func (g *ConsumerGroup) RemovePending(consumerID string, offset uint64) bool {
	g.mu.Lock()
	defer g.mu.Unlock()

	entries, ok := g.PEL[consumerID]
	if !ok {
		return false
	}

	for i, e := range entries {
		if e.Offset == offset {
			g.PEL[consumerID] = append(entries[:i], entries[i+1:]...)
			g.UpdatedAt = time.Now()
			return true
		}
	}
	return false
}

// DeleteConsumerPEL removes all pending entries for a consumer.
func (g *ConsumerGroup) DeleteConsumerPEL(consumerID string) {
	g.mu.Lock()
	defer g.mu.Unlock()

	delete(g.PEL, consumerID)
	g.UpdatedAt = time.Now()
}

// FindPending finds a pending entry by offset across all consumers.
func (g *ConsumerGroup) FindPending(offset uint64) (*PendingEntry, string) {
	g.mu.RLock()
	defer g.mu.RUnlock()

	for consumerID, entries := range g.PEL {
		for _, e := range entries {
			if e.Offset == offset {
				return e, consumerID
			}
		}
	}
	return nil, ""
}

// TransferPending moves a pending entry from one consumer to another.
func (g *ConsumerGroup) TransferPending(offset uint64, fromConsumer, toConsumer string) bool {
	g.mu.Lock()
	defer g.mu.Unlock()

	entries, ok := g.PEL[fromConsumer]
	if !ok {
		return false
	}

	for i, e := range entries {
		if e.Offset == offset {
			g.PEL[fromConsumer] = append(entries[:i], entries[i+1:]...)
			e.ConsumerID = toConsumer
			e.ClaimedAt = time.Now()
			e.DeliveryCount++
			g.PEL[toConsumer] = append(g.PEL[toConsumer], e)
			g.UpdatedAt = time.Now()
			return true
		}
	}
	return false
}

// MinPendingOffset returns the minimum offset across all PEL entries.
// This is used to calculate the committed offset.
func (g *ConsumerGroup) MinPendingOffset() (uint64, bool) {
	g.mu.RLock()
	defer g.mu.RUnlock()

	var minOffset uint64
	found := false

	for _, entries := range g.PEL {
		for _, e := range entries {
			if !found || e.Offset < minOffset {
				minOffset = e.Offset
				found = true
			}
		}
	}

	return minOffset, found
}

// PendingCount returns the total number of pending entries.
func (g *ConsumerGroup) PendingCount() int {
	g.mu.RLock()
	defer g.mu.RUnlock()

	count := 0
	for _, entries := range g.PEL {
		count += len(entries)
	}
	return count
}

// StealableEntries returns entries that are older than the visibility timeout.
func (g *ConsumerGroup) StealableEntries(visibilityTimeout time.Duration, excludeConsumer string) []*PendingEntry {
	g.mu.RLock()
	defer g.mu.RUnlock()

	var stealable []*PendingEntry
	cutoff := time.Now().Add(-visibilityTimeout)

	for consumerID, entries := range g.PEL {
		if consumerID == excludeConsumer {
			continue
		}
		for _, e := range entries {
			if e.ClaimedAt.Before(cutoff) {
				stealable = append(stealable, e)
			}
		}
	}

	return stealable
}

// GetConsumer returns a consumer by ID, or nil if not found.
func (g *ConsumerGroup) GetConsumer(consumerID string) *ConsumerInfo {
	g.mu.RLock()
	defer g.mu.RUnlock()

	return g.Consumers[consumerID]
}

// SetConsumer adds or updates a consumer.
func (g *ConsumerGroup) SetConsumer(consumerID string, info *ConsumerInfo) {
	g.mu.Lock()
	defer g.mu.Unlock()

	g.Consumers[consumerID] = info
	g.UpdatedAt = time.Now()
}

// DeleteConsumer removes a consumer by ID.
func (g *ConsumerGroup) DeleteConsumer(consumerID string) {
	g.mu.Lock()
	defer g.mu.Unlock()

	delete(g.Consumers, consumerID)
	g.UpdatedAt = time.Now()
}

// ConsumerCount returns the number of consumers in the group.
func (g *ConsumerGroup) ConsumerCount() int {
	g.mu.RLock()
	defer g.mu.RUnlock()

	return len(g.Consumers)
}

// ConsumerIDs returns a slice of all consumer IDs.
func (g *ConsumerGroup) ConsumerIDs() []string {
	g.mu.RLock()
	defer g.mu.RUnlock()

	ids := make([]string, 0, len(g.Consumers))
	for id := range g.Consumers {
		ids = append(ids, id)
	}
	return ids
}

// ForEachConsumer iterates over all consumers with the lock held.
// Return false from fn to stop iteration.
func (g *ConsumerGroup) ForEachConsumer(fn func(id string, info *ConsumerInfo) bool) {
	g.mu.RLock()
	defer g.mu.RUnlock()

	for id, info := range g.Consumers {
		if !fn(id, info) {
			return
		}
	}
}

// RoutingKey extracts the routing key from a full topic.
// For topic "$queue/tasks/images/png", if queue root is "$queue/tasks",
// the routing key is "images/png".
func ExtractRoutingKey(topic, queueRoot string) string {
	if topic == "" || queueRoot == "" {
		return ""
	}

	topicLevels := strings.Split(topic, "/")
	rootLevels := strings.Split(queueRoot, "/")

	t := 0
	for _, r := range rootLevels {
		if r == "#" {
			// '#' matches the rest of the topic, so routing key is what's left.
			return strings.Join(topicLevels[t:], "/")
		}

		if t >= len(topicLevels) {
			return ""
		}

		if r == "+" {
			// '+' matches exactly one level.
			t++
			continue
		}

		if r != topicLevels[t] {
			return ""
		}
		t++
	}

	// Remaining topic levels are the routing key.
	return strings.Join(topicLevels[t:], "/")
}

// ExtractQueueRoot extracts the queue root from a topic.
// Convention: $queue/{name} is the root, everything after is routing key.
// Example: "$queue/tasks/images" -> queue root is "$queue/tasks"
func ExtractQueueRoot(topic string) string {
	if !strings.HasPrefix(topic, "$queue/") {
		return ""
	}

	// Remove $queue/ prefix
	rest := strings.TrimPrefix(topic, "$queue/")

	// Find the first segment (queue name)
	parts := strings.SplitN(rest, "/", 2)
	if len(parts) == 0 {
		return ""
	}

	return "$queue/" + parts[0]
}

// IsQueueWildcard returns true if the pattern contains wildcards.
func IsQueueWildcard(pattern string) bool {
	return strings.Contains(pattern, "+") || strings.Contains(pattern, "#")
}
