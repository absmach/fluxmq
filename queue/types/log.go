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
	Offset        uint64    // Message offset in the partition log
	PartitionID   int       // Partition this entry belongs to
	ConsumerID    string    // Consumer that claimed this entry
	ClaimedAt     time.Time // When the entry was claimed
	DeliveryCount int       // Number of times this message has been delivered
}

// PartitionCursor tracks consumption state for a single partition within a consumer group.
type PartitionCursor struct {
	PartitionID int    // Partition identifier
	Cursor      uint64 // Next offset to deliver (read position)
	Committed   uint64 // Oldest unacknowledged offset (safe truncation point)
}

// ConsumerInfo represents a consumer within a consumer group.
type ConsumerInfo struct {
	ID            string    // Consumer identifier (usually client ID)
	ClientID      string    // MQTT client ID
	ProxyNodeID   string    // Cluster node handling this consumer
	RegisteredAt  time.Time // When the consumer joined the group
	LastHeartbeat time.Time // Last activity timestamp
}

// ConsumerGroupState represents the complete state of a consumer group.
// This includes cursors, PEL, and consumer membership.
// All map access is protected by an internal mutex for thread safety.
type ConsumerGroupState struct {
	mu sync.RWMutex `json:"-"`

	// Identity
	ID        string // Group identifier
	QueueName string // Queue this group consumes from
	Pattern   string // Subscription pattern (e.g., "$queue/tasks/#")

	// Per-partition state
	Cursors map[int]*PartitionCursor // PartitionID -> cursor state

	// Pending Entry List (PEL) - messages delivered but not acked
	// Organized by consumer for efficient work stealing
	PEL map[string][]*PendingEntry // ConsumerID -> pending entries

	// Consumer membership
	Consumers map[string]*ConsumerInfo // ConsumerID -> consumer info

	// Timestamps
	CreatedAt time.Time
	UpdatedAt time.Time
}

// NewConsumerGroupState creates a new consumer group state.
func NewConsumerGroupState(queueName, groupID, pattern string, partitionCount int) *ConsumerGroupState {
	cursors := make(map[int]*PartitionCursor, partitionCount)
	for i := 0; i < partitionCount; i++ {
		cursors[i] = &PartitionCursor{
			PartitionID: i,
			Cursor:      0,
			Committed:   0,
		}
	}

	now := time.Now()
	return &ConsumerGroupState{
		ID:        groupID,
		QueueName: queueName,
		Pattern:   pattern,
		Cursors:   cursors,
		PEL:       make(map[string][]*PendingEntry),
		Consumers: make(map[string]*ConsumerInfo),
		CreatedAt: now,
		UpdatedAt: now,
	}
}

// GetCursor returns the cursor for a partition, creating if needed.
func (g *ConsumerGroupState) GetCursor(partitionID int) *PartitionCursor {
	g.mu.Lock()
	defer g.mu.Unlock()

	if cursor, ok := g.Cursors[partitionID]; ok {
		return cursor
	}
	cursor := &PartitionCursor{
		PartitionID: partitionID,
		Cursor:      0,
		Committed:   0,
	}
	g.Cursors[partitionID] = cursor
	return cursor
}

// AddPending adds a pending entry for a consumer.
func (g *ConsumerGroupState) AddPending(consumerID string, entry *PendingEntry) {
	g.mu.Lock()
	defer g.mu.Unlock()

	g.PEL[consumerID] = append(g.PEL[consumerID], entry)
	g.UpdatedAt = time.Now()
}

// RemovePending removes a pending entry for a consumer by offset and partition.
func (g *ConsumerGroupState) RemovePending(consumerID string, partitionID int, offset uint64) bool {
	g.mu.Lock()
	defer g.mu.Unlock()

	entries, ok := g.PEL[consumerID]
	if !ok {
		return false
	}

	for i, e := range entries {
		if e.PartitionID == partitionID && e.Offset == offset {
			g.PEL[consumerID] = append(entries[:i], entries[i+1:]...)
			g.UpdatedAt = time.Now()
			return true
		}
	}
	return false
}

// DeleteConsumerPEL removes all pending entries for a consumer.
func (g *ConsumerGroupState) DeleteConsumerPEL(consumerID string) {
	g.mu.Lock()
	defer g.mu.Unlock()

	delete(g.PEL, consumerID)
	g.UpdatedAt = time.Now()
}

// FindPending finds a pending entry by partition and offset across all consumers.
func (g *ConsumerGroupState) FindPending(partitionID int, offset uint64) (*PendingEntry, string) {
	g.mu.RLock()
	defer g.mu.RUnlock()

	for consumerID, entries := range g.PEL {
		for _, e := range entries {
			if e.PartitionID == partitionID && e.Offset == offset {
				return e, consumerID
			}
		}
	}
	return nil, ""
}

// TransferPending moves a pending entry from one consumer to another.
func (g *ConsumerGroupState) TransferPending(partitionID int, offset uint64, fromConsumer, toConsumer string) bool {
	g.mu.Lock()
	defer g.mu.Unlock()

	entries, ok := g.PEL[fromConsumer]
	if !ok {
		return false
	}

	for i, e := range entries {
		if e.PartitionID == partitionID && e.Offset == offset {
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

// MinPendingOffset returns the minimum offset across all PEL entries for a partition.
// This is used to calculate the committed offset.
func (g *ConsumerGroupState) MinPendingOffset(partitionID int) (uint64, bool) {
	g.mu.RLock()
	defer g.mu.RUnlock()

	var minOffset uint64
	found := false

	for _, entries := range g.PEL {
		for _, e := range entries {
			if e.PartitionID == partitionID {
				if !found || e.Offset < minOffset {
					minOffset = e.Offset
					found = true
				}
			}
		}
	}

	return minOffset, found
}

// PendingCount returns the total number of pending entries for a partition.
func (g *ConsumerGroupState) PendingCount(partitionID int) int {
	g.mu.RLock()
	defer g.mu.RUnlock()

	count := 0
	for _, entries := range g.PEL {
		for _, e := range entries {
			if e.PartitionID == partitionID {
				count++
			}
		}
	}
	return count
}

// TotalPendingCount returns the total number of pending entries across all partitions.
func (g *ConsumerGroupState) TotalPendingCount() int {
	g.mu.RLock()
	defer g.mu.RUnlock()

	count := 0
	for _, entries := range g.PEL {
		count += len(entries)
	}
	return count
}

// StealableEntries returns entries that are older than the visibility timeout.
func (g *ConsumerGroupState) StealableEntries(partitionID int, visibilityTimeout time.Duration, excludeConsumer string) []*PendingEntry {
	g.mu.RLock()
	defer g.mu.RUnlock()

	var stealable []*PendingEntry
	cutoff := time.Now().Add(-visibilityTimeout)

	for consumerID, entries := range g.PEL {
		if consumerID == excludeConsumer {
			continue
		}
		for _, e := range entries {
			if e.PartitionID == partitionID && e.ClaimedAt.Before(cutoff) {
				stealable = append(stealable, e)
			}
		}
	}

	return stealable
}

// GetConsumer returns a consumer by ID, or nil if not found.
func (g *ConsumerGroupState) GetConsumer(consumerID string) *ConsumerInfo {
	g.mu.RLock()
	defer g.mu.RUnlock()

	return g.Consumers[consumerID]
}

// SetConsumer adds or updates a consumer.
func (g *ConsumerGroupState) SetConsumer(consumerID string, info *ConsumerInfo) {
	g.mu.Lock()
	defer g.mu.Unlock()

	g.Consumers[consumerID] = info
	g.UpdatedAt = time.Now()
}

// DeleteConsumer removes a consumer by ID.
func (g *ConsumerGroupState) DeleteConsumer(consumerID string) {
	g.mu.Lock()
	defer g.mu.Unlock()

	delete(g.Consumers, consumerID)
	g.UpdatedAt = time.Now()
}

// ConsumerCount returns the number of consumers in the group.
func (g *ConsumerGroupState) ConsumerCount() int {
	g.mu.RLock()
	defer g.mu.RUnlock()

	return len(g.Consumers)
}

// ConsumerIDs returns a slice of all consumer IDs.
func (g *ConsumerGroupState) ConsumerIDs() []string {
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
func (g *ConsumerGroupState) ForEachConsumer(fn func(id string, info *ConsumerInfo) bool) {
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
	if !strings.HasPrefix(topic, queueRoot) {
		return ""
	}

	// Remove queue root prefix
	key := strings.TrimPrefix(topic, queueRoot)
	// Remove leading slash
	key = strings.TrimPrefix(key, "/")

	return key
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
