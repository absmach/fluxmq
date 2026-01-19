// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package types

import (
	"strings"
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
type ConsumerGroupState struct {
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
	if cursor, ok := g.Cursors[partitionID]; ok {
		return cursor
	}
	// Create new cursor if not exists
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
	g.PEL[consumerID] = append(g.PEL[consumerID], entry)
	g.UpdatedAt = time.Now()
}

// RemovePending removes a pending entry for a consumer by offset and partition.
func (g *ConsumerGroupState) RemovePending(consumerID string, partitionID int, offset uint64) bool {
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

// FindPending finds a pending entry by partition and offset across all consumers.
func (g *ConsumerGroupState) FindPending(partitionID int, offset uint64) (*PendingEntry, string) {
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
	entries, ok := g.PEL[fromConsumer]
	if !ok {
		return false
	}

	for i, e := range entries {
		if e.PartitionID == partitionID && e.Offset == offset {
			// Remove from source
			g.PEL[fromConsumer] = append(entries[:i], entries[i+1:]...)

			// Update entry
			e.ConsumerID = toConsumer
			e.ClaimedAt = time.Now()
			e.DeliveryCount++

			// Add to destination
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
	count := 0
	for _, entries := range g.PEL {
		count += len(entries)
	}
	return count
}

// StealableEntries returns entries that are older than the visibility timeout.
func (g *ConsumerGroupState) StealableEntries(partitionID int, visibilityTimeout time.Duration, excludeConsumer string) []*PendingEntry {
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
