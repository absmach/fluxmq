// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package types

import (
	"encoding/json"
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

// CursorView returns a copy of the group's cursor positions.
//
// It is a copy rather than the live pointer because a pointer escaping the lock
// is a write nobody can see coming: callers mutated it freely, racing every
// reader of the group and every encode of it. Advancing a cursor goes through
// SetCursor, SetCursorPosition, SetCommitted or AdvanceCommitted, each of which
// takes the group's lock.
func (g *ConsumerGroup) CursorView() QueueCursor {
	g.mu.RLock()
	defer g.mu.RUnlock()

	if g.Cursor == nil {
		return QueueCursor{}
	}
	return *g.Cursor
}

// SetCursor atomically updates both cursor positions.
//
// GetCursor hands out the live cursor pointer, so mutating it through that
// pointer writes group state without the group's lock. Callers advancing a
// cursor must come through here instead.
func (g *ConsumerGroup) SetCursor(cursor, committed uint64) {
	g.mu.Lock()
	defer g.mu.Unlock()

	if g.Cursor == nil {
		g.Cursor = &QueueCursor{}
	}
	g.Cursor.Cursor = cursor
	g.Cursor.Committed = committed
	g.UpdatedAt = time.Now()
}

// SetCursorPosition moves the cursor, leaving the committed safe point alone.
func (g *ConsumerGroup) SetCursorPosition(cursor uint64) {
	g.mu.Lock()
	defer g.mu.Unlock()

	if g.Cursor == nil {
		g.Cursor = &QueueCursor{}
	}
	g.Cursor.Cursor = cursor
	g.UpdatedAt = time.Now()
}

// SetCommitted records the committed safe point, leaving the cursor alone.
func (g *ConsumerGroup) SetCommitted(committed uint64) {
	g.mu.Lock()
	defer g.mu.Unlock()

	if g.Cursor == nil {
		g.Cursor = &QueueCursor{}
	}
	g.Cursor.Committed = committed
	g.UpdatedAt = time.Now()
}

// AdvanceCommitted records committed as the safe point, pulling the cursor up
// to meet it when a caller has committed past where the group had read.
func (g *ConsumerGroup) AdvanceCommitted(committed uint64) {
	g.mu.Lock()
	defer g.mu.Unlock()

	if g.Cursor == nil {
		g.Cursor = &QueueCursor{}
	}
	if committed > g.Cursor.Cursor {
		g.Cursor.Cursor = committed
	}
	g.Cursor.Committed = committed
	g.UpdatedAt = time.Now()
}

// MarshalJSON encodes the group under its own read lock.
//
// Encoding reads PEL, Consumers and Cursor directly, which is exactly what
// Snapshot exists to prevent callers from doing: without the lock a group being
// persisted races every consumer mutating it. The field set and names are the
// default ones, so the encoding is unchanged.
func (g *ConsumerGroup) MarshalJSON() ([]byte, error) {
	g.mu.RLock()
	defer g.mu.RUnlock()

	// plain sheds the methods, so this does not recurse. The conversion is on
	// the pointer, so the mutex is never copied.
	type plain ConsumerGroup
	return json.Marshal((*plain)(g))
}

// ReplacePEL atomically replaces the entire PEL map.
func (g *ConsumerGroup) ReplacePEL(pel map[string][]*PendingEntry) {
	g.mu.Lock()
	defer g.mu.Unlock()

	g.PEL = pel
	g.UpdatedAt = time.Now()
}

// ReplaceConsumers atomically replaces the entire consumer membership map.
func (g *ConsumerGroup) ReplaceConsumers(consumers map[string]*ConsumerInfo) {
	g.mu.Lock()
	defer g.mu.Unlock()

	g.Consumers = consumers
	g.UpdatedAt = time.Now()
}

// ConsumerGroupSnapshot is a point-in-time copy of a group's replicated state.
type ConsumerGroupSnapshot struct {
	ID         string
	QueueName  string
	Pattern    string
	Mode       ConsumerGroupMode
	AutoCommit bool
	Cursor     QueueCursor
	PEL        map[string][]*PendingEntry
	Consumers  map[string]*ConsumerInfo
	CreatedAt  time.Time
	UpdatedAt  time.Time
}

// Snapshot copies the group's state under its own lock.
//
// The PEL and consumer maps are mutated by consumers as they claim and settle
// records, so anything that serializes a group — replication, diagnostics —
// must read them through here. Reading the fields directly races those
// mutations even when the reader only intends to look.
//
// Entries are copied, not aliased, so the returned snapshot cannot be used to
// mutate group state after the lock is released.
func (g *ConsumerGroup) Snapshot() ConsumerGroupSnapshot {
	g.mu.RLock()
	defer g.mu.RUnlock()

	snapshot := ConsumerGroupSnapshot{
		ID:         g.ID,
		QueueName:  g.QueueName,
		Pattern:    g.Pattern,
		Mode:       g.Mode,
		AutoCommit: g.AutoCommit,
		PEL:        make(map[string][]*PendingEntry, len(g.PEL)),
		Consumers:  make(map[string]*ConsumerInfo, len(g.Consumers)),
		CreatedAt:  g.CreatedAt,
		UpdatedAt:  g.UpdatedAt,
	}
	if g.Cursor != nil {
		snapshot.Cursor = *g.Cursor
	}
	for consumerID, entries := range g.PEL {
		copied := make([]*PendingEntry, 0, len(entries))
		for _, entry := range entries {
			if entry == nil {
				continue
			}
			value := *entry
			copied = append(copied, &value)
		}
		snapshot.PEL[consumerID] = copied
	}
	for consumerID, info := range g.Consumers {
		if info == nil {
			continue
		}
		value := *info
		snapshot.Consumers[consumerID] = &value
	}
	return snapshot
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
// Example: "$queue/tasks/images" -> queue root is "$queue/tasks".
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
