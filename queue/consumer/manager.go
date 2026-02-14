// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package consumer

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/absmach/fluxmq/logstorage"
	"github.com/absmach/fluxmq/queue/storage"
	"github.com/absmach/fluxmq/queue/types"
)

// Manager errors.
var (
	ErrNoMessages                    = errors.New("no messages available")
	ErrGroupNotFound                 = errors.New("consumer group not found")
	ErrConsumerNotFound              = errors.New("consumer not found")
	ErrMessageNotPending             = errors.New("message not in pending list")
	ErrInvalidOffset                 = errors.New("invalid offset")
	ErrGroupModeMismatch             = errors.New("consumer group mode mismatch")
	ErrCommitOffsetOnlyForStreamMode = errors.New("commit offset only supported for stream groups")
	ErrPELFull                       = errors.New("pending entry list at capacity")
)

// Manager handles consumer group operations including claiming,
// acknowledging, and work stealing for the  queue.
type Manager struct {
	queueStore storage.QueueStore
	groupStore storage.ConsumerGroupStore
	config     Config
	lastCommit map[string]time.Time
	mu         sync.RWMutex
}

// Config defines configuration for the consumer group manager.
type Config struct {
	// VisibilityTimeout is how long a message stays claimed before it can be stolen.
	VisibilityTimeout time.Duration

	// MaxDeliveryCount is the maximum number of times a message can be delivered
	// before being sent to the DLQ.
	MaxDeliveryCount int

	// ClaimBatchSize is the maximum number of messages to claim at once.
	ClaimBatchSize int

	// StealBatchSize is the maximum number of messages to steal at once.
	StealBatchSize int

	// AutoCommitInterval controls how often stream groups auto-commit offsets.
	// Zero means commit on every delivery batch.
	AutoCommitInterval time.Duration

	// MaxPELSize is the maximum number of pending entries per consumer group.
	// When reached, new claims are rejected until entries are acknowledged.
	// Zero means unlimited (not recommended for production).
	MaxPELSize int
}

// DefaultConfig returns default manager configuration.
func DefaultConfig() Config {
	return Config{
		VisibilityTimeout:  30 * time.Second,
		MaxDeliveryCount:   5,
		ClaimBatchSize:     10,
		StealBatchSize:     5,
		AutoCommitInterval: 5 * time.Second,
		MaxPELSize:         100_000,
	}
}

// NewManager creates a new consumer group manager.
func NewManager(queueStore storage.QueueStore, groupStore storage.ConsumerGroupStore, config Config) *Manager {
	return &Manager{
		queueStore: queueStore,
		groupStore: groupStore,
		config:     config,
		lastCommit: make(map[string]time.Time),
	}
}

// GetOrCreateGroup retrieves or creates a consumer group.
func (m *Manager) GetOrCreateGroup(ctx context.Context, queueName, groupID, pattern string, mode types.ConsumerGroupMode, autoCommit bool) (*types.ConsumerGroup, error) {
	// Try to get existing group
	group, err := m.groupStore.GetConsumerGroup(ctx, queueName, groupID)
	if err == nil {
		if mode == "" {
			return group, nil
		}
		if group.Mode == "" {
			group.Mode = mode
			group.AutoCommit = autoCommit
			_ = m.groupStore.UpdateConsumerGroup(ctx, group)
			return group, nil
		}
		if group.Mode != mode {
			return nil, ErrGroupModeMismatch
		}
		return group, nil
	}

	// Check for "not found" errors from various storage implementations
	if err != storage.ErrConsumerNotFound && err != logstorage.ErrGroupNotFound {
		return nil, err
	}

	// Create new group
	group = types.NewConsumerGroupState(queueName, groupID, pattern)
	if mode != "" {
		group.Mode = mode
	}
	group.AutoCommit = autoCommit

	if err := m.groupStore.CreateConsumerGroup(ctx, group); err != nil {
		// Handle race condition - another process might have created it
		if err == storage.ErrConsumerGroupExists {
			return m.groupStore.GetConsumerGroup(ctx, queueName, groupID)
		}
		return nil, err
	}

	return group, nil
}

// RegisterConsumer adds a consumer to a group.
func (m *Manager) RegisterConsumer(ctx context.Context, queueName, groupID, consumerID, clientID, proxyNodeID string) error {
	consumer := &types.ConsumerInfo{
		ID:            consumerID,
		ClientID:      clientID,
		ProxyNodeID:   proxyNodeID,
		RegisteredAt:  time.Now(),
		LastHeartbeat: time.Now(),
	}

	return m.groupStore.RegisterConsumer(ctx, queueName, groupID, consumer)
}

// UnregisterConsumer removes a consumer from a group.
func (m *Manager) UnregisterConsumer(ctx context.Context, queueName, groupID, consumerID string) error {
	return m.groupStore.UnregisterConsumer(ctx, queueName, groupID, consumerID)
}

// Claim retrieves the next available message for a consumer.
// It first tries to get a new message from the log, then falls back to work stealing.
func (m *Manager) Claim(ctx context.Context, queueName, groupID, consumerID string, filter *Filter) (*types.Message, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Get consumer group
	group, err := m.groupStore.GetConsumerGroup(ctx, queueName, groupID)
	if err != nil {
		return nil, err
	}

	// Try to claim from cursor position
	msg, err := m.claimFromCursor(ctx, group, consumerID, filter)
	if err == nil {
		return msg, nil
	}

	if err != ErrNoMessages {
		return nil, err
	}

	// No new messages - try work stealing
	return m.stealWork(ctx, group, consumerID, filter)
}

// ClaimBatch retrieves multiple messages for a consumer.
func (m *Manager) ClaimBatch(ctx context.Context, queueName, groupID, consumerID string, filter *Filter, limit int) ([]*types.Message, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if limit <= 0 {
		limit = m.config.ClaimBatchSize
	}

	// Get consumer group
	group, err := m.groupStore.GetConsumerGroup(ctx, queueName, groupID)
	if err != nil {
		return nil, err
	}

	var messages []*types.Message

	// Claim from cursor
	for len(messages) < limit {
		msg, err := m.claimFromCursor(ctx, group, consumerID, filter)
		if err != nil {
			break
		}
		messages = append(messages, msg)
	}

	// Try work stealing if we didn't get enough
	for len(messages) < limit {
		msg, err := m.stealWork(ctx, group, consumerID, filter)
		if err != nil {
			break
		}
		messages = append(messages, msg)
	}

	if len(messages) == 0 {
		return nil, ErrNoMessages
	}

	return messages, nil
}

// ClaimBatchStream retrieves multiple messages for a stream consumer without PEL tracking.
// It advances the cursor once per batch for efficiency.
func (m *Manager) ClaimBatchStream(ctx context.Context, queueName, groupID, consumerID string, filter *Filter, limit int) ([]*types.Message, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if limit <= 0 {
		limit = m.config.ClaimBatchSize
	}

	group, err := m.groupStore.GetConsumerGroup(ctx, queueName, groupID)
	if err != nil {
		return nil, err
	}

	cursor := group.GetCursor()
	tail, err := m.queueStore.Tail(ctx, group.QueueName)
	if err != nil {
		return nil, err
	}

	var messages []*types.Message
	var newCursor uint64 = cursor.Cursor

	for newCursor < tail && len(messages) < limit {
		offset := newCursor
		newCursor++

		msg, err := m.queueStore.Read(ctx, group.QueueName, offset)
		if err != nil {
			if err == storage.ErrOffsetOutOfRange {
				continue
			}
			return nil, err
		}

		if filter != nil {
			queueRoot := "$queue/" + group.QueueName
			routingKey := types.ExtractRoutingKey(msg.Topic, queueRoot)
			if !filter.Matches(routingKey) {
				continue
			}
		}

		messages = append(messages, msg)
	}

	if len(messages) == 0 {
		return nil, ErrNoMessages
	}

	if newCursor > cursor.Cursor {
		if err := m.groupStore.UpdateCursor(ctx, group.QueueName, group.ID, newCursor); err != nil {
			return nil, err
		}
		// Only auto-commit if the group has AutoCommit enabled.
		if group.AutoCommit {
			if m.config.AutoCommitInterval <= 0 {
				if err := m.groupStore.UpdateCommitted(ctx, group.QueueName, group.ID, newCursor); err != nil {
					return nil, err
				}
			} else {
				key := group.QueueName + "/" + group.ID
				now := time.Now()
				last, ok := m.lastCommit[key]
				if !ok || now.Sub(last) >= m.config.AutoCommitInterval {
					if err := m.groupStore.UpdateCommitted(ctx, group.QueueName, group.ID, newCursor); err != nil {
						return nil, err
					}
					m.lastCommit[key] = now
				}
			}
		}
	}

	return messages, nil
}

// claimFromCursor tries to claim a message from the cursor position.
func (m *Manager) claimFromCursor(ctx context.Context, group *types.ConsumerGroup, consumerID string, filter *Filter) (*types.Message, error) {
	// Check PEL capacity before claiming
	if m.config.MaxPELSize > 0 {
		pelCount := group.PendingCount()
		if pelCount >= m.config.MaxPELSize {
			return nil, ErrPELFull
		}
	}

	cursor := group.GetCursor()

	// Get log tail
	tail, err := m.queueStore.Tail(ctx, group.QueueName)
	if err != nil {
		return nil, err
	}

	// Scan from cursor until we find a matching message or hit tail
	for cursor.Cursor < tail {
		offset := cursor.Cursor
		cursor.Cursor++

		// Read message
		msg, err := m.queueStore.Read(ctx, group.QueueName, offset)
		if err != nil {
			if err == storage.ErrOffsetOutOfRange {
				continue // Message was truncated, skip
			}
			return nil, err
		}

		// Check if message matches filter
		if filter != nil {
			queueRoot := "$queue/" + group.QueueName
			routingKey := types.ExtractRoutingKey(msg.Topic, queueRoot)
			if !filter.Matches(routingKey) {
				continue // Skip non-matching messages
			}
		}

		// Add to PEL
		entry := &types.PendingEntry{
			Offset:        offset,
			ConsumerID:    consumerID,
			ClaimedAt:     time.Now(),
			DeliveryCount: 1,
		}

		if err := m.groupStore.AddPendingEntry(ctx, group.QueueName, group.ID, entry); err != nil {
			return nil, err
		}

		// Update cursor
		if err := m.groupStore.UpdateCursor(ctx, group.QueueName, group.ID, cursor.Cursor); err != nil {
			return nil, err
		}

		return msg, nil
	}

	return nil, ErrNoMessages
}

// stealWork tries to steal a message from another consumer's PEL.
func (m *Manager) stealWork(ctx context.Context, group *types.ConsumerGroup, consumerID string, filter *Filter) (*types.Message, error) {
	// Get stealable entries
	stealable := group.StealableEntries(m.config.VisibilityTimeout, consumerID)
	if len(stealable) == 0 {
		return nil, ErrNoMessages
	}

	// Try to steal the oldest entry
	for _, entry := range stealable {
		// Check delivery count
		if entry.DeliveryCount >= m.config.MaxDeliveryCount {
			// TODO: Move to DLQ instead of stealing
			continue
		}

		// Read message
		msg, err := m.queueStore.Read(ctx, group.QueueName, entry.Offset)
		if err != nil {
			continue // Message might be truncated
		}

		// Check filter
		if filter != nil {
			queueRoot := "$queue/" + group.QueueName
			routingKey := types.ExtractRoutingKey(msg.Topic, queueRoot)
			if !filter.Matches(routingKey) {
				continue
			}
		}

		// Transfer pending entry
		err = m.groupStore.TransferPendingEntry(
			ctx,
			group.QueueName,
			group.ID,
			entry.Offset,
			entry.ConsumerID,
			consumerID,
		)
		if err != nil {
			continue
		}

		return msg, nil
	}

	return nil, ErrNoMessages
}

// Ack acknowledges successful processing of a message.
func (m *Manager) Ack(ctx context.Context, queueName, groupID, consumerID string, offset uint64) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Remove from PEL
	if err := m.groupStore.RemovePendingEntry(ctx, queueName, groupID, consumerID, offset); err != nil {
		return err
	}

	// Get group to update committed offset
	group, err := m.groupStore.GetConsumerGroup(ctx, queueName, groupID)
	if err != nil {
		return err
	}

	// Advance committed offset if possible
	return m.advanceCommitted(ctx, group)
}

// AckBatch acknowledges multiple messages.
func (m *Manager) AckBatch(ctx context.Context, queueName, groupID, consumerID string, offsets []uint64) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, offset := range offsets {
		if err := m.groupStore.RemovePendingEntry(ctx, queueName, groupID, consumerID, offset); err != nil {
			// Continue even if some fail
			continue
		}
	}

	// Get group to update committed offset
	group, err := m.groupStore.GetConsumerGroup(ctx, queueName, groupID)
	if err != nil {
		return err
	}

	return m.advanceCommitted(ctx, group)
}

// Nack negatively acknowledges a message, making it available for redelivery.
func (m *Manager) Nack(ctx context.Context, queueName, groupID, consumerID string, offset uint64) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Get group
	group, err := m.groupStore.GetConsumerGroup(ctx, queueName, groupID)
	if err != nil {
		return err
	}

	// Find the pending entry
	entry, ownerID := group.FindPending(offset)
	if entry == nil {
		return ErrMessageNotPending
	}

	// Only the owner can nack
	if ownerID != consumerID {
		return ErrConsumerNotFound
	}

	// Reset claim time to make it immediately stealable
	// The entry stays in the owner's PEL but with updated timestamp
	entry.ClaimedAt = time.Now().Add(-m.config.VisibilityTimeout - time.Second)
	entry.DeliveryCount++

	return m.groupStore.UpdateConsumerGroup(ctx, group)
}

// Reject rejects a message, moving it to the DLQ.
func (m *Manager) Reject(ctx context.Context, queueName, groupID, consumerID string, offset uint64, reason string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Remove from PEL (message goes to DLQ via separate mechanism)
	if err := m.groupStore.RemovePendingEntry(ctx, queueName, groupID, consumerID, offset); err != nil {
		return err
	}

	// Get group to update committed offset
	group, err := m.groupStore.GetConsumerGroup(ctx, queueName, groupID)
	if err != nil {
		return err
	}

	// Advance committed offset if possible
	return m.advanceCommitted(ctx, group)
}

// advanceCommitted updates the committed offset to the minimum pending offset.
func (m *Manager) advanceCommitted(ctx context.Context, group *types.ConsumerGroup) error {
	cursor := group.GetCursor()

	// Find minimum pending offset
	minOffset, found := group.MinPendingOffset()

	if !found {
		// No pending entries - committed = cursor
		cursor.Committed = cursor.Cursor
	} else if minOffset > cursor.Committed {
		cursor.Committed = minOffset
	}

	return m.groupStore.UpdateCommitted(ctx, group.QueueName, group.ID, cursor.Committed)
}

// GetPendingCount returns the number of pending messages for a group.
func (m *Manager) GetPendingCount(ctx context.Context, queueName, groupID string) (int, error) {
	group, err := m.groupStore.GetConsumerGroup(ctx, queueName, groupID)
	if err != nil {
		return 0, err
	}

	return group.PendingCount(), nil
}

// GetLag returns the consumer lag (unprocessed messages).
func (m *Manager) GetLag(ctx context.Context, queueName, groupID string) (uint64, error) {
	group, err := m.groupStore.GetConsumerGroup(ctx, queueName, groupID)
	if err != nil {
		return 0, err
	}

	cursor := group.GetCursor()

	tail, err := m.queueStore.Tail(ctx, queueName)
	if err != nil {
		return 0, err
	}

	// Lag = messages not yet delivered + pending messages
	if tail > cursor.Committed {
		return tail - cursor.Committed, nil
	}

	return 0, nil
}

// GetCommittedOffset returns the committed offset.
// This is the safe point for log truncation.
func (m *Manager) GetCommittedOffset(ctx context.Context, queueName, groupID string) (uint64, error) {
	group, err := m.groupStore.GetConsumerGroup(ctx, queueName, groupID)
	if err != nil {
		return 0, err
	}

	cursor := group.GetCursor()
	return cursor.Committed, nil
}

// CommitOffset explicitly commits an offset for a stream consumer group.
// This is used when AutoCommit is disabled for manual commit control.
func (m *Manager) CommitOffset(ctx context.Context, queueName, groupID string, offset uint64) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	group, err := m.groupStore.GetConsumerGroup(ctx, queueName, groupID)
	if err != nil {
		return err
	}

	if group.Mode != types.GroupModeStream {
		return ErrCommitOffsetOnlyForStreamMode
	}

	cursor := group.GetCursor()
	if offset > cursor.Cursor {
		return ErrInvalidOffset
	}

	return m.groupStore.UpdateCommitted(ctx, queueName, groupID, offset)
}

// GetMinCommittedOffset returns the minimum committed offset across all groups for a stream.
// This is the global safe point for log truncation.
func (m *Manager) GetMinCommittedOffset(ctx context.Context, queueName string) (uint64, error) {
	groups, err := m.groupStore.ListConsumerGroups(ctx, queueName)
	if err != nil {
		return 0, err
	}

	if len(groups) == 0 {
		// No consumers - return tail (can truncate everything)
		return m.queueStore.Tail(ctx, queueName)
	}

	var minCommitted uint64
	first := true

	for _, group := range groups {
		cursor := group.GetCursor()
		if first || cursor.Committed < minCommitted {
			minCommitted = cursor.Committed
			first = false
		}
	}

	return minCommitted, nil
}

// GetMinCommittedOffsetByMode returns the minimum committed offset for groups matching mode.
// If no groups of that mode exist, returns the queue tail.
func (m *Manager) GetMinCommittedOffsetByMode(ctx context.Context, queueName string, mode types.ConsumerGroupMode) (uint64, error) {
	groups, err := m.groupStore.ListConsumerGroups(ctx, queueName)
	if err != nil {
		return 0, err
	}

	var minCommitted uint64
	first := true

	for _, group := range groups {
		if mode != "" && group.Mode != mode {
			continue
		}
		cursor := group.GetCursor()
		if first || cursor.Committed < minCommitted {
			minCommitted = cursor.Committed
			first = false
		}
	}

	if first {
		return m.queueStore.Tail(ctx, queueName)
	}

	return minCommitted, nil
}

// UpdateHeartbeat updates the heartbeat timestamp for a consumer.
func (m *Manager) UpdateHeartbeat(ctx context.Context, queueName, groupID, consumerID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	group, err := m.groupStore.GetConsumerGroup(ctx, queueName, groupID)
	if err != nil {
		return err
	}

	consumer := group.GetConsumer(consumerID)
	if consumer == nil {
		return ErrConsumerNotFound
	}

	consumer.LastHeartbeat = time.Now()
	return m.groupStore.RegisterConsumer(ctx, queueName, groupID, consumer)
}

// CleanupStaleConsumers removes consumers that haven't sent a heartbeat within the timeout.
func (m *Manager) CleanupStaleConsumers(ctx context.Context, queueName, groupID string, timeout time.Duration) ([]string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	group, err := m.groupStore.GetConsumerGroup(ctx, queueName, groupID)
	if err != nil {
		return nil, err
	}

	cutoff := time.Now().Add(-timeout)
	var removed []string

	// Collect stale consumer IDs
	group.ForEachConsumer(func(id string, consumer *types.ConsumerInfo) bool {
		if consumer.LastHeartbeat.Before(cutoff) {
			removed = append(removed, id)
		}
		return true
	})

	// Delete stale consumers
	for _, id := range removed {
		group.DeleteConsumer(id)
	}

	if len(removed) > 0 {
		if err := m.groupStore.UpdateConsumerGroup(ctx, group); err != nil {
			return nil, err
		}
	}

	return removed, nil
}
