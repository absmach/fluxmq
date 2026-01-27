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
	ErrNoMessages        = errors.New("no messages available")
	ErrGroupNotFound     = errors.New("consumer group not found")
	ErrConsumerNotFound  = errors.New("consumer not found")
	ErrMessageNotPending = errors.New("message not in pending list")
	ErrInvalidOffset     = errors.New("invalid offset")
)

// Manager handles consumer group operations including claiming, acknowledging,
// and work stealing for the stream-based model.
type Manager struct {
	streamStore storage.QueueStore
	groupStore  storage.ConsumerGroupStore
	config      Config
	mu          sync.RWMutex
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
}

// DefaultConfig returns default manager configuration.
func DefaultConfig() Config {
	return Config{
		VisibilityTimeout: 30 * time.Second,
		MaxDeliveryCount:  5,
		ClaimBatchSize:    10,
		StealBatchSize:    5,
	}
}

// NewManager creates a new consumer group manager.
func NewManager(streamStore storage.QueueStore, groupStore storage.ConsumerGroupStore, config Config) *Manager {
	return &Manager{
		streamStore: streamStore,
		groupStore:  groupStore,
		config:      config,
	}
}

// GetOrCreateGroup retrieves or creates a consumer group.
func (m *Manager) GetOrCreateGroup(ctx context.Context, streamName, groupID, pattern string) (*types.ConsumerGroupState, error) {
	// Try to get existing group
	group, err := m.groupStore.GetConsumerGroup(ctx, streamName, groupID)
	if err == nil {
		return group, nil
	}

	// Check for "not found" errors from various storage implementations
	if err != storage.ErrConsumerNotFound && err != logstorage.ErrGroupNotFound {
		return nil, err
	}

	// Create new group
	group = types.NewConsumerGroupState(streamName, groupID, pattern)

	if err := m.groupStore.CreateConsumerGroup(ctx, group); err != nil {
		// Handle race condition - another process might have created it
		if err == storage.ErrConsumerGroupExists {
			return m.groupStore.GetConsumerGroup(ctx, streamName, groupID)
		}
		return nil, err
	}

	return group, nil
}

// RegisterConsumer adds a consumer to a group.
func (m *Manager) RegisterConsumer(ctx context.Context, streamName, groupID, consumerID, clientID, proxyNodeID string) error {
	consumer := &types.ConsumerInfo{
		ID:            consumerID,
		ClientID:      clientID,
		ProxyNodeID:   proxyNodeID,
		RegisteredAt:  time.Now(),
		LastHeartbeat: time.Now(),
	}

	return m.groupStore.RegisterConsumer(ctx, streamName, groupID, consumer)
}

// UnregisterConsumer removes a consumer from a group.
func (m *Manager) UnregisterConsumer(ctx context.Context, streamName, groupID, consumerID string) error {
	return m.groupStore.UnregisterConsumer(ctx, streamName, groupID, consumerID)
}

// Claim retrieves the next available message for a consumer.
// It first tries to get a new message from the log, then falls back to work stealing.
func (m *Manager) Claim(ctx context.Context, streamName, groupID, consumerID string, filter *Filter) (*types.Message, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Get consumer group
	group, err := m.groupStore.GetConsumerGroup(ctx, streamName, groupID)
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
func (m *Manager) ClaimBatch(ctx context.Context, streamName, groupID, consumerID string, filter *Filter, limit int) ([]*types.Message, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if limit <= 0 {
		limit = m.config.ClaimBatchSize
	}

	// Get consumer group
	group, err := m.groupStore.GetConsumerGroup(ctx, streamName, groupID)
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

// claimFromCursor tries to claim a message from the cursor position.
func (m *Manager) claimFromCursor(ctx context.Context, group *types.ConsumerGroupState, consumerID string, filter *Filter) (*types.Message, error) {
	cursor := group.GetCursor()

	// Get log tail
	tail, err := m.streamStore.Tail(ctx, group.QueueName)
	if err != nil {
		return nil, err
	}

	// Scan from cursor until we find a matching message or hit tail
	for cursor.Cursor < tail {
		offset := cursor.Cursor
		cursor.Cursor++

		// Read message
		msg, err := m.streamStore.Read(ctx, group.QueueName, offset)
		if err != nil {
			if err == storage.ErrOffsetOutOfRange {
				continue // Message was truncated, skip
			}
			return nil, err
		}

		// Check if message matches filter
		if filter != nil {
			routingKey := types.ExtractRoutingKey(msg.Topic, group.QueueName)
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
func (m *Manager) stealWork(ctx context.Context, group *types.ConsumerGroupState, consumerID string, filter *Filter) (*types.Message, error) {
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
		msg, err := m.streamStore.Read(ctx, group.QueueName, entry.Offset)
		if err != nil {
			continue // Message might be truncated
		}

		// Check filter
		if filter != nil {
			routingKey := types.ExtractRoutingKey(msg.Topic, group.QueueName)
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
func (m *Manager) Ack(ctx context.Context, streamName, groupID, consumerID string, offset uint64) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Remove from PEL
	if err := m.groupStore.RemovePendingEntry(ctx, streamName, groupID, consumerID, offset); err != nil {
		return err
	}

	// Get group to update committed offset
	group, err := m.groupStore.GetConsumerGroup(ctx, streamName, groupID)
	if err != nil {
		return err
	}

	// Advance committed offset if possible
	return m.advanceCommitted(ctx, group)
}

// AckBatch acknowledges multiple messages.
func (m *Manager) AckBatch(ctx context.Context, streamName, groupID, consumerID string, offsets []uint64) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, offset := range offsets {
		if err := m.groupStore.RemovePendingEntry(ctx, streamName, groupID, consumerID, offset); err != nil {
			// Continue even if some fail
			continue
		}
	}

	// Get group to update committed offset
	group, err := m.groupStore.GetConsumerGroup(ctx, streamName, groupID)
	if err != nil {
		return err
	}

	return m.advanceCommitted(ctx, group)
}

// Nack negatively acknowledges a message, making it available for redelivery.
func (m *Manager) Nack(ctx context.Context, streamName, groupID, consumerID string, offset uint64) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Get group
	group, err := m.groupStore.GetConsumerGroup(ctx, streamName, groupID)
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
func (m *Manager) Reject(ctx context.Context, streamName, groupID, consumerID string, offset uint64, reason string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Remove from PEL (message goes to DLQ via separate mechanism)
	if err := m.groupStore.RemovePendingEntry(ctx, streamName, groupID, consumerID, offset); err != nil {
		return err
	}

	// Get group to update committed offset
	group, err := m.groupStore.GetConsumerGroup(ctx, streamName, groupID)
	if err != nil {
		return err
	}

	// Advance committed offset if possible
	return m.advanceCommitted(ctx, group)
}

// advanceCommitted updates the committed offset to the minimum pending offset.
func (m *Manager) advanceCommitted(ctx context.Context, group *types.ConsumerGroupState) error {
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
func (m *Manager) GetPendingCount(ctx context.Context, streamName, groupID string) (int, error) {
	group, err := m.groupStore.GetConsumerGroup(ctx, streamName, groupID)
	if err != nil {
		return 0, err
	}

	return group.PendingCount(), nil
}

// GetLag returns the consumer lag (unprocessed messages).
func (m *Manager) GetLag(ctx context.Context, streamName, groupID string) (uint64, error) {
	group, err := m.groupStore.GetConsumerGroup(ctx, streamName, groupID)
	if err != nil {
		return 0, err
	}

	cursor := group.GetCursor()

	tail, err := m.streamStore.Tail(ctx, streamName)
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
func (m *Manager) GetCommittedOffset(ctx context.Context, streamName, groupID string) (uint64, error) {
	group, err := m.groupStore.GetConsumerGroup(ctx, streamName, groupID)
	if err != nil {
		return 0, err
	}

	cursor := group.GetCursor()
	return cursor.Committed, nil
}

// GetMinCommittedOffset returns the minimum committed offset across all groups for a stream.
// This is the global safe point for log truncation.
func (m *Manager) GetMinCommittedOffset(ctx context.Context, streamName string) (uint64, error) {
	groups, err := m.groupStore.ListConsumerGroups(ctx, streamName)
	if err != nil {
		return 0, err
	}

	if len(groups) == 0 {
		// No consumers - return tail (can truncate everything)
		return m.streamStore.Tail(ctx, streamName)
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

// UpdateHeartbeat updates the heartbeat timestamp for a consumer.
func (m *Manager) UpdateHeartbeat(ctx context.Context, streamName, groupID, consumerID string) error {
	group, err := m.groupStore.GetConsumerGroup(ctx, streamName, groupID)
	if err != nil {
		return err
	}

	consumer := group.GetConsumer(consumerID)
	if consumer == nil {
		return ErrConsumerNotFound
	}

	consumer.LastHeartbeat = time.Now()
	return m.groupStore.UpdateConsumerGroup(ctx, group)
}

// CleanupStaleConsumers removes consumers that haven't sent a heartbeat within the timeout.
func (m *Manager) CleanupStaleConsumers(ctx context.Context, streamName, groupID string, timeout time.Duration) ([]string, error) {
	group, err := m.groupStore.GetConsumerGroup(ctx, streamName, groupID)
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
