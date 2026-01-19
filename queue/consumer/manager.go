// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package consumer

import (
	"context"
	"errors"
	"sync"
	"time"

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
// and work stealing for the log-based queue model.
type Manager struct {
	logStore   storage.LogStore
	groupStore storage.ConsumerGroupStore
	config     Config
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
func NewManager(logStore storage.LogStore, groupStore storage.ConsumerGroupStore, config Config) *Manager {
	return &Manager{
		logStore:   logStore,
		groupStore: groupStore,
		config:     config,
	}
}

// GetOrCreateGroup retrieves or creates a consumer group.
func (m *Manager) GetOrCreateGroup(ctx context.Context, queueName, groupID, pattern string) (*types.ConsumerGroupState, error) {
	// Try to get existing group
	group, err := m.groupStore.GetConsumerGroup(ctx, queueName, groupID)
	if err == nil {
		return group, nil
	}

	if err != storage.ErrConsumerNotFound {
		return nil, err
	}

	// Get queue config for partition count
	queueConfig, err := m.logStore.GetQueue(ctx, queueName)
	if err != nil {
		return nil, err
	}

	// Create new group
	group = types.NewConsumerGroupState(queueName, groupID, pattern, queueConfig.Partitions)

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
func (m *Manager) Claim(ctx context.Context, queueName, groupID, consumerID string, partitionID int, filter *Filter) (*types.Message, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Get consumer group
	group, err := m.groupStore.GetConsumerGroup(ctx, queueName, groupID)
	if err != nil {
		return nil, err
	}

	// Try to claim from cursor position
	msg, err := m.claimFromCursor(ctx, group, consumerID, partitionID, filter)
	if err == nil {
		return msg, nil
	}

	if err != ErrNoMessages {
		return nil, err
	}

	// No new messages - try work stealing
	return m.stealWork(ctx, group, consumerID, partitionID, filter)
}

// ClaimBatch retrieves multiple messages for a consumer.
func (m *Manager) ClaimBatch(ctx context.Context, queueName, groupID, consumerID string, partitionID int, filter *Filter, limit int) ([]*types.Message, error) {
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
		msg, err := m.claimFromCursor(ctx, group, consumerID, partitionID, filter)
		if err != nil {
			break
		}
		messages = append(messages, msg)
	}

	// Try work stealing if we didn't get enough
	for len(messages) < limit {
		msg, err := m.stealWork(ctx, group, consumerID, partitionID, filter)
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
func (m *Manager) claimFromCursor(ctx context.Context, group *types.ConsumerGroupState, consumerID string, partitionID int, filter *Filter) (*types.Message, error) {
	cursor := group.GetCursor(partitionID)

	// Get log tail
	tail, err := m.logStore.Tail(ctx, group.QueueName, partitionID)
	if err != nil {
		return nil, err
	}

	// Scan from cursor until we find a matching message or hit tail
	for cursor.Cursor < tail {
		offset := cursor.Cursor
		cursor.Cursor++

		// Read message
		msg, err := m.logStore.Read(ctx, group.QueueName, partitionID, offset)
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
			PartitionID:   partitionID,
			ConsumerID:    consumerID,
			ClaimedAt:     time.Now(),
			DeliveryCount: 1,
		}

		if err := m.groupStore.AddPendingEntry(ctx, group.QueueName, group.ID, entry); err != nil {
			return nil, err
		}

		// Update cursor
		if err := m.groupStore.UpdateCursor(ctx, group.QueueName, group.ID, partitionID, cursor.Cursor); err != nil {
			return nil, err
		}

		return msg, nil
	}

	return nil, ErrNoMessages
}

// stealWork tries to steal a message from another consumer's PEL.
func (m *Manager) stealWork(ctx context.Context, group *types.ConsumerGroupState, consumerID string, partitionID int, filter *Filter) (*types.Message, error) {
	// Get stealable entries
	stealable := group.StealableEntries(partitionID, m.config.VisibilityTimeout, consumerID)
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
		msg, err := m.logStore.Read(ctx, group.QueueName, partitionID, entry.Offset)
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
			partitionID,
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
func (m *Manager) Ack(ctx context.Context, queueName, groupID, consumerID string, partitionID int, offset uint64) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Remove from PEL
	if err := m.groupStore.RemovePendingEntry(ctx, queueName, groupID, consumerID, partitionID, offset); err != nil {
		return err
	}

	// Get group to update committed offset
	group, err := m.groupStore.GetConsumerGroup(ctx, queueName, groupID)
	if err != nil {
		return err
	}

	// Advance committed offset if possible
	return m.advanceCommitted(ctx, group, partitionID)
}

// AckBatch acknowledges multiple messages.
func (m *Manager) AckBatch(ctx context.Context, queueName, groupID, consumerID string, partitionID int, offsets []uint64) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, offset := range offsets {
		if err := m.groupStore.RemovePendingEntry(ctx, queueName, groupID, consumerID, partitionID, offset); err != nil {
			// Continue even if some fail
			continue
		}
	}

	// Get group to update committed offset
	group, err := m.groupStore.GetConsumerGroup(ctx, queueName, groupID)
	if err != nil {
		return err
	}

	return m.advanceCommitted(ctx, group, partitionID)
}

// Nack negatively acknowledges a message, making it available for redelivery.
func (m *Manager) Nack(ctx context.Context, queueName, groupID, consumerID string, partitionID int, offset uint64) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Get group
	group, err := m.groupStore.GetConsumerGroup(ctx, queueName, groupID)
	if err != nil {
		return err
	}

	// Find the pending entry
	entry, ownerID := group.FindPending(partitionID, offset)
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
func (m *Manager) Reject(ctx context.Context, queueName, groupID, consumerID string, partitionID int, offset uint64, reason string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Remove from PEL (message goes to DLQ via separate mechanism)
	if err := m.groupStore.RemovePendingEntry(ctx, queueName, groupID, consumerID, partitionID, offset); err != nil {
		return err
	}

	// Get group to update committed offset
	group, err := m.groupStore.GetConsumerGroup(ctx, queueName, groupID)
	if err != nil {
		return err
	}

	// Advance committed offset if possible
	return m.advanceCommitted(ctx, group, partitionID)
}

// advanceCommitted updates the committed offset to the minimum pending offset.
func (m *Manager) advanceCommitted(ctx context.Context, group *types.ConsumerGroupState, partitionID int) error {
	cursor := group.GetCursor(partitionID)

	// Find minimum pending offset for this partition
	minOffset, found := group.MinPendingOffset(partitionID)

	if !found {
		// No pending entries - committed = cursor
		cursor.Committed = cursor.Cursor
	} else if minOffset > cursor.Committed {
		cursor.Committed = minOffset
	}

	return m.groupStore.UpdateCommitted(ctx, group.QueueName, group.ID, partitionID, cursor.Committed)
}

// GetPendingCount returns the number of pending messages for a group and partition.
func (m *Manager) GetPendingCount(ctx context.Context, queueName, groupID string, partitionID int) (int, error) {
	group, err := m.groupStore.GetConsumerGroup(ctx, queueName, groupID)
	if err != nil {
		return 0, err
	}

	return group.PendingCount(partitionID), nil
}

// GetLag returns the consumer lag (unprocessed messages) for a partition.
func (m *Manager) GetLag(ctx context.Context, queueName, groupID string, partitionID int) (uint64, error) {
	group, err := m.groupStore.GetConsumerGroup(ctx, queueName, groupID)
	if err != nil {
		return 0, err
	}

	cursor := group.GetCursor(partitionID)

	tail, err := m.logStore.Tail(ctx, queueName, partitionID)
	if err != nil {
		return 0, err
	}

	// Lag = messages not yet delivered + pending messages
	if tail > cursor.Committed {
		return tail - cursor.Committed, nil
	}

	return 0, nil
}

// GetCommittedOffset returns the committed offset for a partition.
// This is the safe point for log truncation.
func (m *Manager) GetCommittedOffset(ctx context.Context, queueName, groupID string, partitionID int) (uint64, error) {
	group, err := m.groupStore.GetConsumerGroup(ctx, queueName, groupID)
	if err != nil {
		return 0, err
	}

	cursor := group.GetCursor(partitionID)
	return cursor.Committed, nil
}

// GetMinCommittedOffset returns the minimum committed offset across all groups for a queue.
// This is the global safe point for log truncation.
func (m *Manager) GetMinCommittedOffset(ctx context.Context, queueName string, partitionID int) (uint64, error) {
	groups, err := m.groupStore.ListConsumerGroups(ctx, queueName)
	if err != nil {
		return 0, err
	}

	if len(groups) == 0 {
		// No consumers - return tail (can truncate everything)
		return m.logStore.Tail(ctx, queueName, partitionID)
	}

	var minCommitted uint64
	first := true

	for _, group := range groups {
		cursor := group.GetCursor(partitionID)
		if first || cursor.Committed < minCommitted {
			minCommitted = cursor.Committed
			first = false
		}
	}

	return minCommitted, nil
}

// UpdateHeartbeat updates the heartbeat timestamp for a consumer.
func (m *Manager) UpdateHeartbeat(ctx context.Context, queueName, groupID, consumerID string) error {
	group, err := m.groupStore.GetConsumerGroup(ctx, queueName, groupID)
	if err != nil {
		return err
	}

	consumer, ok := group.Consumers[consumerID]
	if !ok {
		return ErrConsumerNotFound
	}

	consumer.LastHeartbeat = time.Now()
	return m.groupStore.UpdateConsumerGroup(ctx, group)
}

// CleanupStaleConsumers removes consumers that haven't sent a heartbeat within the timeout.
func (m *Manager) CleanupStaleConsumers(ctx context.Context, queueName, groupID string, timeout time.Duration) ([]string, error) {
	group, err := m.groupStore.GetConsumerGroup(ctx, queueName, groupID)
	if err != nil {
		return nil, err
	}

	cutoff := time.Now().Add(-timeout)
	var removed []string

	for id, consumer := range group.Consumers {
		if consumer.LastHeartbeat.Before(cutoff) {
			removed = append(removed, id)
			delete(group.Consumers, id)
			// Note: PEL entries for stale consumers will be stolen via work stealing
		}
	}

	if len(removed) > 0 {
		if err := m.groupStore.UpdateConsumerGroup(ctx, group); err != nil {
			return nil, err
		}
	}

	return removed, nil
}
