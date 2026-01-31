// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package consumer

import (
	"context"
	"fmt"
	"time"

	"github.com/absmach/fluxmq/queue/storage"
	"github.com/absmach/fluxmq/queue/types"
)

// DLQHandler handles moving messages to the dead-letter queue
// when they exceed the maximum delivery count.
type DLQHandler struct {
	queueStore storage.QueueStore
	groupStore storage.ConsumerGroupStore
	config     DLQConfig
	metrics    *Metrics
}

// DLQConfig defines DLQ handler configuration.
type DLQConfig struct {
	// MaxDeliveryCount is the maximum number of delivery attempts before DLQ.
	MaxDeliveryCount int

	// DLQTopicPrefix is the prefix for DLQ topics.
	// Default: "$dlq/"
	DLQTopicPrefix string

	// IncludeMetadata adds failure metadata to DLQ messages.
	IncludeMetadata bool
}

// DefaultDLQConfig returns default DLQ configuration.
func DefaultDLQConfig() DLQConfig {
	return DLQConfig{
		MaxDeliveryCount: 5,
		DLQTopicPrefix:   "$dlq/",
		IncludeMetadata:  true,
	}
}

// NewDLQHandler creates a new DLQ handler.
func NewDLQHandler(queueStore storage.QueueStore, groupStore storage.ConsumerGroupStore, config DLQConfig, metrics *Metrics) *DLQHandler {
	return &DLQHandler{
		queueStore: queueStore,
		groupStore: groupStore,
		config:     config,
		metrics:    metrics,
	}
}

// ShouldMoveToDLQ returns true if the message should be moved to DLQ.
func (h *DLQHandler) ShouldMoveToDLQ(deliveryCount int) bool {
	return deliveryCount >= h.config.MaxDeliveryCount
}

// MoveToDLQ moves a message to the dead-letter queue.
func (h *DLQHandler) MoveToDLQ(ctx context.Context, queueName, groupID string, entry *types.PendingEntry, reason string) error {
	// Read the original message
	msg, err := h.queueStore.Read(ctx, queueName, entry.Offset)
	if err != nil {
		return fmt.Errorf("failed to read message for DLQ: %w", err)
	}

	// Create DLQ queue name
	dlq := h.getDLQ(queueName)

	// Ensure DLQ exists
	if err := h.ensureDLQ(ctx, dlq); err != nil {
		return fmt.Errorf("failed to ensure DLQ: %w", err)
	}

	// Create DLQ message with metadata
	dlqMsg := h.createDLQMessage(msg, queueName, groupID, entry, reason)

	// Append to DLQ
	_, err = h.queueStore.Append(ctx, dlq, dlqMsg)
	if err != nil {
		return fmt.Errorf("failed to append to DLQ: %w", err)
	}

	// Remove from original PEL
	if err := h.groupStore.RemovePendingEntry(ctx, queueName, groupID, entry.ConsumerID, entry.Offset); err != nil {
		return fmt.Errorf("failed to remove from PEL: %w", err)
	}

	// Record metrics
	if h.metrics != nil {
		h.metrics.RecordDLQ()
	}

	return nil
}

// ProcessExpiredEntries scans for entries that have exceeded max delivery count
// and moves them to the DLQ.
func (h *DLQHandler) ProcessExpiredEntries(ctx context.Context, queueName, groupID string) (int, error) {
	group, err := h.groupStore.GetConsumerGroup(ctx, queueName, groupID)
	if err != nil {
		return 0, err
	}

	moved := 0

	// Scan all PEL entries across all consumers
	for consumerID, entries := range group.PEL {
		// Process in reverse to safely remove during iteration
		for i := len(entries) - 1; i >= 0; i-- {
			entry := entries[i]

			if entry.DeliveryCount >= h.config.MaxDeliveryCount {
				reason := fmt.Sprintf("exceeded max delivery count (%d/%d)", entry.DeliveryCount, h.config.MaxDeliveryCount)

				// Create a copy of entry with correct consumer ID
				entryCopy := *entry
				entryCopy.ConsumerID = consumerID

				if err := h.MoveToDLQ(ctx, queueName, groupID, &entryCopy, reason); err != nil {
					// Log error but continue processing
					continue
				}

				moved++
			}
		}
	}

	return moved, nil
}

// getDLQ returns the DLQ queue name for a queue.
func (h *DLQHandler) getDLQ(queueName string) string {
	return h.config.DLQTopicPrefix + queueName
}

// ensureDLQ creates the DLQ queue if it doesn't exist.
func (h *DLQHandler) ensureDLQ(ctx context.Context, dlq string) error {
	_, err := h.queueStore.GetQueue(ctx, dlq)
	if err == nil {
		return nil // queue exists
	}

	if err != storage.ErrQueueNotFound {
		return err
	}

	// Create DLQ with minimal config
	config := types.QueueConfig{
		Name:             dlq,
		Topics:           []string{dlq}, // DLQ matches its own name
		Reserved:         false,
		MaxMessageSize:   1024 * 1024,         // 1MB
		MaxDepth:         1000000,             // 1M messages
		MessageTTL:       30 * 24 * time.Hour, // 30 days retention
		DeliveryTimeout:  30 * time.Second,
		BatchSize:        100,
		HeartbeatTimeout: 2 * time.Minute,
	}

	return h.queueStore.CreateQueue(ctx, config)
}

// createDLQMessage creates a message for the DLQ with failure metadata.
func (h *DLQHandler) createDLQMessage(original *types.Message, queueName, groupID string, entry *types.PendingEntry, reason string) *types.Message {
	now := time.Now()

	dlqMsg := &types.Message{
		ID:            original.ID + "-dlq",
		Payload:       original.Payload,
		Topic:         original.Topic,
		Properties:    make(map[string]string),
		State:         types.StateDLQ,
		CreatedAt:     now,
		FailureReason: reason,
		FirstAttempt:  entry.ClaimedAt,
		LastAttempt:   now,
		MovedToDLQAt:  now,
	}

	// Copy original properties
	for k, v := range original.Properties {
		dlqMsg.Properties[k] = v
	}

	// Add DLQ metadata
	if h.config.IncludeMetadata {
		dlqMsg.Properties["dlq-original-queue"] = queueName
		dlqMsg.Properties["dlq-original-offset"] = fmt.Sprintf("%d", entry.Offset)
		dlqMsg.Properties["dlq-consumer-group"] = groupID
		dlqMsg.Properties["dlq-consumer-id"] = entry.ConsumerID
		dlqMsg.Properties["dlq-delivery-count"] = fmt.Sprintf("%d", entry.DeliveryCount)
		dlqMsg.Properties["dlq-failure-reason"] = reason
		dlqMsg.Properties["dlq-moved-at"] = now.Format(time.RFC3339)
	}

	return dlqMsg
}

// ReplayFromDLQ moves a message from DLQ back to the original queue.
func (h *DLQHandler) ReplayFromDLQ(ctx context.Context, dlq string, offset uint64) error {
	// Read DLQ message
	msg, err := h.queueStore.Read(ctx, dlq, offset)
	if err != nil {
		return fmt.Errorf("failed to read DLQ message: %w", err)
	}

	// Get original queue from metadata
	originalQueue, ok := msg.Properties["dlq-original-queue"]
	if !ok {
		return fmt.Errorf("DLQ message missing original queue metadata")
	}

	// Reset message state
	replayMsg := &types.Message{
		ID:         msg.ID + "-replay",
		Payload:    msg.Payload,
		Topic:      msg.Topic,
		Properties: make(map[string]string),
		State:      types.StateQueued,
		CreatedAt:  time.Now(),
	}

	// Copy properties except DLQ metadata
	for k, v := range msg.Properties {
		if len(k) < 4 || k[:4] != "dlq-" {
			replayMsg.Properties[k] = v
		}
	}

	// Add replay metadata
	replayMsg.Properties["replayed-from-dlq"] = dlq
	replayMsg.Properties["replayed-at"] = time.Now().Format(time.RFC3339)

	// Append to original queue
	_, err = h.queueStore.Append(ctx, originalQueue, replayMsg)
	return err
}

// ListDLQMessages lists messages in the DLQ for a queue.
func (h *DLQHandler) ListDLQMessages(ctx context.Context, queueName string, limit int) ([]*types.Message, error) {
	dlq := h.getDLQ(queueName)

	// Check if DLQ exists
	_, err := h.queueStore.GetQueue(ctx, dlq)
	if err != nil {
		if err == storage.ErrQueueNotFound {
			return []*types.Message{}, nil
		}
		return nil, err
	}

	// Read from head
	head, err := h.queueStore.Head(ctx, dlq)
	if err != nil {
		return nil, err
	}

	return h.queueStore.ReadBatch(ctx, dlq, head, limit)
}

// GetDLQCount returns the number of messages in the DLQ.
func (h *DLQHandler) GetDLQCount(ctx context.Context, queueName string) (uint64, error) {
	dlq := h.getDLQ(queueName)

	_, err := h.queueStore.GetQueue(ctx, dlq)
	if err != nil {
		if err == storage.ErrQueueNotFound {
			return 0, nil
		}
		return 0, err
	}

	return h.queueStore.Count(ctx, dlq)
}
