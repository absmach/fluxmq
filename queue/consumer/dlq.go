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
	groupStore  storage.ConsumerGroupStore
	config      DLQConfig
	metrics     *Metrics
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
		groupStore:  groupStore,
		config:      config,
		metrics:     metrics,
	}
}

// ShouldMoveToDLQ returns true if the message should be moved to DLQ.
func (h *DLQHandler) ShouldMoveToDLQ(deliveryCount int) bool {
	return deliveryCount >= h.config.MaxDeliveryCount
}

// MoveToDLQ moves a message to the dead-letter queue.
func (h *DLQHandler) MoveToDLQ(ctx context.Context, streamName, groupID string, entry *types.PendingEntry, reason string) error {
	// Read the original message
	msg, err := h.queueStore.Read(ctx, streamName, entry.Offset)
	if err != nil {
		return fmt.Errorf("failed to read message for DLQ: %w", err)
	}

	// Create DLQ stream name
	dlqStream := h.getDLQStream(streamName)

	// Ensure DLQ stream exists
	if err := h.ensureDLQStream(ctx, dlqStream); err != nil {
		return fmt.Errorf("failed to ensure DLQ stream: %w", err)
	}

	// Create DLQ message with metadata
	dlqMsg := h.createDLQMessage(msg, streamName, groupID, entry, reason)

	// Append to DLQ
	_, err = h.queueStore.Append(ctx, dlqStream, dlqMsg)
	if err != nil {
		return fmt.Errorf("failed to append to DLQ: %w", err)
	}

	// Remove from original PEL
	if err := h.groupStore.RemovePendingEntry(ctx, streamName, groupID, entry.ConsumerID, entry.Offset); err != nil {
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
func (h *DLQHandler) ProcessExpiredEntries(ctx context.Context, streamName, groupID string) (int, error) {
	group, err := h.groupStore.GetConsumerGroup(ctx, streamName, groupID)
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

				if err := h.MoveToDLQ(ctx, streamName, groupID, &entryCopy, reason); err != nil {
					// Log error but continue processing
					continue
				}

				moved++
			}
		}
	}

	return moved, nil
}

// getDLQStream returns the DLQ stream name for a stream.
func (h *DLQHandler) getDLQStream(streamName string) string {
	return h.config.DLQTopicPrefix + streamName
}

// ensureDLQStream creates the DLQ stream if it doesn't exist.
func (h *DLQHandler) ensureDLQStream(ctx context.Context, dlqStream string) error {
	_, err := h.queueStore.GetQueue(ctx, dlqStream)
	if err == nil {
		return nil // Stream exists
	}

	if err != storage.ErrQueueNotFound {
		return err
	}

	// Create DLQ stream with minimal config
	config := types.QueueConfig{
		Name:             dlqStream,
		Topics:           []string{dlqStream}, // DLQ stream matches its own name
		Reserved:         false,
		MaxMessageSize:   1024 * 1024,         // 1MB
		MaxStreamDepth:   1000000,             // 1M messages
		MessageTTL:       30 * 24 * time.Hour, // 30 days retention
		DeliveryTimeout:  30 * time.Second,
		BatchSize:        100,
		HeartbeatTimeout: 30 * time.Second,
	}

	return h.queueStore.CreateQueue(ctx, config)
}

// createDLQMessage creates a message for the DLQ with failure metadata.
func (h *DLQHandler) createDLQMessage(original *types.Message, streamName, groupID string, entry *types.PendingEntry, reason string) *types.Message {
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
		dlqMsg.Properties["dlq-original-stream"] = streamName
		dlqMsg.Properties["dlq-original-offset"] = fmt.Sprintf("%d", entry.Offset)
		dlqMsg.Properties["dlq-consumer-group"] = groupID
		dlqMsg.Properties["dlq-consumer-id"] = entry.ConsumerID
		dlqMsg.Properties["dlq-delivery-count"] = fmt.Sprintf("%d", entry.DeliveryCount)
		dlqMsg.Properties["dlq-failure-reason"] = reason
		dlqMsg.Properties["dlq-moved-at"] = now.Format(time.RFC3339)
	}

	return dlqMsg
}

// ReplayFromDLQ moves a message from DLQ back to the original stream.
func (h *DLQHandler) ReplayFromDLQ(ctx context.Context, dlqStream string, offset uint64) error {
	// Read DLQ message
	msg, err := h.queueStore.Read(ctx, dlqStream, offset)
	if err != nil {
		return fmt.Errorf("failed to read DLQ message: %w", err)
	}

	// Get original stream from metadata
	originalStream, ok := msg.Properties["dlq-original-stream"]
	if !ok {
		return fmt.Errorf("DLQ message missing original stream metadata")
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
	replayMsg.Properties["replayed-from-dlq"] = dlqStream
	replayMsg.Properties["replayed-at"] = time.Now().Format(time.RFC3339)

	// Append to original stream
	_, err = h.queueStore.Append(ctx, originalStream, replayMsg)
	return err
}

// ListDLQMessages lists messages in the DLQ for a stream.
func (h *DLQHandler) ListDLQMessages(ctx context.Context, streamName string, limit int) ([]*types.Message, error) {
	dlqStream := h.getDLQStream(streamName)

	// Check if DLQ exists
	_, err := h.queueStore.GetQueue(ctx, dlqStream)
	if err != nil {
		if err == storage.ErrQueueNotFound {
			return []*types.Message{}, nil
		}
		return nil, err
	}

	// Read from head
	head, err := h.queueStore.Head(ctx, dlqStream)
	if err != nil {
		return nil, err
	}

	return h.queueStore.ReadBatch(ctx, dlqStream, head, limit)
}

// GetDLQCount returns the number of messages in the DLQ.
func (h *DLQHandler) GetDLQCount(ctx context.Context, streamName string) (uint64, error) {
	dlqStream := h.getDLQStream(streamName)

	_, err := h.queueStore.GetQueue(ctx, dlqStream)
	if err != nil {
		if err == storage.ErrQueueNotFound {
			return 0, nil
		}
		return 0, err
	}

	return h.queueStore.Count(ctx, dlqStream)
}
