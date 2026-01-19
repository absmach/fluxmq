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
	logStore   storage.LogStore
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
func NewDLQHandler(logStore storage.LogStore, groupStore storage.ConsumerGroupStore, config DLQConfig, metrics *Metrics) *DLQHandler {
	return &DLQHandler{
		logStore:   logStore,
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
	msg, err := h.logStore.Read(ctx, queueName, entry.PartitionID, entry.Offset)
	if err != nil {
		return fmt.Errorf("failed to read message for DLQ: %w", err)
	}

	// Create DLQ topic name
	dlqTopic := h.getDLQTopic(queueName)

	// Ensure DLQ queue exists
	if err := h.ensureDLQQueue(ctx, dlqTopic); err != nil {
		return fmt.Errorf("failed to ensure DLQ queue: %w", err)
	}

	// Create DLQ message with metadata
	dlqMsg := h.createDLQMessage(msg, queueName, groupID, entry, reason)

	// Append to DLQ (partition 0 for simplicity)
	_, err = h.logStore.Append(ctx, dlqTopic, 0, dlqMsg)
	if err != nil {
		return fmt.Errorf("failed to append to DLQ: %w", err)
	}

	// Remove from original PEL
	if err := h.groupStore.RemovePendingEntry(ctx, queueName, groupID, entry.ConsumerID, entry.PartitionID, entry.Offset); err != nil {
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

// getDLQTopic returns the DLQ topic name for a queue.
func (h *DLQHandler) getDLQTopic(queueName string) string {
	// Convert $queue/tasks to $dlq/tasks
	if len(queueName) > 7 && queueName[:7] == "$queue/" {
		return h.config.DLQTopicPrefix + queueName[7:]
	}
	return h.config.DLQTopicPrefix + queueName
}

// ensureDLQQueue creates the DLQ queue if it doesn't exist.
func (h *DLQHandler) ensureDLQQueue(ctx context.Context, dlqTopic string) error {
	_, err := h.logStore.GetQueue(ctx, dlqTopic)
	if err == nil {
		return nil // Queue exists
	}

	if err != storage.ErrQueueNotFound {
		return err
	}

	// Create DLQ queue with minimal config
	config := types.QueueConfig{
		Name:             dlqTopic,
		Partitions:       1, // Single partition for DLQ
		Ordering:         types.OrderingPartition,
		MaxMessageSize:   1024 * 1024, // 1MB
		MaxQueueDepth:    1000000,     // 1M messages
		MessageTTL:       30 * 24 * time.Hour, // 30 days retention
		DeliveryTimeout:  30 * time.Second,
		BatchSize:        100,
		HeartbeatTimeout: 30 * time.Second,
	}

	return h.logStore.CreateQueue(ctx, config)
}

// createDLQMessage creates a message for the DLQ with failure metadata.
func (h *DLQHandler) createDLQMessage(original *types.Message, queueName, groupID string, entry *types.PendingEntry, reason string) *types.Message {
	now := time.Now()

	dlqMsg := &types.Message{
		ID:            original.ID + "-dlq",
		Payload:       original.Payload,
		Topic:         original.Topic,
		PartitionKey:  original.PartitionKey,
		PartitionID:   0, // DLQ uses single partition
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
		dlqMsg.Properties["dlq-original-partition"] = fmt.Sprintf("%d", entry.PartitionID)
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
func (h *DLQHandler) ReplayFromDLQ(ctx context.Context, dlqTopic string, partitionID int, offset uint64) error {
	// Read DLQ message
	msg, err := h.logStore.Read(ctx, dlqTopic, partitionID, offset)
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
		ID:           msg.ID + "-replay",
		Payload:      msg.Payload,
		Topic:        msg.Topic,
		PartitionKey: msg.PartitionKey,
		Properties:   make(map[string]string),
		State:        types.StateQueued,
		CreatedAt:    time.Now(),
	}

	// Copy properties except DLQ metadata
	for k, v := range msg.Properties {
		if len(k) < 4 || k[:4] != "dlq-" {
			replayMsg.Properties[k] = v
		}
	}

	// Add replay metadata
	replayMsg.Properties["replayed-from-dlq"] = dlqTopic
	replayMsg.Properties["replayed-at"] = time.Now().Format(time.RFC3339)

	// Determine partition for replay (use original or hash)
	partID := 0
	if pk := replayMsg.PartitionKey; pk != "" {
		// Hash partition key to get partition ID
		config, err := h.logStore.GetQueue(ctx, originalQueue)
		if err == nil && config.Partitions > 0 {
			partID = hashPartitionKey(pk, config.Partitions)
		}
	}

	// Append to original queue
	_, err = h.logStore.Append(ctx, originalQueue, partID, replayMsg)
	return err
}

// hashPartitionKey returns a partition ID for a key.
func hashPartitionKey(key string, partitions int) int {
	if partitions <= 0 {
		return 0
	}

	// Simple FNV-1a hash
	var hash uint32 = 2166136261
	for i := 0; i < len(key); i++ {
		hash ^= uint32(key[i])
		hash *= 16777619
	}

	return int(hash % uint32(partitions))
}

// ListDLQMessages lists messages in the DLQ for a queue.
func (h *DLQHandler) ListDLQMessages(ctx context.Context, queueName string, limit int) ([]*types.Message, error) {
	dlqTopic := h.getDLQTopic(queueName)

	// Check if DLQ exists
	_, err := h.logStore.GetQueue(ctx, dlqTopic)
	if err != nil {
		if err == storage.ErrQueueNotFound {
			return []*types.Message{}, nil
		}
		return nil, err
	}

	// Read from head
	head, err := h.logStore.Head(ctx, dlqTopic, 0)
	if err != nil {
		return nil, err
	}

	return h.logStore.ReadBatch(ctx, dlqTopic, 0, head, limit)
}

// GetDLQCount returns the number of messages in the DLQ.
func (h *DLQHandler) GetDLQCount(ctx context.Context, queueName string) (uint64, error) {
	dlqTopic := h.getDLQTopic(queueName)

	_, err := h.logStore.GetQueue(ctx, dlqTopic)
	if err != nil {
		if err == storage.ErrQueueNotFound {
			return 0, nil
		}
		return 0, err
	}

	return h.logStore.Count(ctx, dlqTopic, 0)
}
