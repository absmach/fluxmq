// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package queue

import (
	"context"
	"fmt"
	"time"

	"github.com/absmach/mqtt/core"
	queueStorage "github.com/absmach/mqtt/queue/storage"
	brokerStorage "github.com/absmach/mqtt/storage"
)

// PartitionWorker handles message delivery for a single partition.
// One worker runs per partition, enabling true parallelism and
// providing the foundation for both lock-free optimization (Option B)
// and future cluster scaling (partition-based work distribution).
type PartitionWorker struct {
	queueName    string
	partitionID  int
	queue        *Queue
	messageStore queueStorage.MessageStore
	broker       BrokerInterface

	notifyCh chan struct{} // Event notification channel
	stopCh   chan struct{} // Stop signal

	batchSize       int           // Max messages to process per wake
	debounceTimeout time.Duration // Small delay to batch rapid enqueues
}

// NewPartitionWorker creates a new partition worker.
func NewPartitionWorker(
	queueName string,
	partitionID int,
	queue *Queue,
	messageStore queueStorage.MessageStore,
	broker BrokerInterface,
	batchSize int,
) *PartitionWorker {
	if batchSize <= 0 {
		batchSize = 100 // Default batch size
	}

	return &PartitionWorker{
		queueName:       queueName,
		partitionID:     partitionID,
		queue:           queue,
		messageStore:    messageStore,
		broker:          broker,
		notifyCh:        make(chan struct{}, 1), // Buffered to prevent blocking
		stopCh:          make(chan struct{}),
		batchSize:       batchSize,
		debounceTimeout: 5 * time.Millisecond, // Small debounce for batching
	}
}

// Start starts the partition worker main loop.
func (pw *PartitionWorker) Start(ctx context.Context) {
	// Ticker as fallback in case notifications are missed
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-pw.stopCh:
			return
		case <-pw.notifyCh:
			// Event-driven: notified of new messages
			pw.ProcessMessages(ctx)
		case <-ticker.C:
			// Fallback: periodic check for missed notifications or retry messages
			pw.ProcessMessages(ctx)
		}
	}
}

// Stop stops the partition worker.
func (pw *PartitionWorker) Stop() {
	close(pw.stopCh)
}

// Notify signals the worker that messages are available.
// Non-blocking - uses buffered channel and select to avoid blocking enqueue.
func (pw *PartitionWorker) Notify() {
	select {
	case pw.notifyCh <- struct{}{}:
		// Notification sent
	default:
		// Channel already has notification pending, skip
	}
}

// ProcessMessages processes up to batchSize messages from the partition.
func (pw *PartitionWorker) ProcessMessages(ctx context.Context) {
	// Get partition assignment
	partition, exists := pw.queue.GetPartition(pw.partitionID)
	if !exists || !partition.IsAssigned() {
		// Partition not assigned to any consumer, skip
		return
	}

	// Get consumer groups
	groups := pw.queue.ConsumerGroups().ListGroups()
	if len(groups) == 0 {
		// No consumer groups, skip
		return
	}

	config := pw.queue.Config()

	// Batch dequeue messages
	messages, err := pw.messageStore.DequeueBatch(ctx, pw.queueName, pw.partitionID, pw.batchSize)
	if err != nil {
		// Log error but continue (will retry on next tick)
		return
	}

	if len(messages) == 0 {
		// No messages available
		return
	}

	// Round-robin messages across consumer groups
	// This ensures fair distribution when multiple groups consume from same partition
	groupIndex := 0
	for _, msg := range messages {
		if len(groups) == 0 {
			continue
		}

		// Get next group in round-robin fashion
		group := groups[groupIndex%len(groups)]
		groupIndex++

		consumer, exists := group.GetConsumerForPartition(pw.partitionID)
		if !exists {
			continue
		}

		if err := pw.deliverMessage(ctx, msg, consumer, config); err != nil {
			// Log error but continue with next message
			// Delivery failures will be retried via timeout mechanism
			continue
		}
	}
}

// deliverMessage delivers a single message to a consumer.
func (pw *PartitionWorker) deliverMessage(
	ctx context.Context,
	msg *queueStorage.QueueMessage,
	consumer *queueStorage.Consumer,
	config queueStorage.QueueConfig,
) error {
	// Check ordering constraints
	if pw.queue.OrderingEnforcer() != nil {
		canDeliver, err := pw.queue.OrderingEnforcer().CanDeliver(msg)
		if err != nil {
			return fmt.Errorf("ordering check failed: %w", err)
		}
		if !canDeliver {
			return fmt.Errorf("message violates ordering constraints")
		}
	}

	// Mark as inflight
	deliveryState := &queueStorage.DeliveryState{
		MessageID:   msg.ID,
		QueueName:   pw.queueName,
		PartitionID: pw.partitionID,
		ConsumerID:  consumer.ID,
		DeliveredAt: time.Now(),
		Timeout:     time.Now().Add(config.DeliveryTimeout),
		RetryCount:  msg.RetryCount,
	}

	if err := pw.messageStore.MarkInflight(ctx, deliveryState); err != nil {
		return fmt.Errorf("failed to mark inflight: %w", err)
	}

	// Update message state
	msg.State = queueStorage.StateDelivered
	msg.DeliveredAt = time.Now()

	if err := pw.messageStore.UpdateMessage(ctx, pw.queueName, msg); err != nil {
		return fmt.Errorf("failed to update message: %w", err)
	}

	// Deliver to consumer via broker
	if pw.broker != nil {
		storageMsg := toStorageMessage(msg, pw.queueName)
		if err := pw.broker.DeliverToSession(ctx, consumer.ClientID, storageMsg); err != nil {
			// Delivery failed, but message is marked inflight
			// Retry logic will handle re-delivery
			return fmt.Errorf("failed to deliver to client %s: %w", consumer.ClientID, err)
		}
	}

	// Mark message as delivered in ordering enforcer
	if pw.queue.OrderingEnforcer() != nil {
		pw.queue.OrderingEnforcer().MarkDelivered(msg)
	}

	return nil
}

// toStorageMessage converts a queue message to a broker storage message for MQTT delivery.
func toStorageMessage(msg *queueStorage.QueueMessage, queueTopic string) interface{} {
	// Create a broker storage.Message with zero-copy buffer
	buf := core.GetBufferWithData(msg.Payload)

	storageMsg := &brokerStorage.Message{
		Topic:      queueTopic,
		QoS:        1, // Queue messages always use QoS 1 for delivery confirmation
		Retain:     false,
		Properties: msg.Properties,
	}
	storageMsg.SetPayloadFromBuffer(buf)

	return storageMsg
}
