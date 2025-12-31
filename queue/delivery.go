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

// DeliveryWorker handles message delivery for a queue.
type DeliveryWorker struct {
	queue        *Queue
	messageStore queueStorage.MessageStore
	broker       BrokerInterface
	stopCh       chan struct{}
	tickInterval time.Duration
}

// NewDeliveryWorker creates a new delivery worker for a queue.
func NewDeliveryWorker(queue *Queue, messageStore queueStorage.MessageStore, broker BrokerInterface) *DeliveryWorker {
	return &DeliveryWorker{
		queue:        queue,
		messageStore: messageStore,
		broker:       broker,
		stopCh:       make(chan struct{}),
		tickInterval: 100 * time.Millisecond, // Check for messages every 100ms
	}
}

// Start starts the delivery worker.
func (dw *DeliveryWorker) Start(ctx context.Context) {
	ticker := time.NewTicker(dw.tickInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-dw.stopCh:
			return
		case <-ticker.C:
			dw.deliverMessages(ctx)
		}
	}
}

// Stop stops the delivery worker.
func (dw *DeliveryWorker) Stop() {
	close(dw.stopCh)
}

// deliverMessages attempts to deliver pending messages to consumers.
func (dw *DeliveryWorker) deliverMessages(ctx context.Context) {
	config := dw.queue.Config()

	// Process each partition
	for _, partition := range dw.queue.Partitions() {
		// Skip if partition is not assigned
		if !partition.IsAssigned() {
			continue
		}

		// Get all consumer groups
		groups := dw.queue.ConsumerGroups().ListGroups()
		for _, group := range groups {
			// Get consumer assigned to this partition in this group
			consumer, exists := group.GetConsumerForPartition(partition.ID())
			if !exists {
				continue
			}

			// Try to deliver next message
			if err := dw.deliverNext(ctx, partition.ID(), consumer, config); err != nil {
				// Log error but continue (delivery will be retried on next tick)
				continue
			}
		}
	}
}

// deliverNext delivers the next available message from a partition to a consumer.
func (dw *DeliveryWorker) deliverNext(ctx context.Context, partitionID int, consumer *queueStorage.Consumer, config queueStorage.QueueConfig) error {
	// Dequeue next message
	msg, err := dw.messageStore.Dequeue(ctx, dw.queue.Name(), partitionID)
	if err != nil {
		return fmt.Errorf("failed to dequeue: %w", err)
	}

	if msg == nil {
		// No messages available
		return nil
	}

	// Check ordering constraints
	if dw.queue.OrderingEnforcer() != nil {
		canDeliver, err := dw.queue.OrderingEnforcer().CanDeliver(msg)
		if err != nil {
			return fmt.Errorf("ordering check failed: %w", err)
		}
		if !canDeliver {
			// Message cannot be delivered due to ordering constraints
			// This shouldn't happen in practice since Dequeue returns messages in order
			return fmt.Errorf("message violates ordering constraints")
		}
	}

	// Mark as inflight
	deliveryState := &queueStorage.DeliveryState{
		MessageID:   msg.ID,
		QueueName:   dw.queue.Name(),
		PartitionID: partitionID,
		ConsumerID:  consumer.ID,
		DeliveredAt: time.Now(),
		Timeout:     time.Now().Add(config.DeliveryTimeout),
		RetryCount:  msg.RetryCount,
	}

	if err := dw.messageStore.MarkInflight(ctx, deliveryState); err != nil {
		return fmt.Errorf("failed to mark inflight: %w", err)
	}

	// Update message state
	msg.State = queueStorage.StateDelivered
	msg.DeliveredAt = time.Now()

	if err := dw.messageStore.UpdateMessage(ctx, dw.queue.Name(), msg); err != nil {
		return fmt.Errorf("failed to update message: %w", err)
	}

	// Deliver to consumer via broker
	if dw.broker != nil {
		storageMsg := toStorageMessage(msg, dw.queue.Name())
		if err := dw.broker.DeliverToSession(ctx, consumer.ClientID, storageMsg); err != nil {
			// Delivery failed, but message is marked inflight
			// Retry logic will handle re-delivery
			return fmt.Errorf("failed to deliver to client %s: %w", consumer.ClientID, err)
		}
	}

	// Mark message as delivered in ordering enforcer
	if dw.queue.OrderingEnforcer() != nil {
		dw.queue.OrderingEnforcer().MarkDelivered(msg)
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
