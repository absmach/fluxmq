// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package queue

import (
	"context"
	"fmt"
	"time"

	"github.com/absmach/mqtt/queue/storage"
)

// DeliveryWorker handles message delivery for a queue.
type DeliveryWorker struct {
	queue        *Queue
	messageStore storage.MessageStore
	broker       BrokerInterface
	stopCh       chan struct{}
	tickInterval time.Duration
}

// NewDeliveryWorker creates a new delivery worker for a queue.
func NewDeliveryWorker(queue *Queue, messageStore storage.MessageStore, broker BrokerInterface) *DeliveryWorker {
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
func (dw *DeliveryWorker) deliverNext(ctx context.Context, partitionID int, consumer *storage.Consumer, config storage.QueueConfig) error {
	// Dequeue next message
	msg, err := dw.messageStore.Dequeue(ctx, dw.queue.Name(), partitionID)
	if err != nil {
		return fmt.Errorf("failed to dequeue: %w", err)
	}

	if msg == nil {
		// No messages available
		return nil
	}

	// Mark as inflight
	deliveryState := &storage.DeliveryState{
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
	msg.State = storage.StateDelivered
	msg.DeliveredAt = time.Now()

	if err := dw.messageStore.Update(ctx, dw.queue.Name(), msg); err != nil {
		return fmt.Errorf("failed to update message: %w", err)
	}

	// Deliver to consumer via broker
	if dw.broker != nil {
		mqttMsg := toMQTTMessage(msg, dw.queue.Name())
		if err := dw.broker.DeliverToSession(ctx, consumer.ClientID, mqttMsg); err != nil {
			// Delivery failed, but message is marked inflight
			// Retry logic will handle re-delivery
			return fmt.Errorf("failed to deliver to client %s: %w", consumer.ClientID, err)
		}
	}

	return nil
}

// MQTTMessage represents a message to be delivered via MQTT.
// This is a simplified structure - the actual broker will convert this to proper MQTT packets.
type MQTTMessage struct {
	Topic      string
	Payload    []byte
	QoS        byte
	Retain     bool
	Properties map[string]string
}

// toMQTTMessage converts a queue message to an MQTT message.
func toMQTTMessage(msg *storage.QueueMessage, queueTopic string) *MQTTMessage {
	return &MQTTMessage{
		Topic:      queueTopic,
		Payload:    msg.Payload,
		QoS:        1, // Queue messages always use QoS 1 for delivery confirmation
		Retain:     false,
		Properties: msg.Properties,
	}
}
