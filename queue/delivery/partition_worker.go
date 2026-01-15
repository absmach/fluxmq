// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package delivery

import (
	"context"
	"fmt"
	"time"

	"github.com/absmach/mqtt/cluster"
	"github.com/absmach/mqtt/core"
	queueStorage "github.com/absmach/mqtt/queue/storage"
	brokerStorage "github.com/absmach/mqtt/storage"
)

// PartitionWorker handles message delivery for a single partition.
type PartitionWorker struct {
	queueName    string
	partitionID  int
	queue        QueueSource
	messageStore queueStorage.MessageStore
	deliverFn    DeliverFn

	notifyCh chan struct{} // Event notification channel
	stopCh   chan struct{} // Stop signal

	batchSize       int           // Max messages to process per wake
	debounceTimeout time.Duration // Small delay to batch rapid enqueues

	// Cluster support (nil for single-node mode)
	cluster             cluster.Cluster
	localNodeID         string
	consumerRoutingMode ConsumerRoutingMode

	// Raft support (nil for non-replicated queues)
	raftManager RaftManager

	// Retention support (nil if retention not configured)
	retentionManager RetentionManager
}

// NewPartitionWorker creates a new partition worker.
func NewPartitionWorker(
	queueName string,
	partitionID int,
	queue QueueSource,
	messageStore queueStorage.MessageStore,
	deliverFn DeliverFn,
	batchSize int,
	c cluster.Cluster,
	localNodeID string,
	routingMode ConsumerRoutingMode,
	raftMgr RaftManager,
	retentionMgr RetentionManager,
) *PartitionWorker {
	// Default batch size
	if batchSize <= 0 {
		batchSize = 100
	}

	return &PartitionWorker{
		queueName:           queueName,
		partitionID:         partitionID,
		queue:               queue,
		messageStore:        messageStore,
		deliverFn:           deliverFn,
		notifyCh:            make(chan struct{}, 1), // Buffered to prevent blocking
		stopCh:              make(chan struct{}),
		batchSize:           batchSize,
		debounceTimeout:     5 * time.Millisecond,
		cluster:             c,
		localNodeID:         localNodeID,
		consumerRoutingMode: routingMode,
		raftManager:         raftMgr,
		retentionManager:    retentionMgr,
	}
}

// Start starts the partition worker main loop.
func (pw *PartitionWorker) Start(ctx context.Context) {
	// Start retention background jobs (only if leader in replicated mode)
	if pw.retentionManager != nil {
		shouldRunRetention := true

		// In replicated mode, only leader runs retention
		if pw.raftManager != nil {
			shouldRunRetention = pw.raftManager.IsLeader(pw.partitionID)
		}

		if shouldRunRetention {
			pw.retentionManager.Start(ctx, pw.partitionID)
			defer pw.retentionManager.Stop()
		}
	}

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
	// For replicated queues, only the Raft leader can deliver messages
	if pw.raftManager != nil {
		if !pw.raftManager.IsLeader(pw.partitionID) {
			// Not the Raft leader for this partition, skip processing
			return
		}
	} else {
		// Non-replicated mode: check partition ownership (distributed mode)
		if pw.cluster != nil {
			owner, exists, err := pw.cluster.GetPartitionOwner(ctx, pw.queueName, pw.partitionID)
			if err != nil {
				// Error checking ownership, skip processing
				return
			}

			// Skip processing if this node doesn't own the partition
			if exists && owner != pw.localNodeID {
				return
			}
		}
	}

	// Get consumer groups
	groups := pw.queue.ConsumerGroups().ListGroups()
	if len(groups) == 0 {
		return
	}

	config := pw.queue.Config()

	// Batch dequeue messages
	messages, err := pw.messageStore.DequeueBatch(ctx, pw.queueName, pw.partitionID, pw.batchSize)
	if err != nil {
		return
	}

	if len(messages) == 0 {
		return
	}

	// Broadcast each message to ALL consumer groups
	for _, msg := range messages {
		delivered := false

		for _, group := range groups {
			consumer, exists := group.GetConsumerForPartition(pw.partitionID)
			if !exists {
				continue
			}

			if err := pw.deliverMessage(ctx, msg, consumer, config); err != nil {
				continue
			}
			delivered = true
		}

		if !delivered {
			// No group could receive - message stays queued for retry
		}
	}
}

// deliverMessage delivers a single message to a consumer.
func (pw *PartitionWorker) deliverMessage(
	ctx context.Context,
	msg *queueStorage.Message,
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

	// Check if consumer is on a remote node (proxy mode)
	if pw.consumerRoutingMode == ProxyMode && consumer.ProxyNodeID != "" && consumer.ProxyNodeID != pw.localNodeID {
		// Route message to remote node
		if pw.cluster == nil {
			return fmt.Errorf("consumer on remote node %s but cluster not configured", consumer.ProxyNodeID)
		}

		err := pw.cluster.RouteQueueMessage(
			ctx,
			consumer.ProxyNodeID,
			consumer.ClientID,
			pw.queueName,
			msg.ID,
			msg.Payload,
			msg.Properties,
			int64(msg.Sequence),
			pw.partitionID,
		)
		if err != nil {
			return fmt.Errorf("failed to route message to remote node %s: %w", consumer.ProxyNodeID, err)
		}
	} else {
		// Deliver to local consumer via broker
		if pw.deliverFn != nil {
			storageMsg := ToStorageMessage(msg, pw.queueName)
			if err := pw.deliverFn(ctx, consumer.ClientID, storageMsg); err != nil {
				return fmt.Errorf("failed to deliver to client %s: %w", consumer.ClientID, err)
			}
		}
	}

	// Mark message as delivered in ordering enforcer
	if pw.queue.OrderingEnforcer() != nil {
		pw.queue.OrderingEnforcer().MarkDelivered(msg)
	}

	return nil
}

// ToStorageMessage converts a queue message to a broker storage message for MQTT delivery.
func ToStorageMessage(msg *queueStorage.Message, queueTopic string) interface{} {
	storageMsg := &brokerStorage.Message{
		Topic:      queueTopic,
		QoS:        1, // Queue messages always use QoS 1 for delivery confirmation
		Retain:     false,
		Properties: msg.Properties,
	}

	// Use existing PayloadBuf if available (zero-copy), otherwise create from Payload
	if msg.PayloadBuf != nil {
		// Retain the buffer for the broker message
		msg.PayloadBuf.Retain()
		storageMsg.SetPayloadFromBuffer(msg.PayloadBuf)
	} else if len(msg.Payload) > 0 {
		// Fallback for legacy Payload field
		buf := core.GetBufferWithData(msg.Payload)
		storageMsg.SetPayloadFromBuffer(buf)
	}

	return storageMsg
}
