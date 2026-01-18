// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package lifecycle

import (
	"context"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/absmach/fluxmq/queue/storage"
	"github.com/absmach/fluxmq/queue/types"
)

// RaftProposer defines the interface for proposing operations via Raft.
// This allows the retry manager to replicate operations when replication is enabled.
type RaftProposer interface {
	// IsLeader returns true if this node is the leader for the partition.
	IsLeader(partitionID int) bool
	// ApplyUpdateMessage replicates a message update via Raft.
	ApplyUpdateMessage(ctx context.Context, partitionID int, msg *types.Message) error
	// ApplyMoveToDLQ replicates a move to DLQ operation via Raft.
	ApplyMoveToDLQ(ctx context.Context, partitionID int, msg *types.Message, dlqTopic string) error
}

// RetryManager monitors inflight messages and handles retry logic.
type RetryManager struct {
	queues        map[string]QueueInfo
	raftProposers map[string]RaftProposer // queueName -> RaftProposer (for replicated queues)
	messageStore  storage.MessageStore
	dlqManager    *DLQManager
	checkInterval time.Duration
	stopCh        chan struct{}
	wg            sync.WaitGroup
	mu            sync.RWMutex
}

// NewRetryManager creates a new retry manager.
func NewRetryManager(messageStore storage.MessageStore, dlqManager *DLQManager) *RetryManager {
	return &RetryManager{
		queues:        make(map[string]QueueInfo),
		raftProposers: make(map[string]RaftProposer),
		messageStore:  messageStore,
		dlqManager:    dlqManager,
		checkInterval: 1 * time.Second, // Check every second
		stopCh:        make(chan struct{}),
	}
}

// RegisterQueue adds a queue to be monitored for retry timeouts.
func (rm *RetryManager) RegisterQueue(queue QueueInfo) {
	rm.mu.Lock()
	defer rm.mu.Unlock()
	rm.queues[queue.Name()] = queue
}

// UnregisterQueue removes a queue from monitoring.
func (rm *RetryManager) UnregisterQueue(queueName string) {
	rm.mu.Lock()
	defer rm.mu.Unlock()
	delete(rm.queues, queueName)
	delete(rm.raftProposers, queueName)
}

// RegisterRaftProposer registers a Raft proposer for a replicated queue.
// When set, retry operations will be replicated via Raft.
func (rm *RetryManager) RegisterRaftProposer(queueName string, proposer RaftProposer) {
	rm.mu.Lock()
	defer rm.mu.Unlock()
	rm.raftProposers[queueName] = proposer
}

// Start starts the retry manager background task.
func (rm *RetryManager) Start(ctx context.Context) {
	rm.wg.Add(1)
	go func() {
		defer rm.wg.Done()
		rm.run(ctx)
	}()
}

// Stop stops the retry manager.
func (rm *RetryManager) Stop() {
	close(rm.stopCh)
	rm.wg.Wait()
}

// run is the main loop that checks for inflight timeouts.
func (rm *RetryManager) run(ctx context.Context) {
	ticker := time.NewTicker(rm.checkInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-rm.stopCh:
			return
		case <-ticker.C:
			rm.checkInflightTimeouts(ctx)
			rm.checkRetrySchedule(ctx)
		}
	}
}

// checkInflightTimeouts checks all inflight messages for delivery timeout.
func (rm *RetryManager) checkInflightTimeouts(ctx context.Context) {
	rm.mu.RLock()
	queues := make([]QueueInfo, 0, len(rm.queues))
	for _, queue := range rm.queues {
		queues = append(queues, queue)
	}
	rm.mu.RUnlock()

	for _, queue := range queues {
		inflight, err := rm.messageStore.GetInflight(ctx, queue.Name())
		if err != nil {
			continue
		}

		for _, deliveryState := range inflight {
			if time.Now().After(deliveryState.Timeout) {
				rm.handleInflightTimeout(ctx, queue, deliveryState)
			}
		}
	}
}

// checkRetrySchedule checks for messages scheduled for retry.
func (rm *RetryManager) checkRetrySchedule(ctx context.Context) {
	rm.mu.RLock()
	queues := make([]QueueInfo, 0, len(rm.queues))
	for _, queue := range rm.queues {
		queues = append(queues, queue)
	}
	rm.mu.RUnlock()

	for _, queue := range queues {
		config := queue.Config()

		// Check each partition for retry messages
		for i := 0; i < config.Partitions; i++ {
			messages, err := rm.messageStore.ListRetry(ctx, queue.Name(), i)
			if err != nil {
				continue
			}

			for _, msg := range messages {
				if time.Now().After(msg.NextRetryAt) || msg.NextRetryAt.IsZero() {
					rm.processRetry(ctx, queue, msg)
				}
			}
		}
	}
}

// handleInflightTimeout handles a message that has timed out while inflight.
func (rm *RetryManager) handleInflightTimeout(ctx context.Context, queue QueueInfo, deliveryState *types.DeliveryState) {
	// Get the actual message
	msg, err := rm.messageStore.GetMessage(ctx, queue.Name(), deliveryState.MessageID)
	if err != nil {
		return
	}

	// Remove this group's inflight tracking
	if err := rm.messageStore.RemoveInflight(ctx, queue.Name(), deliveryState.MessageID, deliveryState.GroupID); err != nil {
		return
	}

	// Check if we should retry or move to DLQ
	rm.processRetry(ctx, queue, msg)
}

// processRetry determines if a message should be retried or moved to DLQ.
func (rm *RetryManager) processRetry(ctx context.Context, queue QueueInfo, msg *types.Message) {
	config := queue.Config()

	// For replicated queues, only the leader should process retries
	rm.mu.RLock()
	proposer := rm.raftProposers[queue.Name()]
	rm.mu.RUnlock()

	if proposer != nil {
		// Check if this node is the leader for this message's partition
		if !proposer.IsLeader(msg.PartitionID) {
			// Not the leader, skip - the leader will handle this
			return
		}
	}

	// Check if max retries exceeded
	if msg.RetryCount >= config.RetryPolicy.MaxRetries {
		rm.moveToDLQ(ctx, queue, msg, "max retries exceeded")
		return
	}

	// Check if total time exceeded
	totalElapsed := time.Since(msg.CreatedAt)
	if totalElapsed > config.RetryPolicy.TotalTimeout {
		rm.moveToDLQ(ctx, queue, msg, "total timeout exceeded")
		return
	}

	// Schedule retry
	msg.RetryCount++
	msg.State = types.StateRetry
	msg.NextRetryAt = time.Now().Add(rm.calculateBackoff(msg.RetryCount, config.RetryPolicy))

	// Use Raft if available, otherwise direct storage
	if proposer != nil {
		if err := proposer.ApplyUpdateMessage(ctx, msg.PartitionID, msg); err != nil {
			// Failed to replicate, log but continue
			return
		}
	} else {
		if err := rm.messageStore.UpdateMessage(ctx, queue.Name(), msg); err != nil {
			// Failed to update, log but continue
			return
		}
	}
}

// moveToDLQ moves a message to the dead letter queue.
func (rm *RetryManager) moveToDLQ(ctx context.Context, queue QueueInfo, msg *types.Message, reason string) {
	config := queue.Config()

	// For replicated queues, only the leader should process DLQ moves
	rm.mu.RLock()
	proposer := rm.raftProposers[queue.Name()]
	rm.mu.RUnlock()

	if proposer != nil {
		// Check if this node is the leader for this message's partition
		if !proposer.IsLeader(msg.PartitionID) {
			// Not the leader, skip - the leader will handle this
			return
		}
	}

	// Prepare message for DLQ
	msg.State = types.StateDLQ
	msg.FailureReason = reason
	msg.MovedToDLQAt = time.Now()

	// Determine DLQ topic
	dlqTopic := config.DLQConfig.Topic
	if dlqTopic == "" {
		dlqTopic = "$queue/dlq/" + queue.Name()[7:] // Remove "$queue/" prefix
	}

	// Use Raft if available, otherwise use DLQ manager or direct storage
	if proposer != nil {
		if err := proposer.ApplyMoveToDLQ(ctx, msg.PartitionID, msg, dlqTopic); err != nil {
			// Failed to replicate, log but continue
			return
		}
	} else if rm.dlqManager != nil {
		if err := rm.dlqManager.MoveToDLQ(ctx, queue, msg, reason); err != nil {
			// Failed to move to DLQ, log but continue
			return
		}
	} else {
		// No DLQ manager and no Raft, just delete the message
		rm.messageStore.DeleteMessage(ctx, queue.Name(), msg.ID)
	}
}

// calculateBackoff calculates exponential backoff duration for a retry.
func (rm *RetryManager) calculateBackoff(retryCount int, policy types.RetryPolicy) time.Duration {
	// Calculate exponential backoff: initialBackoff * (multiplier ^ (retryCount - 1))
	backoff := float64(policy.InitialBackoff) * math.Pow(policy.BackoffMultiplier, float64(retryCount-1))

	// Cap at max backoff
	if backoff > float64(policy.MaxBackoff) {
		backoff = float64(policy.MaxBackoff)
	}

	return time.Duration(backoff)
}

// GetStats returns retry statistics for a queue.
func (rm *RetryManager) GetStats(ctx context.Context, queueName string) (*RetryStats, error) {
	rm.mu.RLock()
	queue, exists := rm.queues[queueName]
	rm.mu.RUnlock()

	if !exists {
		return nil, fmt.Errorf("queue not found: %s", queueName)
	}

	// Count retry messages
	config := queue.Config()
	totalRetrying := int64(0)
	for i := 0; i < config.Partitions; i++ {
		messages, err := rm.messageStore.ListRetry(ctx, queueName, i)
		if err != nil {
			continue
		}
		totalRetrying += int64(len(messages))
	}

	// Count inflight messages
	inflight, err := rm.messageStore.GetInflight(ctx, queueName)
	if err != nil {
		return nil, err
	}

	return &RetryStats{
		QueueName:     queueName,
		InflightCount: int64(len(inflight)),
		RetryingCount: totalRetrying,
		CheckInterval: rm.checkInterval,
	}, nil
}

// RetryStats holds retry-related statistics.
type RetryStats struct {
	QueueName     string
	InflightCount int64
	RetryingCount int64
	CheckInterval time.Duration
}
