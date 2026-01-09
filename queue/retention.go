// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package queue

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/absmach/mqtt/queue/storage"
)

// RetentionManager handles automatic message cleanup based on retention policies.
// It implements both time-based (background) and size-based (active) retention strategies.
type RetentionManager struct {
	queueName string
	policy    storage.RetentionPolicy
	store     storage.MessageStore

	// Raft integration (optional)
	raftManager *RaftManager

	// Size-based retention tracking
	enqueueCounter int
	mu             sync.Mutex

	// Background job control
	stopCh chan struct{}
	wg     sync.WaitGroup

	logger *slog.Logger
}

// RetentionStats tracks retention operation metrics.
type RetentionStats struct {
	MessagesDeleted int64
	BytesFreed      int64
	LastRunTime     time.Time
	LastRunDuration time.Duration
}

// NewRetentionManager creates a new retention manager for a queue.
func NewRetentionManager(queueName string, policy storage.RetentionPolicy, store storage.MessageStore, raftManager *RaftManager, logger *slog.Logger) *RetentionManager {
	if logger == nil {
		logger = slog.Default()
	}

	// Set defaults for optional fields
	if policy.TimeCheckInterval == 0 {
		policy.TimeCheckInterval = 5 * time.Minute
	}
	if policy.SizeCheckEvery == 0 {
		policy.SizeCheckEvery = 100
	}
	if policy.CompactionInterval == 0 {
		policy.CompactionInterval = 10 * time.Minute
	}
	if policy.CompactionLag == 0 {
		policy.CompactionLag = 5 * time.Minute
	}

	return &RetentionManager{
		queueName:   queueName,
		policy:      policy,
		store:       store,
		raftManager: raftManager,
		stopCh:      make(chan struct{}),
		logger:      logger,
	}
}

// Start begins background retention jobs.
// Only the partition leader should call this to avoid duplicate cleanup.
func (rm *RetentionManager) Start(ctx context.Context, partitionID int) {
	// Time-based retention background job
	if rm.policy.RetentionTime > 0 {
		rm.wg.Add(1)
		go rm.timeBasedCleanupLoop(ctx, partitionID)
	}

	// Compaction background job (if enabled)
	if rm.policy.CompactionEnabled {
		rm.wg.Add(1)
		go rm.compactionLoop(ctx, partitionID)
	}

	rm.logger.Info("retention manager started",
		slog.String("queue", rm.queueName),
		slog.Int("partition", partitionID),
		slog.Duration("time_retention", rm.policy.RetentionTime),
		slog.Int64("size_bytes", rm.policy.RetentionBytes),
		slog.Int64("size_messages", rm.policy.RetentionMessages))
}

// Stop halts all background retention jobs.
func (rm *RetentionManager) Stop() {
	close(rm.stopCh)
	rm.wg.Wait()

	rm.logger.Info("retention manager stopped",
		slog.String("queue", rm.queueName))
}

// CheckSizeRetention checks size-based retention limits and triggers cleanup if needed.
// This is called on the enqueue path (active retention).
// Returns the number of messages deleted, bytes freed, and any error.
func (rm *RetentionManager) CheckSizeRetention(ctx context.Context, partitionID int) (int64, int64, error) {
	// Only run retention on the leader to avoid duplicate cleanup
	if rm.raftManager != nil && !rm.raftManager.IsLeader(partitionID) {
		return 0, 0, nil
	}

	// Optimization: only check every N enqueues to minimize overhead
	rm.mu.Lock()
	rm.enqueueCounter++
	shouldCheck := rm.enqueueCounter >= rm.policy.SizeCheckEvery
	if shouldCheck {
		rm.enqueueCounter = 0
	}
	rm.mu.Unlock()

	if !shouldCheck {
		return 0, 0, nil
	}

	// Check if size-based retention is enabled
	if rm.policy.RetentionBytes == 0 && rm.policy.RetentionMessages == 0 {
		return 0, 0, nil
	}

	// Get current queue size
	count, err := rm.store.Count(ctx, rm.queueName)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to get message count: %w", err)
	}

	var totalDeleted int64
	var totalBytesFreed int64

	// For byte-based retention, we need to loop until under the limit
	// because we can't accurately estimate message sizes
	if rm.policy.RetentionBytes > 0 {
		currentSize, err := rm.store.GetQueueSize(ctx, rm.queueName)
		if err != nil {
			rm.logger.Warn("failed to get queue size for retention check",
				slog.String("queue", rm.queueName),
				slog.String("error", err.Error()))
		} else if currentSize > rm.policy.RetentionBytes {
			// Keep deleting oldest messages until under byte limit
			const batchSize = 100 // Delete in batches to avoid edge cases
			for currentSize > rm.policy.RetentionBytes {
				// Calculate how many messages to delete in this batch
				// Use estimation as a starting point but limit to batchSize
				var deleteCount int
				if count > 0 {
					avgSize := currentSize / count
					if avgSize > 0 {
						bytesToFree := currentSize - rm.policy.RetentionBytes
						estimatedCount := (bytesToFree / avgSize) + 1 // Round up
						deleteCount = int(estimatedCount)
						if deleteCount > batchSize {
							deleteCount = batchSize
						}
					} else {
						deleteCount = 10 // Fallback if avgSize is 0
					}
				} else {
					break // No messages left
				}

				deleted, bytesFreed, err := rm.deleteOldestMessages(ctx, partitionID, deleteCount)
				if err != nil {
					return totalDeleted, totalBytesFreed, fmt.Errorf("failed to delete oldest messages: %w", err)
				}

				if deleted == 0 {
					break // No more messages to delete
				}

				totalDeleted += deleted
				totalBytesFreed += bytesFreed

				// Get updated size and count
				currentSize, err = rm.store.GetQueueSize(ctx, rm.queueName)
				if err != nil {
					return totalDeleted, totalBytesFreed, fmt.Errorf("failed to get queue size: %w", err)
				}

				count, err = rm.store.Count(ctx, rm.queueName)
				if err != nil {
					return totalDeleted, totalBytesFreed, fmt.Errorf("failed to get message count: %w", err)
				}
			}
		}
	}

	// Check message count-based retention
	if rm.policy.RetentionMessages > 0 && count > rm.policy.RetentionMessages {
		targetDeleteCount := count - rm.policy.RetentionMessages
		deleted, bytesFreed, err := rm.deleteOldestMessages(ctx, partitionID, int(targetDeleteCount))
		if err != nil {
			return totalDeleted, totalBytesFreed, fmt.Errorf("failed to delete oldest messages: %w", err)
		}
		totalDeleted += deleted
		totalBytesFreed += bytesFreed
	}

	if totalDeleted == 0 {
		return 0, 0, nil
	}

	deletedCount := totalDeleted
	bytesFreed := totalBytesFreed

	if deletedCount > 0 {
		rm.logger.Info("size-based retention cleanup",
			slog.String("queue", rm.queueName),
			slog.Int("partition", partitionID),
			slog.Int64("deleted", deletedCount),
			slog.Int64("bytes_freed", bytesFreed))
	}

	return deletedCount, bytesFreed, nil
}

// timeBasedCleanupLoop runs periodic time-based retention cleanup.
func (rm *RetentionManager) timeBasedCleanupLoop(ctx context.Context, partitionID int) {
	defer rm.wg.Done()

	ticker := time.NewTicker(rm.policy.TimeCheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-rm.stopCh:
			return
		case <-ticker.C:
			stats, err := rm.runTimeBasedCleanup(ctx, partitionID)
			if err != nil {
				rm.logger.Error("time-based cleanup failed",
					slog.String("queue", rm.queueName),
					slog.Int("partition", partitionID),
					slog.String("error", err.Error()))
				continue
			}

			if stats.MessagesDeleted > 0 {
				rm.logger.Info("time-based retention cleanup",
					slog.String("queue", rm.queueName),
					slog.Int("partition", partitionID),
					slog.Int64("deleted", stats.MessagesDeleted),
					slog.Int64("bytes_freed", stats.BytesFreed),
					slog.Duration("duration", stats.LastRunDuration))
			}
		}
	}
}

// runTimeBasedCleanup deletes messages older than RetentionTime.
func (rm *RetentionManager) runTimeBasedCleanup(ctx context.Context, partitionID int) (*RetentionStats, error) {
	startTime := time.Now()
	stats := &RetentionStats{
		LastRunTime: startTime,
	}

	// Calculate cutoff time
	cutoffTime := time.Now().Add(-rm.policy.RetentionTime)

	// Get messages older than cutoff
	// Note: This requires MessageStore to support querying by time
	// For now, we'll implement a simple approach that scans messages
	deletedCount, bytesFreed, err := rm.deleteMessagesBefore(ctx, partitionID, cutoffTime)
	if err != nil {
		return nil, err
	}

	stats.MessagesDeleted = deletedCount
	stats.BytesFreed = bytesFreed
	stats.LastRunDuration = time.Since(startTime)

	return stats, nil
}

// compactionLoop runs periodic log compaction (Kafka-style).
func (rm *RetentionManager) compactionLoop(ctx context.Context, partitionID int) {
	defer rm.wg.Done()

	ticker := time.NewTicker(rm.policy.CompactionInterval)
	defer ticker.Stop()

	for {
		select {
		case <-rm.stopCh:
			return
		case <-ticker.C:
			// TODO: Implement log compaction in Week 2
			// This will scan messages, group by compaction key,
			// and delete all but the latest message per key
		}
	}
}

// deleteOldestMessages deletes the N oldest messages from a partition.
func (rm *RetentionManager) deleteOldestMessages(ctx context.Context, partitionID int, count int) (int64, int64, error) {
	if count <= 0 {
		return 0, 0, nil
	}

	// Get oldest messages
	messages, err := rm.store.ListOldestMessages(ctx, rm.queueName, partitionID, count)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to list oldest messages: %w", err)
	}

	if len(messages) == 0 {
		return 0, 0, nil
	}

	// Collect message IDs and calculate bytes
	messageIDs := make([]string, len(messages))
	var totalBytes int64
	for i, msg := range messages {
		messageIDs[i] = msg.ID
		totalBytes += int64(len(msg.GetPayload()))
	}

	// Delete via Raft if available, otherwise delete directly
	var deletedCount int64
	if rm.raftManager != nil {
		// Replicate deletion via Raft
		if err := rm.raftManager.ApplyRetentionDelete(ctx, partitionID, messageIDs); err != nil {
			return 0, 0, fmt.Errorf("failed to replicate retention delete: %w", err)
		}
		deletedCount = int64(len(messageIDs))
	} else {
		// Direct deletion (non-replicated mode)
		deletedCount, err = rm.store.DeleteMessageBatch(ctx, rm.queueName, messageIDs)
		if err != nil {
			return 0, 0, fmt.Errorf("failed to delete message batch: %w", err)
		}
	}

	return deletedCount, totalBytes, nil
}

// deleteMessagesBefore deletes all messages created before the given cutoff time.
func (rm *RetentionManager) deleteMessagesBefore(ctx context.Context, partitionID int, cutoffTime time.Time) (int64, int64, error) {
	const batchSize = 1000 // Process in batches to avoid loading too many messages

	var totalDeleted int64
	var totalBytesFreed int64

	for {
		// Get batch of messages before cutoff
		messages, err := rm.store.ListMessagesBefore(ctx, rm.queueName, partitionID, cutoffTime, batchSize)
		if err != nil {
			return totalDeleted, totalBytesFreed, fmt.Errorf("failed to list messages before cutoff: %w", err)
		}

		if len(messages) == 0 {
			break // No more messages to delete
		}

		// Collect message IDs and calculate bytes
		messageIDs := make([]string, len(messages))
		var batchBytes int64
		for i, msg := range messages {
			messageIDs[i] = msg.ID
			batchBytes += int64(len(msg.GetPayload()))
		}

		// Delete via Raft if available, otherwise delete directly
		var deletedCount int64
		if rm.raftManager != nil {
			// Replicate deletion via Raft
			if err := rm.raftManager.ApplyRetentionDelete(ctx, partitionID, messageIDs); err != nil {
				return totalDeleted, totalBytesFreed, fmt.Errorf("failed to replicate retention delete: %w", err)
			}
			deletedCount = int64(len(messageIDs))
		} else {
			// Direct deletion (non-replicated mode)
			deletedCount, err = rm.store.DeleteMessageBatch(ctx, rm.queueName, messageIDs)
			if err != nil {
				return totalDeleted, totalBytesFreed, fmt.Errorf("failed to delete message batch: %w", err)
			}
		}

		totalDeleted += deletedCount
		totalBytesFreed += batchBytes

		// If we got less than a full batch, we're done
		if len(messages) < batchSize {
			break
		}
	}

	return totalDeleted, totalBytesFreed, nil
}

// DeleteBatch efficiently deletes a batch of messages by ID.
// This will be used by both time-based and size-based retention.
func (rm *RetentionManager) DeleteBatch(ctx context.Context, messageIDs []string) (int64, error) {
	if len(messageIDs) == 0 {
		return 0, nil
	}

	deletedCount, err := rm.store.DeleteMessageBatch(ctx, rm.queueName, messageIDs)
	if err != nil {
		return 0, fmt.Errorf("failed to delete message batch: %w", err)
	}

	rm.logger.Debug("batch delete",
		slog.String("queue", rm.queueName),
		slog.Int("count", len(messageIDs)),
		slog.Int64("deleted", deletedCount))

	return deletedCount, nil
}
