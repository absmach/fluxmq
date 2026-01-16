// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package raft

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"time"

	"github.com/absmach/fluxmq/queue/storage"
	"github.com/hashicorp/raft"
)

// OpType represents the type of operation in the Raft log.
type OpType uint8

const (
	OpEnqueue OpType = iota
	OpAck
	OpNack
	OpReject
	OpUpdateMessage
	OpRetentionDelete // Batch delete messages for retention
)

// Operation represents a queue operation to be replicated via Raft.
type Operation struct {
	Type      OpType
	Timestamp time.Time

	// For OpEnqueue
	Message *storage.Message

	// For OpAck, OpNack, OpReject
	MessageID string
	Reason    string

	// For OpRetentionDelete
	MessageIDs []string // Batch of message IDs to delete
}

// PartitionFSM implements the Raft FSM interface for a queue partition.
// It applies committed operations to the underlying message store.
type PartitionFSM struct {
	queueName    string
	partitionID  int
	messageStore storage.MessageStore
	logger       *slog.Logger

	// ISR (In-Sync Replicas) tracking for async mode
	isr   map[string]time.Time // replicaID -> lastHeartbeat
	isrMu sync.RWMutex
}

// NewPartitionFSM creates a new FSM for a queue partition.
func NewPartitionFSM(queueName string, partitionID int, messageStore storage.MessageStore, logger *slog.Logger) *PartitionFSM {
	return &PartitionFSM{
		queueName:    queueName,
		partitionID:  partitionID,
		messageStore: messageStore,
		logger:       logger,
		isr:          make(map[string]time.Time),
	}
}

// Apply applies a Raft log entry to the FSM.
// This is called by Raft when a log entry is committed.
func (f *PartitionFSM) Apply(l *raft.Log) interface{} {
	var op Operation
	if err := json.Unmarshal(l.Data, &op); err != nil {
		f.logger.Error("failed to unmarshal operation",
			slog.String("queue", f.queueName),
			slog.Int("partition", f.partitionID),
			slog.String("error", err.Error()))
		return err
	}

	ctx := context.Background()

	switch op.Type {
	case OpEnqueue:
		return f.applyEnqueue(ctx, op.Message)
	case OpAck:
		return f.applyAck(ctx, op.MessageID)
	case OpNack:
		return f.applyNack(ctx, op.MessageID, op.Reason)
	case OpReject:
		return f.applyReject(ctx, op.MessageID, op.Reason)
	case OpUpdateMessage:
		return f.applyUpdateMessage(ctx, op.Message)
	case OpRetentionDelete:
		return f.applyRetentionDelete(ctx, op.MessageIDs)
	default:
		err := fmt.Errorf("unknown operation type: %d", op.Type)
		f.logger.Error("unknown operation",
			slog.String("queue", f.queueName),
			slog.Int("partition", f.partitionID),
			slog.Int("op_type", int(op.Type)))
		return err
	}
}

// applyEnqueue applies an enqueue operation.
func (f *PartitionFSM) applyEnqueue(ctx context.Context, msg *storage.Message) error {
	if msg == nil {
		return fmt.Errorf("nil message in enqueue operation")
	}

	f.logger.Info("FSM applying enqueue",
		slog.String("queue", f.queueName),
		slog.Int("partition", f.partitionID),
		slog.String("message_id", msg.ID),
		slog.Uint64("sequence", msg.Sequence))

	if err := f.messageStore.Enqueue(ctx, f.queueName, msg); err != nil {
		f.logger.Error("failed to apply enqueue",
			slog.String("queue", f.queueName),
			slog.Int("partition", f.partitionID),
			slog.String("message_id", msg.ID),
			slog.String("error", err.Error()))
		return err
	}

	f.logger.Info("FSM applied enqueue successfully",
		slog.String("queue", f.queueName),
		slog.Int("partition", f.partitionID),
		slog.String("message_id", msg.ID),
		slog.Uint64("sequence", msg.Sequence))

	return nil
}

// applyAck applies an ACK operation.
func (f *PartitionFSM) applyAck(ctx context.Context, messageID string) error {
	// Remove from inflight
	if err := f.messageStore.RemoveInflight(ctx, f.queueName, messageID); err != nil {
		if err != storage.ErrMessageNotFound {
			f.logger.Error("failed to remove inflight",
				slog.String("queue", f.queueName),
				slog.Int("partition", f.partitionID),
				slog.String("message_id", messageID),
				slog.String("error", err.Error()))
			return err
		}
	}

	// Delete message
	if err := f.messageStore.DeleteMessage(ctx, f.queueName, messageID); err != nil {
		if err != storage.ErrMessageNotFound {
			f.logger.Error("failed to delete message",
				slog.String("queue", f.queueName),
				slog.Int("partition", f.partitionID),
				slog.String("message_id", messageID),
				slog.String("error", err.Error()))
			return err
		}
	}

	f.logger.Debug("applied ack",
		slog.String("queue", f.queueName),
		slog.Int("partition", f.partitionID),
		slog.String("message_id", messageID))

	return nil
}

// applyNack applies a NACK operation (message delivery failed, should retry).
func (f *PartitionFSM) applyNack(ctx context.Context, messageID string, reason string) error {
	msg, err := f.messageStore.GetMessage(ctx, f.queueName, messageID)
	if err != nil {
		f.logger.Error("failed to get message for nack",
			slog.String("queue", f.queueName),
			slog.Int("partition", f.partitionID),
			slog.String("message_id", messageID),
			slog.String("error", err.Error()))
		return err
	}

	// Update state to retry
	msg.State = storage.StateRetry
	msg.RetryCount++
	msg.FailureReason = reason
	msg.LastAttempt = time.Now()

	if err := f.messageStore.UpdateMessage(ctx, f.queueName, msg); err != nil {
		f.logger.Error("failed to update message for nack",
			slog.String("queue", f.queueName),
			slog.Int("partition", f.partitionID),
			slog.String("message_id", messageID),
			slog.String("error", err.Error()))
		return err
	}

	// Remove from inflight
	if err := f.messageStore.RemoveInflight(ctx, f.queueName, messageID); err != nil {
		if err != storage.ErrMessageNotFound {
			f.logger.Error("failed to remove inflight for nack",
				slog.String("queue", f.queueName),
				slog.Int("partition", f.partitionID),
				slog.String("message_id", messageID),
				slog.String("error", err.Error()))
			return err
		}
	}

	f.logger.Debug("applied nack",
		slog.String("queue", f.queueName),
		slog.Int("partition", f.partitionID),
		slog.String("message_id", messageID),
		slog.String("reason", reason))

	return nil
}

// applyReject applies a REJECT operation (message should go to DLQ).
func (f *PartitionFSM) applyReject(ctx context.Context, messageID string, reason string) error {
	msg, err := f.messageStore.GetMessage(ctx, f.queueName, messageID)
	if err != nil {
		f.logger.Error("failed to get message for reject",
			slog.String("queue", f.queueName),
			slog.Int("partition", f.partitionID),
			slog.String("message_id", messageID),
			slog.String("error", err.Error()))
		return err
	}

	// Move to DLQ
	msg.State = storage.StateDLQ
	msg.FailureReason = reason
	msg.MovedToDLQAt = time.Now()

	if err := f.messageStore.EnqueueDLQ(ctx, f.queueName, msg); err != nil {
		f.logger.Error("failed to enqueue to DLQ",
			slog.String("queue", f.queueName),
			slog.Int("partition", f.partitionID),
			slog.String("message_id", messageID),
			slog.String("error", err.Error()))
		return err
	}

	// Remove from inflight and delete original message
	f.messageStore.RemoveInflight(ctx, f.queueName, messageID)
	f.messageStore.DeleteMessage(ctx, f.queueName, messageID)

	f.logger.Debug("applied reject",
		slog.String("queue", f.queueName),
		slog.Int("partition", f.partitionID),
		slog.String("message_id", messageID),
		slog.String("reason", reason))

	return nil
}

// applyUpdateMessage applies a message update operation.
func (f *PartitionFSM) applyUpdateMessage(ctx context.Context, msg *storage.Message) error {
	if msg == nil {
		return fmt.Errorf("nil message in update operation")
	}

	if err := f.messageStore.UpdateMessage(ctx, f.queueName, msg); err != nil {
		f.logger.Error("failed to apply update",
			slog.String("queue", f.queueName),
			slog.Int("partition", f.partitionID),
			slog.String("message_id", msg.ID),
			slog.String("error", err.Error()))
		return err
	}

	f.logger.Debug("applied update",
		slog.String("queue", f.queueName),
		slog.Int("partition", f.partitionID),
		slog.String("message_id", msg.ID))

	return nil
}

// applyRetentionDelete applies a batch retention delete operation.
func (f *PartitionFSM) applyRetentionDelete(ctx context.Context, messageIDs []string) error {
	if len(messageIDs) == 0 {
		return nil
	}

	deletedCount, err := f.messageStore.DeleteMessageBatch(ctx, f.queueName, messageIDs)
	if err != nil {
		f.logger.Error("failed to apply retention delete",
			slog.String("queue", f.queueName),
			slog.Int("partition", f.partitionID),
			slog.Int("message_count", len(messageIDs)),
			slog.String("error", err.Error()))
		return err
	}

	f.logger.Info("applied retention delete",
		slog.String("queue", f.queueName),
		slog.Int("partition", f.partitionID),
		slog.Int("requested", len(messageIDs)),
		slog.Int64("deleted", deletedCount))

	return nil
}

// Snapshot creates a point-in-time snapshot of the FSM state.
func (f *PartitionFSM) Snapshot() (raft.FSMSnapshot, error) {
	f.logger.Info("creating snapshot",
		slog.String("queue", f.queueName),
		slog.Int("partition", f.partitionID))

	// The snapshot will capture all messages in this partition
	snapshot := &PartitionSnapshot{
		queueName:    f.queueName,
		partitionID:  f.partitionID,
		messageStore: f.messageStore,
		logger:       f.logger,
	}

	return snapshot, nil
}

// Restore restores the FSM state from a snapshot.
func (f *PartitionFSM) Restore(rc io.ReadCloser) error {
	defer rc.Close()

	f.logger.Info("restoring from snapshot",
		slog.String("queue", f.queueName),
		slog.Int("partition", f.partitionID))

	var snapshot SnapshotData
	if err := json.NewDecoder(rc).Decode(&snapshot); err != nil {
		f.logger.Error("failed to decode snapshot",
			slog.String("queue", f.queueName),
			slog.Int("partition", f.partitionID),
			slog.String("error", err.Error()))
		return err
	}

	ctx := context.Background()

	// Restore all messages
	for _, msg := range snapshot.Messages {
		if err := f.messageStore.Enqueue(ctx, f.queueName, msg); err != nil {
			f.logger.Error("failed to restore message",
				slog.String("queue", f.queueName),
				slog.Int("partition", f.partitionID),
				slog.String("message_id", msg.ID),
				slog.String("error", err.Error()))
			return err
		}
	}

	f.logger.Info("restored snapshot",
		slog.String("queue", f.queueName),
		slog.Int("partition", f.partitionID),
		slog.Int("message_count", len(snapshot.Messages)))

	return nil
}

// ISR management (for async mode with min_in_sync_replicas)

// UpdateISR updates the in-sync replica list.
func (f *PartitionFSM) UpdateISR(replicaID string) {
	f.isrMu.Lock()
	defer f.isrMu.Unlock()
	f.isr[replicaID] = time.Now()
}

// GetISR returns the current in-sync replicas.
func (f *PartitionFSM) GetISR() []string {
	f.isrMu.RLock()
	defer f.isrMu.RUnlock()

	replicas := make([]string, 0, len(f.isr))
	for replicaID := range f.isr {
		replicas = append(replicas, replicaID)
	}
	return replicas
}

// ISRCount returns the number of in-sync replicas.
func (f *PartitionFSM) ISRCount() int {
	f.isrMu.RLock()
	defer f.isrMu.RUnlock()
	return len(f.isr)
}

// PartitionSnapshot implements raft.FSMSnapshot for a queue partition.
type PartitionSnapshot struct {
	queueName    string
	partitionID  int
	messageStore storage.MessageStore
	logger       *slog.Logger
}

// SnapshotData represents the serialized snapshot data.
type SnapshotData struct {
	QueueName   string
	PartitionID int
	Messages    []*storage.Message
	Timestamp   time.Time
}

// Persist writes the snapshot to the given sink.
func (s *PartitionSnapshot) Persist(sink raft.SnapshotSink) error {
	// Get all messages from partition
	// Note: This is a simplified implementation
	// In production, you'd want to page through messages
	messages := make([]*storage.Message, 0)

	// Get first and last sequences to determine range
	// This would need to be implemented in the message store
	// For now, we'll just capture what we can retrieve

	snapshot := SnapshotData{
		QueueName:   s.queueName,
		PartitionID: s.partitionID,
		Messages:    messages,
		Timestamp:   time.Now(),
	}

	if err := json.NewEncoder(sink).Encode(snapshot); err != nil {
		sink.Cancel()
		s.logger.Error("failed to encode snapshot",
			slog.String("queue", s.queueName),
			slog.Int("partition", s.partitionID),
			slog.String("error", err.Error()))
		return err
	}

	if err := sink.Close(); err != nil {
		s.logger.Error("failed to close snapshot sink",
			slog.String("queue", s.queueName),
			slog.Int("partition", s.partitionID),
			slog.String("error", err.Error()))
		return err
	}

	s.logger.Info("persisted snapshot",
		slog.String("queue", s.queueName),
		slog.Int("partition", s.partitionID),
		slog.Int("message_count", len(messages)))

	return nil
}

// Release releases resources held by the snapshot.
func (s *PartitionSnapshot) Release() {
	// Nothing to release in this implementation
}
