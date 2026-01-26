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
	"github.com/absmach/fluxmq/queue/types"
	"github.com/hashicorp/raft"
)

// OpType represents the type of operation in the Raft log.
type OpType uint8

const (
	// Log operations
	OpAppend OpType = iota
	OpAppendBatch
	OpTruncate

	// Consumer group operations
	OpCreateGroup
	OpDeleteGroup
	OpUpdateCursor
	OpUpdateCommitted
	OpAddPending
	OpRemovePending
	OpTransferPending
	OpRegisterConsumer
	OpUnregisterConsumer
)

// Operation represents a queue operation to be replicated via Raft.
type Operation struct {
	Type      OpType    `json:"type"`
	Timestamp time.Time `json:"timestamp"`

	// Target identifiers
	QueueName   string `json:"queue_name,omitempty"`
	PartitionID int    `json:"partition_id,omitempty"`
	GroupID     string `json:"group_id,omitempty"`
	ConsumerID  string `json:"consumer_id,omitempty"`

	// For OpAppend
	Message *types.Message `json:"message,omitempty"`

	// For OpAppendBatch
	Messages []*types.Message `json:"messages,omitempty"`

	// For OpTruncate
	MinOffset uint64 `json:"min_offset,omitempty"`

	// For OpUpdateCursor, OpUpdateCommitted
	Cursor    uint64 `json:"cursor,omitempty"`
	Committed uint64 `json:"committed,omitempty"`

	// For OpAddPending, OpRemovePending, OpTransferPending
	PendingEntry *types.PendingEntry `json:"pending_entry,omitempty"`
	Offset       uint64              `json:"offset,omitempty"`
	FromConsumer string              `json:"from_consumer,omitempty"`
	ToConsumer   string              `json:"to_consumer,omitempty"`

	// For OpRegisterConsumer
	ConsumerInfo *types.ConsumerInfo `json:"consumer_info,omitempty"`

	// For OpCreateGroup
	GroupState *types.ConsumerGroupState `json:"group_state,omitempty"`
	Pattern    string                    `json:"pattern,omitempty"`
}

// ApplyResult holds the result of an FSM apply operation.
type ApplyResult struct {
	Offset uint64 // For append operations
	Error  error
}

// LogFSM implements the Raft FSM interface for a queue partition.
// It applies committed operations to the underlying log and consumer group stores.
type LogFSM struct {
	queueName   string
	partitionID int
	logStore    storage.LogStore
	groupStore  storage.ConsumerGroupStore
	logger      *slog.Logger

	mu sync.RWMutex
}

// NewLogFSM creates a new FSM for a queue partition.
func NewLogFSM(queueName string, partitionID int, logStore storage.LogStore, groupStore storage.ConsumerGroupStore, logger *slog.Logger) *LogFSM {
	if logger == nil {
		logger = slog.Default()
	}
	return &LogFSM{
		queueName:   queueName,
		partitionID: partitionID,
		logStore:    logStore,
		groupStore:  groupStore,
		logger:      logger,
	}
}

// Apply applies a Raft log entry to the FSM.
// This is called by Raft when a log entry is committed.
func (f *LogFSM) Apply(l *raft.Log) interface{} {
	var op Operation
	if err := json.Unmarshal(l.Data, &op); err != nil {
		f.logger.Error("failed to unmarshal operation",
			slog.String("queue", f.queueName),
			slog.Int("partition", f.partitionID),
			slog.String("error", err.Error()))
		return &ApplyResult{Error: err}
	}

	ctx := context.Background()

	switch op.Type {
	case OpAppend:
		return f.applyAppend(ctx, &op)
	case OpAppendBatch:
		return f.applyAppendBatch(ctx, &op)
	case OpTruncate:
		return f.applyTruncate(ctx, &op)
	case OpCreateGroup:
		return f.applyCreateGroup(ctx, &op)
	case OpDeleteGroup:
		return f.applyDeleteGroup(ctx, &op)
	case OpUpdateCursor:
		return f.applyUpdateCursor(ctx, &op)
	case OpUpdateCommitted:
		return f.applyUpdateCommitted(ctx, &op)
	case OpAddPending:
		return f.applyAddPending(ctx, &op)
	case OpRemovePending:
		return f.applyRemovePending(ctx, &op)
	case OpTransferPending:
		return f.applyTransferPending(ctx, &op)
	case OpRegisterConsumer:
		return f.applyRegisterConsumer(ctx, &op)
	case OpUnregisterConsumer:
		return f.applyUnregisterConsumer(ctx, &op)
	default:
		err := fmt.Errorf("unknown operation type: %d", op.Type)
		f.logger.Error("unknown operation",
			slog.String("queue", f.queueName),
			slog.Int("partition", f.partitionID),
			slog.Int("op_type", int(op.Type)))
		return &ApplyResult{Error: err}
	}
}

func (f *LogFSM) applyAppend(ctx context.Context, op *Operation) *ApplyResult {
	if op.Message == nil {
		return &ApplyResult{Error: fmt.Errorf("nil message in append operation")}
	}

	offset, err := f.logStore.Append(ctx, f.queueName, f.partitionID, op.Message)
	if err != nil {
		f.logger.Error("failed to apply append",
			slog.String("queue", f.queueName),
			slog.Int("partition", f.partitionID),
			slog.String("message_id", op.Message.ID),
			slog.String("error", err.Error()))
		return &ApplyResult{Error: err}
	}

	f.logger.Debug("applied append",
		slog.String("queue", f.queueName),
		slog.Int("partition", f.partitionID),
		slog.String("message_id", op.Message.ID),
		slog.Uint64("offset", offset))

	return &ApplyResult{Offset: offset}
}

func (f *LogFSM) applyAppendBatch(ctx context.Context, op *Operation) *ApplyResult {
	if len(op.Messages) == 0 {
		return &ApplyResult{Error: fmt.Errorf("empty messages in append batch operation")}
	}

	offset, err := f.logStore.AppendBatch(ctx, f.queueName, f.partitionID, op.Messages)
	if err != nil {
		f.logger.Error("failed to apply append batch",
			slog.String("queue", f.queueName),
			slog.Int("partition", f.partitionID),
			slog.Int("count", len(op.Messages)),
			slog.String("error", err.Error()))
		return &ApplyResult{Error: err}
	}

	f.logger.Debug("applied append batch",
		slog.String("queue", f.queueName),
		slog.Int("partition", f.partitionID),
		slog.Int("count", len(op.Messages)),
		slog.Uint64("first_offset", offset))

	return &ApplyResult{Offset: offset}
}

func (f *LogFSM) applyTruncate(ctx context.Context, op *Operation) *ApplyResult {
	err := f.logStore.Truncate(ctx, f.queueName, f.partitionID, op.MinOffset)
	if err != nil {
		f.logger.Error("failed to apply truncate",
			slog.String("queue", f.queueName),
			slog.Int("partition", f.partitionID),
			slog.Uint64("min_offset", op.MinOffset),
			slog.String("error", err.Error()))
		return &ApplyResult{Error: err}
	}

	f.logger.Debug("applied truncate",
		slog.String("queue", f.queueName),
		slog.Int("partition", f.partitionID),
		slog.Uint64("min_offset", op.MinOffset))

	return &ApplyResult{}
}

func (f *LogFSM) applyCreateGroup(ctx context.Context, op *Operation) *ApplyResult {
	if op.GroupState == nil {
		return &ApplyResult{Error: fmt.Errorf("nil group state in create group operation")}
	}

	err := f.groupStore.CreateConsumerGroup(ctx, op.GroupState)
	if err != nil && err != storage.ErrConsumerGroupExists {
		f.logger.Error("failed to apply create group",
			slog.String("queue", f.queueName),
			slog.String("group", op.GroupID),
			slog.String("error", err.Error()))
		return &ApplyResult{Error: err}
	}

	f.logger.Debug("applied create group",
		slog.String("queue", f.queueName),
		slog.String("group", op.GroupID))

	return &ApplyResult{}
}

func (f *LogFSM) applyDeleteGroup(ctx context.Context, op *Operation) *ApplyResult {
	err := f.groupStore.DeleteConsumerGroup(ctx, f.queueName, op.GroupID)
	if err != nil {
		f.logger.Error("failed to apply delete group",
			slog.String("queue", f.queueName),
			slog.String("group", op.GroupID),
			slog.String("error", err.Error()))
		return &ApplyResult{Error: err}
	}

	f.logger.Debug("applied delete group",
		slog.String("queue", f.queueName),
		slog.String("group", op.GroupID))

	return &ApplyResult{}
}

func (f *LogFSM) applyUpdateCursor(ctx context.Context, op *Operation) *ApplyResult {
	err := f.groupStore.UpdateCursor(ctx, f.queueName, op.GroupID, f.partitionID, op.Cursor)
	if err != nil {
		f.logger.Error("failed to apply update cursor",
			slog.String("queue", f.queueName),
			slog.String("group", op.GroupID),
			slog.Int("partition", f.partitionID),
			slog.Uint64("cursor", op.Cursor),
			slog.String("error", err.Error()))
		return &ApplyResult{Error: err}
	}

	return &ApplyResult{}
}

func (f *LogFSM) applyUpdateCommitted(ctx context.Context, op *Operation) *ApplyResult {
	err := f.groupStore.UpdateCommitted(ctx, f.queueName, op.GroupID, f.partitionID, op.Committed)
	if err != nil {
		f.logger.Error("failed to apply update committed",
			slog.String("queue", f.queueName),
			slog.String("group", op.GroupID),
			slog.Int("partition", f.partitionID),
			slog.Uint64("committed", op.Committed),
			slog.String("error", err.Error()))
		return &ApplyResult{Error: err}
	}

	return &ApplyResult{}
}

func (f *LogFSM) applyAddPending(ctx context.Context, op *Operation) *ApplyResult {
	if op.PendingEntry == nil {
		return &ApplyResult{Error: fmt.Errorf("nil pending entry in add pending operation")}
	}

	err := f.groupStore.AddPendingEntry(ctx, f.queueName, op.GroupID, op.PendingEntry)
	if err != nil {
		f.logger.Error("failed to apply add pending",
			slog.String("queue", f.queueName),
			slog.String("group", op.GroupID),
			slog.Uint64("offset", op.PendingEntry.Offset),
			slog.String("error", err.Error()))
		return &ApplyResult{Error: err}
	}

	return &ApplyResult{}
}

func (f *LogFSM) applyRemovePending(ctx context.Context, op *Operation) *ApplyResult {
	err := f.groupStore.RemovePendingEntry(ctx, f.queueName, op.GroupID, op.ConsumerID, f.partitionID, op.Offset)
	if err != nil && err != storage.ErrPendingEntryNotFound {
		f.logger.Error("failed to apply remove pending",
			slog.String("queue", f.queueName),
			slog.String("group", op.GroupID),
			slog.String("consumer", op.ConsumerID),
			slog.Uint64("offset", op.Offset),
			slog.String("error", err.Error()))
		return &ApplyResult{Error: err}
	}

	return &ApplyResult{}
}

func (f *LogFSM) applyTransferPending(ctx context.Context, op *Operation) *ApplyResult {
	err := f.groupStore.TransferPendingEntry(ctx, f.queueName, op.GroupID, f.partitionID, op.Offset, op.FromConsumer, op.ToConsumer)
	if err != nil {
		f.logger.Error("failed to apply transfer pending",
			slog.String("queue", f.queueName),
			slog.String("group", op.GroupID),
			slog.Uint64("offset", op.Offset),
			slog.String("from", op.FromConsumer),
			slog.String("to", op.ToConsumer),
			slog.String("error", err.Error()))
		return &ApplyResult{Error: err}
	}

	return &ApplyResult{}
}

func (f *LogFSM) applyRegisterConsumer(ctx context.Context, op *Operation) *ApplyResult {
	if op.ConsumerInfo == nil {
		return &ApplyResult{Error: fmt.Errorf("nil consumer info in register consumer operation")}
	}

	err := f.groupStore.RegisterConsumer(ctx, f.queueName, op.GroupID, op.ConsumerInfo)
	if err != nil {
		f.logger.Error("failed to apply register consumer",
			slog.String("queue", f.queueName),
			slog.String("group", op.GroupID),
			slog.String("consumer", op.ConsumerInfo.ID),
			slog.String("error", err.Error()))
		return &ApplyResult{Error: err}
	}

	f.logger.Debug("applied register consumer",
		slog.String("queue", f.queueName),
		slog.String("group", op.GroupID),
		slog.String("consumer", op.ConsumerInfo.ID))

	return &ApplyResult{}
}

func (f *LogFSM) applyUnregisterConsumer(ctx context.Context, op *Operation) *ApplyResult {
	err := f.groupStore.UnregisterConsumer(ctx, f.queueName, op.GroupID, op.ConsumerID)
	if err != nil {
		f.logger.Error("failed to apply unregister consumer",
			slog.String("queue", f.queueName),
			slog.String("group", op.GroupID),
			slog.String("consumer", op.ConsumerID),
			slog.String("error", err.Error()))
		return &ApplyResult{Error: err}
	}

	f.logger.Debug("applied unregister consumer",
		slog.String("queue", f.queueName),
		slog.String("group", op.GroupID),
		slog.String("consumer", op.ConsumerID))

	return &ApplyResult{}
}

// Snapshot creates a point-in-time snapshot of the FSM state.
func (f *LogFSM) Snapshot() (raft.FSMSnapshot, error) {
	f.logger.Info("creating snapshot",
		slog.String("queue", f.queueName),
		slog.Int("partition", f.partitionID))

	ctx := context.Background()

	// Get log bounds
	head, err := f.logStore.Head(ctx, f.queueName, f.partitionID)
	if err != nil {
		return nil, fmt.Errorf("failed to get head: %w", err)
	}

	tail, err := f.logStore.Tail(ctx, f.queueName, f.partitionID)
	if err != nil {
		return nil, fmt.Errorf("failed to get tail: %w", err)
	}

	// Get consumer groups for this queue
	groups, err := f.groupStore.ListConsumerGroups(ctx, f.queueName)
	if err != nil {
		return nil, fmt.Errorf("failed to list consumer groups: %w", err)
	}

	return &LogSnapshot{
		queueName:   f.queueName,
		partitionID: f.partitionID,
		head:        head,
		tail:        tail,
		groups:      groups,
		logStore:    f.logStore,
		logger:      f.logger,
	}, nil
}

// Restore restores the FSM state from a snapshot.
func (f *LogFSM) Restore(rc io.ReadCloser) error {
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

	// Restore messages
	for _, msg := range snapshot.Messages {
		if _, err := f.logStore.Append(ctx, f.queueName, f.partitionID, msg); err != nil {
			f.logger.Error("failed to restore message",
				slog.String("queue", f.queueName),
				slog.Int("partition", f.partitionID),
				slog.String("message_id", msg.ID),
				slog.String("error", err.Error()))
			return err
		}
	}

	// Restore consumer groups
	for _, group := range snapshot.Groups {
		if err := f.groupStore.CreateConsumerGroup(ctx, group); err != nil {
			if err != storage.ErrConsumerGroupExists {
				f.logger.Error("failed to restore consumer group",
					slog.String("queue", f.queueName),
					slog.String("group", group.ID),
					slog.String("error", err.Error()))
				return err
			}
		}
	}

	f.logger.Info("restored snapshot",
		slog.String("queue", f.queueName),
		slog.Int("partition", f.partitionID),
		slog.Int("message_count", len(snapshot.Messages)),
		slog.Int("group_count", len(snapshot.Groups)))

	return nil
}

// LogSnapshot implements raft.FSMSnapshot for a queue partition.
type LogSnapshot struct {
	queueName   string
	partitionID int
	head        uint64
	tail        uint64
	groups      []*types.ConsumerGroupState
	logStore    storage.LogStore
	logger      *slog.Logger
}

// SnapshotData represents the serialized snapshot data.
type SnapshotData struct {
	QueueName   string                      `json:"queue_name"`
	PartitionID int                         `json:"partition_id"`
	Head        uint64                      `json:"head"`
	Tail        uint64                      `json:"tail"`
	Messages    []*types.Message            `json:"messages"`
	Groups      []*types.ConsumerGroupState `json:"groups"`
	Timestamp   time.Time                   `json:"timestamp"`
}

// Persist writes the snapshot to the given sink.
func (s *LogSnapshot) Persist(sink raft.SnapshotSink) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	// Read all messages from head to tail
	var messages []*types.Message
	if s.tail > s.head {
		msgs, err := s.logStore.ReadBatch(ctx, s.queueName, s.partitionID, s.head, int(s.tail-s.head))
		if err != nil {
			sink.Cancel()
			s.logger.Error("failed to read messages for snapshot",
				slog.String("queue", s.queueName),
				slog.Int("partition", s.partitionID),
				slog.String("error", err.Error()))
			return fmt.Errorf("failed to read messages for snapshot: %w", err)
		}
		messages = msgs
	}

	snapshot := SnapshotData{
		QueueName:   s.queueName,
		PartitionID: s.partitionID,
		Head:        s.head,
		Tail:        s.tail,
		Messages:    messages,
		Groups:      s.groups,
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
		slog.Int("message_count", len(messages)),
		slog.Int("group_count", len(s.groups)))

	return nil
}

// Release releases resources held by the snapshot.
func (s *LogSnapshot) Release() {
	// Nothing to release
}
