// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package raft

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
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

	// Queue config operations
	OpCreateQueue
	OpUpdateQueue
	OpDeleteQueue
)

// Operation represents a queue operation to be replicated via Raft.
type Operation struct {
	Type      OpType    `json:"type"`
	Timestamp time.Time `json:"timestamp"`

	// Target identifiers
	QueueName  string `json:"queue_name,omitempty"`
	GroupID    string `json:"group_id,omitempty"`
	ConsumerID string `json:"consumer_id,omitempty"`

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
	GroupState *types.ConsumerGroup `json:"group_state,omitempty"`
	Pattern    string               `json:"pattern,omitempty"`

	// For OpCreateQueue, OpUpdateQueue
	QueueConfig *types.QueueConfig `json:"queue_config,omitempty"`
}

// ApplyResult holds the result of an FSM apply operation.
type ApplyResult struct {
	Offset uint64 // For append operations
	Error  error
}

// LogFSM implements the Raft FSM interface for all queue operations.
// It applies committed operations to the underlying queue and consumer group stores.
// This is a shared FSM that handles all queues based on operation data.
type LogFSM struct {
	queueStore storage.QueueStore
	groupStore storage.ConsumerGroupStore
	logger     *slog.Logger
}

// NewLogFSM creates a new FSM for queue operations.
func NewLogFSM(queueStore storage.QueueStore, groupStore storage.ConsumerGroupStore, logger *slog.Logger) *LogFSM {
	if logger == nil {
		logger = slog.Default()
	}
	return &LogFSM{
		queueStore: queueStore,
		groupStore: groupStore,
		logger:     logger,
	}
}

// Apply applies a Raft log entry to the FSM.
// This is called by Raft when a log entry is committed.
func (f *LogFSM) Apply(l *raft.Log) interface{} {
	var op Operation
	if err := json.Unmarshal(l.Data, &op); err != nil {
		f.logger.Error("failed to unmarshal operation",
			slog.String("error", err.Error()))
		return &ApplyResult{Error: err}
	}

	ctx := context.Background()

	switch op.Type {
	case OpCreateQueue:
		return f.applyCreateQueue(ctx, &op)
	case OpUpdateQueue:
		return f.applyUpdateQueue(ctx, &op)
	case OpDeleteQueue:
		return f.applyDeleteQueue(ctx, &op)
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
			slog.String("queue", op.QueueName),
			slog.Int("op_type", int(op.Type)))
		return &ApplyResult{Error: err}
	}
}

func (f *LogFSM) applyCreateQueue(ctx context.Context, op *Operation) *ApplyResult {
	if op.QueueConfig == nil {
		return &ApplyResult{Error: fmt.Errorf("nil queue config in create queue operation")}
	}

	err := f.queueStore.CreateQueue(ctx, *op.QueueConfig)
	if err != nil && err != storage.ErrQueueAlreadyExists {
		f.logger.Error("failed to apply create queue",
			slog.String("queue", op.QueueConfig.Name),
			slog.String("error", err.Error()))
		return &ApplyResult{Error: err}
	}

	return &ApplyResult{}
}

func (f *LogFSM) applyUpdateQueue(ctx context.Context, op *Operation) *ApplyResult {
	if op.QueueConfig == nil {
		return &ApplyResult{Error: fmt.Errorf("nil queue config in update queue operation")}
	}

	if err := f.queueStore.UpdateQueue(ctx, *op.QueueConfig); err != nil {
		f.logger.Error("failed to apply update queue",
			slog.String("queue", op.QueueConfig.Name),
			slog.String("error", err.Error()))
		return &ApplyResult{Error: err}
	}

	return &ApplyResult{}
}

func (f *LogFSM) applyDeleteQueue(ctx context.Context, op *Operation) *ApplyResult {
	if op.QueueName == "" {
		return &ApplyResult{Error: fmt.Errorf("empty queue name in delete queue operation")}
	}

	if err := f.queueStore.DeleteQueue(ctx, op.QueueName); err != nil && err != storage.ErrQueueNotFound {
		f.logger.Error("failed to apply delete queue",
			slog.String("queue", op.QueueName),
			slog.String("error", err.Error()))
		return &ApplyResult{Error: err}
	}

	return &ApplyResult{}
}

func (f *LogFSM) applyAppend(ctx context.Context, op *Operation) *ApplyResult {
	if op.Message == nil {
		return &ApplyResult{Error: fmt.Errorf("nil message in append operation")}
	}

	offset, err := f.queueStore.Append(ctx, op.QueueName, op.Message)
	if err == storage.ErrQueueNotFound {
		if createErr := f.ensureQueueExists(ctx, op.QueueName); createErr != nil {
			f.logger.Error("failed to auto-create queue for append",
				slog.String("queue", op.QueueName),
				slog.String("error", createErr.Error()))
			return &ApplyResult{Error: createErr}
		}
		offset, err = f.queueStore.Append(ctx, op.QueueName, op.Message)
	}
	if err != nil {
		f.logger.Error("failed to apply append",
			slog.String("queue", op.QueueName),
			slog.String("message_id", op.Message.ID),
			slog.String("error", err.Error()))
		return &ApplyResult{Error: err}
	}

	f.logger.Debug("applied append",
		slog.String("queue", op.QueueName),
		slog.String("message_id", op.Message.ID),
		slog.Uint64("offset", offset))

	return &ApplyResult{Offset: offset}
}

func (f *LogFSM) applyAppendBatch(ctx context.Context, op *Operation) *ApplyResult {
	if len(op.Messages) == 0 {
		return &ApplyResult{Error: fmt.Errorf("empty messages in append batch operation")}
	}

	offset, err := f.queueStore.AppendBatch(ctx, op.QueueName, op.Messages)
	if err == storage.ErrQueueNotFound {
		if createErr := f.ensureQueueExists(ctx, op.QueueName); createErr != nil {
			f.logger.Error("failed to auto-create queue for append batch",
				slog.String("queue", op.QueueName),
				slog.String("error", createErr.Error()))
			return &ApplyResult{Error: createErr}
		}
		offset, err = f.queueStore.AppendBatch(ctx, op.QueueName, op.Messages)
	}
	if err != nil {
		f.logger.Error("failed to apply append batch",
			slog.String("queue", op.QueueName),
			slog.Int("count", len(op.Messages)),
			slog.String("error", err.Error()))
		return &ApplyResult{Error: err}
	}

	f.logger.Debug("applied append batch",
		slog.String("queue", op.QueueName),
		slog.Int("count", len(op.Messages)),
		slog.Uint64("first_offset", offset))

	return &ApplyResult{Offset: offset}
}

func (f *LogFSM) ensureQueueExists(ctx context.Context, queueName string) error {
	cfg := types.DefaultEphemeralQueueConfig(queueName, "$queue/"+queueName+"/#")
	if err := f.queueStore.CreateQueue(ctx, cfg); err != nil && err != storage.ErrQueueAlreadyExists {
		return err
	}
	return nil
}

func (f *LogFSM) applyTruncate(ctx context.Context, op *Operation) *ApplyResult {
	err := f.queueStore.Truncate(ctx, op.QueueName, op.MinOffset)
	if err != nil {
		f.logger.Error("failed to apply truncate",
			slog.String("queue", op.QueueName),
			slog.Uint64("min_offset", op.MinOffset),
			slog.String("error", err.Error()))
		return &ApplyResult{Error: err}
	}

	f.logger.Debug("applied truncate",
		slog.String("queue", op.QueueName),
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
			slog.String("queue", op.QueueName),
			slog.String("group", op.GroupID),
			slog.String("error", err.Error()))
		return &ApplyResult{Error: err}
	}

	f.logger.Debug("applied create group",
		slog.String("queue", op.QueueName),
		slog.String("group", op.GroupID))

	return &ApplyResult{}
}

func (f *LogFSM) applyDeleteGroup(ctx context.Context, op *Operation) *ApplyResult {
	err := f.groupStore.DeleteConsumerGroup(ctx, op.QueueName, op.GroupID)
	if err != nil {
		f.logger.Error("failed to apply delete group",
			slog.String("queue", op.QueueName),
			slog.String("group", op.GroupID),
			slog.String("error", err.Error()))
		return &ApplyResult{Error: err}
	}

	f.logger.Debug("applied delete group",
		slog.String("queue", op.QueueName),
		slog.String("group", op.GroupID))

	return &ApplyResult{}
}

func (f *LogFSM) applyUpdateCursor(ctx context.Context, op *Operation) *ApplyResult {
	err := f.groupStore.UpdateCursor(ctx, op.QueueName, op.GroupID, op.Cursor)
	if err != nil {
		f.logger.Error("failed to apply update cursor",
			slog.String("queue", op.QueueName),
			slog.String("group", op.GroupID),
			slog.Uint64("cursor", op.Cursor),
			slog.String("error", err.Error()))
		return &ApplyResult{Error: err}
	}

	return &ApplyResult{}
}

func (f *LogFSM) applyUpdateCommitted(ctx context.Context, op *Operation) *ApplyResult {
	err := f.groupStore.UpdateCommitted(ctx, op.QueueName, op.GroupID, op.Committed)
	if err != nil {
		f.logger.Error("failed to apply update committed",
			slog.String("queue", op.QueueName),
			slog.String("group", op.GroupID),
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

	err := f.groupStore.AddPendingEntry(ctx, op.QueueName, op.GroupID, op.PendingEntry)
	if err != nil {
		f.logger.Error("failed to apply add pending",
			slog.String("queue", op.QueueName),
			slog.String("group", op.GroupID),
			slog.Uint64("offset", op.PendingEntry.Offset),
			slog.String("error", err.Error()))
		return &ApplyResult{Error: err}
	}

	return &ApplyResult{}
}

func (f *LogFSM) applyRemovePending(ctx context.Context, op *Operation) *ApplyResult {
	err := f.groupStore.RemovePendingEntry(ctx, op.QueueName, op.GroupID, op.ConsumerID, op.Offset)
	if err != nil && err != storage.ErrPendingEntryNotFound {
		f.logger.Error("failed to apply remove pending",
			slog.String("queue", op.QueueName),
			slog.String("group", op.GroupID),
			slog.String("consumer", op.ConsumerID),
			slog.Uint64("offset", op.Offset),
			slog.String("error", err.Error()))
		return &ApplyResult{Error: err}
	}

	return &ApplyResult{}
}

func (f *LogFSM) applyTransferPending(ctx context.Context, op *Operation) *ApplyResult {
	err := f.groupStore.TransferPendingEntry(ctx, op.QueueName, op.GroupID, op.Offset, op.FromConsumer, op.ToConsumer)
	if err != nil {
		f.logger.Error("failed to apply transfer pending",
			slog.String("queue", op.QueueName),
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

	err := f.groupStore.RegisterConsumer(ctx, op.QueueName, op.GroupID, op.ConsumerInfo)
	if err != nil {
		f.logger.Error("failed to apply register consumer",
			slog.String("queue", op.QueueName),
			slog.String("group", op.GroupID),
			slog.String("consumer", op.ConsumerInfo.ID),
			slog.String("error", err.Error()))
		return &ApplyResult{Error: err}
	}

	f.logger.Debug("applied register consumer",
		slog.String("queue", op.QueueName),
		slog.String("group", op.GroupID),
		slog.String("consumer", op.ConsumerInfo.ID))

	return &ApplyResult{}
}

func (f *LogFSM) applyUnregisterConsumer(ctx context.Context, op *Operation) *ApplyResult {
	err := f.groupStore.UnregisterConsumer(ctx, op.QueueName, op.GroupID, op.ConsumerID)
	if err != nil {
		f.logger.Error("failed to apply unregister consumer",
			slog.String("queue", op.QueueName),
			slog.String("group", op.GroupID),
			slog.String("consumer", op.ConsumerID),
			slog.String("error", err.Error()))
		return &ApplyResult{Error: err}
	}

	f.logger.Debug("applied unregister consumer",
		slog.String("queue", op.QueueName),
		slog.String("group", op.GroupID),
		slog.String("consumer", op.ConsumerID))

	return &ApplyResult{}
}

// Snapshot creates a point-in-time snapshot of the FSM state.
// For a shared FSM, we snapshot all queues and consumer groups.
func (f *LogFSM) Snapshot() (raft.FSMSnapshot, error) {
	f.logger.Info("creating snapshot")

	ctx := context.Background()

	// List all queues
	queues, err := f.queueStore.ListQueues(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to list queues: %w", err)
	}

	// Collect all queue data including configs
	var queueSnapshots []QueueSnapshotData
	for _, queueCfg := range queues {
		queueName := queueCfg.Name
		cfgCopy := queueCfg

		groups, err := f.groupStore.ListConsumerGroups(ctx, queueName)
		if err != nil {
			f.logger.Warn("failed to list consumer groups for queue",
				slog.String("queue", queueName),
				slog.String("error", err.Error()))
			continue
		}

		queueSnapshots = append(queueSnapshots, QueueSnapshotData{
			QueueName:   queueName,
			QueueConfig: &cfgCopy,
			Groups:      groups,
		})
	}

	return &GlobalSnapshot{
		queues:     queueSnapshots,
		queueStore: f.queueStore,
		logger:     f.logger,
	}, nil
}

// Restore restores the FSM state from a snapshot.
func (f *LogFSM) Restore(rc io.ReadCloser) error {
	defer rc.Close()

	f.logger.Info("restoring from snapshot")

	var snapshot GlobalSnapshotData
	if err := json.NewDecoder(rc).Decode(&snapshot); err != nil {
		f.logger.Error("failed to decode snapshot",
			slog.String("error", err.Error()))
		return err
	}

	ctx := context.Background()

	// Restore queue configs and consumer groups
	for _, q := range snapshot.Queues {
		if q.QueueConfig != nil {
			if err := f.queueStore.CreateQueue(ctx, *q.QueueConfig); err != nil {
				if err == storage.ErrQueueAlreadyExists {
					if updateErr := f.queueStore.UpdateQueue(ctx, *q.QueueConfig); updateErr != nil {
						f.logger.Error("failed to restore queue config",
							slog.String("queue", q.QueueName),
							slog.String("error", updateErr.Error()))
						return updateErr
					}
				} else {
					f.logger.Error("failed to restore queue config",
						slog.String("queue", q.QueueName),
						slog.String("error", err.Error()))
					return err
				}
			}
		} else if q.QueueName != "" {
			// Pre-upgrade snapshot without QueueConfig — ensure the queue
			// exists so consumer groups below don't become orphaned.
			if err := f.ensureQueueExists(ctx, q.QueueName); err != nil {
				f.logger.Error("failed to ensure queue for legacy snapshot entry",
					slog.String("queue", q.QueueName),
					slog.String("error", err.Error()))
				return err
			}
		}

		for _, group := range q.Groups {
			if err := f.groupStore.CreateConsumerGroup(ctx, group); err != nil {
				if err != storage.ErrConsumerGroupExists {
					f.logger.Error("failed to restore consumer group",
						slog.String("queue", q.QueueName),
						slog.String("group", group.ID),
						slog.String("error", err.Error()))
					return err
				}
			}
		}
	}

	f.logger.Info("restored snapshot",
		slog.Int("queue_count", len(snapshot.Queues)))

	return nil
}

// QueueSnapshotData holds snapshot data for a single queue.
type QueueSnapshotData struct {
	QueueName   string                 `json:"queue_name"`
	QueueConfig *types.QueueConfig     `json:"queue_config,omitempty"`
	Groups      []*types.ConsumerGroup `json:"groups"`
}

// GlobalSnapshotData represents the serialized snapshot data for all queues.
type GlobalSnapshotData struct {
	Queues    []QueueSnapshotData `json:"queues"`
	Timestamp time.Time           `json:"timestamp"`
}

// GlobalSnapshot implements raft.FSMSnapshot for all queues.
type GlobalSnapshot struct {
	queues     []QueueSnapshotData
	queueStore storage.QueueStore
	logger     *slog.Logger
}

// Persist writes the snapshot to the given sink.
func (s *GlobalSnapshot) Persist(sink raft.SnapshotSink) error {
	snapshot := GlobalSnapshotData{
		Queues:    s.queues,
		Timestamp: time.Now(),
	}

	if err := json.NewEncoder(sink).Encode(snapshot); err != nil {
		sink.Cancel()
		s.logger.Error("failed to encode snapshot",
			slog.String("error", err.Error()))
		return err
	}

	if err := sink.Close(); err != nil {
		s.logger.Error("failed to close snapshot sink",
			slog.String("error", err.Error()))
		return err
	}

	s.logger.Info("persisted snapshot",
		slog.Int("queue_count", len(s.queues)))

	return nil
}

// Release releases resources held by the snapshot.
func (s *GlobalSnapshot) Release() {
	// Nothing to release
}
