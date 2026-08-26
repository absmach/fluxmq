// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package raft

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"time"

	"github.com/absmach/fluxmq/message"
	"github.com/absmach/fluxmq/queue/storage"
	"github.com/absmach/fluxmq/queue/types"
	"github.com/hashicorp/raft"
)

// OpType represents the type of operation in the Raft log.
type OpType uint8

const (
	// Log operations.
	OpAppend OpType = iota
	OpTruncate

	// Consumer group operations.
	OpCreateGroup
	OpUpdateGroup
	OpDeleteGroup
	OpUpdateCursor
	OpUpdateCommitted
	OpAddPending
	OpRemovePending
	OpTransferPending
	OpRegisterConsumer
	OpUnregisterConsumer

	// Queue config operations.
	OpCreateQueue
	OpUpdateQueue
	OpDeleteQueue
)

// Operation is the in-process form of a queue mutation replicated via Raft.
// operation_codec.go is the only boundary between this domain type and its
// versioned protobuf representation in the Raft log.
type Operation struct {
	Type      OpType
	Timestamp time.Time

	// Target identifiers
	QueueName  string
	GroupID    string
	ConsumerID string

	// Message is a binary-encoded envelope for OpAppend.
	//
	// Keeping the encoded envelope here gives the operation one canonical
	// message representation and lets protobuf carry it as bytes without an
	// intermediate object tree or base64 layer.
	Message []byte

	// DedupeKey, when set on OpAppend, makes the append conditional: each
	// replica appends only if it does not already hold a record with this key.
	// The key is part of the replicated entry so every replica asks the same
	// question, rather than the leader answering it alone and shipping a
	// result the followers cannot verify.
	DedupeKey string

	// For OpTruncate
	MinOffset uint64

	// For OpUpdateCursor, OpUpdateCommitted
	Cursor    uint64
	Committed uint64

	// For OpAddPending, OpRemovePending, OpTransferPending
	PendingEntry *types.PendingEntry
	Offset       uint64
	FromConsumer string
	ToConsumer   string

	// For OpRegisterConsumer
	ConsumerInfo *types.ConsumerInfo

	// For OpCreateGroup, OpUpdateGroup
	GroupState *types.ConsumerGroup

	// For OpCreateQueue, OpUpdateQueue
	QueueConfig *types.QueueConfig
}

// ApplyResult holds the result of an FSM apply operation.
type ApplyResult struct {
	Offset uint64 // For append operations
	Error  error

	// Deduplicated reports that a keyed append found the record already
	// present. The offset still points at it, so a caller settling against
	// the record can proceed either way.
	Deduplicated bool
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
func (f *LogFSM) Apply(l *raft.Log) any {
	op, err := unmarshalOperation(l.Data)
	if err != nil {
		// The entry is already committed, so every other replica applies it.
		// Returning an error here would leave this node quietly one mutation
		// behind the rest of the group, with nothing to notice the gap and
		// nothing to repair it. Stopping is the only honest response.
		f.logger.Error("undecodable committed log entry",
			slog.Uint64("index", l.Index),
			slog.Uint64("term", l.Term),
			slog.String("error", err.Error()))
		panic(fmt.Errorf("queue raft fsm: undecodable committed entry at index %d: %w", l.Index, err))
	}

	ctx := context.Background()

	switch op.Type {
	case OpCreateQueue:
		return f.applyCreateQueue(ctx, op)
	case OpUpdateQueue:
		return f.applyUpdateQueue(ctx, op)
	case OpDeleteQueue:
		return f.applyDeleteQueue(ctx, op)
	case OpAppend:
		return f.applyAppend(ctx, op)
	case OpTruncate:
		return f.applyTruncate(ctx, op)
	case OpCreateGroup:
		return f.applyCreateGroup(ctx, op)
	case OpUpdateGroup:
		return f.applyUpdateGroup(ctx, op)
	case OpDeleteGroup:
		return f.applyDeleteGroup(ctx, op)
	case OpUpdateCursor:
		return f.applyUpdateCursor(ctx, op)
	case OpUpdateCommitted:
		return f.applyUpdateCommitted(ctx, op)
	case OpAddPending:
		return f.applyAddPending(ctx, op)
	case OpRemovePending:
		return f.applyRemovePending(ctx, op)
	case OpTransferPending:
		return f.applyTransferPending(ctx, op)
	case OpRegisterConsumer:
		return f.applyRegisterConsumer(ctx, op)
	case OpUnregisterConsumer:
		return f.applyUnregisterConsumer(ctx, op)
	default:
		// Same divergence as an undecodable entry: the operation committed,
		// the peers applied it, and this binary has no case for it.
		f.logger.Error("unknown operation type in committed entry",
			slog.Uint64("index", l.Index),
			slog.Int("type", int(op.Type)))
		panic(fmt.Errorf("queue raft fsm: unknown operation type %d at index %d", op.Type, l.Index))
	}
}

func (f *LogFSM) applyCreateQueue(ctx context.Context, op *Operation) *ApplyResult {
	if op.QueueConfig == nil {
		return &ApplyResult{Error: fmt.Errorf("nil queue config in create queue operation")}
	}

	err := f.queueStore.CreateQueue(ctx, *op.QueueConfig)
	if err != nil && !errors.Is(err, storage.ErrQueueAlreadyExists) {
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

	if err := f.queueStore.DeleteQueue(ctx, op.QueueName); err != nil && !errors.Is(err, storage.ErrQueueNotFound) {
		f.logger.Error("failed to apply delete queue",
			slog.String("queue", op.QueueName),
			slog.String("error", err.Error()))
		return &ApplyResult{Error: err}
	}

	return &ApplyResult{}
}

// decodeOperationMessage decodes a replicated envelope. The caller owns the
// result and must release it.
func decodeOperationMessage(encoded []byte) (*message.Envelope, error) {
	if len(encoded) == 0 {
		return nil, errors.New("empty message in append operation")
	}
	return message.UnmarshalBinary(encoded)
}

func (f *LogFSM) applyAppend(ctx context.Context, op *Operation) *ApplyResult {
	if op.DedupeKey != "" {
		return f.applyAppendOnce(ctx, op)
	}

	envelope, err := decodeOperationMessage(op.Message)
	if err != nil {
		return &ApplyResult{Error: err}
	}

	offset, err := f.queueStore.Append(ctx, op.QueueName, envelope)
	if errors.Is(err, storage.ErrQueueNotFound) {
		if createErr := f.ensureQueueExists(ctx, op.QueueName); createErr != nil {
			message.Release(envelope)
			f.logger.Error("failed to auto-create queue for append",
				slog.String("queue", op.QueueName),
				slog.String("error", createErr.Error()))
			return &ApplyResult{Error: createErr}
		}
		offset, err = f.queueStore.Append(ctx, op.QueueName, envelope)
	}
	if err != nil {
		message.Release(envelope)
		f.logger.Error("failed to apply append",
			slog.String("queue", op.QueueName),
			slog.String("error", err.Error()))
		return &ApplyResult{Error: err}
	}

	f.logger.Debug("applied append",
		slog.String("queue", op.QueueName),
		slog.Uint64("offset", offset))

	return &ApplyResult{Offset: offset}
}

// applyAppendOnce applies an append that must not produce a second record.
//
// The check runs here, on each replica against its own store, rather than once
// on the leader. The leader's answer is not part of the replicated log, so a
// follower has no way to verify it; what every replica does hold is the same
// log content, and the key is written into the record itself. Asking the local
// store therefore gives every replica the same answer for the same reason.
func (f *LogFSM) applyAppendOnce(ctx context.Context, op *Operation) *ApplyResult {
	deduplicating, ok := f.queueStore.(storage.DeduplicatingQueueStore)
	if !ok {
		// Falling back to a plain append would make this a per-node decision:
		// a replica that can deduplicate would skip the record while one that
		// cannot would write it, and the two would disagree about what the
		// queue holds. Refusing keeps the replicas identical and leaves the
		// source entry pending for a later retry.
		f.logger.Error("store cannot apply a deduplicated append",
			slog.String("queue", op.QueueName),
			slog.String("dedupe_key", op.DedupeKey))
		return &ApplyResult{Error: storage.ErrDeduplicationUnsupported}
	}

	envelope, err := decodeOperationMessage(op.Message)
	if err != nil {
		return &ApplyResult{Error: err}
	}

	offset, deduplicated, err := deduplicating.AppendOnce(ctx, op.QueueName, op.DedupeKey, envelope)
	if errors.Is(err, storage.ErrQueueNotFound) {
		if createErr := f.ensureQueueExists(ctx, op.QueueName); createErr != nil {
			message.Release(envelope)
			f.logger.Error("failed to auto-create queue for deduplicated append",
				slog.String("queue", op.QueueName),
				slog.String("error", createErr.Error()))
			return &ApplyResult{Error: createErr}
		}
		offset, deduplicated, err = deduplicating.AppendOnce(ctx, op.QueueName, op.DedupeKey, envelope)
	}
	if err != nil {
		message.Release(envelope)
		f.logger.Error("failed to apply deduplicated append",
			slog.String("queue", op.QueueName),
			slog.String("dedupe_key", op.DedupeKey),
			slog.String("error", err.Error()))
		return &ApplyResult{Error: err}
	}

	f.logger.Debug("applied deduplicated append",
		slog.String("queue", op.QueueName),
		slog.String("dedupe_key", op.DedupeKey),
		slog.Uint64("offset", offset),
		slog.Bool("deduplicated", deduplicated))

	return &ApplyResult{Offset: offset, Deduplicated: deduplicated}
}

func (f *LogFSM) ensureQueueExists(ctx context.Context, queueName string) error {
	cfg := types.DefaultEphemeralQueueConfig(queueName, "$queue/"+queueName+"/#")
	if err := f.queueStore.CreateQueue(ctx, cfg); err != nil && !errors.Is(err, storage.ErrQueueAlreadyExists) {
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
	if err != nil && !errors.Is(err, storage.ErrConsumerGroupExists) {
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

func (f *LogFSM) applyUpdateGroup(ctx context.Context, op *Operation) *ApplyResult {
	if op.GroupState == nil {
		return &ApplyResult{Error: fmt.Errorf("nil group state in update group operation")}
	}

	err := f.groupStore.UpdateConsumerGroup(ctx, op.GroupState)
	if err != nil {
		f.logger.Error("failed to apply update group",
			slog.String("queue", op.QueueName),
			slog.String("group", op.GroupID),
			slog.String("error", err.Error()))
		return &ApplyResult{Error: err}
	}

	f.logger.Debug("applied update group",
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
	if err != nil && !errors.Is(err, storage.ErrPendingEntryNotFound) {
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

	data, err := io.ReadAll(rc)
	if err != nil {
		f.logger.Error("failed to read snapshot",
			slog.String("error", err.Error()))
		return err
	}
	snapshot, err := unmarshalSnapshot(data)
	if err != nil {
		f.logger.Error("failed to decode snapshot",
			slog.String("error", err.Error()))
		return err
	}

	ctx := context.Background()

	// Restore queue configs and consumer groups
	for _, q := range snapshot.Queues {
		if q.QueueConfig != nil {
			if err := f.queueStore.CreateQueue(ctx, *q.QueueConfig); err != nil {
				if errors.Is(err, storage.ErrQueueAlreadyExists) {
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
				if !errors.Is(err, storage.ErrConsumerGroupExists) {
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

// GlobalSnapshot implements raft.FSMSnapshot for all queues.
type GlobalSnapshot struct {
	queues     []QueueSnapshotData
	queueStore storage.QueueStore
	logger     *slog.Logger
}

// Persist writes the snapshot to the given sink.
func (s *GlobalSnapshot) Persist(sink raft.SnapshotSink) error {
	data, err := marshalSnapshot(&GlobalSnapshotData{
		Queues:    s.queues,
		Timestamp: time.Now(),
	})
	if err != nil {
		sink.Cancel() //nolint:errcheck // best-effort cancellation after encode failure
		s.logger.Error("failed to encode snapshot",
			slog.String("error", err.Error()))
		return err
	}

	if _, err := sink.Write(data); err != nil {
		sink.Cancel() //nolint:errcheck // best-effort cancellation after write failure
		s.logger.Error("failed to write snapshot",
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
