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
		return stopLocalFailure("create queue", op, err)
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
		return stopLocalFailure("update queue", op, err)
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
		return stopLocalFailure("delete queue", op, err)
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
			return stopLocalFailure("queue auto-create", op, createErr)
		}
		offset, err = f.queueStore.Append(ctx, op.QueueName, envelope)
	}
	if err != nil {
		message.Release(envelope)
		f.logger.Error("failed to apply append",
			slog.String("queue", op.QueueName),
			slog.String("error", err.Error()))
		return stopLocalFailure("append", op, err)
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
			return stopLocalFailure("queue auto-create", op, createErr)
		}
		offset, deduplicated, err = deduplicating.AppendOnce(ctx, op.QueueName, op.DedupeKey, envelope)
	}
	if err != nil {
		message.Release(envelope)
		f.logger.Error("failed to apply deduplicated append",
			slog.String("queue", op.QueueName),
			slog.String("dedupe_key", op.DedupeKey),
			slog.String("error", err.Error()))
		return stopLocalFailure("deduplicated append", op, err)
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
		return stopLocalFailure("truncate", op, err)
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
		return stopLocalFailure("create group", op, err)
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
		return stopLocalFailure("delete group", op, err)
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
		return stopLocalFailure("update group", op, err)
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
		return stopLocalFailure("update cursor", op, err)
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
		return stopLocalFailure("update committed", op, err)
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
		return stopLocalFailure("add pending", op, err)
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
		return stopLocalFailure("remove pending", op, err)
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
		return stopLocalFailure("transfer pending", op, err)
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
		return stopLocalFailure("register consumer", op, err)
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
		return stopLocalFailure("unregister consumer", op, err)
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

	snapshotable, ok := f.queueStore.(storage.SnapshotableQueueStore)
	if !ok {
		// A snapshot that cannot carry the records is not a snapshot: raft
		// compacts the log once one exists, and a follower that installed it
		// would advance past entries whose records it never received. Refusing
		// keeps the log intact instead.
		return nil, fmt.Errorf("queue store cannot be snapshotted: %T", f.queueStore)
	}

	queues, err := f.queueStore.ListQueues(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to list queues: %w", err)
	}

	snapshot := &GlobalSnapshot{queueStore: f.queueStore, logger: f.logger}
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

		head, records, err := snapshotable.SnapshotQueue(ctx, queueName)
		if err != nil {
			snapshot.Release()
			return nil, fmt.Errorf("failed to capture queue %q: %w", queueName, err)
		}

		snapshot.queues = append(snapshot.queues, capturedQueue{
			QueueSnapshotData: QueueSnapshotData{
				QueueName:   queueName,
				QueueConfig: &cfgCopy,
				Groups:      groups,
				Head:        head,
				Tail:        head + uint64(len(records)),
			},
			records: records,
		})
	}

	return snapshot, nil
}

// Restore rebuilds the FSM from a snapshot.
//
// A snapshot is the authoritative state of the group at the index it was taken,
// not a set of changes to merge: raft installs one precisely when this node is
// too far behind for the log to catch it up. Anything already here describes a
// past the group has compacted away, so it is discarded first.
func (f *LogFSM) Restore(rc io.ReadCloser) error {
	defer rc.Close()

	f.logger.Info("restoring from snapshot")

	snapshotable, ok := f.queueStore.(storage.SnapshotableQueueStore)
	if !ok {
		return fmt.Errorf("queue store cannot be restored: %T", f.queueStore)
	}

	ctx := context.Background()
	reader := newSnapshotReader(rc)
	if err := reader.ReadHeader(); err != nil {
		f.logger.Error("failed to decode snapshot header",
			slog.String("error", err.Error()))
		return err
	}
	if err := f.resetState(ctx, snapshotable); err != nil {
		return err
	}

	var (
		current    string
		queueCount int
		records    uint64
	)
	for {
		entry, err := reader.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			f.logger.Error("failed to decode snapshot",
				slog.String("error", err.Error()))
			return err
		}

		switch {
		case entry.Queue != nil:
			if err := f.restoreQueue(ctx, snapshotable, entry.Queue); err != nil {
				return err
			}
			current = entry.Queue.QueueName
			queueCount++
		case entry.Record != nil:
			if current == "" {
				return fmt.Errorf("%w: record before any queue", errMalformedSnapshot)
			}
			envelope, err := decodeOperationMessage(entry.Record.Envelope)
			if err != nil {
				return fmt.Errorf("%w: queue %q record %d: %w", errMalformedSnapshot, current, entry.Record.Offset, err)
			}
			if err := snapshotable.RestoreRecord(ctx, current, entry.Record.Offset, envelope); err != nil {
				f.logger.Error("failed to restore record",
					slog.String("queue", current),
					slog.Uint64("offset", entry.Record.Offset),
					slog.String("error", err.Error()))
				return err
			}
			records++
		}
	}

	f.logger.Info("restored snapshot",
		slog.Int("queue_count", queueCount),
		slog.Uint64("record_count", records))

	return nil
}

func (f *LogFSM) restoreQueue(ctx context.Context, store storage.SnapshotableQueueStore, queue *QueueSnapshotData) error {
	config := queue.QueueConfig
	if config == nil {
		// A queue frame without a config predates nothing this build writes,
		// but a snapshot is still expected to name what it restores. Fall back
		// to the ephemeral default so its groups do not become orphaned.
		fallback := types.DefaultEphemeralQueueConfig(queue.QueueName, "$queue/"+queue.QueueName+"/#")
		config = &fallback
	}
	if err := store.RestoreQueue(ctx, *config, queue.Head); err != nil {
		f.logger.Error("failed to restore queue config",
			slog.String("queue", queue.QueueName),
			slog.String("error", err.Error()))
		return err
	}

	for _, group := range queue.Groups {
		if err := f.groupStore.CreateConsumerGroup(ctx, group); err != nil && !errors.Is(err, storage.ErrConsumerGroupExists) {
			f.logger.Error("failed to restore consumer group",
				slog.String("queue", queue.QueueName),
				slog.String("group", group.ID),
				slog.String("error", err.Error()))
			return err
		}
	}
	return nil
}

// resetState drops what this node holds before a snapshot is laid down over it.
//
// Groups go first, while the queues that name them are still listable, and the
// queue store clears itself after. Both halves are needed: the group store may
// be a different object from the queue store, and a queue the snapshot never
// mentions has to go too.
func (f *LogFSM) resetState(ctx context.Context, store storage.SnapshotableQueueStore) error {
	queues, err := f.queueStore.ListQueues(ctx)
	if err != nil {
		return fmt.Errorf("failed to list queues for restore: %w", err)
	}
	for _, queueCfg := range queues {
		groups, err := f.groupStore.ListConsumerGroups(ctx, queueCfg.Name)
		if err != nil {
			f.logger.Warn("failed to list consumer groups for restore",
				slog.String("queue", queueCfg.Name),
				slog.String("error", err.Error()))
			continue
		}
		for _, group := range groups {
			if err := f.groupStore.DeleteConsumerGroup(ctx, queueCfg.Name, group.ID); err != nil {
				f.logger.Warn("failed to drop consumer group for restore",
					slog.String("queue", queueCfg.Name),
					slog.String("group", group.ID),
					slog.String("error", err.Error()))
			}
		}
	}

	if err := store.ResetForRestore(ctx); err != nil {
		return fmt.Errorf("failed to clear queues for restore: %w", err)
	}
	return nil
}

// capturedQueue is a queue's metadata plus the records captured with it. The
// records are clones, so they stay valid while the FSM moves on and Persist
// serializes them.
type capturedQueue struct {
	QueueSnapshotData
	records []*message.Envelope
}

// GlobalSnapshot implements raft.FSMSnapshot for all queues.
type GlobalSnapshot struct {
	queues     []capturedQueue
	queueStore storage.QueueStore
	logger     *slog.Logger
}

// Persist streams the snapshot to the sink.
//
// Records are encoded one frame at a time rather than gathered into a single
// message, so what this costs in memory does not grow with what the queues
// hold.
func (s *GlobalSnapshot) Persist(sink raft.SnapshotSink) error {
	writer := newSnapshotWriter(sink)
	if err := s.write(writer); err != nil {
		sink.Cancel() //nolint:errcheck // best-effort cancellation after a write failure
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
		slog.Int("queue_count", len(s.queues)),
		slog.Int64("bytes", writer.written))

	return nil
}

func (s *GlobalSnapshot) write(writer *snapshotWriter) error {
	if err := writer.WriteHeader(time.Now()); err != nil {
		return err
	}
	for _, queue := range s.queues {
		if err := writer.WriteQueue(queue.QueueSnapshotData); err != nil {
			return err
		}
		offset := queue.Head
		for _, record := range queue.records {
			encoded, err := message.MarshalBinary(record)
			if err != nil {
				return fmt.Errorf("encode record %d of queue %q: %w", offset, queue.QueueName, err)
			}
			if err := writer.WriteRecord(offset, encoded); err != nil {
				return err
			}
			offset++
		}
	}
	return nil
}

// Release drops the record clones captured for this snapshot.
func (s *GlobalSnapshot) Release() {
	for _, queue := range s.queues {
		for _, record := range queue.records {
			message.Release(record)
		}
	}
	s.queues = nil
}

// stopLocalFailure ends the process on a committed mutation this node could not
// apply locally. It never returns.
//
// The entry is in the log on every peer, and hashicorp/raft advances the applied
// index as soon as Apply returns, whatever it returns — on a follower nothing
// reads the result at all. A store error that is logged and stepped over is
// therefore a replica that has quietly stopped matching its peers, with nothing
// left to detect the gap or repair it.
//
// Crashing is the conservative choice precisely because it is recoverable: the
// node restarts and rejoins from a snapshot and the leader's log. A silent gap
// is the one outcome nothing recovers from.
//
// Deterministic refusals do not come here. An entry whose payload every replica
// decodes the same way, or that every replica declines for the same reason, is
// skipped identically everywhere and leaves the group consistent; those still
// return an error.
func stopLocalFailure(what string, op *Operation, err error) *ApplyResult {
	panic(fmt.Errorf("queue raft fsm: %s failed locally for queue %q: %w", what, op.QueueName, err))
}
