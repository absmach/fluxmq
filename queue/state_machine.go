// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package queue

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/absmach/fluxmq/message"
	"github.com/absmach/fluxmq/queue/consumer"
	"github.com/absmach/fluxmq/queue/storage"
	"github.com/absmach/fluxmq/queue/types"
)

// ErrInvalidCommand identifies a structurally invalid queue command.
var ErrInvalidCommand = errors.New("invalid queue command")

// AppendCommand appends one or more messages to exactly one named queue.
//
// It borrows every envelope in Envelopes: the core clones each one into the
// record it stores, and a successful append takes ownership of that clone. The
// caller keeps its envelopes and releases them itself, which is a different
// contract from storage.QueueStore.Append, where a successful append takes the
// envelope it was given.
type AppendCommand struct {
	QueueName               string
	Envelopes               []*message.Envelope
	AtomicBatch             bool
	RequireProtectedDurable bool
}

// QueuePublishCommand routes one message to every queue whose topic pattern
// matches it, which is a different operation from AppendCommand: the
// destinations are resolved rather than named, and there may be none.
//
// It borrows Envelope on the same terms as AppendCommand.
type QueuePublishCommand struct {
	Envelope *message.Envelope
	Mode     types.PublishMode
	// ForcedTargets names the queues the message must land in, bypassing topic
	// resolution. It is set when a peer already resolved them, and it is a
	// routing control the publisher cannot supply.
	ForcedTargets []string
}

// AppendOutcome describes the offset range assigned by an append.
//
// Timestamp is the record timestamp assigned by the append, not the time the
// call returned. For a batch it is the last appended record's timestamp, which
// is the one that pairs with LastOffset.
type AppendOutcome struct {
	FirstOffset uint64
	LastOffset  uint64
	Count       uint32
	Timestamp   time.Time
}

// ConsumeCommand claims the next records for a consumer group.
type ConsumeCommand struct {
	QueueName  string
	GroupID    string
	ConsumerID string
	Filter     string
	Limit      int
}

// ConsumeOutcome contains records selected by Consume. Queue-mode records are
// already in the PEL. Stream-mode records must be committed after delivery.
type ConsumeOutcome struct {
	Messages       []*message.Envelope
	Mode           types.ConsumerGroupMode
	NextOffset     uint64
	CommitRequired bool
}

// CommitConsumeCommand advances a stream cursor after successful delivery.
type CommitConsumeCommand struct {
	QueueName string
	GroupID   string
	Offset    uint64
}

// AckCommand acknowledges queue offsets. ConsumerID is optional for the MQTT
// and AMQP compatibility adapters; when present, ownership is enforced.
type AckCommand struct {
	QueueName  string
	GroupID    string
	ConsumerID string
	Offsets    []uint64
}

// NackCommand releases queue offsets for redelivery after an optional delay.
type NackCommand struct {
	QueueName  string
	GroupID    string
	ConsumerID string
	Offsets    []uint64
	Delay      time.Duration
}

// RejectCommand moves queue offsets to the DLQ before source settlement.
type RejectCommand struct {
	QueueName  string
	GroupID    string
	ConsumerID string
	Offsets    []uint64
	Reason     string
}

// SettlementOutcome reports the successfully settled prefix and group cursor.
// On error it makes any partial transition explicit to in-process callers.
type SettlementOutcome struct {
	Offsets   []uint64
	Cursor    uint64
	Committed uint64
}

// ClaimCommand transfers idle pending records to another consumer.
type ClaimCommand struct {
	QueueName  string
	GroupID    string
	ConsumerID string
	MinIdle    time.Duration
	Limit      int
}

// ClaimOutcome describes pending records whose ownership was transferred.
type ClaimOutcome struct {
	Messages []*message.Envelope
	Offsets  []uint64
}

// SeekKind selects the coordinate used by a SeekCommand.
//
// A string, not an int: SeekCommand is part of the frozen command model, and
// changing the underlying type breaks every external caller that writes
// SeekKind("offset"). The zero value being invalid is a wart, not a defect
// worth a breaking change.
type SeekKind string

const (
	SeekOffset    SeekKind = "offset"
	SeekTimestamp SeekKind = "timestamp"
)

// SeekCommand resolves an offset without changing group state.
type SeekCommand struct {
	QueueName string
	Kind      SeekKind
	Offset    uint64
	Timestamp time.Time
}

// SeekOutcome is the canonical seek result used by protocol adapters.
type SeekOutcome struct {
	Offset     uint64
	Timestamp  time.Time
	ExactMatch bool
}

// CommitOffsetCommand records how far a stream group has processed.
//
// Distinct from CommitConsumeCommand: that one advances the read cursor after a
// consume, this one advances the committed safe point behind it. Collapsing
// them would let a commit move the read position, skipping records.
type CommitOffsetCommand struct {
	QueueName string
	GroupID   string
	Offset    uint64
}

// OffsetCommitter records a stream group's processed position.
//
// It is a separate optional capability rather than a method on CommandProcessor
// because that interface is frozen, and API-COMPATIBILITY.md states that adding
// a method to a frozen Go interface breaks every external implementation. A
// caller obtains it by asserting on the value StateMachine() returns.
type OffsetCommitter interface {
	CommitOffset(context.Context, CommitOffsetCommand) error
}

// CommandProcessor is the stable protocol-independent queue operation surface.
// Protocol adapters depend on this interface rather than the concrete machine.
type CommandProcessor interface {
	Append(context.Context, AppendCommand) (AppendOutcome, error)
	Consume(context.Context, ConsumeCommand) (ConsumeOutcome, error)
	CommitConsume(context.Context, CommitConsumeCommand) error
	Ack(context.Context, AckCommand) (SettlementOutcome, error)
	Nack(context.Context, NackCommand) (SettlementOutcome, error)
	Reject(context.Context, RejectCommand) (SettlementOutcome, error)
	Claim(context.Context, ClaimCommand) (ClaimOutcome, error)
	Seek(context.Context, SeekCommand) (SeekOutcome, error)
}

// stateMachine owns protocol-independent queue operation semantics.
//
// Every dependency is required. There is no degraded construction mode, so no
// method needs to ask whether the machine it is running on is fully built.
type stateMachine struct {
	records    *recordCore
	queueStore storage.QueueStore
	groupStore storage.ConsumerGroupStore
	consumers  *consumer.Manager

	// timeIndex resolves a timestamp to a starting offset. Both shipped stores
	// provide it; one that does not makes a timestamp seek fail rather than
	// silently fall back to reading the log from the beginning.
	timeIndex storage.TimeOffsetProvider
}

func newStateMachine(records *recordCore, queueStore storage.QueueStore, groupStore storage.ConsumerGroupStore, consumers *consumer.Manager) *stateMachine {
	machine := &stateMachine{
		records:    records,
		queueStore: queueStore,
		groupStore: groupStore,
		consumers:  consumers,
	}
	if provider, ok := queueStore.(storage.TimeOffsetProvider); ok {
		machine.timeIndex = provider
	}
	return machine
}

// StateMachine returns the manager's canonical command processor.
func (m *Manager) StateMachine() CommandProcessor { return m.stateMachine }

// Append applies an exact single-queue append command.
func (s *stateMachine) Append(ctx context.Context, command AppendCommand) (AppendOutcome, error) {
	if command.QueueName == "" {
		return AppendOutcome{}, fmt.Errorf("%w: queue name is required", ErrInvalidCommand)
	}
	// An empty append is rejected rather than reported as a success at offset 0.
	// Offset 0 is a valid offset, so "nothing to do" and "wrote at offset 0"
	// would otherwise be indistinguishable to the caller.
	if len(command.Envelopes) == 0 {
		return AppendOutcome{}, fmt.Errorf("%w: at least one message is required", ErrInvalidCommand)
	}
	// Validate the whole borrowed batch before any append runs. Besides keeping
	// invalid schema versions out of every backend, this prevents a later bad
	// entry from turning an atomic command into a partially applied one.
	for i, envelope := range command.Envelopes {
		if err := validateEnvelope(envelope); err != nil {
			return AppendOutcome{}, fmt.Errorf("envelope %d: %w", i, err)
		}
	}
	if command.RequireProtectedDurable {
		if len(command.Envelopes) != 1 || command.AtomicBatch {
			return AppendOutcome{}, fmt.Errorf("%w: protected durable append requires exactly one message", ErrInvalidCommand)
		}
		offset, createdAt, err := s.records.publishToDurableStream(ctx, command.QueueName, command.Envelopes[0])
		if err != nil {
			return AppendOutcome{}, err
		}
		return AppendOutcome{FirstOffset: offset, LastOffset: offset, Count: 1, Timestamp: createdAt}, nil
	}
	if len(command.Envelopes) == 1 && !command.AtomicBatch {
		offset, createdAt, err := s.records.appendToQueue(ctx, command.QueueName, command.Envelopes[0])
		if err != nil {
			return AppendOutcome{}, err
		}
		return AppendOutcome{FirstOffset: offset, LastOffset: offset, Count: 1, Timestamp: createdAt}, nil
	}

	first, count, lastCreatedAt, err := s.records.appendBatchToQueue(ctx, command.QueueName, command.Envelopes)
	if err != nil {
		return AppendOutcome{}, err
	}
	last := first
	if count > 0 {
		last += uint64(count - 1)
	}
	return AppendOutcome{FirstOffset: first, LastOffset: last, Count: count, Timestamp: lastCreatedAt}, nil
}

func validateEnvelope(envelope *message.Envelope) error {
	if envelope == nil {
		return fmt.Errorf("%w: an envelope is required", ErrInvalidCommand)
	}
	if err := envelope.Validate(); err != nil {
		return fmt.Errorf("%w: envelope: %w", ErrInvalidCommand, err)
	}
	return nil
}

// CommitOffset records a stream group's processed position.
func (s *stateMachine) CommitOffset(ctx context.Context, command CommitOffsetCommand) error {
	if command.QueueName == "" || command.GroupID == "" {
		return fmt.Errorf("%w: queue name and group id are required", ErrInvalidCommand)
	}

	return s.consumers.CommitOffset(ctx, command.QueueName, command.GroupID, command.Offset)
}

// Consume selects and claims the next records for a consumer.
func (s *stateMachine) Consume(ctx context.Context, command ConsumeCommand) (ConsumeOutcome, error) {
	if err := validateConsumerCommand(command.QueueName, command.GroupID, command.ConsumerID); err != nil {
		return ConsumeOutcome{}, err
	}
	group, err := s.groupStore.GetConsumerGroup(ctx, command.QueueName, command.GroupID)
	if err != nil {
		return ConsumeOutcome{}, err
	}
	var filter *consumer.Filter
	if command.Filter != "" {
		filter = consumer.NewFilter(command.Filter)
	}
	if group.Mode == types.GroupModeStream {
		if !group.AutoCommitEnabled() {
			// command.Limit does not apply: an ordered stream settled by hand
			// hands out one unsettled delivery per consumer at a time, so that a
			// nack can redeliver ahead of the next record. See ClaimManualStream.
			msg, err := s.consumers.ClaimManualStream(ctx, command.QueueName, command.GroupID, command.ConsumerID, filter)
			if err != nil {
				return ConsumeOutcome{Mode: types.GroupModeStream}, err
			}
			messages := []*message.Envelope{msg}
			// Re-read: the claim advanced the cursor, so the copy fetched above
			// reports where the group stood before this call.
			fresh, err := s.groupStore.GetConsumerGroup(ctx, command.QueueName, command.GroupID)
			if err != nil {
				releaseEnvelopes(messages)
				return ConsumeOutcome{}, err
			}
			return ConsumeOutcome{Messages: messages, Mode: types.GroupModeStream, NextOffset: fresh.CursorView().Cursor}, nil
		}
		messages, next, err := s.consumers.PeekBatchStream(ctx, command.QueueName, command.GroupID, command.ConsumerID, filter, command.Limit)
		if err != nil {
			return ConsumeOutcome{}, err
		}
		return ConsumeOutcome{Messages: messages, Mode: types.GroupModeStream, NextOffset: next, CommitRequired: true}, nil
	}

	messages, err := s.consumers.ClaimBatch(ctx, command.QueueName, command.GroupID, command.ConsumerID, filter, command.Limit)
	if err != nil {
		return ConsumeOutcome{Messages: messages, Mode: types.GroupModeQueue}, err
	}
	fresh, err := s.groupStore.GetConsumerGroup(ctx, command.QueueName, command.GroupID)
	if err != nil {
		releaseEnvelopes(messages)
		return ConsumeOutcome{}, err
	}
	return ConsumeOutcome{Messages: messages, Mode: types.GroupModeQueue, NextOffset: fresh.CursorView().Cursor}, nil
}

// CommitConsume advances a stream cursor after an adapter confirms delivery.
func (s *stateMachine) CommitConsume(ctx context.Context, command CommitConsumeCommand) error {
	if command.QueueName == "" || command.GroupID == "" {
		return fmt.Errorf("%w: queue name and group id are required", ErrInvalidCommand)
	}
	return s.consumers.CommitStreamCursor(ctx, command.QueueName, command.GroupID, command.Offset)
}

// Ack applies an acknowledgement command.
func (s *stateMachine) Ack(ctx context.Context, command AckCommand) (SettlementOutcome, error) {
	if err := validateSettlementCommand(command.QueueName, command.Offsets); err != nil {
		return SettlementOutcome{}, err
	}
	resolver, err := s.newSettlementResolver(ctx, command.QueueName, command.GroupID)
	if err != nil {
		return SettlementOutcome{}, err
	}
	outcome := SettlementOutcome{Offsets: make([]uint64, 0, len(command.Offsets))}

	// The pending-list gauge is reported once for the whole command. Reading it
	// per offset cost a store lookup per settled record to publish a number
	// that only the final value of matters.
	var settledGroup *types.ConsumerGroup

	for _, offset := range command.Offsets {
		group, owner, err := resolver.resolve(offset)
		if err != nil {
			return s.partialSettlement(ctx, command.QueueName, command.GroupID, outcome, fmt.Errorf("ack offset %d: %w", offset, err))
		}
		if command.ConsumerID != "" && owner != "" && owner != command.ConsumerID {
			return s.partialSettlement(ctx, command.QueueName, command.GroupID, outcome, fmt.Errorf("ack offset %d: %w", offset, consumer.ErrConsumerNotFound))
		}
		if group.Mode == types.GroupModeStream && group.AutoCommitEnabled() {
			err = s.ackStream(ctx, group, offset)
		} else {
			err = s.consumers.Ack(ctx, command.QueueName, group.ID, owner, offset)
		}
		if err != nil {
			return s.partialSettlement(ctx, command.QueueName, command.GroupID, outcome, fmt.Errorf("ack offset %d: %w", offset, err))
		}
		s.records.metrics.RecordAck(0)
		settledGroup = group
		outcome.Offsets = append(outcome.Offsets, offset)
	}

	if settledGroup != nil {
		s.records.metrics.UpdatePELSize(uint64(settledGroup.PendingCount()))
	}

	return s.finishSettlement(ctx, command.QueueName, command.GroupID, outcome)
}

// Nack applies a negative acknowledgement command.
func (s *stateMachine) Nack(ctx context.Context, command NackCommand) (SettlementOutcome, error) {
	if err := validateSettlementCommand(command.QueueName, command.Offsets); err != nil {
		return SettlementOutcome{}, err
	}
	if command.Delay < 0 {
		return SettlementOutcome{}, fmt.Errorf("%w: nack delay cannot be negative", ErrInvalidCommand)
	}
	resolver, err := s.newSettlementResolver(ctx, command.QueueName, command.GroupID)
	if err != nil {
		return SettlementOutcome{}, err
	}
	outcome := SettlementOutcome{Offsets: make([]uint64, 0, len(command.Offsets))}
	for _, offset := range command.Offsets {
		group, owner, err := resolver.resolve(offset)
		if err != nil {
			return s.partialSettlement(ctx, command.QueueName, command.GroupID, outcome, fmt.Errorf("nack offset %d: %w", offset, err))
		}
		if command.ConsumerID != "" && owner != "" && owner != command.ConsumerID {
			return s.partialSettlement(ctx, command.QueueName, command.GroupID, outcome, fmt.Errorf("nack offset %d: %w", offset, consumer.ErrConsumerNotFound))
		}
		if group.Mode == types.GroupModeStream && group.AutoCommitEnabled() {
			return s.partialSettlement(ctx, command.QueueName, command.GroupID, outcome,
				fmt.Errorf("nack offset %d: %w", offset, consumer.ErrNackNotSupportedForStream))
		}
		if err := s.consumers.NackWithDelay(ctx, command.QueueName, group.ID, owner, offset, command.Delay); err != nil {
			return s.partialSettlement(ctx, command.QueueName, command.GroupID, outcome, fmt.Errorf("nack offset %d: %w", offset, err))
		}
		s.records.metrics.RecordNack()
		outcome.Offsets = append(outcome.Offsets, offset)
	}
	return s.finishSettlement(ctx, command.QueueName, command.GroupID, outcome)
}

// Reject applies a dead-letter rejection command.
func (s *stateMachine) Reject(ctx context.Context, command RejectCommand) (SettlementOutcome, error) {
	if err := validateSettlementCommand(command.QueueName, command.Offsets); err != nil {
		return SettlementOutcome{}, err
	}
	resolver, err := s.newSettlementResolver(ctx, command.QueueName, command.GroupID)
	if err != nil {
		return SettlementOutcome{}, err
	}
	outcome := SettlementOutcome{Offsets: make([]uint64, 0, len(command.Offsets))}
	for _, offset := range command.Offsets {
		group, owner, err := resolver.resolve(offset)
		if err != nil {
			return s.partialSettlement(ctx, command.QueueName, command.GroupID, outcome, fmt.Errorf("reject offset %d: %w", offset, err))
		}
		if command.ConsumerID != "" && owner != "" && owner != command.ConsumerID {
			return s.partialSettlement(ctx, command.QueueName, command.GroupID, outcome, fmt.Errorf("reject offset %d: %w", offset, consumer.ErrConsumerNotFound))
		}
		if group.Mode == types.GroupModeStream && group.AutoCommitEnabled() {
			err = s.records.rejectStream(ctx, command.QueueName, group, offset, command.Reason)
		} else {
			err = s.consumers.Reject(ctx, command.QueueName, group.ID, owner, offset, command.Reason)
		}
		if err != nil {
			return s.partialSettlement(ctx, command.QueueName, command.GroupID, outcome, fmt.Errorf("reject offset %d: %w", offset, err))
		}
		if group.Mode != types.GroupModeStream {
			s.records.metrics.RecordReject()
		}
		outcome.Offsets = append(outcome.Offsets, offset)
	}
	return s.finishSettlement(ctx, command.QueueName, command.GroupID, outcome)
}

// Claim transfers idle pending records to the command's consumer.
func (s *stateMachine) Claim(ctx context.Context, command ClaimCommand) (ClaimOutcome, error) {
	if err := validateConsumerCommand(command.QueueName, command.GroupID, command.ConsumerID); err != nil {
		return ClaimOutcome{}, err
	}
	if command.MinIdle < 0 {
		return ClaimOutcome{}, fmt.Errorf("%w: minimum idle time cannot be negative", ErrInvalidCommand)
	}
	messages, err := s.consumers.ClaimPendingBatch(ctx, command.QueueName, command.GroupID, command.ConsumerID, command.MinIdle, command.Limit)
	if err != nil {
		return ClaimOutcome{}, err
	}
	outcome := ClaimOutcome{Messages: messages, Offsets: make([]uint64, len(messages))}
	for i, message := range messages {
		outcome.Offsets[i] = message.BrokerMeta.Queue.Offset
	}
	return outcome, nil
}

// Seek resolves a bounded queue offset.
func (s *stateMachine) Seek(ctx context.Context, command SeekCommand) (SeekOutcome, error) {
	if command.QueueName == "" {
		return SeekOutcome{}, fmt.Errorf("%w: queue name is required", ErrInvalidCommand)
	}
	head, err := s.queueStore.Head(ctx, command.QueueName)
	if err != nil {
		return SeekOutcome{}, err
	}
	tail, err := s.queueStore.Tail(ctx, command.QueueName)
	if err != nil {
		return SeekOutcome{}, err
	}
	switch command.Kind {
	case SeekOffset:
		offset := command.Offset
		if offset < head {
			offset = head
		}
		if offset > tail {
			offset = tail
		}
		return SeekOutcome{Offset: offset}, nil
	case SeekTimestamp:
		// Start from the time index rather than from head. Scanning from head
		// decodes every envelope in the queue, which on the client-facing seek
		// means a timestamp far in the future reads the whole log.
		if s.timeIndex == nil {
			return SeekOutcome{}, fmt.Errorf("%w: queue store provides no time index", ErrInvalidCommand)
		}
		offset, err := s.timeIndex.OffsetByTime(ctx, command.QueueName, command.Timestamp)
		if err != nil {
			return SeekOutcome{}, err
		}
		// The index is interval-based, so it lands at or before the first
		// matching record; the scan below covers the remaining distance.
		if offset < head {
			offset = head
		}
		for offset < tail {
			batch, err := s.queueStore.ReadBatch(ctx, command.QueueName, offset, 128)
			if err != nil {
				if errors.Is(err, storage.ErrOffsetOutOfRange) {
					break
				}
				return SeekOutcome{}, err
			}
			if len(batch) == 0 {
				break
			}
			for _, envelope := range batch {
				if !envelope.BrokerMeta.Queue.CreatedAt.Before(command.Timestamp) {
					outcome := SeekOutcome{Offset: envelope.BrokerMeta.Queue.Offset, Timestamp: envelope.BrokerMeta.Queue.CreatedAt, ExactMatch: envelope.BrokerMeta.Queue.CreatedAt.Equal(command.Timestamp)}
					releaseEnvelopes(batch)
					return outcome, nil
				}
			}
			offset = batch[len(batch)-1].BrokerMeta.Queue.Offset + 1
			releaseEnvelopes(batch)
		}
		return SeekOutcome{Offset: tail, Timestamp: command.Timestamp}, nil
	default:
		return SeekOutcome{}, fmt.Errorf("%w: unsupported seek kind %q", ErrInvalidCommand, command.Kind)
	}
}

func releaseEnvelopes(envelopes []*message.Envelope) {
	for _, envelope := range envelopes {
		message.Release(envelope)
	}
}

// settlementResolver resolves the owning group for every offset in one command.
//
// The store lookup happens once per command rather than once per offset: with
// an explicit group ID that is a single read, and without one — the MQTT and
// AMQP adapter paths, which settle against whichever group holds the offset —
// a single listing. Both stores hand back live group objects, so the cached
// groups still reflect entries removed as the command proceeds; only the read
// is saved, not the state.
//
// Offsets are still resolved and applied one at a time, in request order, and
// the command still stops at the first failure. The outcome reports the settled
// prefix, so grouping offsets by owner and settling each owner's set together
// would let a later offset settle ahead of an earlier one that failed and make
// that prefix a lie.
type settlementResolver struct {
	groupStore storage.ConsumerGroupStore

	// group is the explicitly named group, resolved once.
	group *types.ConsumerGroup

	// candidates are every group on the queue, ordered by ID, used when no
	// group was named. stream is the first stream-mode group among them.
	candidates []*types.ConsumerGroup
	stream     *types.ConsumerGroup
}

func (s *stateMachine) newSettlementResolver(ctx context.Context, queueName, groupID string) (*settlementResolver, error) {
	resolver := &settlementResolver{groupStore: s.groupStore}
	if groupID != "" {
		group, err := s.groupStore.GetConsumerGroup(ctx, queueName, groupID)
		if err != nil {
			return nil, err
		}
		resolver.group = group
		return resolver, nil
	}

	groups, err := s.groupStore.ListConsumerGroups(ctx, queueName)
	if err != nil {
		return nil, err
	}
	slices.SortFunc(groups, func(a, b *types.ConsumerGroup) int { return cmp.Compare(a.ID, b.ID) })
	resolver.candidates = groups
	for _, group := range groups {
		if group.Mode == types.GroupModeStream {
			resolver.stream = group
			break
		}
	}
	return resolver, nil
}

// resolve names the group and owning consumer for one offset. The pending
// lookup runs per offset because settling an earlier one changes it.
func (r *settlementResolver) resolve(offset uint64) (*types.ConsumerGroup, string, error) {
	if r.group != nil {
		if r.group.Mode == types.GroupModeStream {
			if !r.group.AutoCommitEnabled() {
				_, owner := r.group.FindPending(offset)
				if owner == "" {
					return nil, "", consumer.ErrMessageNotPending
				}
				return r.group, owner, nil
			}
			return r.group, "", nil
		}
		_, owner := r.group.FindPending(offset)
		if owner == "" {
			return nil, "", consumer.ErrMessageNotPending
		}
		return r.group, owner, nil
	}

	// Pending lookup first, and across stream groups too: a manual-commit stream
	// group settles through its pending list like a queue group does, so
	// skipping stream groups here hid the one group actually holding the offset.
	for _, group := range r.candidates {
		if _, owner := group.FindPending(offset); owner != "" {
			return group, owner, nil
		}
	}
	if r.stream != nil {
		return r.stream, "", nil
	}
	return nil, "", consumer.ErrMessageNotPending
}

func (s *stateMachine) ackStream(ctx context.Context, group *types.ConsumerGroup, offset uint64) error {
	// Through the consumer manager, which owns the group lock. Reading the
	// cursor here and writing the store directly is a read-modify-write with
	// nothing serialising it: two acknowledgements both read the old position
	// and the lower one lands last, moving the committed offset backwards and
	// redelivering messages that were already settled.
	return s.consumers.AdvanceCommitted(ctx, group.QueueName, group.ID, offset+1)
}

// partialSettlement enriches an outcome that is being returned with an error so
// the settled prefix still reports the group cursor it left behind. Without it a
// caller learns which offsets settled but not where the group now stands.
func (s *stateMachine) partialSettlement(ctx context.Context, queueName, groupID string, outcome SettlementOutcome, cause error) (SettlementOutcome, error) {
	if len(outcome.Offsets) == 0 || groupID == "" {
		return outcome, cause
	}
	if group, err := s.groupStore.GetConsumerGroup(ctx, queueName, groupID); err == nil {
		cursor := group.CursorView()
		outcome.Cursor = cursor.Cursor
		outcome.Committed = cursor.Committed
	}
	return outcome, cause
}

func (s *stateMachine) finishSettlement(ctx context.Context, queueName, groupID string, outcome SettlementOutcome) (SettlementOutcome, error) {
	if len(outcome.Offsets) > 0 {
		s.records.delivery.Schedule(queueName)
	}
	if groupID == "" {
		return outcome, nil
	}
	group, err := s.groupStore.GetConsumerGroup(ctx, queueName, groupID)
	if err != nil {
		return outcome, err
	}
	cursor := group.CursorView()
	outcome.Cursor = cursor.Cursor
	outcome.Committed = cursor.Committed
	return outcome, nil
}

func validateConsumerCommand(queueName, groupID, consumerID string) error {
	if queueName == "" || groupID == "" || consumerID == "" {
		return fmt.Errorf("%w: queue name, group id, and consumer id are required", ErrInvalidCommand)
	}
	return nil
}

func validateSettlementCommand(queueName string, offsets []uint64) error {
	if queueName == "" {
		return fmt.Errorf("%w: queue name is required", ErrInvalidCommand)
	}
	if len(offsets) == 0 {
		return fmt.Errorf("%w: at least one offset is required", ErrInvalidCommand)
	}
	return nil
}
