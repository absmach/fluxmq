// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package queue

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/absmach/fluxmq/message"
	"github.com/absmach/fluxmq/queue/consumer"
	"github.com/absmach/fluxmq/queue/storage"
	"github.com/absmach/fluxmq/queue/types"
)

// ErrInvalidCommand identifies a structurally invalid queue command.
var ErrInvalidCommand = errors.New("invalid queue command")

// AppendCommand appends one or more messages to exactly one named queue.
type AppendCommand struct {
	QueueName               string
	Messages                []types.PublishRequest
	AtomicBatch             bool
	RequireProtectedDurable bool
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
type stateMachine struct {
	manager    *Manager
	queueStore storage.QueueStore
	groupStore storage.ConsumerGroupStore
	consumers  *consumer.Manager
}

func newStateMachine(manager *Manager) *stateMachine {
	return &stateMachine{
		manager:    manager,
		queueStore: manager.queueStore,
		groupStore: manager.groupStore,
		consumers:  manager.consumerManager,
	}
}

func newConsumerStateMachine(queueStore storage.QueueStore, groupStore storage.ConsumerGroupStore, consumers *consumer.Manager) *stateMachine {
	return &stateMachine{queueStore: queueStore, groupStore: groupStore, consumers: consumers}
}

// StateMachine returns the manager's canonical command processor.
func (m *Manager) StateMachine() CommandProcessor { return m.stateMachine }

// Append applies an exact single-queue append command.
func (s *stateMachine) Append(ctx context.Context, command AppendCommand) (AppendOutcome, error) {
	if s == nil || s.manager == nil {
		return AppendOutcome{}, fmt.Errorf("%w: append runtime is unavailable", ErrInvalidCommand)
	}
	if command.QueueName == "" {
		return AppendOutcome{}, fmt.Errorf("%w: queue name is required", ErrInvalidCommand)
	}
	// An empty append is rejected rather than reported as a success at offset 0.
	// Offset 0 is a valid offset, so "nothing to do" and "wrote at offset 0"
	// would otherwise be indistinguishable to the caller.
	if len(command.Messages) == 0 {
		return AppendOutcome{}, fmt.Errorf("%w: at least one message is required", ErrInvalidCommand)
	}
	if command.RequireProtectedDurable {
		if len(command.Messages) != 1 || command.AtomicBatch {
			return AppendOutcome{}, fmt.Errorf("%w: protected durable append requires exactly one message", ErrInvalidCommand)
		}
		offset, createdAt, err := s.manager.publishToDurableStream(ctx, command.QueueName, command.Messages[0])
		if err != nil {
			return AppendOutcome{}, err
		}
		return AppendOutcome{FirstOffset: offset, LastOffset: offset, Count: 1, Timestamp: createdAt}, nil
	}
	if len(command.Messages) == 1 && !command.AtomicBatch {
		offset, createdAt, err := s.manager.appendToQueue(ctx, command.QueueName, command.Messages[0])
		if err != nil {
			return AppendOutcome{}, err
		}
		return AppendOutcome{FirstOffset: offset, LastOffset: offset, Count: 1, Timestamp: createdAt}, nil
	}

	first, count, lastCreatedAt, err := s.manager.appendBatchToQueue(ctx, command.QueueName, command.Messages)
	if err != nil {
		return AppendOutcome{}, err
	}
	last := first
	if count > 0 {
		last += uint64(count - 1)
	}
	return AppendOutcome{FirstOffset: first, LastOffset: last, Count: count, Timestamp: lastCreatedAt}, nil
}

func (s *stateMachine) appendResolved(ctx context.Context, queueName string, publish types.PublishRequest, config *types.QueueConfig) (AppendOutcome, error) {
	if config == nil {
		return AppendOutcome{}, fmt.Errorf("append to queue %q: missing queue configuration", queueName)
	}
	message := newQueuedMessage(publish, config)
	createdAt := message.Broker.Queue.CreatedAt
	offset, err := s.manager.appendConfiguredMessage(ctx, queueName, config, message)
	if err := s.manager.completeAppend(queueName, publish.Topic, offset, err); err != nil {
		return AppendOutcome{}, err
	}
	return AppendOutcome{FirstOffset: offset, LastOffset: offset, Count: 1, Timestamp: createdAt}, nil
}

// Consume selects and claims the next records for a consumer.
func (s *stateMachine) Consume(ctx context.Context, command ConsumeCommand) (ConsumeOutcome, error) {
	if err := validateConsumerCommand(command.QueueName, command.GroupID, command.ConsumerID); err != nil {
		return ConsumeOutcome{}, err
	}
	if s == nil || s.consumers == nil || s.groupStore == nil {
		return ConsumeOutcome{}, fmt.Errorf("%w: consume runtime is unavailable", ErrInvalidCommand)
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
	if s == nil || s.consumers == nil {
		return fmt.Errorf("%w: consume runtime is unavailable", ErrInvalidCommand)
	}
	return s.consumers.CommitStreamCursor(ctx, command.QueueName, command.GroupID, command.Offset)
}

// Ack applies an acknowledgement command.
func (s *stateMachine) Ack(ctx context.Context, command AckCommand) (SettlementOutcome, error) {
	if err := validateSettlementCommand(command.QueueName, command.Offsets); err != nil {
		return SettlementOutcome{}, err
	}
	outcome := SettlementOutcome{Offsets: make([]uint64, 0, len(command.Offsets))}
	for _, offset := range command.Offsets {
		group, owner, err := s.resolveSettlement(ctx, command.QueueName, command.GroupID, offset)
		if err != nil {
			return s.partialSettlement(ctx, command.QueueName, command.GroupID, outcome, fmt.Errorf("ack offset %d: %w", offset, err))
		}
		if command.ConsumerID != "" && group.Mode != types.GroupModeStream && owner != command.ConsumerID {
			return s.partialSettlement(ctx, command.QueueName, command.GroupID, outcome, fmt.Errorf("ack offset %d: %w", offset, consumer.ErrConsumerNotFound))
		}
		if group.Mode == types.GroupModeStream {
			err = s.ackStream(ctx, group, offset)
		} else {
			err = s.consumers.Ack(ctx, command.QueueName, group.ID, owner, offset)
		}
		if err != nil {
			return s.partialSettlement(ctx, command.QueueName, command.GroupID, outcome, fmt.Errorf("ack offset %d: %w", offset, err))
		}
		if s.manager != nil {
			s.manager.metrics.RecordAck(0)
			if fresh, readErr := s.groupStore.GetConsumerGroup(ctx, command.QueueName, group.ID); readErr == nil {
				s.manager.metrics.UpdatePELSize(uint64(fresh.PendingCount()))
			}
		}
		outcome.Offsets = append(outcome.Offsets, offset)
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
	outcome := SettlementOutcome{Offsets: make([]uint64, 0, len(command.Offsets))}
	for _, offset := range command.Offsets {
		group, owner, err := s.resolveSettlement(ctx, command.QueueName, command.GroupID, offset)
		if err != nil {
			return s.partialSettlement(ctx, command.QueueName, command.GroupID, outcome, fmt.Errorf("nack offset %d: %w", offset, err))
		}
		if command.ConsumerID != "" && group.Mode != types.GroupModeStream && owner != command.ConsumerID {
			return s.partialSettlement(ctx, command.QueueName, command.GroupID, outcome, fmt.Errorf("nack offset %d: %w", offset, consumer.ErrConsumerNotFound))
		}
		if group.Mode != types.GroupModeStream {
			if err := s.consumers.NackWithDelay(ctx, command.QueueName, group.ID, owner, offset, command.Delay); err != nil {
				return s.partialSettlement(ctx, command.QueueName, command.GroupID, outcome, fmt.Errorf("nack offset %d: %w", offset, err))
			}
		}
		if s.manager != nil {
			s.manager.metrics.RecordNack()
		}
		outcome.Offsets = append(outcome.Offsets, offset)
	}
	return s.finishSettlement(ctx, command.QueueName, command.GroupID, outcome)
}

// Reject applies a dead-letter rejection command.
func (s *stateMachine) Reject(ctx context.Context, command RejectCommand) (SettlementOutcome, error) {
	if err := validateSettlementCommand(command.QueueName, command.Offsets); err != nil {
		return SettlementOutcome{}, err
	}
	if s == nil || s.manager == nil {
		return SettlementOutcome{}, fmt.Errorf("%w: reject runtime is unavailable", ErrInvalidCommand)
	}
	outcome := SettlementOutcome{Offsets: make([]uint64, 0, len(command.Offsets))}
	for _, offset := range command.Offsets {
		group, owner, err := s.resolveSettlement(ctx, command.QueueName, command.GroupID, offset)
		if err != nil {
			return s.partialSettlement(ctx, command.QueueName, command.GroupID, outcome, fmt.Errorf("reject offset %d: %w", offset, err))
		}
		if command.ConsumerID != "" && group.Mode != types.GroupModeStream && owner != command.ConsumerID {
			return s.partialSettlement(ctx, command.QueueName, command.GroupID, outcome, fmt.Errorf("reject offset %d: %w", offset, consumer.ErrConsumerNotFound))
		}
		if group.Mode == types.GroupModeStream {
			err = s.manager.rejectStream(ctx, command.QueueName, group, offset, command.Reason)
		} else {
			err = s.consumers.Reject(ctx, command.QueueName, group.ID, owner, offset, command.Reason)
		}
		if err != nil {
			return s.partialSettlement(ctx, command.QueueName, command.GroupID, outcome, fmt.Errorf("reject offset %d: %w", offset, err))
		}
		if s.manager != nil && group.Mode != types.GroupModeStream {
			s.manager.metrics.RecordReject()
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
	if s == nil || s.consumers == nil {
		return ClaimOutcome{}, fmt.Errorf("%w: claim runtime is unavailable", ErrInvalidCommand)
	}
	messages, err := s.consumers.ClaimPendingBatch(ctx, command.QueueName, command.GroupID, command.ConsumerID, command.MinIdle, command.Limit)
	if err != nil {
		return ClaimOutcome{}, err
	}
	outcome := ClaimOutcome{Messages: messages, Offsets: make([]uint64, len(messages))}
	for i, message := range messages {
		outcome.Offsets[i] = message.Broker.Queue.Offset
	}
	return outcome, nil
}

// Seek resolves a bounded queue offset.
func (s *stateMachine) Seek(ctx context.Context, command SeekCommand) (SeekOutcome, error) {
	if s == nil || s.queueStore == nil {
		return SeekOutcome{}, fmt.Errorf("%w: seek runtime is unavailable", ErrInvalidCommand)
	}
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
		offset := head
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
				if !envelope.Broker.Queue.CreatedAt.Before(command.Timestamp) {
					outcome := SeekOutcome{Offset: envelope.Broker.Queue.Offset, Timestamp: envelope.Broker.Queue.CreatedAt, ExactMatch: envelope.Broker.Queue.CreatedAt.Equal(command.Timestamp)}
					releaseEnvelopes(batch)
					return outcome, nil
				}
			}
			offset = batch[len(batch)-1].Broker.Queue.Offset + 1
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

func (s *stateMachine) resolveSettlement(ctx context.Context, queueName, groupID string, offset uint64) (*types.ConsumerGroup, string, error) {
	if s == nil || s.groupStore == nil || s.consumers == nil {
		return nil, "", fmt.Errorf("%w: settlement runtime is unavailable", ErrInvalidCommand)
	}
	if groupID != "" {
		group, err := s.groupStore.GetConsumerGroup(ctx, queueName, groupID)
		if err != nil {
			return nil, "", err
		}
		if group.Mode == types.GroupModeStream {
			return group, "", nil
		}
		_, owner := group.FindPending(offset)
		if owner == "" {
			return nil, "", consumer.ErrMessageNotPending
		}
		return group, owner, nil
	}

	groups, err := s.groupStore.ListConsumerGroups(ctx, queueName)
	if err != nil {
		return nil, "", err
	}
	sort.Slice(groups, func(i, j int) bool { return groups[i].ID < groups[j].ID })
	var streamGroup *types.ConsumerGroup
	for _, group := range groups {
		if group.Mode == types.GroupModeStream {
			if streamGroup == nil {
				streamGroup = group
			}
			continue
		}
		_, owner := group.FindPending(offset)
		if owner != "" {
			return group, owner, nil
		}
	}
	if streamGroup != nil {
		return streamGroup, "", nil
	}
	return nil, "", consumer.ErrMessageNotPending
}

func (s *stateMachine) ackStream(ctx context.Context, group *types.ConsumerGroup, offset uint64) error {
	if !group.AutoCommit {
		return nil
	}
	cursor := group.CursorView()
	next := offset + 1
	if next > cursor.Cursor {
		return consumer.ErrInvalidOffset
	}
	if next <= cursor.Committed {
		return nil
	}
	return s.groupStore.UpdateCommitted(ctx, group.QueueName, group.ID, next)
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
	if s.manager != nil && len(outcome.Offsets) > 0 {
		s.manager.delivery.Schedule(queueName)
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
