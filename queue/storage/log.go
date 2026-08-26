// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package storage

import (
	"context"
	"errors"
	"time"

	"github.com/absmach/fluxmq/message"
	"github.com/absmach/fluxmq/queue/types"
)

// Queue storage errors.
var (
	ErrOffsetOutOfRange     = errors.New("offset out of range")
	ErrLogFull              = errors.New("log is full")
	ErrInvalidOffset        = errors.New("invalid offset")
	ErrConsumerGroupExists  = errors.New("consumer group already exists")
	ErrPendingEntryNotFound = errors.New("pending entry not found")

	// ErrDurabilityUnconfirmed reports that a write was accepted but its
	// durability barrier did not complete. The returned offset identifies the
	// accepted record; callers must retry by identity or establish a barrier
	// over that record, never blindly append it again.
	ErrDurabilityUnconfirmed = errors.New("write accepted but durability is unconfirmed")
)

var (
	ErrQueueNotFound      = errors.New("queue not found")
	ErrMessageNotFound    = errors.New("message not found")
	ErrConsumerNotFound   = errors.New("consumer not found")
	ErrQueueAlreadyExists = errors.New("queue already exists")
)

// Deduplication errors, reported by every DeduplicatingQueueStore so callers
// can distinguish "this store will not deduplicate" from an ordinary append
// failure without knowing which implementation they hold.
var (
	// ErrDeduplicationKeyRequired reports an AppendOnce with no key. Appending
	// without one cannot be deduplicated, and silently degrading to a plain
	// append would leave the caller believing otherwise.
	ErrDeduplicationKeyRequired = errors.New("deduplication key is required")

	// ErrDeduplicationUnsupported reports that a deduplicated append reached a
	// store or a replication path that cannot perform the check.
	ErrDeduplicationUnsupported = errors.New("deduplicated append is not supported")

	// ErrDeduplicationStateUnconfirmed reports that the record was accepted but
	// the durable identity index could not confirm it. Retrying the same key is
	// safe; appending under a different key is not.
	ErrDeduplicationStateUnconfirmed = errors.New("deduplication state is unconfirmed")
)

// QueueStore provides append-only log storage with offset-based access.
// Each queue has a single log where messages matching any of its topic patterns are stored.
type QueueStore interface {
	// Queue lifecycle
	CreateQueue(ctx context.Context, config types.QueueConfig) error
	GetQueue(ctx context.Context, queueName string) (*types.QueueConfig, error)
	UpdateQueue(ctx context.Context, config types.QueueConfig) error
	DeleteQueue(ctx context.Context, queueName string) error
	ListQueues(ctx context.Context) ([]types.QueueConfig, error)

	// FindMatchingQueues returns all queues whose topic patterns match the given topic.
	// This is used to route a published message to all relevant queues.
	FindMatchingQueues(ctx context.Context, topic string) ([]string, error)

	// Append adds a message to the end of a queue's log and returns the
	// assigned offset. A successful call takes ownership of msg. On error the
	// caller retains ownership and may retry or release it.
	Append(ctx context.Context, queueName string, msg *message.Envelope) (uint64, error)

	// AppendBatch adds multiple messages to a queue's log and returns the first
	// assigned offset. A successful call takes ownership of every envelope. On
	// error the caller retains ownership of the entire batch.
	AppendBatch(ctx context.Context, queueName string, msgs []*message.Envelope) (uint64, error)

	// Read retrieves an owned envelope. The caller must release it.
	Read(ctx context.Context, queueName string, offset uint64) (*message.Envelope, error)

	// ReadBatch returns owned envelopes in offset order. The caller must release
	// every element.
	ReadBatch(ctx context.Context, queueName string, startOffset uint64, limit int) ([]*message.Envelope, error)

	// Head returns the first valid offset in the queue (after truncation).
	Head(ctx context.Context, queueName string) (uint64, error)

	// Tail returns the next offset that will be assigned (one past the last message).
	Tail(ctx context.Context, queueName string) (uint64, error)

	// Truncate removes all messages with offset < minOffset.
	// Used for retention policy enforcement.
	Truncate(ctx context.Context, queueName string, minOffset uint64) error

	// Count returns the number of messages in the queue (tail - head).
	Count(ctx context.Context, queueName string) (uint64, error)
}

// QueueSnapshotReader streams one queue's records in ascending offset order
// from the view held open when it was opened. Close releases whatever the view
// holds and must be called.
type QueueSnapshotReader interface {
	// Head is the offset the captured log begins at.
	Head() uint64

	// Tail is one past the last offset the captured log holds.
	Tail() uint64

	// Next reports the next record and its offset, or ok false at the end of
	// the view. The caller takes ownership of the envelope and must release it.
	Next(ctx context.Context) (offset uint64, msg *message.Envelope, ok bool, err error)

	// Close releases the view.
	Close() error
}

// SnapshotableQueueStore is a queue store whose logs can be captured for a
// Raft snapshot and rebuilt from one.
//
// Messages are replicated state: they arrive through the Raft log like every
// other mutation, so a snapshot that carries only queue configs and consumer
// groups is incomplete. A follower that installs such a snapshot advances past
// the log entries the leader compacted and never acquires the records they
// carried, with nothing left to detect the loss.
//
// Reconstructing a queue needs the records and the offset its log begins at.
// Truncation moves that offset away from zero, and offsets are what consumers
// hold, so a queue rebuilt from zero hands every consumer the wrong record.
//
// Deduplication state is not carried separately: the key lives in the record
// (BrokerMeta.Transfer.ID), so the index is rebuilt from the records restored.
type SnapshotableQueueStore interface {
	QueueStore

	// OpenQueueSnapshot captures a stable view of a queue's log that can be
	// read after the store has moved on.
	//
	// It returns a reader rather than the records themselves because a snapshot
	// is taken on the raft goroutine, which cannot apply entries while it runs,
	// and is serialized afterwards. Materializing every retained record here
	// would stop writes for a whole scan of the queue and hold every payload at
	// once. What a store captures to make the view stable is its own business;
	// what it must not do is make that capture proportional to the log.
	OpenQueueSnapshot(ctx context.Context, queueName string) (QueueSnapshotReader, error)

	// RestoreQueue replaces any queue of this name with an empty log whose next
	// offset is head, so restored records keep the offsets they were written at.
	RestoreQueue(ctx context.Context, config types.QueueConfig, head uint64) error

	// RestoreRecord appends one record to a queue opened by RestoreQueue and
	// takes ownership of msg. Records must arrive in ascending offset order
	// with no gaps; an offset that does not continue the log is an error.
	RestoreRecord(ctx context.Context, queueName string, offset uint64, msg *message.Envelope) error

	// ResetForRestore drops the named queues and their records, reserved queues
	// included, so a snapshot can be laid down over them. A queue the snapshot
	// does not mention is one its group no longer has; leaving it behind would
	// keep this replica holding records nothing else does.
	//
	// Only the named queues are touched. One store backs every raft group in
	// the process along with the queues no group replicates, so clearing it
	// wholesale would delete state this caller does not own.
	ResetForRestore(ctx context.Context, queueNames []string) error
}

// DeduplicatingQueueStore appends at most one record per deduplication key.
//
// It exists so a transfer that must not duplicate — a dead-letter move, which
// appends to the destination before settling the source — can be retried after
// a crash or a failed settlement without producing a second record.
//
// The key must be derivable from the source coordinates rather than generated
// per attempt, so a retry computes the same key. It is persisted both in the
// record and in a durable derived index. The record remains authoritative;
// the index makes recovery bounded by the uncertain append suffix rather than
// the queue's retained history.
type DeduplicatingQueueStore interface {
	// AppendOnce appends msg unless a record with the same dedupeKey is already
	// present within the store's deduplication window, in which case it returns
	// that record's offset and reports deduplicated.
	//
	// Ownership follows QueueStore.Append, with one addition: a nil error takes
	// ownership of msg in the deduplicated case too, where the envelope is
	// released rather than stored. An error leaves ownership with the caller.
	AppendOnce(ctx context.Context, queueName, dedupeKey string, msg *message.Envelope) (offset uint64, deduplicated bool, err error)

	// AppendOnceAndSync is AppendOnce with the durability barrier of
	// DurableQueueStore.AppendAndSync: it returns only once the record it
	// appended is durable.
	//
	// A barrier that fails after the record is written returns
	// ErrDurabilityUnconfirmed and must not be reported as a plain failure and
	// forgotten: the retry would append the record twice.
	// Nor may it be reported as success: the caller settles its source on that
	// answer. Such an attempt returns an error, and a later attempt with the
	// same key establishes the barrier over the record that already exists
	// before reporting it deduplicated.
	//
	// A dead-letter transfer settles its source once the destination reports
	// success, so on a queue configured for fsync the destination has to be
	// durable before that success is reported — otherwise the settlement
	// outlives the record it was settling against. A store that can deduplicate
	// but cannot do so durably must not be used for such a queue, which is why
	// this lives on the same interface rather than in a separate optional one:
	// offering half the contract is what leaves a caller with no path that
	// provides both.
	AppendOnceAndSync(ctx context.Context, queueName, dedupeKey string, msg *message.Envelope) (offset uint64, deduplicated bool, err error)

	// DeduplicationWindow reports how far back AppendOnce can recognise a
	// repeated key, in records. Beyond it a retry appends again, so callers that
	// need a guarantee rather than a mitigation must retry within it. Zero means
	// every record retained by the queue is covered.
	DeduplicationWindow() int
}

// DurableQueueStore atomically appends and establishes a durability barrier for
// the exact record written to a single queue. Implementations must serialize
// segment rotation with the entire operation; Append followed by a separate
// active-segment sync does not satisfy this contract.
type DurableQueueStore interface {
	// AppendAndSync follows QueueStore.Append's ownership contract: success
	// transfers ownership to the store; errors leave ownership with the caller.
	AppendAndSync(ctx context.Context, queueName string, msg *message.Envelope) (uint64, error)

	// SupportsDurableSync reports whether AppendAndSync survives process and
	// machine crashes. Implementing the interface is not sufficient evidence:
	// a volatile store can order an append without making it durable, so
	// callers that acknowledge writes on behalf of a publisher must consult
	// this method before promising durability.
	SupportsDurableSync() bool
}

// ConsumerGroupStore manages cursor-based consumer groups with PEL tracking.
type ConsumerGroupStore interface {
	// CreateConsumerGroup creates a new consumer group for a queue.
	CreateConsumerGroup(ctx context.Context, group *types.ConsumerGroup) error

	// GetConsumerGroup retrieves a consumer group's state.
	GetConsumerGroup(ctx context.Context, queueName, groupID string) (*types.ConsumerGroup, error)

	// UpdateConsumerGroup updates a consumer group's state (cursor, PEL).
	UpdateConsumerGroup(ctx context.Context, group *types.ConsumerGroup) error

	// DeleteConsumerGroup removes a consumer group.
	DeleteConsumerGroup(ctx context.Context, queueName, groupID string) error

	// ListConsumerGroups lists all consumer groups for a queue.
	ListConsumerGroups(ctx context.Context, queueName string) ([]*types.ConsumerGroup, error)

	// AddPendingEntry adds an entry to a consumer's PEL.
	AddPendingEntry(ctx context.Context, queueName, groupID string, entry *types.PendingEntry) error

	// RemovePendingEntry removes an entry from a consumer's PEL.
	RemovePendingEntry(ctx context.Context, queueName, groupID, consumerID string, offset uint64) error

	// GetPendingEntries retrieves all pending entries for a consumer.
	GetPendingEntries(ctx context.Context, queueName, groupID, consumerID string) ([]*types.PendingEntry, error)

	// GetAllPendingEntries retrieves all pending entries for a group (across all consumers).
	GetAllPendingEntries(ctx context.Context, queueName, groupID string) ([]*types.PendingEntry, error)

	// TransferPendingEntry moves a pending entry from one consumer to another (work stealing).
	TransferPendingEntry(ctx context.Context, queueName, groupID string, offset uint64, fromConsumer, toConsumer string) error

	// UpdateCursor updates the cursor position.
	UpdateCursor(ctx context.Context, queueName, groupID string, cursor uint64) error

	// UpdateCommitted updates the committed offset.
	UpdateCommitted(ctx context.Context, queueName, groupID string, committed uint64) error

	// RegisterConsumer adds a consumer to a group.
	RegisterConsumer(ctx context.Context, queueName, groupID string, consumer *types.ConsumerInfo) error

	// UnregisterConsumer removes a consumer from a group.
	UnregisterConsumer(ctx context.Context, queueName, groupID, consumerID string) error

	// ListConsumers lists all consumers in a group.
	ListConsumers(ctx context.Context, queueName, groupID string) ([]*types.ConsumerInfo, error)
}

// PendingEntryRequeuer is the optional atomic pending-entry mutation used by
// nack. attemptedAt is the logical last delivery attempt; setting it relative
// to the visibility timeout supports both immediate and delayed redelivery
// without removing the entry from its current owner's PEL.
type PendingEntryRequeuer interface {
	RequeuePendingEntry(ctx context.Context, queueName, groupID, consumerID string, offset uint64, attemptedAt time.Time) error
}

// ConsumerStore manages consumer group state.
type ConsumerStore interface {
	RegisterConsumer(ctx context.Context, consumer *types.Consumer) error
	UnregisterConsumer(ctx context.Context, queueName, groupID, consumerID string) error
	GetConsumer(ctx context.Context, queueName, groupID, consumerID string) (*types.Consumer, error)
	ListConsumers(ctx context.Context, queueName, groupID string) ([]*types.Consumer, error)
	ListGroups(ctx context.Context, queueName string) ([]string, error)
	UpdateHeartbeat(ctx context.Context, queueName, groupID, consumerID string, timestamp time.Time) error
}
