// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package queue

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/absmach/fluxmq/message"
	"github.com/absmach/fluxmq/queue/consumer"
	"github.com/absmach/fluxmq/queue/raft"
	"github.com/absmach/fluxmq/queue/storage"
	"github.com/absmach/fluxmq/queue/types"
)

// appendResolved appends one record to a queue whose configuration the caller
// has already resolved, skipping the lookup the command path performs.
//
// It takes a copy rather than a pointer, and lives on the core rather than on
// the command surface: a resolved configuration is an internal shortcut for the
// fanout path, and putting it in a public command would let a caller inject a
// configuration the core never validated, and freeze QueueConfig's shape into
// that command.
func (c *recordCore) appendResolved(ctx context.Context, queueName string, msg *message.Envelope, config types.QueueConfig) (AppendOutcome, error) {
	queued := newQueuedRecord(msg, queueName, &config)
	createdAt := queued.BrokerMeta.Queue.CreatedAt

	offset, err := c.appendConfiguredMessage(ctx, queueName, &config, queued)
	if err := c.completeAppend(queueName, queued.Topic, offset, err); err != nil {
		return AppendOutcome{}, err
	}

	return AppendOutcome{FirstOffset: offset, LastOffset: offset, Count: 1, Timestamp: createdAt}, nil
}

// unmanagedServices supplies intentionally absent optional capabilities to
// focused core tests. Production construction uses queueControl.
type unmanagedServices struct{}

func (unmanagedServices) protectedQueueContract(string) (types.QueueConfig, bool) {
	return types.QueueConfig{}, false
}

func (unmanagedServices) replicationWriteReadiness(string) error { return ErrReplicationUnavailable }

func (unmanagedServices) CreateQueue(context.Context, types.QueueConfig) error {
	return ErrDLQDisabled
}

func (unmanagedServices) replicationCoordinator() queueRaftCoordinator { return nil }

// newRecordCore builds a complete core. Every field is supplied here and none
// is assigned afterwards: a half-built core that is filled in later is what
// produced the degraded construction modes this replaced, and what let comments
// describe a dependency the code did not have.
func newRecordCore(
	queueStore storage.QueueStore,
	groupStore storage.ConsumerGroupStore,
	config Config,
	metrics *consumer.Metrics,
	logger *slog.Logger,
	schedule deliveryScheduler,
	services recordServices,
	ackDurability AckDurability,
	storeSupportsSync bool,
) *recordCore {
	return &recordCore{
		queueStore:        queueStore,
		groupStore:        groupStore,
		delivery:          schedule,
		metrics:           metrics,
		logger:            logger,
		config:            config,
		ackDurability:     ackDurability,
		storeSupportsSync: storeSupportsSync,
		services:          services,
	}
}

// durableStore returns the queue store only when it can make a single append
// durable atomically. A protected queue that cannot get that guarantee must
// fail rather than accept a write it cannot honour.
func (c *recordCore) durableStore() (storage.DurableQueueStore, error) {
	durableStore, ok := c.queueStore.(storage.DurableQueueStore)
	if !ok || !durableStore.SupportsDurableSync() {
		return nil, fmt.Errorf("%w: protected queues require durable sync support with atomic append", ErrDurableSyncUnsupported)
	}
	return durableStore, nil
}

// recordCore owns record semantics: what it takes to put a record in a queue,
// how durable that has to be before the write is acknowledged, and where a
// record goes when it cannot be delivered.
//
// It exists as its own type because Manager is a routing and lifecycle facade —
// topic resolution, subscriptions, heartbeats, cluster forwarding, background
// loops — and while these functions hung off Manager, neither could be read or
// changed without the other. The dependencies below are what record semantics
// actually needs; everything else Manager owns stays there.
//
// deliveryScheduler wakes delivery for a queue once a record has landed.
//
// An interface rather than *DeliveryEngine so the core states what it needs —
// a wake-up — instead of holding the component that performs delivery.
type deliveryScheduler interface {
	Schedule(queueName string)
}

// recordServices is the Manager-independent policy surface consulted by record
// semantics. queueControl implements it in production; the facade does not.
type recordServices interface {
	// protectedQueueContract resolves an exact internal publisher's contract.
	protectedQueueContract(queueName string) (types.QueueConfig, bool)
	// replicationWriteReadiness reports whether this node may write to a
	// replicated queue.
	replicationWriteReadiness(queueName string) error
	// CreateQueue creates a queue on demand, for a dead-letter destination that
	// does not exist yet.
	CreateQueue(ctx context.Context, config types.QueueConfig) error
	// replicationCoordinator resolves the coordinator, which can be installed
	// after the core is built.
	replicationCoordinator() queueRaftCoordinator
}

type recordCore struct {
	queueStore        storage.QueueStore
	groupStore        storage.ConsumerGroupStore
	delivery          deliveryScheduler
	metrics           *consumer.Metrics
	logger            *slog.Logger
	config            Config
	ackDurability     AckDurability
	storeSupportsSync bool

	services recordServices
}

func (c *recordCore) appendToQueue(ctx context.Context, queueName string, msg *message.Envelope) (uint64, time.Time, error) {
	queueConfig, err := c.queueStore.GetQueue(ctx, queueName)
	if err != nil {
		return 0, time.Time{}, err
	}
	if queueConfig == nil {
		return 0, time.Time{}, storage.ErrQueueNotFound
	}
	if queueConfig.Replication.Enabled {
		if err := c.services.replicationWriteReadiness(queueName); err != nil {
			return 0, time.Time{}, err
		}
		if !c.services.replicationCoordinator().IsLeaderForQueue(queueName) {
			return 0, time.Time{}, WithFailure(
				fmt.Errorf("%w: queue %q is not led by this node", ErrReplicationUnavailable, queueName),
				Failure{
					Code:       ErrorCodeUnavailable,
					Retryable:  true,
					Leader:     LeaderNotLocal,
					Durability: DurabilityNotAttempted,
				},
			)
		}
	}

	record := newQueuedRecord(msg, queueName, queueConfig)
	// Read the assigned timestamp before the append: a successful append
	// transfers ownership of the record to the store, which may release it.
	createdAt := record.BrokerMeta.Queue.CreatedAt
	topic := record.Topic
	offset, err := c.appendConfiguredMessage(ctx, queueName, queueConfig, record)
	if err := c.completeAppend(queueName, topic, offset, err); err != nil {
		return 0, time.Time{}, err
	}
	c.delivery.Schedule(queueName)
	return offset, createdAt, nil
}

func (c *recordCore) appendBatchToQueue(ctx context.Context, queueName string, envelopes []*message.Envelope) (uint64, uint32, time.Time, error) {
	if len(envelopes) == 0 {
		return 0, 0, time.Time{}, nil
	}

	queueConfig, err := c.queueStore.GetQueue(ctx, queueName)
	if err != nil {
		return 0, 0, time.Time{}, err
	}
	if queueConfig == nil {
		return 0, 0, time.Time{}, storage.ErrQueueNotFound
	}
	if queueConfig.Replication.Enabled {
		return 0, 0, time.Time{}, ErrAtomicBatchReplicationUnsupported
	}
	if queueConfig.Durable && c.ackDurabilityFor(queueConfig) == AckDurabilityFsync {
		return 0, 0, time.Time{}, ErrAtomicBatchDurabilityUnsupported
	}

	messages := make([]*message.Envelope, len(envelopes))
	for i, envelope := range envelopes {
		messages[i] = newQueuedRecord(envelope, queueName, queueConfig)
	}

	// Read the last record's timestamp before the append: a successful append
	// transfers ownership of every envelope to the store.
	lastCreatedAt := messages[len(messages)-1].BrokerMeta.Queue.CreatedAt

	firstOffset, err := c.queueStore.AppendBatch(ctx, queueName, messages)
	if err != nil {
		releaseEnvelopes(messages)
		return 0, 0, time.Time{}, fmt.Errorf("append batch to queue %q: %w", queueName, err)
	}
	c.logger.Debug("message batch published",
		slog.String("queue", queueName),
		slog.Uint64("first_offset", firstOffset),
		slog.Int("count", len(messages)))
	c.delivery.Schedule(queueName)
	return firstOffset, uint32(len(messages)), lastCreatedAt, nil
}

func (c *recordCore) publishToDurableStream(ctx context.Context, queueName string, msg *message.Envelope) (uint64, time.Time, error) {
	expected, protected := c.services.protectedQueueContract(queueName)
	if !protected {
		return 0, time.Time{}, fmt.Errorf("%w: %s", ErrQueueNotProtected, queueName)
	}

	queueConfig, err := c.queueStore.GetQueue(ctx, queueName)
	if err != nil {
		return 0, time.Time{}, fmt.Errorf("get exact stream %q: %w", queueName, err)
	}
	if queueConfig == nil {
		return 0, time.Time{}, fmt.Errorf("get exact stream %q: %w", queueName, storage.ErrQueueNotFound)
	}
	if err := protectedQueueContractMismatch(expected, *queueConfig); err != nil {
		return 0, time.Time{}, fmt.Errorf("%w: %v", ErrProtectedQueueContractDrift, err)
	}
	if !queueConfig.Reserved {
		return 0, time.Time{}, fmt.Errorf("%w: %s", ErrQueueNotReserved, queueName)
	}
	if queueConfig.Type != types.QueueTypeStream {
		return 0, time.Time{}, fmt.Errorf("%w: %s", ErrQueueNotStream, queueName)
	}
	if !queueConfig.Durable {
		return 0, time.Time{}, fmt.Errorf("%w: %s", ErrQueueNotDurable, queueName)
	}
	if queueConfig.Replication.Enabled {
		return 0, time.Time{}, fmt.Errorf("%w: %s", ErrDurableReplicatedStreamUnsupported, queueName)
	}
	durableStore, err := c.durableStore()
	if err != nil {
		return 0, time.Time{}, fmt.Errorf("%w: %s", err, queueName)
	}
	if payloadSize := len(msg.PayloadBytes()); queueConfig.MaxMessageSize <= 0 || int64(payloadSize) > queueConfig.MaxMessageSize {
		return 0, time.Time{}, fmt.Errorf(
			"%w: queue %s accepts at most %d bytes, got %d",
			ErrQueueMessageTooLarge,
			queueName,
			queueConfig.MaxMessageSize,
			payloadSize,
		)
	}

	record := newQueuedRecord(msg, queueName, queueConfig)
	// Read the assigned timestamp before the append: a successful append
	// transfers ownership of the record to the store.
	createdAt := record.BrokerMeta.Queue.CreatedAt
	topic := record.Topic
	offset, err := durableStore.AppendAndSync(ctx, queueName, record)
	if err != nil {
		message.Release(record)
	}
	if err := c.completeAppend(queueName, topic, offset, err); err != nil {
		return 0, time.Time{}, err
	}
	c.delivery.Schedule(queueName)
	return offset, createdAt, nil
}

func (c *recordCore) appendConfiguredMessage(ctx context.Context, queueName string, queueConfig *types.QueueConfig, msg *message.Envelope) (uint64, error) {
	replicated := queueConfig.Replication.Enabled
	if replicated {
		// Raft serializes the caller's envelope and applies a decoded copy. The
		// original is no longer needed after ApplyAppendWithOptions returns.
		defer message.Release(msg)
		if err := c.services.replicationWriteReadiness(queueName); err != nil {
			return 0, err
		}
		if !c.services.replicationCoordinator().IsLeaderForQueue(queueName) {
			return 0, fmt.Errorf("%w: queue %q is not led by this node", ErrReplicationUnavailable, queueName)
		}
		syncMode := queueConfig.Replication.Mode != types.ReplicationAsync
		return c.services.replicationCoordinator().ApplyAppendWithOptions(ctx, queueName, msg, raft.ApplyOptions{
			SyncMode:   &syncMode,
			AckTimeout: queueConfig.Replication.AckTimeout,
		})
	}
	offset, err := c.appendWithAckDurability(ctx, queueName, queueConfig, msg)
	if err != nil {
		message.Release(msg)
	}
	return offset, err
}

// appendWithAckDurability writes one message under the configured
// acknowledgement policy. Under fsync a durable queue's append reaches the disk
// before completeAppend can report success, which is what makes the publisher's
// acknowledgement mean something after a crash. Ephemeral queues are never
// synced: they do not survive a restart either way, so paying for the barrier
// would buy nothing.
func (c *recordCore) appendWithAckDurability(ctx context.Context, queueName string, queueConfig *types.QueueConfig, msg *message.Envelope) (uint64, error) {
	if c.ackDurabilityFor(queueConfig) != AckDurabilityFsync || !queueConfig.Durable {
		return c.queueStore.Append(ctx, queueName, msg)
	}
	durableStore, err := c.durableStore()
	if err != nil {
		return 0, err
	}
	return durableStore.AppendAndSync(ctx, queueName, msg)
}

func (c *recordCore) completeAppend(queueName, topic string, offset uint64, err error) error {
	if err != nil {
		c.logger.Warn("failed to append to queue",
			slog.String("queue", queueName),
			slog.String("topic", topic),
			slog.String("error", err.Error()))
		return fmt.Errorf("append to queue %q: %w", queueName, err)
	}

	c.logger.Debug("message published",
		slog.String("queue", queueName),
		slog.String("topic", topic),
		slog.Uint64("offset", offset))
	return nil
}

// newQueuedRecord derives the record a queue stores from a borrowed envelope.
//
// The caller keeps its envelope. The clone shares the payload buffer by
// reference rather than copying it, which is the point of the change that
// removed the flattened request this used to build: that shape forced a second
// full copy of every payload and a deep copy of every header on the durable
// publish path. A successful append takes ownership of the clone, never of the
// caller's envelope.
func newQueuedRecord(msg *message.Envelope, queueName string, queueConfig *types.QueueConfig) *message.Envelope {
	now := time.Now()
	record := msg.Clone()
	if record.Topic == "" {
		record.Topic = queueName
	}
	// Properties are re-filtered on the record itself: an ingress may hand over
	// an envelope whose property map still holds reserved names, and the stored
	// record is what every later reader sees.
	record.PublisherMeta.Properties = message.FilterUserProperties(record.PublisherMeta.Properties)
	// A stored record keeps only the publication timestamps from the delivery
	// namespace. The rest of it — packet id, QoS, retain, inflight state — is
	// the ingress connection's transaction state, not the record's.
	record.BrokerMeta.Delivery = message.DeliveryMetadata{
		PublishedAt: msg.BrokerMeta.Delivery.PublishedAt,
		ExpiresAt:   msg.BrokerMeta.Delivery.ExpiresAt,
	}
	record.BrokerMeta.Queue = message.QueueMetadata{
		State:     message.QueueStateQueued,
		CreatedAt: now,
	}
	if queueConfig.MessageTTL > 0 {
		record.BrokerMeta.Queue.ExpiresAt = now.Add(queueConfig.MessageTTL)
	}
	if expiry := msg.BrokerMeta.Delivery.ExpiresAt; !expiry.IsZero() &&
		(record.BrokerMeta.Queue.ExpiresAt.IsZero() || expiry.Before(record.BrokerMeta.Queue.ExpiresAt)) {
		record.BrokerMeta.Queue.ExpiresAt = expiry
	}
	return record
}

// rejectStream handles reject for stream-mode consumer groups.
// Stream queues don't have PEL, so reject advances the cursor past the
// rejected message only after its DLQ append has succeeded.
func (c *recordCore) rejectStream(ctx context.Context, queueName string, group *types.ConsumerGroup, offset uint64, reason string) error {
	msg, err := c.queueStore.Read(ctx, queueName, offset)
	if err != nil {
		return err
	}
	defer message.Release(msg)
	deliveryCount := max(msg.BrokerMeta.Queue.RetryCount+1, 1)
	if err := c.moveToDLQ(ctx, queueName, group.ID, msg, offset, deliveryCount, reason, c.config.DLQTopicPrefix); err != nil {
		return err
	}

	cursor := group.CursorView()
	next := offset + 1
	if next > cursor.Cursor {
		if err := c.groupStore.UpdateCursor(ctx, queueName, group.ID, next); err != nil {
			return err
		}
		if err := c.groupStore.UpdateCommitted(ctx, queueName, group.ID, next); err != nil {
			return err
		}
	}

	c.logger.Info("stream message rejected",
		slog.String("queue", queueName),
		slog.String("group", group.ID),
		slog.Uint64("offset", offset),
		slog.String("reason", reason))
	c.metrics.RecordReject()
	c.delivery.Schedule(queueName)
	return nil
}

// ackDurabilityFor resolves the policy for one queue: what the queue asks for,
// falling back to the broker-wide default. A queue that carries records nobody
// may lose can demand the barrier without taxing the queues beside it.
//
// A queue asking for fsync on a store that cannot sync one append is refused at
// load, so the resolved policy here is always one the store can honour.
func (c *recordCore) ackDurabilityFor(queueConfig *types.QueueConfig) AckDurability {
	if strings.TrimSpace(queueConfig.AckDurability) == "" {
		return c.ackDurability
	}
	if !c.storeSupportsSync {
		return AckDurabilityBuffered
	}
	return NormalizeAckDurability(AckDurability(queueConfig.AckDurability))
}

// moveToDLQ publishes a poison message to the dead-letter queue. It returns
// success only after the destination append has completed, allowing callers to
// keep the source delivery pending on every failure.
func (c *recordCore) moveToDLQ(ctx context.Context, queueName, groupID string, msg *message.Envelope, sourceOffset uint64, deliveryCount int, reason, dlqPrefix string) error {
	queueCfg, err := c.queueStore.GetQueue(ctx, queueName)
	if err != nil {
		return fmt.Errorf("get source queue for DLQ transfer: %w", err)
	}
	if queueCfg == nil || !queueCfg.DLQConfig.Enabled {
		return fmt.Errorf("%w: %s", ErrDLQDisabled, queueName)
	}
	if dlqPrefix == "" {
		dlqPrefix = "$dlq/"
	}

	dlqTopic := queueCfg.DLQConfig.Topic
	if dlqTopic == "" {
		dlqTopic = dlqPrefix + queueName
	}

	dlqQueueName := dlqTopic
	dlqCfg, err := c.queueStore.GetQueue(ctx, dlqQueueName)
	if errors.Is(err, storage.ErrQueueNotFound) {
		newDLQCfg := types.DefaultQueueConfig(dlqQueueName, dlqTopic+"/#")
		newDLQCfg.DLQConfig.Enabled = false // prevent DLQ chains
		newDLQCfg.MessageTTL = 0            // DLQ messages don't expire
		if createErr := c.services.CreateQueue(ctx, newDLQCfg); createErr != nil && !errors.Is(createErr, storage.ErrQueueAlreadyExists) {
			return fmt.Errorf("create DLQ queue %q: %w", dlqQueueName, createErr)
		}
		dlqCfg, err = c.queueStore.GetQueue(ctx, dlqQueueName)
	}
	if err != nil {
		return fmt.Errorf("get DLQ queue %q: %w", dlqQueueName, err)
	}
	if dlqCfg == nil {
		return fmt.Errorf("get DLQ queue %q: %w", dlqQueueName, storage.ErrQueueNotFound)
	}

	// One clock reading for the whole transfer, in UTC like every other
	// broker-owned timestamp, so the record and its transfer cannot disagree.
	now := time.Now().UTC()
	transferID := dlqTransferID(queueName, groupID, sourceOffset)
	dlqMsg := msg.Clone()
	dlqMsg.Topic = dlqTopic
	dlqMsg.BrokerMeta.Delivery = message.DeliveryMetadata{}
	dlqMsg.BrokerMeta.Source.Topic = msg.Topic
	dlqMsg.BrokerMeta.Queue = message.QueueMetadata{
		State:     message.QueueStateDLQ,
		CreatedAt: now,
	}
	dlqMsg.BrokerMeta.Transfer = message.TransferMetadata{
		ID:            transferID,
		FailureReason: reason,
		CompletedAt:   now,
		SourceQueue:   queueName,
		SourceGroup:   groupID,
		SourceOffset:  sourceOffset,
		DeliveryCount: deliveryCount,
	}

	deduplicated, err := c.appendTransferOnce(ctx, dlqQueueName, dlqCfg, transferID, dlqMsg)
	if err != nil {
		c.logger.Warn("failed to append message to DLQ",
			slog.String("queue", queueName),
			slog.String("dlq_queue", dlqQueueName),
			slog.String("transfer_id", transferID),
			slog.String("error", err.Error()))
		return fmt.Errorf("append DLQ transfer %q: %w", transferID, err)
	}
	if deduplicated {
		// A previous attempt already appended this transfer and failed before
		// settling the source. Reporting success lets the caller settle now,
		// which is what completes the transition rather than repeating it.
		c.logger.Info("dead-letter transfer already present; settling source",
			slog.String("queue", queueName),
			slog.String("group", groupID),
			slog.String("dlq_queue", dlqQueueName),
			slog.String("transfer_id", transferID))
		return nil
	}

	c.logger.Warn("message moved to DLQ",
		slog.String("queue", queueName),
		slog.String("group", groupID),
		slog.String("dlq_queue", dlqQueueName),
		slog.String("transfer_id", transferID),
		slog.Int("delivery_count", deliveryCount))
	return nil
}

// appendTransferOnce appends a transfer that must not duplicate.
//
// Deduplicating makes the retry safe: an attempt that appended and then failed
// to settle its source can be repeated without producing a second record. A
// replicated destination takes the check through Raft so every replica performs
// it; a local one takes it through the store directly.
func (c *recordCore) appendTransferOnce(ctx context.Context, queueName string, config *types.QueueConfig, transferID string, msg *message.Envelope) (bool, error) {
	if config.Replication.Enabled {
		return c.replicateTransferOnce(ctx, queueName, config, transferID, msg)
	}

	deduplicating, ok := c.queueStore.(storage.DeduplicatingQueueStore)
	if !ok {
		// Deduplication is required, not preferred. Falling back to a plain
		// append recreates exactly the duplicate this path exists to prevent:
		// the transfer succeeds, the source is settled, and a retry of an
		// earlier attempt that failed to settle appends the record twice.
		// Refusing leaves the entry pending for a later attempt, which is
		// loss-safe; both shipped stores provide the capability, so this is a
		// misconfiguration rather than a routine condition.
		message.Release(msg)
		return false, fmt.Errorf("%w: queue %q", storage.ErrDeduplicationUnsupported, queueName)
	}
	if window := deduplicating.DeduplicationWindow(); window != 0 {
		// The interface permits a finite window, and a transfer retried after a
		// failed settlement has no bounded lifetime: nothing stops more than
		// `window` records arriving before the retry, after which the key is
		// forgotten and the record is appended twice. A window is a mitigation;
		// this path needs the guarantee, so a store offering only the former is
		// refused rather than trusted to be wide enough.
		message.Release(msg)
		return false, fmt.Errorf("%w: queue %q deduplicates only within %d records",
			storage.ErrDeduplicationUnsupported, queueName, window)
	}

	// AppendOnce consumes the envelope unless it fails, storing it or releasing
	// it, so anything needed afterwards is read first.
	topic := msg.Topic

	// The transfer settles its source on this call's success, so on a queue
	// configured for fsync the record has to be durable before that success is
	// reported. Deduplicating must not quietly downgrade the queue's durability.
	appendOnce := deduplicating.AppendOnce
	if c.ackDurabilityFor(config) == AckDurabilityFsync && config.Durable {
		if _, err := c.durableStore(); err != nil {
			return false, err
		}
		appendOnce = deduplicating.AppendOnceAndSync
	}

	offset, deduplicated, err := appendOnce(ctx, queueName, transferID, msg)
	if err != nil {
		message.Release(msg)
		return false, err
	}
	if err := c.completeAppend(queueName, topic, offset, nil); err != nil {
		return false, err
	}
	c.delivery.Schedule(queueName)
	return deduplicated, nil
}

// replicateTransferOnce runs a deduplicated transfer through Raft.
//
// The leader cannot decide alone whether the record already exists, because the
// followers have no way to check its answer. Instead the key is replicated with
// the entry and each replica asks its own store, reaching the same conclusion
// from the same log.
func (c *recordCore) replicateTransferOnce(ctx context.Context, queueName string, config *types.QueueConfig, transferID string, msg *message.Envelope) (bool, error) {
	// Raft serializes the caller's envelope and applies a decoded copy, so the
	// original is finished once the apply returns, on every path.
	defer message.Release(msg)
	topic := msg.Topic

	if c.services.replicationCoordinator() == nil {
		return false, fmt.Errorf("%w: queue %q is replicated but no coordinator is configured", ErrReplicationUnavailable, queueName)
	}
	deduplicating, ok := c.services.replicationCoordinator().(raft.DeduplicatingLogReplicator)
	if !ok {
		return false, fmt.Errorf("%w: coordinator for queue %q", storage.ErrDeduplicationUnsupported, queueName)
	}
	if err := c.services.replicationWriteReadiness(queueName); err != nil {
		return false, err
	}
	if !c.services.replicationCoordinator().IsLeaderForQueue(queueName) {
		return false, fmt.Errorf("%w: queue %q is not led by this node", ErrReplicationUnavailable, queueName)
	}

	// Replication.Mode is deliberately not consulted: an async apply returns
	// before the deduplication answer exists, and the caller settles its source
	// on that answer.
	offset, deduplicated, err := deduplicating.ApplyAppendOnceWithOptions(ctx, queueName, transferID, msg, raft.ApplyOptions{
		AckTimeout: config.Replication.AckTimeout,
	})
	if err != nil {
		return false, err
	}
	if err := c.completeAppend(queueName, topic, offset, nil); err != nil {
		return false, err
	}
	c.delivery.Schedule(queueName)
	return deduplicated, nil
}

func dlqTransferID(queueName, groupID string, sourceOffset uint64) string {
	source := fmt.Sprintf("%d:%s:%d:%s:%d", len(queueName), queueName, len(groupID), groupID, sourceOffset)
	sum := sha256.Sum256([]byte(source))
	return fmt.Sprintf("dlq-%x", sum[:16])
}
