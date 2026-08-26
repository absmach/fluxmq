// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package queue

import (
	"bytes"
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
func (c *recordCore) appendResolved(ctx context.Context, queueName string, publish types.PublishRequest, config types.QueueConfig) (AppendOutcome, error) {
	queued := newQueuedMessage(publish, &config)
	createdAt := queued.Broker.Queue.CreatedAt

	offset, err := c.appendConfiguredMessage(ctx, queueName, &config, queued)
	if err := c.completeAppend(queueName, publish.Topic, offset, err); err != nil {
		return AppendOutcome{}, err
	}

	return AppendOutcome{FirstOffset: offset, LastOffset: offset, Count: 1, Timestamp: createdAt}, nil
}

var _ managerServices = (*Manager)(nil)

// unmanagedServices is the managerServices a core has before a Manager is
// attached: no contracts, no replication, no queue creation. Used by tests and
// by the delivery engine's own core, neither of which performs those.
type unmanagedServices struct{}

func (unmanagedServices) protectedQueueContract(string) (types.QueueConfig, bool) {
	return types.QueueConfig{}, false
}

func (unmanagedServices) replicationWriteReadiness(string) error { return ErrReplicationUnavailable }

func (unmanagedServices) CreateQueue(context.Context, types.QueueConfig) error {
	return ErrDLQDisabled
}

func (unmanagedServices) replicationCoordinator() queueRaftCoordinator { return nil }

// noScheduler drops wake-ups, for a core with no delivery engine behind it.
type noScheduler struct{}

func (noScheduler) Schedule(string) {}

// newRecordCore builds a core from stores and policy alone. A caller that has a
// Manager assigns its services afterwards; without them the core still appends,
// it just cannot resolve protected contracts, replicate, or create a missing
// dead-letter queue.
func newRecordCore(queueStore storage.QueueStore, groupStore storage.ConsumerGroupStore, config Config, metrics *consumer.Metrics, logger *slog.Logger) *recordCore {
	return &recordCore{
		queueStore:        queueStore,
		groupStore:        groupStore,
		delivery:          noScheduler{},
		metrics:           metrics,
		logger:            logger,
		config:            config,
		ackDurability:     NormalizeAckDurability(config.AckDurability),
		storeSupportsSync: storeSupportsDurableSync(queueStore),
		services:          unmanagedServices{},
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
// The three function fields are deliberate: they reach registries Manager owns,
// and holding them as callbacks rather than a *Manager keeps the dependency
// pointing one way. A core that can call back into the facade is the thing this
// separation was meant to end.
// deliveryScheduler wakes delivery for a queue once a record has landed.
//
// An interface rather than *DeliveryEngine so the core states what it needs —
// a wake-up — instead of holding the component that performs delivery.
type deliveryScheduler interface {
	Schedule(queueName string)
}

// managerServices are the registries and policies the record core consults but
// does not own: they live on Manager because they are lifecycle and admin
// concerns, not record semantics.
//
// Declared as an interface rather than reached through closures over the
// Manager. The dependency is real either way — this is a genuine cycle, since
// appending wakes delivery and delivery consumes appends — so the honest thing
// is to name it and let a reader see its shape, not to hide it in four
// anonymous functions and describe the core as independent.
type managerServices interface {
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

	// services is assigned once, immediately after the Manager exists and
	// before anything is started.
	services managerServices
}

func (c *recordCore) appendToQueue(ctx context.Context, queueName string, publish types.PublishRequest) (uint64, time.Time, error) {
	publish = normalizePublishRequest(publish)
	if publish.Topic == "" {
		publish.Topic = queueName
	}

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

	msg := newQueuedMessage(publish, queueConfig)
	// Read the assigned timestamp before the append: a successful append
	// transfers ownership of msg to the store, which may release it.
	createdAt := msg.Broker.Queue.CreatedAt
	offset, err := c.appendConfiguredMessage(ctx, queueName, queueConfig, msg)
	if err := c.completeAppend(queueName, publish.Topic, offset, err); err != nil {
		return 0, time.Time{}, err
	}
	c.delivery.Schedule(queueName)
	return offset, createdAt, nil
}

func (c *recordCore) appendBatchToQueue(ctx context.Context, queueName string, publishes []types.PublishRequest) (uint64, uint32, time.Time, error) {
	if len(publishes) == 0 {
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

	messages := make([]*message.Envelope, len(publishes))
	for i, publish := range publishes {
		publish = normalizePublishRequest(publish)
		if publish.Topic == "" {
			publish.Topic = queueName
		}
		messages[i] = newQueuedMessage(publish, queueConfig)
	}

	// Read the last record's timestamp before the append: a successful append
	// transfers ownership of every envelope to the store.
	lastCreatedAt := messages[len(messages)-1].Broker.Queue.CreatedAt

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

func (c *recordCore) publishToDurableStream(ctx context.Context, queueName string, publish types.PublishRequest) (uint64, time.Time, error) {
	expected, protected := c.services.protectedQueueContract(queueName)
	if !protected {
		return 0, time.Time{}, fmt.Errorf("%w: %s", ErrQueueNotProtected, queueName)
	}

	publish = normalizePublishRequest(publish)
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
	if queueConfig.MaxMessageSize <= 0 || int64(len(publish.Payload)) > queueConfig.MaxMessageSize {
		return 0, time.Time{}, fmt.Errorf(
			"%w: queue %s accepts at most %d bytes, got %d",
			ErrQueueMessageTooLarge,
			queueName,
			queueConfig.MaxMessageSize,
			len(publish.Payload),
		)
	}

	msg := newQueuedMessage(publish, queueConfig)
	// Read the assigned timestamp before the append: a successful append
	// transfers ownership of msg to the store.
	createdAt := msg.Broker.Queue.CreatedAt
	offset, err := durableStore.AppendAndSync(ctx, queueName, msg)
	if err != nil {
		message.Release(msg)
	}
	if err := c.completeAppend(queueName, publish.Topic, offset, err); err != nil {
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

func newQueuedMessage(publish types.PublishRequest, queueConfig *types.QueueConfig) *message.Envelope {
	now := time.Now()
	msg := message.New(publish.Topic, publish.Payload)
	msg.User.Key = bytes.Clone(publish.Key)
	msg.User.Headers = cloneByteMap(publish.Headers)
	msg.User.Properties = message.FilterUserProperties(publish.Properties)
	msg.User.ContentType = publish.ContentType
	msg.User.ContentEncoding = publish.ContentEncoding
	msg.User.ResponseTopic = publish.ResponseTopic
	msg.User.CorrelationData = bytes.Clone(publish.CorrelationData)
	msg.User.PayloadFormat = clonePointer(publish.PayloadFormat)
	msg.User.MessageExpiry = clonePointer(publish.MessageExpiry)
	msg.Broker.Source = publish.Source
	msg.Broker.Trace = publish.Trace
	msg.Broker.Delivery.PublishedAt = publish.PublishedAt
	msg.Broker.Delivery.ExpiresAt = publish.ExpiresAt
	msg.Broker.Queue.State = message.QueueStateQueued
	msg.Broker.Queue.CreatedAt = now
	if queueConfig.MessageTTL > 0 {
		msg.Broker.Queue.ExpiresAt = now.Add(queueConfig.MessageTTL)
	}
	if !publish.ExpiresAt.IsZero() && (msg.Broker.Queue.ExpiresAt.IsZero() || publish.ExpiresAt.Before(msg.Broker.Queue.ExpiresAt)) {
		msg.Broker.Queue.ExpiresAt = publish.ExpiresAt
	}
	return msg
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
	deliveryCount := max(msg.Broker.Queue.RetryCount+1, 1)
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
	dlqMsg.Broker.Delivery = message.DeliveryMetadata{}
	dlqMsg.Broker.Source.Topic = msg.Topic
	dlqMsg.Broker.Queue = message.QueueMetadata{
		State:     message.QueueStateDLQ,
		CreatedAt: now,
	}
	dlqMsg.Broker.Transfer = message.TransferMetadata{
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
