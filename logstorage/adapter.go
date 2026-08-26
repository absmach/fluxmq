// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package logstorage

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/absmach/fluxmq/message"
	"github.com/absmach/fluxmq/queue/storage"
	"github.com/absmach/fluxmq/queue/types"
)

const (
	headerEnvelope  = "_envelope"
	headerDedupeKey = "_dedupe"
)

var (
	_ storage.QueueStore         = (*Adapter)(nil)
	_ storage.DurableQueueStore  = (*Adapter)(nil)
	_ storage.ConsumerGroupStore = (*Adapter)(nil)

	// The production queue store must be snapshottable: an FSM that cannot
	// capture its records refuses to snapshot at all, which stops raft from
	// ever compacting the log.
	_ storage.SnapshotableQueueStore = (*Adapter)(nil)
)

// Adapter wraps the log Store and implements the storage.QueueStore and
// storage.ConsumerGroupStore interfaces for integration with the queue system.
type Adapter struct {
	store      *Store
	queueStore *QueueConfigStore
	groupStore *ConsumerGroupStateStore
	topicIndex *storage.TopicIndex
	dedupe     *dedupeIndexes
}

// AdapterConfig holds adapter configuration.
type AdapterConfig struct {
	StoreConfig
}

// DefaultAdapterConfig returns default adapter configuration.
func DefaultAdapterConfig() AdapterConfig {
	return AdapterConfig{
		StoreConfig: DefaultStoreConfig(),
	}
}

// NewAdapter creates a new adapter wrapping the log store.
func NewAdapter(baseDir string, config AdapterConfig) (*Adapter, error) {
	store, err := NewStore(baseDir, config.StoreConfig)
	if err != nil {
		return nil, err
	}

	queueStore, err := NewQueueConfigStore(baseDir)
	if err != nil {
		store.Close()
		return nil, err
	}

	groupStore, err := NewConsumerGroupStateStore(baseDir)
	if err != nil {
		store.Close()
		queueStore.Close()
		return nil, err
	}

	dedupe, err := newDedupeIndexes(baseDir)
	if err != nil {
		store.Close()
		queueStore.Close()
		groupStore.Close()
		return nil, err
	}

	adapter := &Adapter{
		store:      store,
		queueStore: queueStore,
		groupStore: groupStore,
		topicIndex: storage.NewTopicIndex(),
		dedupe:     dedupe,
	}

	// Rebuild topic index from existing queues.
	//
	// A queue persisted before filters were validated may carry one that can
	// never match. Startup is not the place to refuse it — the data is already
	// on disk and an operator cannot edit it from here — but it must not be
	// bound silently either, because the queue will simply receive nothing.
	// Report it and carry on; the index ignores such a filter regardless.
	queues, err := queueStore.List()
	if err == nil {
		for _, cfg := range queues {
			for _, filter := range cfg.Topics {
				if err := types.ValidateTopicFilters([]string{filter}); err != nil {
					reportMalformedFilter(config.RecoveryLogger, cfg.Name, filter, err)
				}
			}
			adapter.topicIndex.AddQueue(cfg.Name, cfg.Topics)
		}
	}

	return adapter, nil
}

// reportMalformedFilter surfaces a persisted binding that can never match. The
// queue keeps working for its other filters; this one contributes nothing.
func reportMalformedFilter(logger func(string, ...any), queueName, filter string, err error) {
	if logger == nil {
		logger = func(msg string, args ...any) { slog.Warn(msg, args...) }
	}
	logger("queue topic filter can never match; queue will not receive traffic through it",
		"queue", queueName, "filter", filter, "error", err)
}

// Close closes the adapter and underlying store.
func (a *Adapter) Close() error {
	var lastErr error

	if err := a.dedupe.state.close(); err != nil {
		lastErr = err
	}

	if err := a.groupStore.Close(); err != nil {
		lastErr = err
	}

	if err := a.queueStore.Close(); err != nil {
		lastErr = err
	}

	if err := a.store.Close(); err != nil {
		lastErr = err
	}

	return lastErr
}

// Store returns the underlying log store for direct access.
func (a *Adapter) Store() *Store {
	return a.store
}

// OffsetByTime returns the offset for the given timestamp.
func (a *Adapter) OffsetByTime(ctx context.Context, queueName string, ts time.Time) (uint64, error) {
	return a.store.LookupByTime(queueName, ts)
}

// OffsetBySize returns the offset to keep when enforcing size retention.
func (a *Adapter) OffsetBySize(ctx context.Context, queueName string, retentionBytes int64) (uint64, error) {
	return a.store.RetentionOffsetBySize(queueName, retentionBytes)
}

// QueueStore interface implementation

// CreateQueue creates a new queue with the given configuration.
//
// A queue is two pieces of state: its log directory and its metadata. A crash
// between the two leaves a log with no metadata, which every other API reads as
// a queue that does not exist. Treat that case as a repair and write the
// missing metadata rather than reporting the queue as already created, so a
// torn creation cannot strand records that were acknowledged as durable.
func (a *Adapter) CreateQueue(ctx context.Context, config types.QueueConfig) error {
	// Defence in depth: the queue manager checks this too, but the adapter is
	// this package's public write path and must not persist a binding that can
	// never match.
	if err := types.ValidateTopicFilters(config.Topics); err != nil {
		return err
	}

	if err := a.store.CreateQueue(config.Name); err != nil {
		if !errors.Is(err, ErrAlreadyExists) {
			return err
		}
		if _, getErr := a.queueStore.Get(config.Name); getErr == nil {
			return storage.ErrQueueAlreadyExists
		}
	}

	if err := a.queueStore.Save(config); err != nil {
		return err
	}

	// Update topic index
	a.topicIndex.AddQueue(config.Name, config.Topics)

	return nil
}

// UpdateQueue updates an existing queue's configuration.
func (a *Adapter) UpdateQueue(ctx context.Context, config types.QueueConfig) error {
	if err := types.ValidateTopicFilters(config.Topics); err != nil {
		return err
	}

	if err := a.queueStore.Save(config); err != nil {
		return err
	}

	// Refresh topic index to ensure matcher reflects updated topic patterns.
	a.topicIndex.AddQueue(config.Name, config.Topics)

	return nil
}

// GetQueue retrieves a queue's configuration.
func (a *Adapter) GetQueue(ctx context.Context, queueName string) (*types.QueueConfig, error) {
	config, err := a.queueStore.Get(queueName)
	if err != nil {
		return nil, storage.ErrQueueNotFound
	}
	return config, nil
}

// DeleteQueue deletes a queue and all its data.
func (a *Adapter) DeleteQueue(ctx context.Context, queueName string) error {
	// Under the queue's deduplication lock for the same reason Truncate is: the
	// records and their keys must disappear as one operation with respect to a
	// deduplicated append, and a recreated queue must start with an empty index.
	queueLock := a.dedupe.locks.Key(queueName)
	queueLock.Lock()
	defer queueLock.Unlock()

	// Remove from topic index
	a.topicIndex.RemoveQueue(queueName)

	if err := a.queueStore.Delete(queueName); err != nil {
		return err
	}
	if err := a.store.DeleteQueue(queueName); err != nil {
		return err
	}
	return a.dedupe.state.forget(queueName)
}

// ListQueues returns all queue configurations.
func (a *Adapter) ListQueues(ctx context.Context) ([]types.QueueConfig, error) {
	return a.queueStore.List()
}

// FindMatchingQueues returns all queues whose topic patterns match the topic.
func (a *Adapter) FindMatchingQueues(ctx context.Context, topic string) ([]string, error) {
	return a.topicIndex.FindMatching(topic), nil
}

func (a *Adapter) queueConfigExists(queueName string) error {
	if _, err := a.queueStore.Get(queueName); err != nil {
		if errors.Is(err, ErrQueueNotFound) {
			return storage.ErrQueueNotFound
		}
		return err
	}
	return nil
}

func encodeMessage(envelope *message.Envelope) ([]byte, []byte, map[string][]byte, error) {
	metadata, err := message.MarshalMetadata(envelope)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("encode queue envelope metadata: %w", err)
	}
	headers := map[string][]byte{headerEnvelope: metadata}
	if envelope.BrokerMeta.Transfer.ID != "" {
		headers[headerDedupeKey] = []byte(envelope.BrokerMeta.Transfer.ID)
	}
	return envelope.PayloadBytes(), envelope.PublisherMeta.Key.Bytes(), headers, nil
}

// Append adds a message to the end of a queue's log.
func (a *Adapter) Append(ctx context.Context, queueName string, msg *message.Envelope) (uint64, error) {
	if err := a.queueConfigExists(queueName); err != nil {
		return 0, err
	}

	value, key, headers, err := encodeMessage(msg)
	if err != nil {
		return 0, err
	}
	offset, err := a.store.Append(queueName, value, key, headers)
	if err == nil {
		message.Release(msg)
	}
	return offset, err
}

// AppendAndSync appends a message and syncs the exact segment containing it as
// one operation. It is the durability primitive used before publisher ACKs.
// A cancelled context aborts before the append, so the caller can NACK without
// the record ever reaching the log; the append and its fsync are not
// interruptible once started.
func (a *Adapter) AppendAndSync(ctx context.Context, queueName string, msg *message.Envelope) (uint64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if err := a.queueConfigExists(queueName); err != nil {
		return 0, err
	}

	value, key, headers, err := encodeMessage(msg)
	if err != nil {
		return 0, err
	}
	offset, err := a.store.AppendAndSync(queueName, value, key, headers)
	if err == nil {
		message.Release(msg)
	}
	return offset, err
}

// SupportsDurableSync reports that AppendAndSync establishes a real crash
// durability barrier: the segment file and its directory entry are fsynced
// before it returns.
func (a *Adapter) SupportsDurableSync() bool { return true }

// SyncQueue flushes the queue's current active segment. AppendAndSync must be
// used when the caller needs a barrier tied to one particular append.
func (a *Adapter) SyncQueue(_ context.Context, queueName string) error {
	return a.store.SyncQueue(queueName)
}

// AppendBatch adds multiple messages to a queue's log.
func (a *Adapter) AppendBatch(ctx context.Context, queueName string, msgs []*message.Envelope) (uint64, error) {
	if len(msgs) == 0 {
		return 0, ErrEmptyBatch
	}

	if err := a.queueConfigExists(queueName); err != nil {
		return 0, err
	}

	batch := NewBatch(0)

	for _, msg := range msgs {
		value, key, headers, err := encodeMessage(msg)
		if err != nil {
			return 0, err
		}
		batch.Append(value, key, headers)
	}

	offset, err := a.store.AppendBatch(queueName, batch)
	if err == nil {
		for _, msg := range msgs {
			message.Release(msg)
		}
	}
	return offset, err
}

// Read retrieves a message at a specific offset.
func (a *Adapter) Read(ctx context.Context, queueName string, offset uint64) (*message.Envelope, error) {
	msg, err := a.store.Read(queueName, offset)
	if err != nil {
		if errors.Is(err, ErrOffsetOutOfRange) {
			return nil, storage.ErrOffsetOutOfRange
		}
		if errors.Is(err, ErrQueueNotFound) {
			return nil, storage.ErrQueueNotFound
		}
		return nil, err
	}

	return logMessageToEnvelope(msg)
}

// ReadBatch reads messages starting from offset up to limit.
func (a *Adapter) ReadBatch(ctx context.Context, queueName string, startOffset uint64, limit int) ([]*message.Envelope, error) {
	if limit <= 0 {
		return []*message.Envelope{}, nil
	}

	tail, err := a.store.Tail(queueName)
	if err != nil {
		if errors.Is(err, ErrQueueNotFound) {
			return nil, storage.ErrQueueNotFound
		}
		return nil, err
	}

	if startOffset >= tail {
		return []*message.Envelope{}, nil
	}

	result := make([]*message.Envelope, 0, limit)
	current := startOffset

	for current < tail && len(result) < limit {
		batch, err := a.store.ReadBatch(queueName, current)
		if err != nil {
			if errors.Is(err, ErrOffsetOutOfRange) {
				break
			}
			if errors.Is(err, ErrQueueNotFound) {
				releaseEnvelopes(result)
				return nil, storage.ErrQueueNotFound
			}
			releaseEnvelopes(result)
			return nil, err
		}

		for _, msg := range batch.ToMessages() {
			if msg.Offset < startOffset {
				continue
			}
			if msg.Offset >= tail {
				return result, nil
			}
			envelope, err := logMessageToEnvelope(&msg)
			if err != nil {
				releaseEnvelopes(result)
				return nil, err
			}
			result = append(result, envelope)
			if len(result) >= limit {
				return result, nil
			}
		}

		next := batch.NextOffset()
		if next <= current {
			break
		}
		current = next
	}

	return result, nil
}

func releaseEnvelopes(envelopes []*message.Envelope) {
	for _, envelope := range envelopes {
		message.Release(envelope)
	}
}

// Head returns the first valid offset in the queue.
func (a *Adapter) Head(ctx context.Context, queueName string) (uint64, error) {
	return a.store.Head(queueName)
}

// Tail returns the next offset that will be assigned.
func (a *Adapter) Tail(ctx context.Context, queueName string) (uint64, error) {
	return a.store.Tail(queueName)
}

// Truncate removes all messages with offset < minOffset.
//
// It holds the queue's deduplication lock across both the removal and the index
// prune, because AppendOnce holds that same lock across its check and its
// append. Without it the two interleave: truncation removes the record, and a
// concurrent retry still sees the key, reports the transfer already present, and
// the caller settles its source against a record that no longer exists. Pruning
// after truncation is not enough on its own — the two have to be one operation
// with respect to a deduplicated append.
func (a *Adapter) Truncate(ctx context.Context, queueName string, minOffset uint64) error {
	queueLock := a.dedupe.locks.Key(queueName)
	queueLock.Lock()
	defer queueLock.Unlock()

	if err := a.store.Truncate(queueName, minOffset); err != nil {
		return err
	}
	return a.dedupe.state.pruneBelow(queueName, minOffset)
}

// Count returns the number of messages in a queue.
func (a *Adapter) Count(ctx context.Context, queueName string) (uint64, error) {
	return a.store.Count(queueName)
}

// TotalCount returns total messages in a queue.
func (a *Adapter) TotalCount(ctx context.Context, queueName string) (uint64, error) {
	return a.store.Count(queueName)
}

// ConsumerGroupStore interface implementation

// CreateConsumerGroup creates a new consumer group for a queue.
func (a *Adapter) CreateConsumerGroup(ctx context.Context, group *types.ConsumerGroup) error {
	existing, _ := a.groupStore.Get(group.QueueName, group.ID)
	if existing != nil {
		return storage.ErrConsumerGroupExists
	}

	return a.groupStore.Save(group)
}

// GetConsumerGroup retrieves a consumer group's state.
func (a *Adapter) GetConsumerGroup(ctx context.Context, queueName, groupID string) (*types.ConsumerGroup, error) {
	group, err := a.groupStore.Get(queueName, groupID)
	if err != nil {
		return nil, err
	}

	// Stream groups use cursor-only semantics without PEL and keep cursor state in groupStore.
	// Syncing from logstore can overwrite explicit stream cursor positioning.
	if group.Mode != types.GroupModeStream {
		// Sync cursors from the log store's cursor state.
		a.syncCursorsFromStore(queueName, groupID, group)

		// Sync PEL from the log store's PEL state.
		a.syncPELFromStore(queueName, groupID, group)
	}

	return group, nil
}

// UpdateConsumerGroup updates a consumer group's state.
func (a *Adapter) UpdateConsumerGroup(ctx context.Context, group *types.ConsumerGroup) error {
	group.UpdatedAt = time.Now()
	return a.groupStore.Save(group)
}

// DeleteConsumerGroup removes a consumer group.
func (a *Adapter) DeleteConsumerGroup(ctx context.Context, queueName, groupID string) error {
	return a.groupStore.Delete(queueName, groupID)
}

// ListConsumerGroups lists all consumer groups for a queue.
func (a *Adapter) ListConsumerGroups(ctx context.Context, queueName string) ([]*types.ConsumerGroup, error) {
	return a.groupStore.List(queueName)
}

// AddPendingEntry adds an entry to a consumer's PEL.
func (a *Adapter) AddPendingEntry(ctx context.Context, queueName, groupID string, entry *types.PendingEntry) error {
	pelEntry := PELEntry{
		Offset:        entry.Offset,
		ConsumerID:    entry.ConsumerID,
		ClaimedAt:     entry.ClaimedAt.UnixMilli(),
		DeliveryCount: uint16(entry.DeliveryCount),
	}

	if err := a.store.AddPending(queueName, groupID, pelEntry); err != nil {
		return err
	}

	// Update the group state's PEL as well
	group, err := a.groupStore.Get(queueName, groupID)
	if err == nil {
		group.AddPending(entry.ConsumerID, entry)
		if err := a.groupStore.Save(group); err != nil {
			return err
		}
	}

	return nil
}

// RemovePendingEntry removes an entry from a consumer's PEL.
func (a *Adapter) RemovePendingEntry(ctx context.Context, queueName, groupID, consumerID string, offset uint64) error {
	if err := a.store.AckPending(queueName, groupID, offset); err != nil {
		if errors.Is(err, ErrPELEntryNotFound) {
			return storage.ErrPendingEntryNotFound
		}
		return err
	}

	// Update the group state's PEL as well
	group, err := a.groupStore.Get(queueName, groupID)
	if err == nil {
		group.RemovePending(consumerID, offset)
		if err := a.groupStore.Save(group); err != nil {
			return err
		}
	}

	return nil
}

// GetPendingEntries retrieves all pending entries for a consumer.
func (a *Adapter) GetPendingEntries(ctx context.Context, queueName, groupID, consumerID string) ([]*types.PendingEntry, error) {
	entries, err := a.store.GetPendingByConsumer(queueName, groupID, consumerID)
	if err != nil {
		return nil, err
	}

	result := make([]*types.PendingEntry, len(entries))
	for i, e := range entries {
		result[i] = pendingEntryToTypes(&e)
	}

	return result, nil
}

// GetAllPendingEntries retrieves all pending entries for a group.
func (a *Adapter) GetAllPendingEntries(ctx context.Context, queueName, groupID string) ([]*types.PendingEntry, error) {
	group, err := a.groupStore.Get(queueName, groupID)
	if err != nil {
		return nil, err
	}

	// Sync from log store first
	a.syncPELFromStore(queueName, groupID, group)

	var result []*types.PendingEntry
	for _, entries := range group.PEL {
		result = append(result, entries...)
	}

	return result, nil
}

// TransferPendingEntry moves a pending entry from one consumer to another.
func (a *Adapter) TransferPendingEntry(ctx context.Context, queueName, groupID string, offset uint64, fromConsumer, toConsumer string) error {
	if err := a.store.ClaimPending(queueName, groupID, offset, toConsumer); err != nil {
		if errors.Is(err, ErrPELEntryNotFound) {
			return storage.ErrPendingEntryNotFound
		}
		return err
	}

	// Update the group state's PEL as well
	group, err := a.groupStore.Get(queueName, groupID)
	if err == nil {
		group.TransferPending(offset, fromConsumer, toConsumer)
		if err := a.groupStore.Save(group); err != nil {
			return err
		}
	}

	return nil
}

// RequeuePendingEntry updates redelivery timing without changing ownership.
func (a *Adapter) RequeuePendingEntry(ctx context.Context, queueName, groupID, consumerID string, offset uint64, attemptedAt time.Time) error {
	group, err := a.groupStore.Get(queueName, groupID)
	if err != nil {
		return err
	}
	_, owner := group.FindPending(offset)
	if owner == "" {
		return storage.ErrPendingEntryNotFound
	}
	if owner != consumerID {
		return storage.ErrConsumerNotFound
	}
	if err := a.store.NackAt(queueName, groupID, offset, attemptedAt); err != nil {
		if errors.Is(err, ErrPELEntryNotFound) {
			return storage.ErrPendingEntryNotFound
		}
		return err
	}
	// Through the group's lock: the entry used to be mutated here through a
	// pointer FindPending handed out, racing every reader and the encoder.
	if !group.RequeuePending(offset, consumerID, attemptedAt) {
		return storage.ErrPendingEntryNotFound
	}
	return a.groupStore.Save(group)
}

// UpdateCursor updates the cursor position for a queue.
func (a *Adapter) UpdateCursor(ctx context.Context, queueName, groupID string, cursor uint64) error {
	if err := a.store.SetCursor(queueName, groupID, cursor); err != nil {
		return err
	}

	// Update the group state's cursor as well
	group, err := a.groupStore.Get(queueName, groupID)
	if err == nil {
		group.SetCursorPosition(cursor)
		if err := a.groupStore.Save(group); err != nil {
			return err
		}
	}

	return nil
}

// UpdateCommitted updates the committed offset for a queue.
func (a *Adapter) UpdateCommitted(ctx context.Context, queueName, groupID string, committed uint64) error {
	// Committed is the next safe offset, not a message to acknowledge. The
	// underlying consumer store's CommitOffset is a legacy alias for Ack and
	// would therefore remove the record at committed. Keep the canonical safe
	// point in the persisted group state; individual Ack calls already update
	// the underlying PEL.
	group, err := a.groupStore.Get(queueName, groupID)
	if err == nil {
		group.AdvanceCommitted(committed)
		if err := a.groupStore.Save(group); err != nil {
			return err
		}
	}

	return nil
}

// RegisterConsumer adds a consumer to a group.
func (a *Adapter) RegisterConsumer(ctx context.Context, queueName, groupID string, consumer *types.ConsumerInfo) error {
	group, err := a.groupStore.Get(queueName, groupID)
	if err != nil {
		return err
	}

	group.SetConsumer(consumer.ID, consumer)

	return a.groupStore.Save(group)
}

// UnregisterConsumer removes a consumer from a group.
func (a *Adapter) UnregisterConsumer(ctx context.Context, queueName, groupID, consumerID string) error {
	group, err := a.groupStore.Get(queueName, groupID)
	if err != nil {
		return err
	}

	group.DeleteConsumer(consumerID)

	return a.groupStore.Save(group)
}

// ListConsumers lists all consumers in a group.
func (a *Adapter) ListConsumers(ctx context.Context, queueName, groupID string) ([]*types.ConsumerInfo, error) {
	group, err := a.groupStore.Get(queueName, groupID)
	if err != nil {
		return nil, err
	}

	result := make([]*types.ConsumerInfo, 0, group.ConsumerCount())
	group.ForEachConsumer(func(id string, info *types.ConsumerInfo) bool {
		result = append(result, info)
		return true
	})

	return result, nil
}

// Sync flushes all pending writes to disk.
func (a *Adapter) Sync() error {
	if err := a.store.Sync(); err != nil {
		return err
	}

	if err := a.queueStore.Sync(); err != nil {
		return err
	}

	return a.groupStore.Sync()
}

// Helper functions

// syncCursorsFromStore syncs cursor state from the log store to the group state.
func (a *Adapter) syncCursorsFromStore(queueName, groupID string, group *types.ConsumerGroup) {
	cursorState, err := a.store.GetCursorState(queueName, groupID)
	if err != nil {
		return
	}

	group.SetCursorPosition(cursorState.Cursor)
}

// syncPELFromStore syncs PEL state from the log store to the group state.
func (a *Adapter) syncPELFromStore(queueName, groupID string, group *types.ConsumerGroup) {
	allEntries, err := a.store.GetAllPending(queueName, groupID)
	if err != nil {
		return
	}

	pel := make(map[string][]*types.PendingEntry)
	for consumerID, entries := range allEntries {
		typeEntries := make([]*types.PendingEntry, len(entries))
		for i, e := range entries {
			typeEntries[i] = pelEntryToTypes(&e)
		}
		pel[consumerID] = typeEntries
	}
	group.ReplacePEL(pel)
}

func logMessageToEnvelope(msg *Message) (*message.Envelope, error) {
	envelope, err := message.UnmarshalMetadata(msg.Headers[headerEnvelope], msg.Value, msg.Key)
	if err != nil {
		return nil, fmt.Errorf("decode queue envelope metadata at offset %d: %w", msg.Offset, err)
	}
	envelope.BrokerMeta.Queue.Offset = msg.Offset
	if envelope.BrokerMeta.Queue.CreatedAt.IsZero() {
		envelope.BrokerMeta.Queue.CreatedAt = msg.Timestamp
	}
	return envelope, nil
}

// pelEntryToTypes converts a log PELEntry to a types.PendingEntry.
func pelEntryToTypes(entry *PELEntry) *types.PendingEntry {
	return &types.PendingEntry{
		Offset:        entry.Offset,
		ConsumerID:    entry.ConsumerID,
		ClaimedAt:     time.UnixMilli(entry.ClaimedAt),
		DeliveryCount: int(entry.DeliveryCount),
	}
}

// pendingEntryToTypes converts a log PendingEntry to a types.PendingEntry.
func pendingEntryToTypes(entry *PendingEntry) *types.PendingEntry {
	return &types.PendingEntry{
		Offset:        entry.Offset,
		ConsumerID:    entry.ConsumerID,
		ClaimedAt:     time.UnixMilli(entry.LastAttempt),
		DeliveryCount: int(entry.DeliveryCount),
	}
}
