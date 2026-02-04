// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package logstorage

import (
	"context"
	"time"

	"github.com/absmach/fluxmq/queue/storage"
	"github.com/absmach/fluxmq/queue/types"
	"github.com/absmach/fluxmq/topics"
)

var (
	_ storage.QueueStore         = (*Adapter)(nil)
	_ storage.ConsumerGroupStore = (*Adapter)(nil)
)

// Adapter wraps the log Store and implements the storage.QueueStore and
// storage.ConsumerGroupStore interfaces for integration with the queue system.
type Adapter struct {
	store      *Store
	queueStore *QueueConfigStore
	groupStore *ConsumerGroupStateStore
	topicIndex *storage.TopicIndex
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

	adapter := &Adapter{
		store:      store,
		queueStore: queueStore,
		groupStore: groupStore,
		topicIndex: storage.NewTopicIndex(),
	}

	// Rebuild topic index from existing queues
	queues, err := queueStore.List()
	if err == nil {
		for _, cfg := range queues {
			adapter.topicIndex.AddQueue(cfg.Name, cfg.Topics)
		}
	}

	return adapter, nil
}

// Close closes the adapter and underlying store.
func (a *Adapter) Close() error {
	var lastErr error

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
func (a *Adapter) CreateQueue(ctx context.Context, config types.QueueConfig) error {
	if err := a.store.CreateQueue(config.Name); err != nil {
		if err == ErrAlreadyExists {
			return storage.ErrQueueAlreadyExists
		}
		return err
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
	return a.queueStore.Save(config)
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
	// Remove from topic index
	a.topicIndex.RemoveQueue(queueName)

	if err := a.queueStore.Delete(queueName); err != nil {
		return err
	}
	return a.store.DeleteQueue(queueName)
}

// ListQueues returns all queue configurations.
func (a *Adapter) ListQueues(ctx context.Context) ([]types.QueueConfig, error) {
	return a.queueStore.List()
}

// FindMatchingQueues returns all queues whose topic patterns match the topic.
func (a *Adapter) FindMatchingQueues(ctx context.Context, topic string) ([]string, error) {
	return a.topicIndex.FindMatching(topic), nil
}

// Append adds a message to the end of a queue's log.
func (a *Adapter) Append(ctx context.Context, queueName string, msg *types.Message) (uint64, error) {
	value := msg.GetPayload()
	key := []byte{}

	headers := make(map[string][]byte)
	for k, v := range msg.Properties {
		headers[k] = []byte(v)
	}

	headers["_topic"] = []byte(msg.Topic)
	headers["_id"] = []byte(msg.ID)
	if msg.State != "" {
		headers["_state"] = []byte(msg.State)
	}

	return a.store.Append(queueName, value, key, headers)
}

// AppendBatch adds multiple messages to a queue's log.
func (a *Adapter) AppendBatch(ctx context.Context, queueName string, msgs []*types.Message) (uint64, error) {
	if len(msgs) == 0 {
		return 0, ErrEmptyBatch
	}

	batch := NewBatch(0)

	for _, msg := range msgs {
		value := msg.GetPayload()
		key := []byte{}

		headers := make(map[string][]byte)
		for k, v := range msg.Properties {
			headers[k] = []byte(v)
		}

		headers["_topic"] = []byte(msg.Topic)
		headers["_id"] = []byte(msg.ID)
		if msg.State != "" {
			headers["_state"] = []byte(msg.State)
		}

		batch.Append(value, key, headers)
	}

	return a.store.AppendBatch(queueName, batch)
}

// Read retrieves a message at a specific offset.
func (a *Adapter) Read(ctx context.Context, queueName string, offset uint64) (*types.Message, error) {
	msg, err := a.store.Read(queueName, offset)
	if err != nil {
		if err == ErrOffsetOutOfRange {
			return nil, storage.ErrOffsetOutOfRange
		}
		if err == ErrQueueNotFound {
			return nil, storage.ErrQueueNotFound
		}
		return nil, err
	}

	return logMessageToTypes(msg), nil
}

// ReadBatch reads messages starting from offset up to limit.
func (a *Adapter) ReadBatch(ctx context.Context, queueName string, startOffset uint64, limit int) ([]*types.Message, error) {
	if limit <= 0 {
		return []*types.Message{}, nil
	}

	tail, err := a.store.Tail(queueName)
	if err != nil {
		if err == ErrQueueNotFound {
			return nil, storage.ErrQueueNotFound
		}
		return nil, err
	}

	if startOffset >= tail {
		return []*types.Message{}, nil
	}

	result := make([]*types.Message, 0, limit)
	current := startOffset

	for current < tail && len(result) < limit {
		batch, err := a.store.ReadBatch(queueName, current)
		if err != nil {
			if err == ErrOffsetOutOfRange {
				break
			}
			if err == ErrQueueNotFound {
				return nil, storage.ErrQueueNotFound
			}
			return nil, err
		}

		for _, msg := range batch.ToMessages() {
			if msg.Offset < startOffset {
				continue
			}
			if msg.Offset >= tail {
				return result, nil
			}
			result = append(result, logMessageToTypes(&msg))
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

// Head returns the first valid offset in the queue.
func (a *Adapter) Head(ctx context.Context, queueName string) (uint64, error) {
	return a.store.Head(queueName)
}

// Tail returns the next offset that will be assigned.
func (a *Adapter) Tail(ctx context.Context, queueName string) (uint64, error) {
	return a.store.Tail(queueName)
}

// Truncate removes all messages with offset < minOffset.
func (a *Adapter) Truncate(ctx context.Context, queueName string, minOffset uint64) error {
	return a.store.Truncate(queueName, minOffset)
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
func (a *Adapter) CreateConsumerGroup(ctx context.Context, group *types.ConsumerGroupState) error {
	existing, _ := a.groupStore.Get(group.QueueName, group.ID)
	if existing != nil {
		return storage.ErrConsumerGroupExists
	}

	return a.groupStore.Save(group)
}

// GetConsumerGroup retrieves a consumer group's state.
func (a *Adapter) GetConsumerGroup(ctx context.Context, queueName, groupID string) (*types.ConsumerGroupState, error) {
	group, err := a.groupStore.Get(queueName, groupID)
	if err != nil {
		return nil, err
	}

	// Sync cursors from the log store's cursor state
	a.syncCursorsFromStore(queueName, groupID, group)

	// Sync PEL from the log store's PEL state
	a.syncPELFromStore(queueName, groupID, group)

	return group, nil
}

// UpdateConsumerGroup updates a consumer group's state.
func (a *Adapter) UpdateConsumerGroup(ctx context.Context, group *types.ConsumerGroupState) error {
	group.UpdatedAt = time.Now()
	return a.groupStore.Save(group)
}

// DeleteConsumerGroup removes a consumer group.
func (a *Adapter) DeleteConsumerGroup(ctx context.Context, queueName, groupID string) error {
	return a.groupStore.Delete(queueName, groupID)
}

// ListConsumerGroups lists all consumer groups for a queue.
func (a *Adapter) ListConsumerGroups(ctx context.Context, queueName string) ([]*types.ConsumerGroupState, error) {
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
		a.groupStore.Save(group)
	}

	return nil
}

// RemovePendingEntry removes an entry from a consumer's PEL.
func (a *Adapter) RemovePendingEntry(ctx context.Context, queueName, groupID, consumerID string, offset uint64) error {
	if err := a.store.AckPending(queueName, groupID, offset); err != nil {
		if err == ErrPELEntryNotFound {
			return storage.ErrPendingEntryNotFound
		}
		return err
	}

	// Update the group state's PEL as well
	group, err := a.groupStore.Get(queueName, groupID)
	if err == nil {
		group.RemovePending(consumerID, offset)
		a.groupStore.Save(group)
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
		if err == ErrPELEntryNotFound {
			return storage.ErrPendingEntryNotFound
		}
		return err
	}

	// Update the group state's PEL as well
	group, err := a.groupStore.Get(queueName, groupID)
	if err == nil {
		group.TransferPending(offset, fromConsumer, toConsumer)
		a.groupStore.Save(group)
	}

	return nil
}

// UpdateCursor updates the cursor position for a queue.
func (a *Adapter) UpdateCursor(ctx context.Context, queueName, groupID string, cursor uint64) error {
	if err := a.store.SetCursor(queueName, groupID, cursor); err != nil {
		return err
	}

	// Update the group state's cursor as well
	group, err := a.groupStore.Get(queueName, groupID)
	if err == nil {
		c := group.GetCursor()
		c.Cursor = cursor
		group.UpdatedAt = time.Now()
		a.groupStore.Save(group)
	}

	return nil
}

// UpdateCommitted updates the committed offset for a queue.
func (a *Adapter) UpdateCommitted(ctx context.Context, queueName, groupID string, committed uint64) error {
	if err := a.store.CommitOffset(queueName, groupID, committed); err != nil {
		return err
	}

	// Update the group state's committed offset as well
	group, err := a.groupStore.Get(queueName, groupID)
	if err == nil {
		c := group.GetCursor()
		c.Committed = committed
		group.UpdatedAt = time.Now()
		a.groupStore.Save(group)
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
func (a *Adapter) syncCursorsFromStore(queueName, groupID string, group *types.ConsumerGroupState) {
	cursorState, err := a.store.GetCursorState(queueName, groupID)
	if err != nil {
		return
	}

	c := group.GetCursor()
	c.Cursor = cursorState.Cursor
	c.Committed = cursorState.Committed
}

// syncPELFromStore syncs PEL state from the log store to the group state.
func (a *Adapter) syncPELFromStore(queueName, groupID string, group *types.ConsumerGroupState) {
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

// logMessageToTypes converts a log Message to a types.Message.
func logMessageToTypes(msg *Message) *types.Message {
	result := &types.Message{
		Sequence:   msg.Offset,
		Payload:    msg.Value,
		CreatedAt:  msg.Timestamp,
		Properties: make(map[string]string),
	}

	for k, v := range msg.Headers {
		switch k {
		case "_topic":
			result.Topic = string(v)
		case "_id":
			result.ID = string(v)
		case "_state":
			result.State = types.MessageState(v)
		default:
			result.Properties[k] = string(v)
		}
	}

	return result
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
		ClaimedAt:     time.UnixMilli(entry.DeliveredAt),
		DeliveryCount: int(entry.DeliveryCount),
	}
}

// matchTopic checks if a topic matches a topic pattern using MQTT-style wildcards.
func matchTopic(pattern, topic string) bool {
	return topics.TopicMatch(pattern, topic)
}
