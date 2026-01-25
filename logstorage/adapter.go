// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package logstorage

import (
	"context"
	"time"

	"github.com/absmach/fluxmq/queue/storage"
	"github.com/absmach/fluxmq/queue/types"
)

// Adapter wraps the log Store and implements the storage.LogStore and
// storage.ConsumerGroupStore interfaces for integration with the queue system.
type Adapter struct {
	store       *Store
	queueConfig *QueueConfigStore
	groupStore  *ConsumerGroupStateStore
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

	queueConfig, err := NewQueueConfigStore(baseDir)
	if err != nil {
		store.Close()
		return nil, err
	}

	groupStore, err := NewConsumerGroupStateStore(baseDir)
	if err != nil {
		store.Close()
		queueConfig.Close()
		return nil, err
	}

	return &Adapter{
		store:       store,
		queueConfig: queueConfig,
		groupStore:  groupStore,
	}, nil
}

// Close closes the adapter and underlying store.
func (a *Adapter) Close() error {
	var lastErr error

	if err := a.groupStore.Close(); err != nil {
		lastErr = err
	}

	if err := a.queueConfig.Close(); err != nil {
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

// LogStore interface implementation

// CreateQueue creates a new queue with the given configuration.
func (a *Adapter) CreateQueue(ctx context.Context, config types.QueueConfig) error {
	if err := a.store.CreateQueue(config.Name, config.Partitions); err != nil {
		if err == ErrAlreadyExists {
			return storage.ErrConsumerGroupExists
		}
		return err
	}

	return a.queueConfig.Save(config)
}

// GetQueue retrieves a queue's configuration.
func (a *Adapter) GetQueue(ctx context.Context, queueName string) (*types.QueueConfig, error) {
	config, err := a.queueConfig.Get(queueName)
	if err != nil {
		return nil, storage.ErrQueueNotFound
	}
	return config, nil
}

// DeleteQueue deletes a queue and all its data.
func (a *Adapter) DeleteQueue(ctx context.Context, queueName string) error {
	if err := a.queueConfig.Delete(queueName); err != nil {
		return err
	}
	return a.store.DeleteQueue(queueName)
}

// ListQueues returns all queue configurations.
func (a *Adapter) ListQueues(ctx context.Context) ([]types.QueueConfig, error) {
	return a.queueConfig.List()
}

// Append adds a message to the end of a partition's log.
func (a *Adapter) Append(ctx context.Context, queueName string, partitionID int, msg *types.Message) (uint64, error) {
	value := msg.GetPayload()
	key := []byte(msg.PartitionKey)

	headers := make(map[string][]byte)
	for k, v := range msg.Properties {
		headers[k] = []byte(v)
	}

	headers["_topic"] = []byte(msg.Topic)
	headers["_id"] = []byte(msg.ID)
	if msg.State != "" {
		headers["_state"] = []byte(msg.State)
	}

	return a.store.Append(queueName, uint32(partitionID), value, key, headers)
}

// AppendBatch adds multiple messages to a partition's log.
func (a *Adapter) AppendBatch(ctx context.Context, queueName string, partitionID int, msgs []*types.Message) (uint64, error) {
	if len(msgs) == 0 {
		return 0, ErrEmptyBatch
	}

	batch := NewBatch(0)

	for _, msg := range msgs {
		value := msg.GetPayload()
		key := []byte(msg.PartitionKey)

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

	return a.store.AppendBatch(queueName, uint32(partitionID), batch)
}

// Read retrieves a message at a specific offset.
func (a *Adapter) Read(ctx context.Context, queueName string, partitionID int, offset uint64) (*types.Message, error) {
	msg, err := a.store.Read(queueName, uint32(partitionID), offset)
	if err != nil {
		if err == ErrOffsetOutOfRange {
			return nil, storage.ErrOffsetOutOfRange
		}
		if err == ErrPartitionNotFound {
			return nil, storage.ErrPartitionNotFound
		}
		return nil, err
	}

	return logMessageToTypes(msg, partitionID), nil
}

// ReadBatch reads messages starting from offset up to limit.
func (a *Adapter) ReadBatch(ctx context.Context, queueName string, partitionID int, startOffset uint64, limit int) ([]*types.Message, error) {
	tail, err := a.store.Tail(queueName, uint32(partitionID))
	if err != nil {
		return nil, err
	}

	messages, err := a.store.ReadRange(queueName, uint32(partitionID), startOffset, tail, limit)
	if err != nil {
		if err == ErrOffsetOutOfRange {
			return nil, storage.ErrOffsetOutOfRange
		}
		return nil, err
	}

	result := make([]*types.Message, len(messages))
	for i, msg := range messages {
		result[i] = logMessageToTypes(&msg, partitionID)
	}

	return result, nil
}

// Head returns the first valid offset in the partition.
func (a *Adapter) Head(ctx context.Context, queueName string, partitionID int) (uint64, error) {
	return a.store.Head(queueName, uint32(partitionID))
}

// Tail returns the next offset that will be assigned.
func (a *Adapter) Tail(ctx context.Context, queueName string, partitionID int) (uint64, error) {
	return a.store.Tail(queueName, uint32(partitionID))
}

// Truncate removes all messages with offset < minOffset.
func (a *Adapter) Truncate(ctx context.Context, queueName string, partitionID int, minOffset uint64) error {
	return a.store.Truncate(queueName, uint32(partitionID), minOffset)
}

// Count returns the number of messages in a partition.
func (a *Adapter) Count(ctx context.Context, queueName string, partitionID int) (uint64, error) {
	return a.store.Count(queueName, uint32(partitionID))
}

// TotalCount returns total messages across all partitions.
func (a *Adapter) TotalCount(ctx context.Context, queueName string) (uint64, error) {
	partitions, err := a.store.ListPartitions(queueName)
	if err != nil {
		return 0, err
	}

	var total uint64
	for _, partitionID := range partitions {
		count, err := a.store.Count(queueName, partitionID)
		if err != nil {
			return 0, err
		}
		total += count
	}

	return total, nil
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
		PartitionID:   uint32(entry.PartitionID),
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
func (a *Adapter) RemovePendingEntry(ctx context.Context, queueName, groupID, consumerID string, partitionID int, offset uint64) error {
	if err := a.store.AckPending(queueName, groupID, offset); err != nil {
		if err == ErrPELEntryNotFound {
			return storage.ErrPendingEntryNotFound
		}
		return err
	}

	// Update the group state's PEL as well
	group, err := a.groupStore.Get(queueName, groupID)
	if err == nil {
		group.RemovePending(consumerID, partitionID, offset)
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
func (a *Adapter) TransferPendingEntry(ctx context.Context, queueName, groupID string, partitionID int, offset uint64, fromConsumer, toConsumer string) error {
	if err := a.store.ClaimPending(queueName, groupID, offset, toConsumer); err != nil {
		if err == ErrPELEntryNotFound {
			return storage.ErrPendingEntryNotFound
		}
		return err
	}

	// Update the group state's PEL as well
	group, err := a.groupStore.Get(queueName, groupID)
	if err == nil {
		group.TransferPending(partitionID, offset, fromConsumer, toConsumer)
		a.groupStore.Save(group)
	}

	return nil
}

// UpdateCursor updates the cursor position for a partition.
func (a *Adapter) UpdateCursor(ctx context.Context, queueName, groupID string, partitionID int, cursor uint64) error {
	if err := a.store.SetCursor(queueName, groupID, uint32(partitionID), cursor); err != nil {
		return err
	}

	// Update the group state's cursor as well
	group, err := a.groupStore.Get(queueName, groupID)
	if err == nil {
		c := group.GetCursor(partitionID)
		c.Cursor = cursor
		group.UpdatedAt = time.Now()
		a.groupStore.Save(group)
	}

	return nil
}

// UpdateCommitted updates the committed offset for a partition.
func (a *Adapter) UpdateCommitted(ctx context.Context, queueName, groupID string, partitionID int, committed uint64) error {
	if err := a.store.CommitOffset(queueName, groupID, uint32(partitionID), committed); err != nil {
		return err
	}

	// Update the group state's committed offset as well
	group, err := a.groupStore.Get(queueName, groupID)
	if err == nil {
		c := group.GetCursor(partitionID)
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

	group.Consumers[consumer.ID] = consumer
	group.UpdatedAt = time.Now()

	return a.groupStore.Save(group)
}

// UnregisterConsumer removes a consumer from a group.
func (a *Adapter) UnregisterConsumer(ctx context.Context, queueName, groupID, consumerID string) error {
	group, err := a.groupStore.Get(queueName, groupID)
	if err != nil {
		return err
	}

	delete(group.Consumers, consumerID)
	group.UpdatedAt = time.Now()

	return a.groupStore.Save(group)
}

// ListConsumers lists all consumers in a group.
func (a *Adapter) ListConsumers(ctx context.Context, queueName, groupID string) ([]*types.ConsumerInfo, error) {
	group, err := a.groupStore.Get(queueName, groupID)
	if err != nil {
		return nil, err
	}

	result := make([]*types.ConsumerInfo, 0, len(group.Consumers))
	for _, consumer := range group.Consumers {
		result = append(result, consumer)
	}

	return result, nil
}

// Sync flushes all pending writes to disk.
func (a *Adapter) Sync() error {
	if err := a.store.Sync(); err != nil {
		return err
	}

	if err := a.queueConfig.Sync(); err != nil {
		return err
	}

	return a.groupStore.Sync()
}

// Helper functions

// syncCursorsFromStore syncs cursor state from the log store to the group state.
func (a *Adapter) syncCursorsFromStore(queueName, groupID string, group *types.ConsumerGroupState) {
	cursors, err := a.store.GetAllCursors(queueName, groupID)
	if err != nil {
		return
	}

	for partitionID, cursor := range cursors {
		c := group.GetCursor(int(partitionID))
		c.Cursor = cursor.Cursor
		c.Committed = cursor.Committed
	}
}

// syncPELFromStore syncs PEL state from the log store to the group state.
func (a *Adapter) syncPELFromStore(queueName, groupID string, group *types.ConsumerGroupState) {
	allEntries, err := a.store.GetAllPending(queueName, groupID)
	if err != nil {
		return
	}

	group.PEL = make(map[string][]*types.PendingEntry)
	for consumerID, entries := range allEntries {
		typeEntries := make([]*types.PendingEntry, len(entries))
		for i, e := range entries {
			typeEntries[i] = pelEntryToTypes(&e)
		}
		group.PEL[consumerID] = typeEntries
	}
}

// logMessageToTypes converts a log Message to a types.Message.
func logMessageToTypes(msg *Message, partitionID int) *types.Message {
	result := &types.Message{
		Sequence:    msg.Offset,
		Payload:     msg.Value,
		PartitionID: partitionID,
		CreatedAt:   msg.Timestamp,
		Properties:  make(map[string]string),
	}

	if msg.Key != nil {
		result.PartitionKey = string(msg.Key)
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
		PartitionID:   int(entry.PartitionID),
		ConsumerID:    entry.ConsumerID,
		ClaimedAt:     time.UnixMilli(entry.ClaimedAt),
		DeliveryCount: int(entry.DeliveryCount),
	}
}

// pendingEntryToTypes converts a log PendingEntry to a types.PendingEntry.
func pendingEntryToTypes(entry *PendingEntry) *types.PendingEntry {
	return &types.PendingEntry{
		Offset:        entry.Offset,
		PartitionID:   int(entry.PartitionID),
		ConsumerID:    entry.ConsumerID,
		ClaimedAt:     time.UnixMilli(entry.DeliveredAt),
		DeliveryCount: int(entry.DeliveryCount),
	}
}

// Compile-time interface assertions
var (
	_ storage.LogStore           = (*Adapter)(nil)
	_ storage.ConsumerGroupStore = (*Adapter)(nil)
)
