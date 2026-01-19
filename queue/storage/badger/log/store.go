// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

// Package log provides a BadgerDB implementation of the log-based queue storage.
// This implementation uses append-only logs with offset-based access, inspired by
// Kafka and Redis Streams.
package log

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"time"

	"github.com/absmach/fluxmq/queue/storage"
	"github.com/absmach/fluxmq/queue/types"
	"github.com/dgraph-io/badger/v4"
)

// Key prefixes for BadgerDB storage.
const (
	queueMetaPrefix  = "log:queue:"   // Queue metadata
	logMessagePrefix = "log:msg:"     // Log messages: log:msg:{queue}:{partition}:{offset}
	logTailPrefix    = "log:tail:"    // Tail offset: log:tail:{queue}:{partition}
	logHeadPrefix    = "log:head:"    // Head offset: log:head:{queue}:{partition}
	groupMetaPrefix  = "log:group:"   // Consumer group: log:group:{queue}:{groupID}
	groupPELPrefix   = "log:pel:"     // PEL entries: log:pel:{queue}:{groupID}:{consumer}:{partition}:{offset}
	consumerPrefix   = "log:consumer:" // Consumer info: log:consumer:{queue}:{groupID}:{consumerID}
)

// Store implements LogStore and ConsumerGroupStore using BadgerDB.
type Store struct {
	db *badger.DB
}

// New creates a new BadgerDB log store.
func New(db *badger.DB) *Store {
	return &Store{db: db}
}

// --- LogStore Implementation ---

// CreateQueue creates a new queue with the specified configuration.
func (s *Store) CreateQueue(ctx context.Context, config types.QueueConfig) error {
	if err := config.Validate(); err != nil {
		return err
	}

	key := queueMetaPrefix + config.Name
	data, err := json.Marshal(config)
	if err != nil {
		return err
	}

	return s.db.Update(func(txn *badger.Txn) error {
		// Check if queue already exists
		_, err := txn.Get([]byte(key))
		if err == nil {
			return storage.ErrQueueAlreadyExists
		}
		if err != badger.ErrKeyNotFound {
			return err
		}

		// Set queue metadata
		if err := txn.Set([]byte(key), data); err != nil {
			return err
		}

		// Initialize head/tail for each partition
		for i := 0; i < config.Partitions; i++ {
			tailKey := s.tailKey(config.Name, i)
			headKey := s.headKey(config.Name, i)

			if err := txn.Set([]byte(tailKey), uint64ToBytes(0)); err != nil {
				return err
			}
			if err := txn.Set([]byte(headKey), uint64ToBytes(0)); err != nil {
				return err
			}
		}

		return nil
	})
}

// GetQueue retrieves queue configuration.
func (s *Store) GetQueue(ctx context.Context, queueName string) (*types.QueueConfig, error) {
	key := queueMetaPrefix + queueName
	var config types.QueueConfig

	err := s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(key))
		if err != nil {
			if err == badger.ErrKeyNotFound {
				return storage.ErrQueueNotFound
			}
			return err
		}

		return item.Value(func(val []byte) error {
			return json.Unmarshal(val, &config)
		})
	})
	if err != nil {
		return nil, err
	}

	return &config, nil
}

// DeleteQueue removes a queue and all its data.
func (s *Store) DeleteQueue(ctx context.Context, queueName string) error {
	return s.db.Update(func(txn *badger.Txn) error {
		// Delete queue metadata
		metaKey := queueMetaPrefix + queueName
		if err := txn.Delete([]byte(metaKey)); err != nil && err != badger.ErrKeyNotFound {
			return err
		}

		// Delete all messages, head/tail markers, and groups
		// Use prefix scan to find and delete all related keys
		prefixes := []string{
			logMessagePrefix + queueName + ":",
			logTailPrefix + queueName + ":",
			logHeadPrefix + queueName + ":",
			groupMetaPrefix + queueName + ":",
			groupPELPrefix + queueName + ":",
			consumerPrefix + queueName + ":",
		}

		for _, prefix := range prefixes {
			if err := s.deleteByPrefix(txn, prefix); err != nil {
				return err
			}
		}

		return nil
	})
}

// ListQueues returns all queue configurations.
func (s *Store) ListQueues(ctx context.Context) ([]types.QueueConfig, error) {
	configs := make([]types.QueueConfig, 0)

	err := s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = []byte(queueMetaPrefix)

		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			err := item.Value(func(val []byte) error {
				var config types.QueueConfig
				if err := json.Unmarshal(val, &config); err != nil {
					return err
				}
				configs = append(configs, config)
				return nil
			})
			if err != nil {
				return err
			}
		}
		return nil
	})

	return configs, err
}

// Append adds a message to the end of a partition's log.
func (s *Store) Append(ctx context.Context, queueName string, partitionID int, msg *types.Message) (uint64, error) {
	var offset uint64

	err := s.db.Update(func(txn *badger.Txn) error {
		// Get current tail
		tailKey := s.tailKey(queueName, partitionID)
		tail, err := s.getUint64(txn, tailKey)
		if err != nil {
			return err
		}

		offset = tail

		// Set message metadata
		msg.Sequence = offset
		msg.PartitionID = partitionID

		// Serialize message
		data, err := s.serializeMessage(msg)
		if err != nil {
			return err
		}

		// Store message
		msgKey := s.messageKey(queueName, partitionID, offset)
		if err := txn.Set([]byte(msgKey), data); err != nil {
			return err
		}

		// Update tail
		return txn.Set([]byte(tailKey), uint64ToBytes(tail+1))
	})

	return offset, err
}

// AppendBatch adds multiple messages to a partition's log.
func (s *Store) AppendBatch(ctx context.Context, queueName string, partitionID int, msgs []*types.Message) (uint64, error) {
	if len(msgs) == 0 {
		return 0, nil
	}

	var firstOffset uint64

	err := s.db.Update(func(txn *badger.Txn) error {
		// Get current tail
		tailKey := s.tailKey(queueName, partitionID)
		tail, err := s.getUint64(txn, tailKey)
		if err != nil {
			return err
		}

		firstOffset = tail

		for _, msg := range msgs {
			offset := tail
			tail++

			// Set message metadata
			msg.Sequence = offset
			msg.PartitionID = partitionID

			// Serialize message
			data, err := s.serializeMessage(msg)
			if err != nil {
				return err
			}

			// Store message
			msgKey := s.messageKey(queueName, partitionID, offset)
			if err := txn.Set([]byte(msgKey), data); err != nil {
				return err
			}
		}

		// Update tail
		return txn.Set([]byte(tailKey), uint64ToBytes(tail))
	})

	return firstOffset, err
}

// Read retrieves a message at a specific offset.
func (s *Store) Read(ctx context.Context, queueName string, partitionID int, offset uint64) (*types.Message, error) {
	var msg *types.Message

	err := s.db.View(func(txn *badger.Txn) error {
		// Check bounds
		head, err := s.getUint64(txn, s.headKey(queueName, partitionID))
		if err != nil {
			return err
		}
		tail, err := s.getUint64(txn, s.tailKey(queueName, partitionID))
		if err != nil {
			return err
		}

		if offset < head || offset >= tail {
			return storage.ErrOffsetOutOfRange
		}

		// Get message
		msgKey := s.messageKey(queueName, partitionID, offset)
		item, err := txn.Get([]byte(msgKey))
		if err != nil {
			if err == badger.ErrKeyNotFound {
				return storage.ErrOffsetOutOfRange
			}
			return err
		}

		return item.Value(func(val []byte) error {
			var err error
			msg, err = s.deserializeMessage(val)
			return err
		})
	})

	return msg, err
}

// ReadBatch reads messages starting from offset up to limit.
func (s *Store) ReadBatch(ctx context.Context, queueName string, partitionID int, startOffset uint64, limit int) ([]*types.Message, error) {
	var msgs []*types.Message

	err := s.db.View(func(txn *badger.Txn) error {
		// Get bounds
		head, err := s.getUint64(txn, s.headKey(queueName, partitionID))
		if err != nil {
			return err
		}
		tail, err := s.getUint64(txn, s.tailKey(queueName, partitionID))
		if err != nil {
			return err
		}

		// Adjust start offset
		if startOffset < head {
			startOffset = head
		}
		if startOffset >= tail {
			msgs = []*types.Message{}
			return nil
		}

		// Calculate end offset
		endOffset := startOffset + uint64(limit)
		if endOffset > tail {
			endOffset = tail
		}

		msgs = make([]*types.Message, 0, endOffset-startOffset)

		for offset := startOffset; offset < endOffset; offset++ {
			msgKey := s.messageKey(queueName, partitionID, offset)
			item, err := txn.Get([]byte(msgKey))
			if err != nil {
				if err == badger.ErrKeyNotFound {
					continue // Skip missing messages (might have been truncated)
				}
				return err
			}

			err = item.Value(func(val []byte) error {
				msg, err := s.deserializeMessage(val)
				if err != nil {
					return err
				}
				msgs = append(msgs, msg)
				return nil
			})
			if err != nil {
				return err
			}
		}

		return nil
	})

	return msgs, err
}

// Head returns the first valid offset in the partition.
func (s *Store) Head(ctx context.Context, queueName string, partitionID int) (uint64, error) {
	var head uint64

	err := s.db.View(func(txn *badger.Txn) error {
		var err error
		head, err = s.getUint64(txn, s.headKey(queueName, partitionID))
		return err
	})

	return head, err
}

// Tail returns the next offset that will be assigned.
func (s *Store) Tail(ctx context.Context, queueName string, partitionID int) (uint64, error) {
	var tail uint64

	err := s.db.View(func(txn *badger.Txn) error {
		var err error
		tail, err = s.getUint64(txn, s.tailKey(queueName, partitionID))
		return err
	})

	return tail, err
}

// Truncate removes all messages with offset < minOffset.
func (s *Store) Truncate(ctx context.Context, queueName string, partitionID int, minOffset uint64) error {
	return s.db.Update(func(txn *badger.Txn) error {
		// Get current head
		headKey := s.headKey(queueName, partitionID)
		head, err := s.getUint64(txn, headKey)
		if err != nil {
			return err
		}

		if minOffset <= head {
			return nil // Nothing to truncate
		}

		// Get tail to ensure we don't truncate past it
		tail, err := s.getUint64(txn, s.tailKey(queueName, partitionID))
		if err != nil {
			return err
		}

		if minOffset > tail {
			minOffset = tail
		}

		// Delete messages in range [head, minOffset)
		for offset := head; offset < minOffset; offset++ {
			msgKey := s.messageKey(queueName, partitionID, offset)
			if err := txn.Delete([]byte(msgKey)); err != nil && err != badger.ErrKeyNotFound {
				return err
			}
		}

		// Update head
		return txn.Set([]byte(headKey), uint64ToBytes(minOffset))
	})
}

// Count returns the number of messages in a partition.
func (s *Store) Count(ctx context.Context, queueName string, partitionID int) (uint64, error) {
	var count uint64

	err := s.db.View(func(txn *badger.Txn) error {
		head, err := s.getUint64(txn, s.headKey(queueName, partitionID))
		if err != nil {
			return err
		}
		tail, err := s.getUint64(txn, s.tailKey(queueName, partitionID))
		if err != nil {
			return err
		}
		count = tail - head
		return nil
	})

	return count, err
}

// TotalCount returns total messages across all partitions.
func (s *Store) TotalCount(ctx context.Context, queueName string) (uint64, error) {
	config, err := s.GetQueue(ctx, queueName)
	if err != nil {
		return 0, err
	}

	var total uint64

	err = s.db.View(func(txn *badger.Txn) error {
		for i := 0; i < config.Partitions; i++ {
			head, err := s.getUint64(txn, s.headKey(queueName, i))
			if err != nil {
				return err
			}
			tail, err := s.getUint64(txn, s.tailKey(queueName, i))
			if err != nil {
				return err
			}
			total += tail - head
		}
		return nil
	})

	return total, err
}

// --- ConsumerGroupStore Implementation ---

// CreateConsumerGroup creates a new consumer group for a queue.
func (s *Store) CreateConsumerGroup(ctx context.Context, group *types.ConsumerGroupState) error {
	key := s.groupKey(group.QueueName, group.ID)
	data, err := json.Marshal(group)
	if err != nil {
		return err
	}

	return s.db.Update(func(txn *badger.Txn) error {
		// Check if group already exists
		_, err := txn.Get([]byte(key))
		if err == nil {
			return storage.ErrConsumerGroupExists
		}
		if err != badger.ErrKeyNotFound {
			return err
		}

		return txn.Set([]byte(key), data)
	})
}

// GetConsumerGroup retrieves a consumer group's state.
func (s *Store) GetConsumerGroup(ctx context.Context, queueName, groupID string) (*types.ConsumerGroupState, error) {
	key := s.groupKey(queueName, groupID)
	var group types.ConsumerGroupState

	err := s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(key))
		if err != nil {
			if err == badger.ErrKeyNotFound {
				return storage.ErrConsumerNotFound
			}
			return err
		}

		return item.Value(func(val []byte) error {
			return json.Unmarshal(val, &group)
		})
	})
	if err != nil {
		return nil, err
	}

	return &group, nil
}

// UpdateConsumerGroup updates a consumer group's state.
func (s *Store) UpdateConsumerGroup(ctx context.Context, group *types.ConsumerGroupState) error {
	key := s.groupKey(group.QueueName, group.ID)
	data, err := json.Marshal(group)
	if err != nil {
		return err
	}

	return s.db.Update(func(txn *badger.Txn) error {
		return txn.Set([]byte(key), data)
	})
}

// DeleteConsumerGroup removes a consumer group.
func (s *Store) DeleteConsumerGroup(ctx context.Context, queueName, groupID string) error {
	return s.db.Update(func(txn *badger.Txn) error {
		// Delete group metadata
		groupKey := s.groupKey(queueName, groupID)
		if err := txn.Delete([]byte(groupKey)); err != nil && err != badger.ErrKeyNotFound {
			return err
		}

		// Delete all PEL entries
		pelPrefix := groupPELPrefix + queueName + ":" + groupID + ":"
		if err := s.deleteByPrefix(txn, pelPrefix); err != nil {
			return err
		}

		// Delete all consumer entries
		consumerPfx := consumerPrefix + queueName + ":" + groupID + ":"
		return s.deleteByPrefix(txn, consumerPfx)
	})
}

// ListConsumerGroups lists all consumer groups for a queue.
func (s *Store) ListConsumerGroups(ctx context.Context, queueName string) ([]*types.ConsumerGroupState, error) {
	var groups []*types.ConsumerGroupState

	err := s.db.View(func(txn *badger.Txn) error {
		prefix := groupMetaPrefix + queueName + ":"
		opts := badger.DefaultIteratorOptions
		opts.Prefix = []byte(prefix)

		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			err := item.Value(func(val []byte) error {
				var group types.ConsumerGroupState
				if err := json.Unmarshal(val, &group); err != nil {
					return err
				}
				groups = append(groups, &group)
				return nil
			})
			if err != nil {
				return err
			}
		}
		return nil
	})

	return groups, err
}

// AddPendingEntry adds an entry to a consumer's PEL.
func (s *Store) AddPendingEntry(ctx context.Context, queueName, groupID string, entry *types.PendingEntry) error {
	group, err := s.GetConsumerGroup(ctx, queueName, groupID)
	if err != nil {
		return err
	}

	group.AddPending(entry.ConsumerID, entry)
	return s.UpdateConsumerGroup(ctx, group)
}

// RemovePendingEntry removes an entry from a consumer's PEL.
func (s *Store) RemovePendingEntry(ctx context.Context, queueName, groupID, consumerID string, partitionID int, offset uint64) error {
	group, err := s.GetConsumerGroup(ctx, queueName, groupID)
	if err != nil {
		return err
	}

	if !group.RemovePending(consumerID, partitionID, offset) {
		return storage.ErrPendingEntryNotFound
	}

	return s.UpdateConsumerGroup(ctx, group)
}

// GetPendingEntries retrieves all pending entries for a consumer.
func (s *Store) GetPendingEntries(ctx context.Context, queueName, groupID, consumerID string) ([]*types.PendingEntry, error) {
	group, err := s.GetConsumerGroup(ctx, queueName, groupID)
	if err != nil {
		return nil, err
	}

	entries, ok := group.PEL[consumerID]
	if !ok {
		return []*types.PendingEntry{}, nil
	}

	result := make([]*types.PendingEntry, len(entries))
	copy(result, entries)
	return result, nil
}

// GetAllPendingEntries retrieves all pending entries for a group.
func (s *Store) GetAllPendingEntries(ctx context.Context, queueName, groupID string) ([]*types.PendingEntry, error) {
	group, err := s.GetConsumerGroup(ctx, queueName, groupID)
	if err != nil {
		return nil, err
	}

	var result []*types.PendingEntry
	for _, entries := range group.PEL {
		result = append(result, entries...)
	}

	return result, nil
}

// TransferPendingEntry moves a pending entry from one consumer to another.
func (s *Store) TransferPendingEntry(ctx context.Context, queueName, groupID string, partitionID int, offset uint64, fromConsumer, toConsumer string) error {
	group, err := s.GetConsumerGroup(ctx, queueName, groupID)
	if err != nil {
		return err
	}

	if !group.TransferPending(partitionID, offset, fromConsumer, toConsumer) {
		return storage.ErrPendingEntryNotFound
	}

	return s.UpdateConsumerGroup(ctx, group)
}

// UpdateCursor updates the cursor position for a partition.
func (s *Store) UpdateCursor(ctx context.Context, queueName, groupID string, partitionID int, cursor uint64) error {
	group, err := s.GetConsumerGroup(ctx, queueName, groupID)
	if err != nil {
		return err
	}

	pc := group.GetCursor(partitionID)
	pc.Cursor = cursor

	return s.UpdateConsumerGroup(ctx, group)
}

// UpdateCommitted updates the committed offset for a partition.
func (s *Store) UpdateCommitted(ctx context.Context, queueName, groupID string, partitionID int, committed uint64) error {
	group, err := s.GetConsumerGroup(ctx, queueName, groupID)
	if err != nil {
		return err
	}

	pc := group.GetCursor(partitionID)
	pc.Committed = committed

	return s.UpdateConsumerGroup(ctx, group)
}

// RegisterConsumer adds a consumer to a group.
func (s *Store) RegisterConsumer(ctx context.Context, queueName, groupID string, consumer *types.ConsumerInfo) error {
	group, err := s.GetConsumerGroup(ctx, queueName, groupID)
	if err != nil {
		return err
	}

	group.Consumers[consumer.ID] = consumer
	return s.UpdateConsumerGroup(ctx, group)
}

// UnregisterConsumer removes a consumer from a group.
func (s *Store) UnregisterConsumer(ctx context.Context, queueName, groupID, consumerID string) error {
	group, err := s.GetConsumerGroup(ctx, queueName, groupID)
	if err != nil {
		return err
	}

	delete(group.Consumers, consumerID)
	delete(group.PEL, consumerID)

	return s.UpdateConsumerGroup(ctx, group)
}

// ListConsumers lists all consumers in a group.
func (s *Store) ListConsumers(ctx context.Context, queueName, groupID string) ([]*types.ConsumerInfo, error) {
	group, err := s.GetConsumerGroup(ctx, queueName, groupID)
	if err != nil {
		return nil, err
	}

	result := make([]*types.ConsumerInfo, 0, len(group.Consumers))
	for _, c := range group.Consumers {
		result = append(result, c)
	}

	return result, nil
}

// --- Helper Methods ---

func (s *Store) messageKey(queueName string, partitionID int, offset uint64) string {
	return fmt.Sprintf("%s%s:%d:%020d", logMessagePrefix, queueName, partitionID, offset)
}

func (s *Store) tailKey(queueName string, partitionID int) string {
	return fmt.Sprintf("%s%s:%d", logTailPrefix, queueName, partitionID)
}

func (s *Store) headKey(queueName string, partitionID int) string {
	return fmt.Sprintf("%s%s:%d", logHeadPrefix, queueName, partitionID)
}

func (s *Store) groupKey(queueName, groupID string) string {
	return fmt.Sprintf("%s%s:%s", groupMetaPrefix, queueName, groupID)
}

func (s *Store) getUint64(txn *badger.Txn, key string) (uint64, error) {
	item, err := txn.Get([]byte(key))
	if err != nil {
		if err == badger.ErrKeyNotFound {
			return 0, storage.ErrQueueNotFound
		}
		return 0, err
	}

	var val uint64
	err = item.Value(func(v []byte) error {
		val = bytesToUint64(v)
		return nil
	})
	return val, err
}

func (s *Store) deleteByPrefix(txn *badger.Txn, prefix string) error {
	opts := badger.DefaultIteratorOptions
	opts.Prefix = []byte(prefix)
	opts.PrefetchValues = false

	it := txn.NewIterator(opts)
	defer it.Close()

	for it.Rewind(); it.Valid(); it.Next() {
		key := it.Item().KeyCopy(nil)
		if err := txn.Delete(key); err != nil {
			return err
		}
	}

	return nil
}

// Message serialization (excluding PayloadBuf which can't be serialized)
type serializedMessage struct {
	ID            string
	Payload       []byte
	Topic         string
	PartitionKey  string
	PartitionID   int
	Sequence      uint64
	Properties    map[string]string
	State         types.MessageState
	CreatedAt     int64 // Unix nano
	DeliveredAt   int64
	NextRetryAt   int64
	RetryCount    int
	FailureReason string
	FirstAttempt  int64
	LastAttempt   int64
	MovedToDLQAt  int64
	ExpiresAt     int64
}

func (s *Store) serializeMessage(msg *types.Message) ([]byte, error) {
	sm := serializedMessage{
		ID:            msg.ID,
		Payload:       msg.GetPayload(), // Get payload from either source
		Topic:         msg.Topic,
		PartitionKey:  msg.PartitionKey,
		PartitionID:   msg.PartitionID,
		Sequence:      msg.Sequence,
		Properties:    msg.Properties,
		State:         msg.State,
		CreatedAt:     msg.CreatedAt.UnixNano(),
		DeliveredAt:   msg.DeliveredAt.UnixNano(),
		NextRetryAt:   msg.NextRetryAt.UnixNano(),
		RetryCount:    msg.RetryCount,
		FailureReason: msg.FailureReason,
		FirstAttempt:  msg.FirstAttempt.UnixNano(),
		LastAttempt:   msg.LastAttempt.UnixNano(),
		MovedToDLQAt:  msg.MovedToDLQAt.UnixNano(),
		ExpiresAt:     msg.ExpiresAt.UnixNano(),
	}
	return json.Marshal(sm)
}

func (s *Store) deserializeMessage(data []byte) (*types.Message, error) {
	var sm serializedMessage
	if err := json.Unmarshal(data, &sm); err != nil {
		return nil, err
	}

	msg := &types.Message{
		ID:            sm.ID,
		Payload:       sm.Payload,
		Topic:         sm.Topic,
		PartitionKey:  sm.PartitionKey,
		PartitionID:   sm.PartitionID,
		Sequence:      sm.Sequence,
		Properties:    sm.Properties,
		State:         sm.State,
		RetryCount:    sm.RetryCount,
		FailureReason: sm.FailureReason,
	}

	if sm.CreatedAt > 0 {
		msg.CreatedAt = unixNanoToTime(sm.CreatedAt)
	}
	if sm.DeliveredAt > 0 {
		msg.DeliveredAt = unixNanoToTime(sm.DeliveredAt)
	}
	if sm.NextRetryAt > 0 {
		msg.NextRetryAt = unixNanoToTime(sm.NextRetryAt)
	}
	if sm.FirstAttempt > 0 {
		msg.FirstAttempt = unixNanoToTime(sm.FirstAttempt)
	}
	if sm.LastAttempt > 0 {
		msg.LastAttempt = unixNanoToTime(sm.LastAttempt)
	}
	if sm.MovedToDLQAt > 0 {
		msg.MovedToDLQAt = unixNanoToTime(sm.MovedToDLQAt)
	}
	if sm.ExpiresAt > 0 {
		msg.ExpiresAt = unixNanoToTime(sm.ExpiresAt)
	}

	return msg, nil
}

func uint64ToBytes(v uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, v)
	return b
}

func bytesToUint64(b []byte) uint64 {
	if len(b) < 8 {
		return 0
	}
	return binary.BigEndian.Uint64(b)
}

func unixNanoToTime(ns int64) time.Time {
	return time.Unix(0, ns)
}

// Compile-time interface assertions
var (
	_ storage.LogStore           = (*Store)(nil)
	_ storage.ConsumerGroupStore = (*Store)(nil)
)
