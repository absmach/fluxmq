// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package memory

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/absmach/mqtt/queue/storage"
)

// Store implements all queue storage interfaces using in-memory maps.
// This implementation is primarily for testing and development.
type Store struct {
	queues    map[string]storage.QueueConfig
	messages  map[string]map[int]map[uint64]*storage.Message     // queueName -> partitionID -> sequence -> message
	inflight  map[string]map[string]*storage.DeliveryState       // queueName -> messageID -> state
	dlq       map[string]map[string]*storage.Message             // dlqTopic -> messageID -> message
	consumers map[string]map[string]map[string]*storage.Consumer // queueName -> groupID -> consumerID -> consumer
	sequences map[string]map[int]uint64                          // queueName -> partitionID -> nextSeq
	offsets   map[string]map[int]uint64                          // queueName -> partitionID -> offset
	counts    map[string]int64                                   // queueName -> message count (for O(1) Count())
	mu        sync.RWMutex
}

// New creates a new in-memory queue store.
func New() *Store {
	return &Store{
		queues:    make(map[string]storage.QueueConfig),
		messages:  make(map[string]map[int]map[uint64]*storage.Message),
		inflight:  make(map[string]map[string]*storage.DeliveryState),
		dlq:       make(map[string]map[string]*storage.Message),
		consumers: make(map[string]map[string]map[string]*storage.Consumer),
		sequences: make(map[string]map[int]uint64),
		offsets:   make(map[string]map[int]uint64),
		counts:    make(map[string]int64),
	}
}

// QueueStore implementation

func (s *Store) CreateQueue(ctx context.Context, config storage.QueueConfig) error {
	if err := config.Validate(); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.queues[config.Name]; exists {
		return storage.ErrQueueAlreadyExists
	}

	s.queues[config.Name] = config
	s.messages[config.Name] = make(map[int]map[uint64]*storage.Message)
	s.inflight[config.Name] = make(map[string]*storage.DeliveryState)
	s.consumers[config.Name] = make(map[string]map[string]*storage.Consumer)
	s.sequences[config.Name] = make(map[int]uint64)
	s.offsets[config.Name] = make(map[int]uint64)
	s.counts[config.Name] = 0 // Initialize message counter

	// Initialize partitions
	for i := 0; i < config.Partitions; i++ {
		s.messages[config.Name][i] = make(map[uint64]*storage.Message)
		s.sequences[config.Name][i] = 0
		s.offsets[config.Name][i] = 0
	}

	return nil
}

func (s *Store) GetQueue(ctx context.Context, queueName string) (*storage.QueueConfig, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	config, exists := s.queues[queueName]
	if !exists {
		return nil, storage.ErrQueueNotFound
	}

	configCopy := config
	return &configCopy, nil
}

func (s *Store) UpdateQueue(ctx context.Context, config storage.QueueConfig) error {
	if err := config.Validate(); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.queues[config.Name]; !exists {
		return storage.ErrQueueNotFound
	}

	s.queues[config.Name] = config
	return nil
}

func (s *Store) DeleteQueue(ctx context.Context, queueName string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.queues[queueName]; !exists {
		return storage.ErrQueueNotFound
	}

	delete(s.queues, queueName)
	delete(s.messages, queueName)
	delete(s.inflight, queueName)
	delete(s.consumers, queueName)
	delete(s.sequences, queueName)
	delete(s.offsets, queueName)

	return nil
}

func (s *Store) ListQueues(ctx context.Context) ([]storage.QueueConfig, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	configs := make([]storage.QueueConfig, 0, len(s.queues))
	for _, config := range s.queues {
		configs = append(configs, config)
	}

	return configs, nil
}

// MessageStore implementation

func (s *Store) Enqueue(ctx context.Context, queueName string, msg *storage.Message) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	partitions, exists := s.messages[queueName]
	if !exists {
		return storage.ErrQueueNotFound
	}

	partition, exists := partitions[msg.PartitionID]
	if !exists {
		return fmt.Errorf("partition %d not found", msg.PartitionID)
	}

	// Deep copy the message
	msgCopy := s.deepCopyMessage(msg)
	partition[msg.Sequence] = msgCopy
	s.counts[queueName]++ // Increment message counter
	return nil
}

// Count returns the number of messages in the queue (across all partitions).
// Optimized to use O(1) counter instead of O(n) scan.
func (s *Store) Count(ctx context.Context, queueName string) (int64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if _, exists := s.queues[queueName]; !exists {
		return 0, storage.ErrQueueNotFound
	}

	return s.counts[queueName], nil
}

// deepCopyMessage creates a deep copy of a message including Properties map.
func (s *Store) deepCopyMessage(msg *storage.Message) *storage.Message {
	// Copy properties map
	props := make(map[string]string, len(msg.Properties))
	for k, v := range msg.Properties {
		props[k] = v
	}

	return &storage.Message{
		ID:            msg.ID,
		Payload:       msg.Payload,
		Topic:         msg.Topic,
		PartitionKey:  msg.PartitionKey,
		PartitionID:   msg.PartitionID,
		Sequence:      msg.Sequence,
		Properties:    props,
		State:         msg.State,
		CreatedAt:     msg.CreatedAt,
		DeliveredAt:   msg.DeliveredAt,
		NextRetryAt:   msg.NextRetryAt,
		RetryCount:    msg.RetryCount,
		FailureReason: msg.FailureReason,
		FirstAttempt:  msg.FirstAttempt,
		LastAttempt:   msg.LastAttempt,
		MovedToDLQAt:  msg.MovedToDLQAt,
		ExpiresAt:     msg.ExpiresAt,
	}
}

func (s *Store) Dequeue(ctx context.Context, queueName string, partitionID int) (*storage.Message, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	partitions, exists := s.messages[queueName]
	if !exists {
		return nil, storage.ErrQueueNotFound
	}

	partition, exists := partitions[partitionID]
	if !exists {
		return nil, fmt.Errorf("partition %d not found", partitionID)
	}

	// Find first queued or ready-to-retry message (ordered by sequence)
	var minSeq uint64 = ^uint64(0)
	var msg *storage.Message

	for seq, m := range partition {
		if seq < minSeq {
			if m.State == storage.StateQueued ||
				(m.State == storage.StateRetry && time.Now().After(m.NextRetryAt)) {
				minSeq = seq
				msg = m
			}
		}
	}

	if msg != nil {
		msgCopy := *msg
		return &msgCopy, nil
	}

	return nil, nil
}

func (s *Store) DequeueBatch(ctx context.Context, queueName string, partitionID int, limit int) ([]*storage.Message, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	partitions, exists := s.messages[queueName]
	if !exists {
		return nil, storage.ErrQueueNotFound
	}

	partition, exists := partitions[partitionID]
	if !exists {
		return nil, fmt.Errorf("partition %d not found", partitionID)
	}

	// Collect all sequences for available messages
	type seqMsg struct {
		seq uint64
		msg *storage.Message
	}
	var available []seqMsg

	now := time.Now()
	for seq, m := range partition {
		if m.State == storage.StateQueued ||
			(m.State == storage.StateRetry && now.After(m.NextRetryAt)) {
			available = append(available, seqMsg{seq, m})
		}
	}

	// Sort by sequence to maintain order
	for i := 0; i < len(available)-1; i++ {
		for j := i + 1; j < len(available); j++ {
			if available[i].seq > available[j].seq {
				available[i], available[j] = available[j], available[i]
			}
		}
	}

	// Take up to limit messages
	count := len(available)
	if limit > 0 && count > limit {
		count = limit
	}

	messages := make([]*storage.Message, 0, count)
	for i := 0; i < count; i++ {
		msgCopy := *available[i].msg
		messages = append(messages, &msgCopy)
	}

	return messages, nil
}

func (s *Store) UpdateMessage(ctx context.Context, queueName string, msg *storage.Message) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	partitions, exists := s.messages[queueName]
	if !exists {
		return storage.ErrQueueNotFound
	}

	partition, exists := partitions[msg.PartitionID]
	if !exists {
		return fmt.Errorf("partition %d not found", msg.PartitionID)
	}

	msgCopy := *msg
	partition[msg.Sequence] = &msgCopy
	return nil
}

func (s *Store) DeleteMessage(ctx context.Context, queueName string, messageID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	partitions, exists := s.messages[queueName]
	if !exists {
		return storage.ErrQueueNotFound
	}

	// Search across all partitions
	for _, partition := range partitions {
		for seq, msg := range partition {
			if msg.ID == messageID {
				delete(partition, seq)
				s.counts[queueName]-- // Decrement message counter
				return nil
			}
		}
	}

	return storage.ErrMessageNotFound
}

func (s *Store) GetMessage(ctx context.Context, queueName string, messageID string) (*storage.Message, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	partitions, exists := s.messages[queueName]
	if !exists {
		return nil, storage.ErrQueueNotFound
	}

	// Search across all partitions
	for _, partition := range partitions {
		for _, msg := range partition {
			if msg.ID == messageID {
				msgCopy := *msg
				return &msgCopy, nil
			}
		}
	}

	return nil, storage.ErrMessageNotFound
}

func (s *Store) MarkInflight(ctx context.Context, state *storage.DeliveryState) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	inflight, exists := s.inflight[state.QueueName]
	if !exists {
		return storage.ErrQueueNotFound
	}

	stateCopy := *state
	inflight[state.MessageID] = &stateCopy
	return nil
}

func (s *Store) GetInflight(ctx context.Context, queueName string) ([]*storage.DeliveryState, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	inflight, exists := s.inflight[queueName]
	if !exists {
		return nil, storage.ErrQueueNotFound
	}

	states := make([]*storage.DeliveryState, 0, len(inflight))
	for _, state := range inflight {
		stateCopy := *state
		states = append(states, &stateCopy)
	}

	return states, nil
}

func (s *Store) GetInflightMessage(ctx context.Context, queueName, messageID string) (*storage.DeliveryState, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	inflight, exists := s.inflight[queueName]
	if !exists {
		return nil, storage.ErrQueueNotFound
	}

	state, exists := inflight[messageID]
	if !exists {
		return nil, storage.ErrMessageNotFound
	}

	stateCopy := *state
	return &stateCopy, nil
}

func (s *Store) RemoveInflight(ctx context.Context, queueName, messageID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	inflight, exists := s.inflight[queueName]
	if !exists {
		return storage.ErrQueueNotFound
	}

	delete(inflight, messageID)
	return nil
}

func (s *Store) EnqueueDLQ(ctx context.Context, dlqTopic string, msg *storage.Message) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.dlq[dlqTopic] == nil {
		s.dlq[dlqTopic] = make(map[string]*storage.Message)
	}

	msgCopy := *msg
	s.dlq[dlqTopic][msg.ID] = &msgCopy
	return nil
}

func (s *Store) ListDLQ(ctx context.Context, dlqTopic string, limit int) ([]*storage.Message, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	messages := make([]*storage.Message, 0)
	dlqMessages, exists := s.dlq[dlqTopic]
	if !exists {
		return messages, nil
	}

	count := 0
	for _, msg := range dlqMessages {
		if limit > 0 && count >= limit {
			break
		}
		msgCopy := *msg
		messages = append(messages, &msgCopy)
		count++
	}

	return messages, nil
}

func (s *Store) DeleteDLQMessage(ctx context.Context, dlqTopic, messageID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	dlqMessages, exists := s.dlq[dlqTopic]
	if !exists {
		return storage.ErrMessageNotFound
	}

	if _, exists := dlqMessages[messageID]; !exists {
		return storage.ErrMessageNotFound
	}

	delete(dlqMessages, messageID)
	return nil
}

func (s *Store) ListRetry(ctx context.Context, queueName string, partitionID int) ([]*storage.Message, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	messages := make([]*storage.Message, 0)
	partitions, exists := s.messages[queueName]
	if !exists {
		return messages, nil
	}

	partition, exists := partitions[partitionID]
	if !exists {
		return messages, nil
	}

	// Iterate through partition messages and collect those in retry state
	for _, msg := range partition {
		if msg.State == storage.StateRetry {
			msgCopy := *msg
			messages = append(messages, &msgCopy)
		}
	}

	return messages, nil
}

func (s *Store) GetNextSequence(ctx context.Context, queueName string, partitionID int) (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	partitions, exists := s.sequences[queueName]
	if !exists {
		return 0, storage.ErrQueueNotFound
	}

	seq := partitions[partitionID]
	seq++
	partitions[partitionID] = seq
	return seq, nil
}

func (s *Store) UpdateOffset(ctx context.Context, queueName string, partitionID int, offset uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	offsets, exists := s.offsets[queueName]
	if !exists {
		return storage.ErrQueueNotFound
	}

	offsets[partitionID] = offset
	return nil
}

func (s *Store) GetOffset(ctx context.Context, queueName string, partitionID int) (uint64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	offsets, exists := s.offsets[queueName]
	if !exists {
		return 0, storage.ErrQueueNotFound
	}

	return offsets[partitionID], nil
}

func (s *Store) ListQueued(ctx context.Context, queueName string, partitionID int, limit int) ([]*storage.Message, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	partitions, exists := s.messages[queueName]
	if !exists {
		return nil, storage.ErrQueueNotFound
	}

	partition, exists := partitions[partitionID]
	if !exists {
		return nil, fmt.Errorf("partition %d not found", partitionID)
	}

	messages := make([]*storage.Message, 0)
	count := 0
	for _, msg := range partition {
		if limit > 0 && count >= limit {
			break
		}
		if msg.State == storage.StateQueued || msg.State == storage.StateRetry {
			msgCopy := *msg
			messages = append(messages, &msgCopy)
			count++
		}
	}

	return messages, nil
}

// ConsumerStore implementation

func (s *Store) RegisterConsumer(ctx context.Context, consumer *storage.Consumer) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	groups, exists := s.consumers[consumer.QueueName]
	if !exists {
		groups = make(map[string]map[string]*storage.Consumer)
		s.consumers[consumer.QueueName] = groups
	}

	group, exists := groups[consumer.GroupID]
	if !exists {
		group = make(map[string]*storage.Consumer)
		groups[consumer.GroupID] = group
	}

	consumerCopy := *consumer
	group[consumer.ID] = &consumerCopy
	return nil
}

func (s *Store) UnregisterConsumer(ctx context.Context, queueName, groupID, consumerID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	groups, exists := s.consumers[queueName]
	if !exists {
		return storage.ErrQueueNotFound
	}

	group, exists := groups[groupID]
	if !exists {
		return storage.ErrConsumerNotFound
	}

	delete(group, consumerID)
	return nil
}

func (s *Store) GetConsumer(ctx context.Context, queueName, groupID, consumerID string) (*storage.Consumer, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	groups, exists := s.consumers[queueName]
	if !exists {
		return nil, storage.ErrQueueNotFound
	}

	group, exists := groups[groupID]
	if !exists {
		return nil, storage.ErrConsumerNotFound
	}

	consumer, exists := group[consumerID]
	if !exists {
		return nil, storage.ErrConsumerNotFound
	}

	consumerCopy := *consumer
	return &consumerCopy, nil
}

func (s *Store) ListConsumers(ctx context.Context, queueName, groupID string) ([]*storage.Consumer, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	groups, exists := s.consumers[queueName]
	if !exists {
		return nil, storage.ErrQueueNotFound
	}

	group, exists := groups[groupID]
	if !exists {
		return []*storage.Consumer{}, nil
	}

	consumers := make([]*storage.Consumer, 0, len(group))
	for _, consumer := range group {
		consumerCopy := *consumer
		consumers = append(consumers, &consumerCopy)
	}

	return consumers, nil
}

func (s *Store) ListGroups(ctx context.Context, queueName string) ([]string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	groups, exists := s.consumers[queueName]
	if !exists {
		return nil, storage.ErrQueueNotFound
	}

	groupIDs := make([]string, 0, len(groups))
	for groupID := range groups {
		groupIDs = append(groupIDs, groupID)
	}

	return groupIDs, nil
}

func (s *Store) UpdateHeartbeat(ctx context.Context, queueName, groupID, consumerID string, timestamp time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	groups, exists := s.consumers[queueName]
	if !exists {
		return storage.ErrQueueNotFound
	}

	group, exists := groups[groupID]
	if !exists {
		return storage.ErrConsumerNotFound
	}

	consumer, exists := group[consumerID]
	if !exists {
		return storage.ErrConsumerNotFound
	}

	consumer.LastHeartbeat = timestamp
	return nil
}

// Retention operations

func (s *Store) ListOldestMessages(ctx context.Context, queueName string, partitionID int, limit int) ([]*storage.Message, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	partitions, exists := s.messages[queueName]
	if !exists {
		return nil, storage.ErrQueueNotFound
	}

	partition, exists := partitions[partitionID]
	if !exists {
		return []*storage.Message{}, nil
	}

	// Collect all messages from partition
	messages := make([]*storage.Message, 0, len(partition))
	for _, msg := range partition {
		messages = append(messages, msg)
	}

	// Sort by sequence (oldest first)
	sort.Slice(messages, func(i, j int) bool {
		return messages[i].Sequence < messages[j].Sequence
	})

	// Apply limit
	if limit > 0 && len(messages) > limit {
		messages = messages[:limit]
	}

	return messages, nil
}

func (s *Store) ListMessagesBefore(ctx context.Context, queueName string, partitionID int, cutoffTime time.Time, limit int) ([]*storage.Message, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	partitions, exists := s.messages[queueName]
	if !exists {
		return nil, storage.ErrQueueNotFound
	}

	partition, exists := partitions[partitionID]
	if !exists {
		return []*storage.Message{}, nil
	}

	// Collect messages older than cutoff time
	messages := make([]*storage.Message, 0)
	for _, msg := range partition {
		if msg.CreatedAt.Before(cutoffTime) {
			messages = append(messages, msg)
		}
	}

	// Sort by sequence (oldest first)
	sort.Slice(messages, func(i, j int) bool {
		return messages[i].Sequence < messages[j].Sequence
	})

	// Apply limit
	if limit > 0 && len(messages) > limit {
		messages = messages[:limit]
	}

	return messages, nil
}

func (s *Store) DeleteMessageBatch(ctx context.Context, queueName string, messageIDs []string) (int64, error) {
	if len(messageIDs) == 0 {
		return 0, nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	partitions, exists := s.messages[queueName]
	if !exists {
		return 0, storage.ErrQueueNotFound
	}

	// Create a set for fast ID lookup
	idSet := make(map[string]struct{}, len(messageIDs))
	for _, id := range messageIDs {
		idSet[id] = struct{}{}
	}

	var deletedCount int64

	// Scan all partitions and delete messages by ID
	for partitionID, partition := range partitions {
		for seq, msg := range partition {
			if _, found := idSet[msg.ID]; found {
				delete(partitions[partitionID], seq)
				deletedCount++
			}
		}
	}

	// Update count
	s.counts[queueName] -= deletedCount

	return deletedCount, nil
}

func (s *Store) GetQueueSize(ctx context.Context, queueName string) (int64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	partitions, exists := s.messages[queueName]
	if !exists {
		return 0, storage.ErrQueueNotFound
	}

	var totalSize int64

	// Calculate total size of all messages
	for _, msgs := range partitions {
		for _, msg := range msgs {
			totalSize += int64(len(msg.GetPayload()))
		}
	}

	return totalSize, nil
}

func (s *Store) ListAllMessages(ctx context.Context, queueName string, partitionID int) ([]*storage.Message, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	partitions, exists := s.messages[queueName]
	if !exists {
		return nil, storage.ErrQueueNotFound
	}

	partition, exists := partitions[partitionID]
	if !exists {
		return []*storage.Message{}, nil
	}

	// Collect all messages from partition
	messages := make([]*storage.Message, 0, len(partition))
	for _, msg := range partition {
		messages = append(messages, msg)
	}

	// Sort by sequence (oldest first)
	sort.Slice(messages, func(i, j int) bool {
		return messages[i].Sequence < messages[j].Sequence
	})

	return messages, nil
}
