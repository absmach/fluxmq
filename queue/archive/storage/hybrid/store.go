// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package hybrid

// import (
// 	"context"
// 	"sync"
// 	"time"

// 	"github.com/absmach/fluxmq/queue/storage"
// 	"github.com/absmach/fluxmq/queue/storage/memory/lockfree"
// 	"github.com/absmach/fluxmq/queue/types"
// )

// // PersistentStore combines all storage interfaces for the persistent layer.
// type PersistentStore interface {
// 	storage.QueueStore
// 	storage.MessageStore
// 	storage.ConsumerStore
// }

// // Store is a hybrid message store combining lock-free ring buffers (hot path)
// // with persistent BadgerDB storage (durability, overflow, metadata).
// //
// // Architecture:
// // - Hot path: Lock-free ring buffers for active message delivery
// // - Overflow: BadgerDB when ring buffers are full
// // - Persistence: All messages written to BadgerDB for durability
// // - Recovery: Load recent messages from BadgerDB on startup
// // - Metadata: Inflight, DLQ, consumers stored in BadgerDB.
// type Store struct {
// 	// Hot path: lock-free ring buffers
// 	lockfree *lockfree.Store

// 	// Cold path: persistent storage (implements all three storage interfaces)
// 	persistent PersistentStore

// 	// Configuration
// 	config Config

// 	// Metrics
// 	metrics *Metrics
// 	mu      sync.RWMutex

// 	// Async batch persistence
// 	batchBuffer []*types.Message
// 	batchMu     sync.Mutex
// 	flushTicker *time.Ticker
// 	stopCh      chan struct{}
// 	wg          sync.WaitGroup
// }

// // Config defines hybrid store configuration.
// type Config struct {
// 	// Ring buffer size per partition (default: 16384)
// 	RingBufferSize uint64

// 	// Enable async write-behind to BadgerDB (default: true)
// 	AsyncPersist bool

// 	// Batch size for async writes (default: 100)
// 	PersistBatchSize int

// 	// Flush interval for async writes (default: 10ms)
// 	PersistInterval time.Duration

// 	// Load recent messages on startup (default: 1000 per partition)
// 	WarmupSize int

// 	// Enable performance metrics (default: false)
// 	EnableMetrics bool
// }

// // Metrics tracks hybrid store performance.
// type Metrics struct {
// 	// Ring buffer hits (served from lock-free)
// 	RingHits uint64

// 	// Ring buffer misses (overflow to BadgerDB)
// 	RingMisses uint64

// 	// BadgerDB reads
// 	DiskReads uint64

// 	// BadgerDB writes
// 	DiskWrites uint64

// 	// Messages in ring buffers
// 	RingMessages uint64

// 	// Messages in BadgerDB
// 	DiskMessages uint64
// }

// // DefaultConfig returns default hybrid store configuration.
// func DefaultConfig() Config {
// 	return Config{
// 		RingBufferSize:   16384, // 16K messages per partition
// 		AsyncPersist:     true,
// 		PersistBatchSize: 100,
// 		PersistInterval:  10 * time.Millisecond,
// 		WarmupSize:       1000,
// 		EnableMetrics:    false,
// 	}
// }

// // New creates a new hybrid message store.
// // persistent is typically a BadgerDB store for durability.
// func New(persistent PersistentStore) *Store {
// 	return NewWithConfig(persistent, DefaultConfig())
// }

// // NewWithConfig creates a new hybrid store with custom configuration.
// func NewWithConfig(persistent PersistentStore, cfg Config) *Store {
// 	// Create lock-free store with configured ring buffer size
// 	lockfreeConfig := lockfree.Config{
// 		RingBufferSize: cfg.RingBufferSize,
// 	}

// 	s := &Store{
// 		lockfree:    lockfree.NewWithConfig(lockfreeConfig),
// 		persistent:  persistent,
// 		config:      cfg,
// 		batchBuffer: make([]*types.Message, 0, cfg.PersistBatchSize),
// 		stopCh:      make(chan struct{}),
// 	}

// 	if cfg.EnableMetrics {
// 		s.metrics = &Metrics{}
// 	}

// 	// Start async batch flusher if enabled
// 	if cfg.AsyncPersist {
// 		s.flushTicker = time.NewTicker(cfg.PersistInterval)
// 		s.wg.Add(1)
// 		go s.flushLoop()
// 	}

// 	return s
// }

// // CreateQueue creates a new queue in both stores.
// func (s *Store) CreateQueue(ctx context.Context, config types.QueueConfig) error {
// 	// Create in persistent store first (source of truth)
// 	if err := s.persistent.CreateQueue(ctx, config); err != nil {
// 		return err
// 	}

// 	// Create in lock-free store
// 	if err := s.lockfree.CreateQueue(ctx, config); err != nil {
// 		// Rollback persistent creation
// 		s.persistent.DeleteQueue(ctx, config.Name)
// 		return err
// 	}

// 	return nil
// }

// // GetQueue retrieves queue configuration from persistent store.
// func (s *Store) GetQueue(ctx context.Context, queueName string) (*types.QueueConfig, error) {
// 	return s.persistent.GetQueue(ctx, queueName)
// }

// // UpdateQueue updates queue configuration in both stores.
// func (s *Store) UpdateQueue(ctx context.Context, config types.QueueConfig) error {
// 	if err := s.persistent.UpdateQueue(ctx, config); err != nil {
// 		return err
// 	}
// 	return s.lockfree.UpdateQueue(ctx, config)
// }

// // DeleteQueue deletes a queue from both stores.
// func (s *Store) DeleteQueue(ctx context.Context, queueName string) error {
// 	s.lockfree.DeleteQueue(ctx, queueName)
// 	return s.persistent.DeleteQueue(ctx, queueName)
// }

// // ListQueues lists queues from persistent store.
// func (s *Store) ListQueues(ctx context.Context) ([]types.QueueConfig, error) {
// 	return s.persistent.ListQueues(ctx)
// }

// // Enqueue adds a message to both stores:
// // 1. Validate queue exists
// // 2. Persist to BadgerDB (async batch if enabled, sync otherwise)
// // 3. Try lock-free ring buffer (best effort hot path)
// // 4. Track metrics for hit/miss rate.
// func (s *Store) Enqueue(ctx context.Context, queueName string, msg *types.Message) error {
// 	// Validate queue exists
// 	_, err := s.GetQueue(ctx, queueName)
// 	if err != nil {
// 		return err
// 	}

// 	// Make a deep copy for storage (original might be reused by caller)
// 	msgCopy := &types.Message{
// 		ID:           msg.ID,
// 		Payload:      make([]byte, len(msg.Payload)),
// 		Topic:        msg.Topic,
// 		PartitionKey: msg.PartitionKey,
// 		PartitionID:  msg.PartitionID,
// 		Sequence:     msg.Sequence,
// 		Properties:   make(map[string]string),
// 		State:        msg.State,
// 		CreatedAt:    msg.CreatedAt,
// 	}
// 	copy(msgCopy.Payload, msg.Payload)
// 	for k, v := range msg.Properties {
// 		msgCopy.Properties[k] = v
// 	}

// 	// Persist to BadgerDB
// 	if s.config.AsyncPersist {
// 		// Cross-partition batch writes
// 		s.batchMu.Lock()
// 		s.batchBuffer = append(s.batchBuffer, msgCopy)
// 		shouldFlush := len(s.batchBuffer) >= s.config.PersistBatchSize
// 		s.batchMu.Unlock()

// 		// Flush if batch is full (immediate flush to prevent unbounded buffering)
// 		if shouldFlush {
// 			s.flushBatch()
// 		}
// 	} else {
// 		// Synchronous write (original behavior)
// 		if err := s.persistent.Enqueue(ctx, queueName, msgCopy); err != nil {
// 			return err
// 		}

// 		if s.metrics != nil {
// 			s.mu.Lock()
// 			s.metrics.DiskWrites++
// 			s.mu.Unlock()
// 		}
// 	}

// 	// Try lock-free ring buffer (best effort)
// 	err = s.lockfree.Enqueue(ctx, queueName, msg)
// 	if s.metrics != nil {
// 		s.mu.Lock()
// 		if err == nil {
// 			// Success: message in ring buffer (hot path)
// 			s.metrics.RingMessages++
// 		} else {
// 			// Ring buffer full: message only in BadgerDB (cold path)
// 			// This is OK - dequeue will fall back to BadgerDB
// 			s.metrics.RingMisses++
// 		}
// 		s.mu.Unlock()
// 	}

// 	return nil
// }

// // Count returns the number of messages in the queue (delegates to persistent store).
// func (s *Store) Count(ctx context.Context, queueName string) (int64, error) {
// 	return s.persistent.Count(ctx, queueName)
// }

// // Dequeue removes a message, trying lock-free first, then BadgerDB.
// // Strategy:
// // 1. Validate queue exists
// // 2. Try lock-free ring buffer first (hot path)
// // 3. Mark as delivered in BadgerDB to avoid duplicates
// // 4. If ring buffer empty, read from BadgerDB (cold path).
// func (s *Store) Dequeue(ctx context.Context, queueName string, partitionID int) (*types.Message, error) {
// 	// Validate queue exists
// 	_, err := s.GetQueue(ctx, queueName)
// 	if err != nil {
// 		return nil, err
// 	}

// 	// Try lock-free ring buffer first (hot path)
// 	msg, err := s.lockfree.Dequeue(ctx, queueName, partitionID)
// 	if err == nil && msg != nil {
// 		// Ring buffer hit - mark as delivered in BadgerDB to avoid returning it again
// 		msg.State = types.StateDelivered
// 		s.persistent.UpdateMessage(ctx, queueName, msg)

// 		if s.metrics != nil {
// 			s.mu.Lock()
// 			s.metrics.RingHits++
// 			s.metrics.RingMessages--
// 			s.mu.Unlock()
// 		}
// 		return msg, nil
// 	}

// 	// Ring buffer empty, fall back to BadgerDB (cold path)
// 	msg, err = s.persistent.Dequeue(ctx, queueName, partitionID)
// 	if err != nil || msg == nil {
// 		return msg, err
// 	}

// 	// Mark as delivered to avoid returning it again
// 	msg.State = types.StateDelivered
// 	s.persistent.UpdateMessage(ctx, queueName, msg)

// 	if s.metrics != nil {
// 		s.mu.Lock()
// 		s.metrics.DiskReads++
// 		s.mu.Unlock()
// 	}

// 	// TODO: Preload next batch from BadgerDB to ring buffer for future requests
// 	// This would improve performance on subsequent dequeues

// 	return msg, nil
// }

// // DequeueBatch removes multiple messages efficiently.
// // Strategy:
// // 1. Drain lock-free ring buffer first
// // 2. Mark ring buffer messages as delivered in BadgerDB
// // 3. If needed, fetch remaining from BadgerDB
// // 4. Combine results.
// func (s *Store) DequeueBatch(ctx context.Context, queueName string, partitionID int, limit int) ([]*types.Message, error) {
// 	var messages []*types.Message

// 	// Drain lock-free ring buffer first (hot path)
// 	ringMessages, err := s.lockfree.DequeueBatch(ctx, queueName, partitionID, limit)
// 	if err == nil && ringMessages != nil {
// 		messages = append(messages, ringMessages...)

// 		// Mark ring buffer messages as delivered in BadgerDB
// 		for _, msg := range ringMessages {
// 			msg.State = types.StateDelivered
// 			s.persistent.UpdateMessage(ctx, queueName, msg)
// 		}

// 		if s.metrics != nil {
// 			s.mu.Lock()
// 			s.metrics.RingHits += uint64(len(ringMessages))
// 			s.metrics.RingMessages -= uint64(len(ringMessages))
// 			s.mu.Unlock()
// 		}
// 	}

// 	// If we got enough, return
// 	if len(messages) >= limit {
// 		return messages, nil
// 	}

// 	// Need more, fetch from BadgerDB (cold path)
// 	remaining := limit - len(messages)
// 	diskMessages, err := s.persistent.DequeueBatch(ctx, queueName, partitionID, remaining)
// 	if err != nil {
// 		// Return what we got from ring buffer
// 		if len(messages) > 0 {
// 			return messages, nil
// 		}
// 		return nil, err
// 	}

// 	if diskMessages != nil {
// 		messages = append(messages, diskMessages...)

// 		// Mark BadgerDB messages as delivered
// 		for _, msg := range diskMessages {
// 			msg.State = types.StateDelivered
// 			s.persistent.UpdateMessage(ctx, queueName, msg)
// 		}

// 		if s.metrics != nil {
// 			s.mu.Lock()
// 			s.metrics.DiskReads += uint64(len(diskMessages))
// 			s.mu.Unlock()
// 		}
// 	}

// 	if len(messages) == 0 {
// 		return nil, nil
// 	}

// 	return messages, nil
// }

// // GetNextSequence delegates to persistent store (source of truth).
// func (s *Store) GetNextSequence(ctx context.Context, queueName string, partitionID int) (uint64, error) {
// 	return s.persistent.GetNextSequence(ctx, queueName, partitionID)
// }

// // All metadata operations delegate to persistent store

// func (s *Store) UpdateMessage(ctx context.Context, queueName string, msg *types.Message) error {
// 	return s.persistent.UpdateMessage(ctx, queueName, msg)
// }

// func (s *Store) DeleteMessage(ctx context.Context, queueName string, messageID string) error {
// 	return s.persistent.DeleteMessage(ctx, queueName, messageID)
// }

// func (s *Store) GetMessage(ctx context.Context, queueName string, messageID string) (*types.Message, error) {
// 	return s.persistent.GetMessage(ctx, queueName, messageID)
// }

// func (s *Store) MarkInflight(ctx context.Context, state *types.DeliveryState) error {
// 	return s.persistent.MarkInflight(ctx, state)
// }

// func (s *Store) GetInflight(ctx context.Context, queueName string) ([]*types.DeliveryState, error) {
// 	return s.persistent.GetInflight(ctx, queueName)
// }

// func (s *Store) GetInflightMessage(ctx context.Context, queueName, messageID, groupID string) (*types.DeliveryState, error) {
// 	return s.persistent.GetInflightMessage(ctx, queueName, messageID, groupID)
// }

// func (s *Store) GetInflightForMessage(ctx context.Context, queueName, messageID string) ([]*types.DeliveryState, error) {
// 	return s.persistent.GetInflightForMessage(ctx, queueName, messageID)
// }

// func (s *Store) RemoveInflight(ctx context.Context, queueName, messageID, groupID string) error {
// 	return s.persistent.RemoveInflight(ctx, queueName, messageID, groupID)
// }

// func (s *Store) EnqueueDLQ(ctx context.Context, dlqTopic string, msg *types.Message) error {
// 	return s.persistent.EnqueueDLQ(ctx, dlqTopic, msg)
// }

// func (s *Store) ListDLQ(ctx context.Context, dlqTopic string, limit int) ([]*types.Message, error) {
// 	return s.persistent.ListDLQ(ctx, dlqTopic, limit)
// }

// func (s *Store) DeleteDLQMessage(ctx context.Context, dlqTopic, messageID string) error {
// 	return s.persistent.DeleteDLQMessage(ctx, dlqTopic, messageID)
// }

// func (s *Store) ListRetry(ctx context.Context, queueName string, partitionID int) ([]*types.Message, error) {
// 	return s.persistent.ListRetry(ctx, queueName, partitionID)
// }

// func (s *Store) UpdateOffset(ctx context.Context, queueName string, partitionID int, offset uint64) error {
// 	return s.persistent.UpdateOffset(ctx, queueName, partitionID, offset)
// }

// func (s *Store) GetOffset(ctx context.Context, queueName string, partitionID int) (uint64, error) {
// 	return s.persistent.GetOffset(ctx, queueName, partitionID)
// }

// func (s *Store) ListQueued(ctx context.Context, queueName string, partitionID int, limit int) ([]*types.Message, error) {
// 	return s.persistent.ListQueued(ctx, queueName, partitionID, limit)
// }

// func (s *Store) RegisterConsumer(ctx context.Context, consumer *types.Consumer) error {
// 	return s.persistent.RegisterConsumer(ctx, consumer)
// }

// func (s *Store) UnregisterConsumer(ctx context.Context, queueName, groupID, consumerID string) error {
// 	return s.persistent.UnregisterConsumer(ctx, queueName, groupID, consumerID)
// }

// func (s *Store) GetConsumer(ctx context.Context, queueName, groupID, consumerID string) (*types.Consumer, error) {
// 	return s.persistent.GetConsumer(ctx, queueName, groupID, consumerID)
// }

// func (s *Store) ListConsumers(ctx context.Context, queueName, groupID string) ([]*types.Consumer, error) {
// 	return s.persistent.ListConsumers(ctx, queueName, groupID)
// }

// func (s *Store) ListGroups(ctx context.Context, queueName string) ([]string, error) {
// 	return s.persistent.ListGroups(ctx, queueName)
// }

// func (s *Store) UpdateHeartbeat(ctx context.Context, queueName, groupID, consumerID string, timestamp time.Time) error {
// 	return s.persistent.UpdateHeartbeat(ctx, queueName, groupID, consumerID, timestamp)
// }

// // GetMetrics returns performance metrics (if enabled).
// func (s *Store) GetMetrics() *Metrics {
// 	s.mu.RLock()
// 	defer s.mu.RUnlock()

// 	if s.metrics == nil {
// 		return nil
// 	}

// 	// Return copy
// 	return &Metrics{
// 		RingHits:     s.metrics.RingHits,
// 		RingMisses:   s.metrics.RingMisses,
// 		DiskReads:    s.metrics.DiskReads,
// 		DiskWrites:   s.metrics.DiskWrites,
// 		RingMessages: s.metrics.RingMessages,
// 		DiskMessages: s.metrics.DiskMessages,
// 	}
// }

// // ResetMetrics resets performance counters.
// func (s *Store) ResetMetrics() {
// 	s.mu.Lock()
// 	defer s.mu.Unlock()

// 	if s.metrics != nil {
// 		s.metrics = &Metrics{}
// 	}
// }

// // HitRate returns the ring buffer hit rate (0.0-1.0).
// func (s *Store) HitRate() float64 {
// 	s.mu.RLock()
// 	defer s.mu.RUnlock()

// 	if s.metrics == nil {
// 		return 0.0
// 	}

// 	total := s.metrics.RingHits + s.metrics.RingMisses
// 	if total == 0 {
// 		return 0.0
// 	}

// 	return float64(s.metrics.RingHits) / float64(total)
// }

// // flushLoop runs in the background and flushes batched messages periodically.
// func (s *Store) flushLoop() {
// 	defer s.wg.Done()

// 	for {
// 		select {
// 		case <-s.flushTicker.C:
// 			s.flushBatch()
// 		case <-s.stopCh:
// 			// Final flush on shutdown
// 			s.flushBatch()
// 			return
// 		}
// 	}
// }

// // flushBatch writes all batched messages to BadgerDB.
// func (s *Store) flushBatch() {
// 	s.batchMu.Lock()
// 	if len(s.batchBuffer) == 0 {
// 		s.batchMu.Unlock()
// 		return
// 	}

// 	// Swap buffer (minimize lock time)
// 	batch := s.batchBuffer
// 	s.batchBuffer = make([]*types.Message, 0, s.config.PersistBatchSize)
// 	s.batchMu.Unlock()

// 	// Write batch to BadgerDB (cross-partition batching)
// 	ctx := context.Background()
// 	for _, msg := range batch {
// 		if err := s.persistent.Enqueue(ctx, msg.Topic, msg); err != nil {
// 			// Log error but continue (best effort)
// 			// TODO: Add proper logging
// 			continue
// 		}
// 	}

// 	// Update metrics
// 	if s.metrics != nil {
// 		s.mu.Lock()
// 		s.metrics.DiskWrites += uint64(len(batch))
// 		s.mu.Unlock()
// 	}
// }

// // Close flushes remaining batched messages and stops background goroutines.
// func (s *Store) Close() error {
// 	if s.config.AsyncPersist {
// 		close(s.stopCh)
// 		s.wg.Wait()
// 		if s.flushTicker != nil {
// 			s.flushTicker.Stop()
// 		}
// 	}
// 	return nil
// }
