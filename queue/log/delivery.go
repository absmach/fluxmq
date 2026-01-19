// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package log

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/absmach/fluxmq/queue/consumer"
	"github.com/absmach/fluxmq/queue/storage"
	"github.com/absmach/fluxmq/queue/types"
)

// DeliveryWorker handles message delivery for a specific queue partition.
type DeliveryWorker struct {
	queueName       string
	partitionID     int
	logStore        storage.LogStore
	groupStore      storage.ConsumerGroupStore
	consumerManager *consumer.Manager
	deliverFn       DeliverFn
	config          DeliveryConfig
	logger          *slog.Logger

	stopCh chan struct{}
	wg     sync.WaitGroup
}

// DeliveryConfig configures the delivery worker.
type DeliveryConfig struct {
	BatchSize       int
	PollInterval    time.Duration
	MaxInFlight     int
	DeliveryTimeout time.Duration
}

// DefaultDeliveryConfig returns default delivery configuration.
func DefaultDeliveryConfig() DeliveryConfig {
	return DeliveryConfig{
		BatchSize:       100,
		PollInterval:    10 * time.Millisecond,
		MaxInFlight:     1000,
		DeliveryTimeout: 30 * time.Second,
	}
}

// NewDeliveryWorker creates a new delivery worker.
func NewDeliveryWorker(
	queueName string,
	partitionID int,
	logStore storage.LogStore,
	groupStore storage.ConsumerGroupStore,
	consumerManager *consumer.Manager,
	deliverFn DeliverFn,
	config DeliveryConfig,
	logger *slog.Logger,
) *DeliveryWorker {
	if logger == nil {
		logger = slog.Default()
	}

	return &DeliveryWorker{
		queueName:       queueName,
		partitionID:     partitionID,
		logStore:        logStore,
		groupStore:      groupStore,
		consumerManager: consumerManager,
		deliverFn:       deliverFn,
		config:          config,
		logger:          logger,
		stopCh:          make(chan struct{}),
	}
}

// Start starts the delivery worker.
func (w *DeliveryWorker) Start() {
	w.wg.Add(1)
	go w.run()

	w.logger.Debug("delivery worker started",
		slog.String("queue", w.queueName),
		slog.Int("partition", w.partitionID))
}

// Stop stops the delivery worker.
func (w *DeliveryWorker) Stop() {
	close(w.stopCh)
	w.wg.Wait()

	w.logger.Debug("delivery worker stopped",
		slog.String("queue", w.queueName),
		slog.Int("partition", w.partitionID))
}

func (w *DeliveryWorker) run() {
	defer w.wg.Done()

	ticker := time.NewTicker(w.config.PollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-w.stopCh:
			return
		case <-ticker.C:
			w.deliverBatch()
		}
	}
}

func (w *DeliveryWorker) deliverBatch() {
	ctx := context.Background()

	// Get all consumer groups for this queue
	groups, err := w.groupStore.ListConsumerGroups(ctx, w.queueName)
	if err != nil {
		return
	}

	for _, group := range groups {
		w.deliverToGroup(ctx, group)
	}
}

func (w *DeliveryWorker) deliverToGroup(ctx context.Context, group *types.ConsumerGroupState) {
	if len(group.Consumers) == 0 {
		return
	}

	// Create filter from group pattern
	var filter *consumer.Filter
	if group.Pattern != "" {
		filter = consumer.NewFilter(group.Pattern)
	}

	// Distribute messages among consumers
	for consumerID, consumerInfo := range group.Consumers {
		// Check pending count to respect max in-flight
		pendingCount := group.PendingCount(w.partitionID)
		if pendingCount >= w.config.MaxInFlight {
			continue
		}

		// Claim batch of messages
		msgs, err := w.consumerManager.ClaimBatch(
			ctx,
			w.queueName,
			group.ID,
			consumerID,
			w.partitionID,
			filter,
			w.config.BatchSize,
		)
		if err != nil {
			continue
		}

		// Deliver each message
		for _, msg := range msgs {
			w.deliverMessage(ctx, msg, group.ID, consumerInfo)
		}
	}
}

func (w *DeliveryWorker) deliverMessage(ctx context.Context, msg *types.Message, groupID string, consumerInfo *types.ConsumerInfo) {
	if w.deliverFn == nil {
		return
	}

	// Create delivery message
	deliveryMsg := &DeliveryMessage{
		ID:          formatMessageID(w.partitionID, msg.Sequence),
		Payload:     msg.GetPayload(),
		Topic:       msg.Topic,
		Properties:  msg.Properties,
		GroupID:     groupID,
		PartitionID: w.partitionID,
		Offset:      msg.Sequence,
		DeliveredAt: time.Now(),
		AckTopic:    msg.Topic + "/$ack",
		NackTopic:   msg.Topic + "/$nack",
		RejectTopic: msg.Topic + "/$reject",
	}

	// Deliver to client
	if err := w.deliverFn(ctx, consumerInfo.ClientID, deliveryMsg); err != nil {
		w.logger.Debug("delivery failed",
			slog.String("client", consumerInfo.ClientID),
			slog.String("error", err.Error()))
	}
}

func formatMessageID(partitionID int, offset uint64) string {
	return string(rune(partitionID)) + ":" + string(rune(offset))
}

// PartitionDeliveryManager manages delivery workers for all partitions of a queue.
type PartitionDeliveryManager struct {
	queueName       string
	workers         map[int]*DeliveryWorker
	logStore        storage.LogStore
	groupStore      storage.ConsumerGroupStore
	consumerManager *consumer.Manager
	deliverFn       DeliverFn
	config          DeliveryConfig
	logger          *slog.Logger
	mu              sync.RWMutex
}

// NewPartitionDeliveryManager creates a new partition delivery manager.
func NewPartitionDeliveryManager(
	queueName string,
	logStore storage.LogStore,
	groupStore storage.ConsumerGroupStore,
	consumerManager *consumer.Manager,
	deliverFn DeliverFn,
	config DeliveryConfig,
	logger *slog.Logger,
) *PartitionDeliveryManager {
	return &PartitionDeliveryManager{
		queueName:       queueName,
		workers:         make(map[int]*DeliveryWorker),
		logStore:        logStore,
		groupStore:      groupStore,
		consumerManager: consumerManager,
		deliverFn:       deliverFn,
		config:          config,
		logger:          logger,
	}
}

// Start starts delivery workers for all partitions.
func (m *PartitionDeliveryManager) Start(ctx context.Context) error {
	config, err := m.logStore.GetQueue(ctx, m.queueName)
	if err != nil {
		return err
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	for i := 0; i < config.Partitions; i++ {
		worker := NewDeliveryWorker(
			m.queueName,
			i,
			m.logStore,
			m.groupStore,
			m.consumerManager,
			m.deliverFn,
			m.config,
			m.logger,
		)
		worker.Start()
		m.workers[i] = worker
	}

	return nil
}

// Stop stops all delivery workers.
func (m *PartitionDeliveryManager) Stop() {
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, worker := range m.workers {
		worker.Stop()
	}

	m.workers = make(map[int]*DeliveryWorker)
}

// NotifyPartition notifies a partition worker that new messages are available.
func (m *PartitionDeliveryManager) NotifyPartition(partitionID int) {
	// In the polling model, this is a no-op
	// Could be enhanced to use channels for immediate notification
}

// GetWorkerCount returns the number of active workers.
func (m *PartitionDeliveryManager) GetWorkerCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.workers)
}
