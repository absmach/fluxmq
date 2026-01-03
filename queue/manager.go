// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package queue

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/absmach/mqtt/cluster"
	"github.com/absmach/mqtt/queue/storage"
	"github.com/google/uuid"
)

// DeliverFn defines the interface for message delivery to MQTT clients.
// This allows the queue manager to deliver messages without depending on the full broker.
type DeliverFn func(ctx context.Context, clientID string, msg any) error

// Manager manages all queues in the system.
type Manager struct {
	queues          map[string]*Queue
	deliveryWorkers map[string]*DeliveryWorker
	queueStore      storage.QueueStore
	messageStore    storage.MessageStore
	consumerStore   storage.ConsumerStore
	broker          DeliverFn
	retryManager    *RetryManager
	dlqManager      *DLQManager
	mu              sync.RWMutex
	stopCh          chan struct{}
	wg              sync.WaitGroup

	// Cluster support (nil for single-node mode)
	cluster     cluster.Cluster
	localNodeID string
}

// Config holds configuration for the queue manager.
type Config struct {
	QueueStore    storage.QueueStore
	MessageStore  storage.MessageStore
	ConsumerStore storage.ConsumerStore
	DeliverFn     DeliverFn

	// Optional cluster support
	Cluster     cluster.Cluster
	LocalNodeID string
}

// NewManager creates a new queue manager.
func NewManager(cfg Config) (*Manager, error) {
	if cfg.QueueStore == nil || cfg.MessageStore == nil || cfg.ConsumerStore == nil {
		return nil, fmt.Errorf("queue stores cannot be nil")
	}

	// Create DLQ manager with HTTP alert handler
	alertHandler := NewHTTPAlertHandler(10 * time.Second)
	dlqManager := NewDLQManager(cfg.MessageStore, alertHandler)

	// Create retry manager
	retryManager := NewRetryManager(cfg.MessageStore, dlqManager)

	// Default to single-node if no cluster or node ID provided
	localNodeID := cfg.LocalNodeID
	if localNodeID == "" {
		localNodeID = "local"
	}

	return &Manager{
		queues:          make(map[string]*Queue),
		deliveryWorkers: make(map[string]*DeliveryWorker),
		queueStore:      cfg.QueueStore,
		messageStore:    cfg.MessageStore,
		consumerStore:   cfg.ConsumerStore,
		broker:          cfg.DeliverFn,
		retryManager:    retryManager,
		dlqManager:      dlqManager,
		stopCh:          make(chan struct{}),
		cluster:         cfg.Cluster,
		localNodeID:     localNodeID,
	}, nil
}

// Start starts the queue manager background tasks.
func (m *Manager) Start(ctx context.Context) error {
	// Load existing queues from storage
	configs, err := m.queueStore.ListQueues(ctx)
	if err != nil {
		return fmt.Errorf("failed to load queues: %w", err)
	}

	for _, config := range configs {
		if err := m.createQueueInstance(config); err != nil {
			return fmt.Errorf("failed to create queue %s: %w", config.Name, err)
		}

		// Acquire ownership for partitions assigned to this node
		queue, err := m.GetQueue(config.Name)
		if err != nil {
			return fmt.Errorf("failed to get queue %s: %w", config.Name, err)
		}

		if err := m.acquirePartitionsForQueue(ctx, queue); err != nil {
			return fmt.Errorf("failed to acquire partitions for queue %s: %w", config.Name, err)
		}
	}

	// Start retry manager
	m.retryManager.Start(ctx)

	return nil
}

// Stop stops the queue manager and all background tasks.
func (m *Manager) Stop() error {
	m.mu.Lock()
	// Stop all delivery workers
	for _, worker := range m.deliveryWorkers {
		worker.Stop()
	}
	m.mu.Unlock()

	// Stop retry manager
	if m.retryManager != nil {
		m.retryManager.Stop()
	}

	close(m.stopCh)
	m.wg.Wait()
	return nil
}

// CreateQueue creates a new queue with the given configuration.
func (m *Manager) CreateQueue(ctx context.Context, config storage.QueueConfig) error {
	// Set defaults
	if config.DLQConfig.Topic == "" && config.DLQConfig.Enabled {
		config.DLQConfig.Topic = "$queue/dlq/" + strings.TrimPrefix(config.Name, "$queue/")
	}

	// Validate config
	if err := config.Validate(); err != nil {
		return err
	}

	// Store in persistent storage
	if err := m.queueStore.CreateQueue(ctx, config); err != nil {
		return err
	}

	// Create queue instance
	return m.createQueueInstance(config)
}

// createQueueInstance creates a queue instance in memory.
func (m *Manager) createQueueInstance(config storage.QueueConfig) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.queues[config.Name]; exists {
		return storage.ErrQueueAlreadyExists
	}

	queue := NewQueue(config, m.messageStore, m.consumerStore)
	m.queues[config.Name] = queue

	// Register queue with retry manager
	if m.retryManager != nil {
		m.retryManager.RegisterQueue(queue)
	}

	// Create and start delivery worker
	worker := NewDeliveryWorker(queue, m.messageStore, m.broker, m.cluster, m.localNodeID)
	m.deliveryWorkers[config.Name] = worker

	// Start worker in background
	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		ctx := context.Background()
		worker.Start(ctx)
	}()

	return nil
}

// GetQueue returns a queue by name.
func (m *Manager) GetQueue(queueName string) (*Queue, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	queue, exists := m.queues[queueName]
	if !exists {
		return nil, storage.ErrQueueNotFound
	}

	return queue, nil
}

// GetOrCreateQueue gets an existing queue or creates one with default config.
func (m *Manager) GetOrCreateQueue(ctx context.Context, queueName string) (*Queue, error) {
	// Try to get existing queue
	queue, err := m.GetQueue(queueName)
	if err == nil {
		return queue, nil
	}

	if err != storage.ErrQueueNotFound {
		return nil, err
	}

	// Create with default config
	config := storage.DefaultQueueConfig(queueName)
	if err := m.CreateQueue(ctx, config); err != nil && err != storage.ErrQueueAlreadyExists {
		return nil, err
	}

	return m.GetQueue(queueName)
}

// Enqueue adds a message to a queue.
func (m *Manager) Enqueue(ctx context.Context, queueTopic string, payload []byte, properties map[string]string) error {
	queue, err := m.GetOrCreateQueue(ctx, queueTopic)
	if err != nil {
		return err
	}

	// Check queue limits
	config := queue.Config()
	if config.MaxQueueDepth > 0 {
		count, err := m.messageStore.Count(ctx, queueTopic)
		if err != nil {
			return fmt.Errorf("failed to get queue depth: %w", err)
		}

		if count >= config.MaxQueueDepth {
			return fmt.Errorf("queue is full (depth: %d, max: %d)", count, config.MaxQueueDepth)
		}
	}

	// Extract partition key from properties
	partitionKey := properties["partition-key"]

	// Get partition ID
	partitionID := queue.GetPartitionForMessage(partitionKey)

	// Check partition ownership (distributed mode)
	if m.cluster != nil {
		owner, err := m.getPartitionOwner(ctx, queueTopic, partitionID)
		if err != nil {
			return fmt.Errorf("failed to determine partition owner: %w", err)
		}

		// Route to remote node if not local
		if owner != m.localNodeID {
			return m.enqueueRemote(ctx, owner, queueTopic, payload, properties)
		}
	}

	// Get next sequence number
	sequence, err := m.messageStore.GetNextSequence(ctx, queueTopic, partitionID)
	if err != nil {
		return fmt.Errorf("failed to get next sequence: %w", err)
	}

	// Get message from pool
	msg := getMessageFromPool()

	// Optimize: avoid pool allocation for properties if input is empty
	var msgProps map[string]string
	if len(properties) == 0 {
		// No properties from caller, just create small map for message-id
		msgProps = make(map[string]string, 1)
	} else {
		// Get property map from pool and copy properties
		msgProps = getPropertyMap()
		copyProperties(msgProps, properties)
	}

	// Generate message ID
	msgID := uuid.New().String()
	msgProps["message-id"] = msgID

	// Populate message
	msg.ID = msgID
	msg.Payload = payload
	msg.Topic = queueTopic
	msg.PartitionKey = partitionKey
	msg.PartitionID = partitionID
	msg.Sequence = sequence
	msg.Properties = msgProps
	msg.State = storage.StateQueued
	msg.CreatedAt = time.Now()
	msg.ExpiresAt = msg.CreatedAt.Add(config.MessageTTL)

	// Enqueue (storage layer will make a deep copy)
	err = m.messageStore.Enqueue(ctx, queueTopic, msg)

	// Return to pools (only if we used pool)
	if len(properties) > 0 {
		putPropertyMap(msgProps)
	}
	putMessageToPool(msg)

	if err != nil {
		return err
	}

	// Notify partition worker that a message is available (event-driven delivery)
	m.mu.RLock()
	worker, exists := m.deliveryWorkers[queueTopic]
	m.mu.RUnlock()

	if exists {
		worker.NotifyPartition(partitionID)
	}

	return nil
}

// Subscribe adds a consumer to a queue.
func (m *Manager) Subscribe(ctx context.Context, queueTopic, clientID, groupID, proxyNodeID string) error {
	queue, err := m.GetOrCreateQueue(ctx, queueTopic)
	if err != nil {
		return err
	}

	// Use client ID as consumer ID (could be enhanced with separate consumer ID)
	consumerID := clientID

	// If no group specified, use client ID prefix as group name
	if groupID == "" {
		// Extract prefix before first dash or use full client ID
		parts := strings.SplitN(clientID, "-", 2)
		groupID = parts[0]
	}

	return queue.AddConsumer(ctx, groupID, consumerID, clientID, proxyNodeID)
}

// Unsubscribe removes a consumer from a queue.
func (m *Manager) Unsubscribe(ctx context.Context, queueTopic, clientID, groupID string) error {
	queue, err := m.GetQueue(queueTopic)
	if err != nil {
		return err
	}

	consumerID := clientID

	// If no group specified, derive from client ID
	if groupID == "" {
		parts := strings.SplitN(clientID, "-", 2)
		groupID = parts[0]
	}

	return queue.RemoveConsumer(ctx, groupID, consumerID)
}

// Ack acknowledges successful processing of a message.
func (m *Manager) Ack(ctx context.Context, queueTopic, messageID string) error {
	queue, err := m.GetQueue(queueTopic)
	if err != nil {
		return err
	}

	// Remove from inflight tracking
	if err := m.messageStore.RemoveInflight(ctx, queue.Name(), messageID); err != nil {
		// Message might not be inflight (already acked), ignore error
		if err != storage.ErrMessageNotFound {
			return err
		}
	}

	// Delete message
	return m.messageStore.DeleteMessage(ctx, queue.Name(), messageID)
}

// Nack negatively acknowledges a message (triggers immediate retry).
func (m *Manager) Nack(ctx context.Context, queueTopic, messageID string) error {
	queue, err := m.GetQueue(queueTopic)
	if err != nil {
		return err
	}

	// Get message
	msg, err := m.messageStore.GetMessage(ctx, queue.Name(), messageID)
	if err != nil {
		return err
	}

	// Remove from inflight
	if err := m.messageStore.RemoveInflight(ctx, queue.Name(), messageID); err != nil {
		return err
	}

	// Update message state for retry
	msg.State = storage.StateRetry
	msg.RetryCount++
	msg.NextRetryAt = time.Now() // Immediate retry

	return m.messageStore.UpdateMessage(ctx, queue.Name(), msg)
}

// Reject permanently rejects a message (move to DLQ).
func (m *Manager) Reject(ctx context.Context, queueTopic, messageID string, reason string) error {
	queue, err := m.GetQueue(queueTopic)
	if err != nil {
		return err
	}

	// Get message
	msg, err := m.messageStore.GetMessage(ctx, queue.Name(), messageID)
	if err != nil {
		return err
	}

	// Remove from inflight
	if err := m.messageStore.RemoveInflight(ctx, queue.Name(), messageID); err != nil {
		return err
	}

	// Move to DLQ
	config := queue.Config()
	if config.DLQConfig.Enabled {
		msg.State = storage.StateDLQ
		msg.FailureReason = reason
		msg.MovedToDLQAt = time.Now()

		if err := m.messageStore.EnqueueDLQ(ctx, config.DLQConfig.Topic, msg); err != nil {
			return err
		}
	}

	// Delete from original queue
	return m.messageStore.DeleteMessage(ctx, queue.Name(), messageID)
}

// GetStats returns statistics for a queue.
func (m *Manager) GetStats(ctx context.Context, queueName string) (*QueueStats, error) {
	queue, err := m.GetQueue(queueName)
	if err != nil {
		return nil, err
	}

	config := queue.Config()

	// Count messages per partition
	totalMessages := int64(0)
	for i := 0; i < config.Partitions; i++ {
		messages, err := m.messageStore.ListQueued(ctx, queueName, i, 0)
		if err != nil {
			return nil, err
		}
		totalMessages += int64(len(messages))
	}

	// Count inflight messages
	inflight, err := m.messageStore.GetInflight(ctx, queueName)
	if err != nil {
		return nil, err
	}

	// Count DLQ messages
	dlqMessages := int64(0)
	if config.DLQConfig.Enabled {
		dlq, err := m.messageStore.ListDLQ(ctx, config.DLQConfig.Topic, 0)
		if err == nil {
			dlqMessages = int64(len(dlq))
		}
	}

	// Count active consumers across all groups
	activeConsumers := 0
	for _, group := range queue.ConsumerGroups().ListGroups() {
		activeConsumers += group.Size()
	}

	return &QueueStats{
		Name:             queueName,
		TotalMessages:    totalMessages,
		InflightMessages: int64(len(inflight)),
		DLQMessages:      dlqMessages,
		ActiveConsumers:  activeConsumers,
		Partitions:       config.Partitions,
	}, nil
}

// QueueStats holds queue statistics.
type QueueStats struct {
	Name             string
	TotalMessages    int64
	InflightMessages int64
	DLQMessages      int64
	ActiveConsumers  int
	Partitions       int
}

// getPartitionOwner determines which node owns a partition using the configured strategy.
func (m *Manager) getPartitionOwner(ctx context.Context, queueName string, partitionID int) (string, error) {
	// Single-node mode: this node owns all partitions
	if m.cluster == nil {
		return m.localNodeID, nil
	}

	queue, err := m.GetQueue(queueName)
	if err != nil {
		return "", err
	}

	config := queue.Config()
	nodes := m.cluster.Nodes()

	// Get the appropriate partition assigner
	assigner := m.getPartitionAssigner(config)

	return assigner.GetOwner(ctx, queueName, partitionID, nodes)
}

// getPartitionAssigner returns the partition assigner based on queue configuration.
func (m *Manager) getPartitionAssigner(config storage.QueueConfig) PartitionAssigner {
	// Check if a partition strategy is configured
	// For now, we'll use hash by default until we add PartitionStrategy to QueueConfig
	// In Phase 2, we'll add this field to storage.QueueConfig

	// Default to hash-based assignment
	if m.cluster == nil {
		// Single-node: use hash assigner (deterministic)
		return NewHashPartitionAssigner()
	}

	// Multi-node: use hash by default for Phase 2
	// TODO: Add config.PartitionStrategy field and switch based on it
	return NewHashPartitionAssigner()
}

// acquirePartitionsForQueue acquires ownership of partitions assigned to this node.
func (m *Manager) acquirePartitionsForQueue(ctx context.Context, queue *Queue) error {
	if m.cluster == nil {
		// Single-node mode: no acquisition needed
		return nil
	}

	config := queue.Config()
	assigner := m.getPartitionAssigner(config)
	nodes := m.cluster.Nodes()

	// Acquire ownership for partitions assigned to this node
	for i := 0; i < config.Partitions; i++ {
		owner, err := assigner.GetOwner(ctx, config.Name, i, nodes)
		if err != nil {
			return fmt.Errorf("failed to determine owner for partition %d: %w", i, err)
		}

		// If this node is the owner, acquire it in etcd
		if owner == m.localNodeID {
			if err := m.cluster.AcquirePartition(ctx, config.Name, i, m.localNodeID); err != nil {
				return fmt.Errorf("failed to acquire partition %d: %w", i, err)
			}
		}
	}

	return nil
}

// enqueueRemote routes an enqueue operation to a remote partition owner via gRPC.
func (m *Manager) enqueueRemote(ctx context.Context, targetNode, queueTopic string, payload []byte, properties map[string]string) error {
	// Get transport from cluster
	if m.cluster == nil {
		return fmt.Errorf("cluster not configured")
	}

	// Call RPC to enqueue on remote node
	// Note: We need to access the transport through the cluster
	// For now, return an error indicating RPC integration needed
	return fmt.Errorf("remote enqueue RPC integration pending (target: %s)", targetNode)
}

// EnqueueLocal implements cluster.QueueHandler.EnqueueLocal.
// This is called by the gRPC handler when a remote node sends an enqueue request.
func (m *Manager) EnqueueLocal(ctx context.Context, queueName string, payload []byte, properties map[string]string) (string, error) {
	// This is the local enqueue path - bypass ownership checks since we're already the owner
	queue, err := m.GetOrCreateQueue(ctx, queueName)
	if err != nil {
		return "", err
	}

	config := queue.Config()

	// Check queue limits
	if config.MaxQueueDepth > 0 {
		count, err := m.messageStore.Count(ctx, queueName)
		if err != nil {
			return "", fmt.Errorf("failed to get queue depth: %w", err)
		}

		if count >= config.MaxQueueDepth {
			return "", fmt.Errorf("queue is full (depth: %d, max: %d)", count, config.MaxQueueDepth)
		}
	}

	// Extract partition key from properties
	partitionKey := properties["partition-key"]

	// Get partition ID
	partitionID := queue.GetPartitionForMessage(partitionKey)

	// Get next sequence number
	sequence, err := m.messageStore.GetNextSequence(ctx, queueName, partitionID)
	if err != nil {
		return "", fmt.Errorf("failed to get next sequence: %w", err)
	}

	// Get message from pool
	msg := getMessageFromPool()

	// Optimize: avoid pool allocation for properties if input is empty
	var msgProps map[string]string
	if len(properties) == 0 {
		msgProps = make(map[string]string, 1)
	} else {
		msgProps = getPropertyMap()
		copyProperties(msgProps, properties)
	}

	// Generate message ID
	msgID := uuid.New().String()
	msgProps["message-id"] = msgID

	// Populate message
	msg.ID = msgID
	msg.Payload = payload
	msg.Topic = queueName
	msg.PartitionKey = partitionKey
	msg.PartitionID = partitionID
	msg.Sequence = sequence
	msg.Properties = msgProps
	msg.State = storage.StateQueued
	msg.CreatedAt = time.Now()
	msg.ExpiresAt = msg.CreatedAt.Add(config.MessageTTL)

	// Enqueue (storage layer will make a deep copy)
	err = m.messageStore.Enqueue(ctx, queueName, msg)

	// Return to pools
	if len(properties) > 0 {
		putPropertyMap(msgProps)
	}
	putMessageToPool(msg)

	if err != nil {
		return "", err
	}

	// Notify partition worker that a message is available
	m.mu.RLock()
	worker, exists := m.deliveryWorkers[queueName]
	m.mu.RUnlock()

	if exists {
		worker.NotifyPartition(partitionID)
	}

	return msgID, nil
}

// DeliverQueueMessage implements cluster.QueueHandler.DeliverQueueMessage.
// This is called by the gRPC handler when a remote partition owner delivers a message.
func (m *Manager) DeliverQueueMessage(ctx context.Context, clientID string, msg any) error {
	// Delegate to the broker's delivery function
	if m.broker == nil {
		return fmt.Errorf("no broker delivery function configured")
	}

	return m.broker(ctx, clientID, msg)
}
