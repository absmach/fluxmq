// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package queue

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/absmach/fluxmq/cluster"
	"github.com/absmach/fluxmq/queue/consumer"
	"github.com/absmach/fluxmq/queue/delivery"
	"github.com/absmach/fluxmq/queue/lifecycle"
	"github.com/absmach/fluxmq/queue/storage"
	"github.com/absmach/fluxmq/queue/types"
	"github.com/google/uuid"
)

// DeliverFn defines the interface for message delivery to MQTT clients.
// This allows the queue manager to deliver messages without depending on the full broker.
type DeliverFn func(ctx context.Context, clientID string, msg any) error

// ConsumerRoutingMode defines how messages are delivered to consumers in a cluster.
type ConsumerRoutingMode string

const (
	// ProxyMode routes messages through the consumer's proxy node.
	// The partition owner sends messages to the node where the consumer is connected.
	// This is the default and recommended mode.
	ProxyMode ConsumerRoutingMode = "proxy"

	// DirectMode requires consumers to connect directly to partition owners.
	// Partition workers only deliver to local consumers.
	// This reduces network hops but requires client-side partition awareness.
	DirectMode ConsumerRoutingMode = "direct"
)

// Manager manages all queues in the system.
//
// For cluster mode, after creating the Manager, you must register it with the cluster
// to enable queue RPC handling:
//
//	if etcdCluster, ok := cluster.(*cluster.EtcdCluster); ok {
//	    etcdCluster.SetQueueHandler(queueManager)
//	}
type Manager struct {
	queues            map[string]*Queue
	deliveryWorkers   map[string]*delivery.Worker
	raftManagers      map[string]*RaftManager                // Per-queue Raft managers
	retentionManagers map[string]*lifecycle.RetentionManager // Per-queue retention managers
	queueStore        storage.QueueStore
	messageStore      storage.MessageStore
	consumerStore     storage.ConsumerStore
	broker            DeliverFn
	retryManager      *lifecycle.RetryManager
	dlqManager        *lifecycle.DLQManager
	mu                sync.RWMutex
	stopCh            chan struct{}
	stopOnce          sync.Once
	wg                sync.WaitGroup

	// Cluster support (nil for single-node mode)
	cluster             cluster.Cluster
	localNodeID         string
	consumerRoutingMode ConsumerRoutingMode
	dataDir             string            // For Raft storage
	nodeAddresses       map[string]string // nodeID -> raft bind address
}

// Config holds configuration for the queue manager.
type Config struct {
	QueueStore    storage.QueueStore
	MessageStore  storage.MessageStore
	ConsumerStore storage.ConsumerStore
	DeliverFn     DeliverFn

	// Optional cluster support
	Cluster             cluster.Cluster
	LocalNodeID         string
	ConsumerRoutingMode ConsumerRoutingMode // Default: ProxyMode

	// Optional Raft replication support
	DataDir       string            // Directory for Raft data storage
	NodeAddresses map[string]string // nodeID -> raft bind address (for Raft transport)
}

// NewManager creates a new queue manager.
func NewManager(cfg Config) (*Manager, error) {
	if cfg.QueueStore == nil || cfg.MessageStore == nil || cfg.ConsumerStore == nil {
		return nil, fmt.Errorf("queue stores cannot be nil")
	}

	// Create DLQ manager with HTTP alert handler
	alertHandler := lifecycle.NewHTTPAlertHandler(10 * time.Second)
	dlqManager := lifecycle.NewDLQManager(cfg.MessageStore, alertHandler)

	// Create retry manager
	retryManager := lifecycle.NewRetryManager(cfg.MessageStore, dlqManager)

	// Default to single-node if no cluster or node ID provided
	localNodeID := cfg.LocalNodeID
	if localNodeID == "" {
		localNodeID = "local"
	}

	// Default to ProxyMode if not specified
	routingMode := cfg.ConsumerRoutingMode
	if routingMode == "" {
		routingMode = ProxyMode
	}

	return &Manager{
		queues:              make(map[string]*Queue),
		deliveryWorkers:     make(map[string]*delivery.Worker),
		raftManagers:        make(map[string]*RaftManager),
		retentionManagers:   make(map[string]*lifecycle.RetentionManager),
		queueStore:          cfg.QueueStore,
		messageStore:        cfg.MessageStore,
		consumerStore:       cfg.ConsumerStore,
		broker:              cfg.DeliverFn,
		retryManager:        retryManager,
		dlqManager:          dlqManager,
		stopCh:              make(chan struct{}),
		cluster:             cfg.Cluster,
		localNodeID:         localNodeID,
		consumerRoutingMode: routingMode,
		dataDir:             cfg.DataDir,
		nodeAddresses:       cfg.NodeAddresses,
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

		// Restore consumers from persistent storage
		if err := m.restoreConsumers(ctx, queue); err != nil {
			return fmt.Errorf("failed to restore consumers for queue %s: %w", config.Name, err)
		}
	}

	// Start retry manager
	m.retryManager.Start(ctx)

	return nil
}

// Stop stops the queue manager and all background tasks.
func (m *Manager) Stop() error {
	m.stopOnce.Do(func() {
		m.mu.Lock()
		// Stop all delivery workers
		for _, worker := range m.deliveryWorkers {
			worker.Stop()
		}

		// Stop all Raft managers
		for _, raftMgr := range m.raftManagers {
			if err := raftMgr.Shutdown(); err != nil {
				slog.Error("failed to shutdown raft manager", slog.String("error", err.Error()))
			}
		}
		m.mu.Unlock()

		// Stop retry manager
		if m.retryManager != nil {
			m.retryManager.Stop()
		}

		close(m.stopCh)
		m.wg.Wait()
	})
	return nil
}

// CreateQueue creates a new queue with the given configuration.
func (m *Manager) CreateQueue(ctx context.Context, config types.QueueConfig) error {
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
func (m *Manager) createQueueInstance(config types.QueueConfig) error {
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

	// Create RaftManager if replication is enabled
	if config.Replication.Enabled {
		raftMgr, err := NewRaftManager(RaftManagerConfig{
			QueueName:     config.Name,
			Config:        config.Replication,
			NodeID:        m.localNodeID,
			DataDir:       m.dataDir,
			MessageStore:  m.messageStore,
			NodeAddresses: m.nodeAddresses,
			Logger:        slog.Default(),
		})
		if err != nil {
			delete(m.queues, config.Name)
			return fmt.Errorf("failed to create raft manager: %w", err)
		}

		m.raftManagers[config.Name] = raftMgr

		// Start Raft partitions
		ctx := context.Background()
		for partID := 0; partID < config.Partitions; partID++ {
			if err := raftMgr.StartPartition(ctx, partID, config.Partitions); err != nil {
				delete(m.queues, config.Name)
				raftMgr.Shutdown()
				delete(m.raftManagers, config.Name)
				return fmt.Errorf("failed to start raft partition %d: %w", partID, err)
			}
		}
	}

	// Get RaftManager if exists (use interface type to avoid Go's typed nil gotcha)
	var raftMgr delivery.RaftManager
	if config.Replication.Enabled {
		if rm := m.raftManagers[config.Name]; rm != nil {
			raftMgr = rm
		}
	}

	// Create RetentionManager if retention policy is configured
	if config.Retention.RetentionTime > 0 || config.Retention.RetentionBytes > 0 || config.Retention.RetentionMessages > 0 || config.Retention.CompactionEnabled {
		retentionMgr := lifecycle.NewRetentionManager(config.Name, config.Retention, m.messageStore, m.raftManagers[config.Name], slog.Default())
		m.retentionManagers[config.Name] = retentionMgr
		// Note: RetentionManager will be started by partition workers (only leaders should run retention)
	}

	// Get RetentionManager if exists (use interface type to avoid Go's typed nil gotcha)
	var retentionMgr delivery.RetentionManager
	if rm := m.retentionManagers[config.Name]; rm != nil {
		retentionMgr = rm
	}

	// Create delivery worker
	worker := delivery.NewWorker(
		queue,
		m.messageStore,
		// Cast m.DeliverQueueMessage to delivery.DeliverFn
		// This works because the signature matches
		m.DeliverQueueMessage,
		m.cluster,
		m.localNodeID,
		delivery.ConsumerRoutingMode(m.consumerRoutingMode),
		raftMgr,
		retentionMgr,
	)

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
	config := types.DefaultQueueConfig(queueName)
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

	// Extract partition key from properties (nil-safe)
	var partitionKey string
	if properties != nil {
		partitionKey = properties["partition-key"]
	}

	// Get partition ID
	partitionID := queue.GetPartitionForMessage(partitionKey)

	// Replicated path: use Raft for consensus
	if config.Replication.Enabled {
		return m.enqueueReplicated(ctx, queueTopic, partitionID, payload, properties, config)
	}

	// Non-replicated path: original logic
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
	msg.State = types.StateQueued
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

	// Size-based retention check (only if retention is configured)
	m.mu.RLock()
	retentionMgr, hasRetention := m.retentionManagers[queueTopic]
	m.mu.RUnlock()

	if hasRetention && (config.Retention.RetentionBytes > 0 || config.Retention.RetentionMessages > 0) {
		// Run size retention check asynchronously to avoid blocking enqueue
		go func() {
			deletedCount, bytesFreed, retErr := retentionMgr.CheckSizeRetention(ctx, partitionID)
			if retErr != nil {
				slog.Error("size retention check failed",
					slog.String("queue", queueTopic),
					slog.Int("partition", partitionID),
					slog.String("error", retErr.Error()))
			} else if deletedCount > 0 {
				slog.Debug("size retention cleanup completed",
					slog.String("queue", queueTopic),
					slog.Int("partition", partitionID),
					slog.Int64("deleted", deletedCount),
					slog.Int64("bytes_freed", bytesFreed))
			}
		}()
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

// enqueueReplicated routes an enqueue through Raft for replication.
func (m *Manager) enqueueReplicated(ctx context.Context, queueTopic string, partitionID int, payload []byte, properties map[string]string, config types.QueueConfig) error {
	// Get Raft manager for this queue
	m.mu.RLock()
	raftMgr, exists := m.raftManagers[queueTopic]
	m.mu.RUnlock()

	if !exists {
		return fmt.Errorf("raft manager not found for queue %s", queueTopic)
	}

	// Check if this node is the Raft leader for this partition
	if !raftMgr.IsLeader(partitionID) {
		// Wait for leader or fail
		if err := raftMgr.WaitForLeader(ctx, partitionID); err != nil {
			return fmt.Errorf("no leader available for partition %d: %w", partitionID, err)
		}

		// After waiting, check again if we're the leader
		// If not, this is a follower and we should reject the enqueue
		if !raftMgr.IsLeader(partitionID) {
			leaderID := ""
			if group, err := raftMgr.GetPartitionGroup(partitionID); err == nil {
				leaderID = group.Leader()
			}
			return fmt.Errorf("not leader for partition %d (leader: %s)", partitionID, leaderID)
		}
	}

	// Prepare message
	sequence, err := m.messageStore.GetNextSequence(ctx, queueTopic, partitionID)
	if err != nil {
		return fmt.Errorf("failed to get next sequence: %w", err)
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

	// Extract partition key
	partitionKey := properties["partition-key"]

	// Populate message
	msg.ID = msgID
	msg.Payload = payload
	msg.Topic = queueTopic
	msg.PartitionKey = partitionKey
	msg.PartitionID = partitionID
	msg.Sequence = sequence
	msg.Properties = msgProps
	msg.State = types.StateQueued
	msg.CreatedAt = time.Now()
	msg.ExpiresAt = msg.CreatedAt.Add(config.MessageTTL)

	// Create a copy for Raft (FSM needs the data to persist)
	// We can't return msg to pool until FSM applies it
	raftMsg := &types.Message{
		ID:           msg.ID,
		Payload:      append([]byte(nil), msg.Payload...), // Copy payload
		Topic:        msg.Topic,
		PartitionKey: msg.PartitionKey,
		PartitionID:  msg.PartitionID,
		Sequence:     msg.Sequence,
		State:        msg.State,
		CreatedAt:    msg.CreatedAt,
		ExpiresAt:    msg.ExpiresAt,
	}
	// Copy properties map
	raftMsg.Properties = make(map[string]string, len(msg.Properties))
	for k, v := range msg.Properties {
		raftMsg.Properties[k] = v
	}

	// Apply via Raft (this will replicate to followers)
	err = raftMgr.ApplyEnqueue(ctx, partitionID, raftMsg)

	// Return to pools (safe now since we made a copy)
	if len(properties) > 0 {
		putPropertyMap(msgProps)
	}
	putMessageToPool(msg)

	if err != nil {
		return fmt.Errorf("raft apply failed: %w", err)
	}

	// Notify partition worker that a message is available
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
	msg.State = types.StateRetry
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
		msg.State = types.StateDLQ
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
func (m *Manager) getPartitionAssigner(config types.QueueConfig) consumer.PartitionAssigner {
	// Check if a partition strategy is configured
	// For now, we'll use hash by default until we add PartitionStrategy to QueueConfig
	// In Phase 2, we'll add this field to types.QueueConfig

	// Default to hash-based assignment
	if m.cluster == nil {
		// Single-node: use hash assigner (deterministic)
		return consumer.NewHashPartitionAssigner()
	}

	// Multi-node: use hash by default for Phase 2
	// TODO: Add config.PartitionStrategy field and switch based on it
	return consumer.NewHashPartitionAssigner()
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
	if m.cluster == nil {
		return fmt.Errorf("cluster not configured")
	}

	// Call RPC to enqueue on remote node
	_, err := m.cluster.EnqueueRemote(ctx, targetNode, queueTopic, payload, properties)
	return err
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

	// Extract partition key from properties (nil-safe)
	var partitionKey string
	if properties != nil {
		partitionKey = properties["partition-key"]
	}

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
	msg.SetPayloadFromBytes(payload) // Use zero-copy buffer
	msg.Topic = queueName
	msg.PartitionKey = partitionKey
	msg.PartitionID = partitionID
	msg.Sequence = sequence
	msg.Properties = msgProps
	msg.State = types.StateQueued
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

// restoreConsumers restores consumers from persistent storage after startup.
func (m *Manager) restoreConsumers(ctx context.Context, queue *Queue) error {
	groups, err := m.consumerStore.ListGroups(ctx, queue.Name())
	if err != nil {
		return err
	}

	for _, groupID := range groups {
		consumers, err := m.consumerStore.ListConsumers(ctx, queue.Name(), groupID)
		if err != nil {
			return err
		}

		for _, consumer := range consumers {
			// Add to in-memory state (skip storage write since already persisted)
			queue.ConsumerGroups().RestoreConsumer(consumer)
		}

		// Rebalance after restoring group
		if len(consumers) > 0 {
			queue.ConsumerGroups().Rebalance(groupID, queue.toConsumerPartitions())
		}
	}
	return nil
}

// UpdateHeartbeat updates the heartbeat timestamp for a consumer across all queues/groups.
// This should be called when a PINGREQ is received from a client.
func (m *Manager) UpdateHeartbeat(ctx context.Context, clientID string) error {
	m.mu.RLock()
	defer m.mu.RUnlock()

	// Update heartbeat for this client across all queues/groups
	for _, queue := range m.queues {
		for _, group := range queue.ConsumerGroups().ListGroups() {
			if consumer, exists := group.GetConsumer(clientID); exists {
				queue.ConsumerGroups().UpdateHeartbeat(ctx, group.ID(), consumer.ID)
			}
		}
	}
	return nil
}
