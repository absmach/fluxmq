// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package queue

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/absmach/fluxmq/cluster"
	"github.com/absmach/fluxmq/queue/consumer"
	"github.com/absmach/fluxmq/queue/raft"
	"github.com/absmach/fluxmq/queue/storage"
	"github.com/absmach/fluxmq/queue/types"
	brokerstorage "github.com/absmach/fluxmq/storage"
	"github.com/absmach/fluxmq/topics"
)

// Manager is the queue-based queue manager.
// It uses append-only logs with cursor-based consumer groups, NATS JetQueue-style.
type Manager struct {
	queueStore      storage.QueueStore
	groupStore      storage.ConsumerGroupStore
	consumerManager *consumer.Manager
	deliveryTarget  Deliverer
	logger          *slog.Logger
	config          Config
	writePolicy     WritePolicy

	// Raft replication manager
	raftManager *raft.Manager

	// Cluster adapter for cross-node message routing
	clusterAdapter ClusterAdapter

	// Lightweight heartbeat index keyed by client/queue/group.
	// Stores only metadata needed to route heartbeat updates.
	subscriptionsMu sync.RWMutex
	subscriptions   map[string]map[string]*subscriptionRef // clientID -> refKey -> ref

	stopCh   chan struct{}
	stopOnce sync.Once
	wg       sync.WaitGroup

	deliveryMu    sync.Mutex
	deliverySet   map[string]struct{}
	deliveryQueue chan string

	// Metrics
	metrics *consumer.Metrics
}

// Config holds configuration for the queue-based queue manager.
type Config struct {
	// Consumer configuration
	VisibilityTimeout  time.Duration
	MaxDeliveryCount   int
	ClaimBatchSize     int
	AutoCommitInterval time.Duration

	// Delivery configuration
	DeliveryInterval  time.Duration
	DeliveryBatchSize int
	HeartbeatInterval time.Duration
	ConsumerTimeout   time.Duration

	// DLQ configuration
	DLQTopicPrefix string

	// Work stealing configuration
	StealInterval time.Duration
	StealEnabled  bool

	// PEL configuration
	MaxPELSize int

	// Retention configuration
	RetentionCheckInterval time.Duration

	// Replication/distribution configuration
	WritePolicy      WritePolicy
	DistributionMode DistributionMode

	// Queue configurations from main config
	QueueConfigs []types.QueueConfig
}

// DefaultConfig returns default configuration.
func DefaultConfig() Config {
	return Config{
		VisibilityTimeout:      30 * time.Second,
		MaxDeliveryCount:       5,
		ClaimBatchSize:         10,
		AutoCommitInterval:     5 * time.Second,
		DeliveryInterval:       10 * time.Millisecond,
		DeliveryBatchSize:      100,
		HeartbeatInterval:      10 * time.Second,
		ConsumerTimeout:        2 * time.Minute,
		MaxPELSize:             100_000,
		DLQTopicPrefix:         "$dlq/",
		StealInterval:          5 * time.Second,
		StealEnabled:           true,
		RetentionCheckInterval: 5 * time.Minute,
		WritePolicy:            WritePolicyLocal,
		DistributionMode:       DistributionForward,
	}
}

// NewManager creates a new queue-based queue manager.
// The cluster parameter is optional (nil for single-node mode).
func NewManager(queueStore storage.QueueStore, groupStore storage.ConsumerGroupStore, dt Deliverer, config Config, logger *slog.Logger, cl cluster.Cluster) *Manager {
	if logger == nil {
		logger = slog.Default()
	}

	metrics := consumer.NewMetrics()

	consumerCfg := consumer.Config{
		VisibilityTimeout:  config.VisibilityTimeout,
		MaxDeliveryCount:   config.MaxDeliveryCount,
		ClaimBatchSize:     config.ClaimBatchSize,
		StealBatchSize:     5,
		AutoCommitInterval: config.AutoCommitInterval,
		MaxPELSize:         config.MaxPELSize,
	}

	consumerMgr := consumer.NewManager(queueStore, groupStore, consumerCfg)

	var ca ClusterAdapter
	if cl != nil {
		ca = &clusterAdapter{
			cluster:          cl,
			localNodeID:      cl.NodeID(),
			distributionMode: normalizeDistributionMode(config.DistributionMode),
			logger:           logger,
		}
	} else {
		ca = noopClusterAdapter{}
	}

	return &Manager{
		queueStore:      queueStore,
		groupStore:      groupStore,
		consumerManager: consumerMgr,
		deliveryTarget:  dt,

		logger:         logger,
		config:         config,
		writePolicy:    normalizeWritePolicy(config.WritePolicy),
		clusterAdapter: ca,
		subscriptions:  make(map[string]map[string]*subscriptionRef),
		stopCh:         make(chan struct{}),
		deliverySet:    make(map[string]struct{}),
		deliveryQueue:  make(chan string, 4096),
		metrics:        metrics,
	}
}

// Start starts background workers.
func (m *Manager) Start(ctx context.Context) error {
	if m.clusterAdapter.DistributionMode() == DistributionReplicate && (m.raftManager == nil || !m.raftManager.IsEnabled()) {
		m.logger.Warn("distribution_mode=replicate requires raft to be enabled; falling back to forward")
		if a, ok := m.clusterAdapter.(*clusterAdapter); ok {
			a.distributionMode = DistributionForward
		}
	}

	// Ensure reserved queues exist
	if err := m.ensureReservedQueues(ctx); err != nil {
		return fmt.Errorf("failed to create reserved queues: %w", err)
	}

	// Cleanup ephemeral queues that expired while broker was down
	m.cleanupEphemeralQueues()

	// Prime delivery for existing queues at startup.
	m.scheduleAllQueues(ctx)

	// Start delivery workers
	m.wg.Add(1)
	go m.runDeliveryLoop()

	// Start work stealing if enabled
	if m.config.StealEnabled {
		m.wg.Add(1)
		go m.runStealLoop()
	}

	// Start consumer cleanup
	m.wg.Add(1)
	go m.runCleanupLoop()

	// Start retention
	m.wg.Add(1)
	go m.runRetentionLoop()

	// Start ephemeral queue cleanup
	m.wg.Add(1)
	go m.runEphemeralCleanupLoop()

	m.logger.Info("queue-based queue manager started")
	return nil
}

func (m *Manager) scheduleAllQueues(ctx context.Context) {
	queues, err := m.queueStore.ListQueues(ctx)
	if err != nil {
		return
	}
	for _, queueConfig := range queues {
		m.scheduleQueueDelivery(queueConfig.Name)
	}
}

func (m *Manager) scheduleQueueDelivery(queueName string) {
	if queueName == "" {
		return
	}

	m.deliveryMu.Lock()
	if _, exists := m.deliverySet[queueName]; exists {
		m.deliveryMu.Unlock()
		return
	}
	m.deliverySet[queueName] = struct{}{}
	m.deliveryMu.Unlock()

	select {
	case m.deliveryQueue <- queueName:
	default:
		// Channel full — delivery trigger dropped. The 1-second sweep ticker
		// will reschedule, but there's a gap where this queue won't get delivered.
		m.logger.Warn("delivery channel full, dropping trigger (will retry on next sweep)",
			slog.String("queue", queueName))
		// Prevent a dropped enqueue from getting stuck in the dedupe set forever.
		m.markQueueDelivered(queueName)
	}
}

func (m *Manager) markQueueDelivered(queueName string) {
	m.deliveryMu.Lock()
	delete(m.deliverySet, queueName)
	m.deliveryMu.Unlock()
}

// ensureReservedQueues creates queues from config or the default mqtt queue if no config provided.
func (m *Manager) ensureReservedQueues(ctx context.Context) error {
	// If no queue configs provided, use the default mqtt queue
	configs := m.config.QueueConfigs
	if len(configs) == 0 {
		configs = []types.QueueConfig{types.MQTTQueueConfig()}
	}

	for _, cfg := range configs {
		if err := m.queueStore.CreateQueue(ctx, cfg); err != nil {
			if err != storage.ErrQueueAlreadyExists {
				return err
			}
		}

		m.logger.Info("queue ready",
			slog.String("queue", cfg.Name),
			slog.Any("topics", cfg.Topics),
			slog.Bool("reserved", cfg.Reserved))
	}

	return nil
}

// Stop stops the manager and all workers.
func (m *Manager) Stop() error {
	m.stopOnce.Do(func() {
		close(m.stopCh)
	})

	m.wg.Wait()

	// Stop Raft manager if enabled
	if m.raftManager != nil {
		if err := m.raftManager.Stop(); err != nil {
			m.logger.Error("failed to stop raft manager", slog.String("error", err.Error()))
		}
	}

	m.logger.Info("queue-based queue manager stopped")
	return nil
}

// SetRaftManager sets the Raft replication manager.
func (m *Manager) SetRaftManager(rm *raft.Manager) {
	m.raftManager = rm
}

// GetRaftManager returns the Raft replication manager.
func (m *Manager) GetRaftManager() *raft.Manager {
	return m.raftManager
}

// --- Queue Operations ---

// CreateQueue creates a new queue.
func (m *Manager) CreateQueue(ctx context.Context, config types.QueueConfig) error {
	if err := m.queueStore.CreateQueue(ctx, config); err != nil {
		return err
	}
	m.scheduleQueueDelivery(config.Name)

	m.logger.Info("queue created",
		slog.String("queue", config.Name),
		slog.Any("topics", config.Topics))

	return nil
}

// UpdateQueue updates an existing queue.
func (m *Manager) UpdateQueue(ctx context.Context, config types.QueueConfig) error {
	return m.queueStore.UpdateQueue(ctx, config)
}

// GetOrCreateQueue gets or creates a queue with default configuration.
func (m *Manager) GetOrCreateQueue(ctx context.Context, queueName string, topics ...string) (*types.QueueConfig, error) {
	// Try to get existing
	config, err := m.queueStore.GetQueue(ctx, queueName)
	if err == nil {
		return config, nil
	}

	if err != storage.ErrQueueNotFound {
		return nil, err
	}

	// Create with ephemeral config (auto-created queues are ephemeral)
	defaultConfig := types.DefaultEphemeralQueueConfig(queueName, topics...)
	if err := m.CreateQueue(ctx, defaultConfig); err != nil {
		if err != storage.ErrQueueAlreadyExists {
			return nil, err
		}
	}

	return m.queueStore.GetQueue(ctx, queueName)
}

// DeleteQueue deletes a queue.
func (m *Manager) DeleteQueue(ctx context.Context, queueName string) error {
	if err := m.queueStore.DeleteQueue(ctx, queueName); err != nil {
		return err
	}
	m.markQueueDelivered(queueName)
	return nil
}

// GetQueue returns the configuration for a queue.
func (m *Manager) GetQueue(ctx context.Context, queueName string) (*types.QueueConfig, error) {
	return m.queueStore.GetQueue(ctx, queueName)
}

// ListQueues returns all queue configurations.
func (m *Manager) ListQueues(ctx context.Context) ([]types.QueueConfig, error) {
	return m.queueStore.ListQueues(ctx)
}

// --- Publish Operations ---

// Publish adds a message to all queues whose topic patterns match the topic.
// This is the NATS JetQueue-style "multi-queue" routing.
// It also forwards the publish to remote nodes that have consumers for the topic.
func (m *Manager) Publish(ctx context.Context, publish types.PublishRequest) error {
	if m.raftManager != nil && m.raftManager.IsEnabled() && !m.raftManager.IsLeader() {
		switch m.writePolicy {
		case WritePolicyReject:
			leaderAddr := m.raftManager.Leader()
			if leaderAddr == "" {
				return fmt.Errorf("raft leader unavailable")
			}
			return fmt.Errorf("raft leader is at %s", leaderAddr)
		case WritePolicyForward:
			return m.forwardPublishToLeader(ctx, publish)
		case WritePolicyLocal:
			// fall through to local append
		default:
			// Unknown policy - default to local append for backward compatibility.
		}
	}

	// Store locally in matching queues
	if err := m.publishLocal(ctx, publish); err != nil {
		return err
	}

	// Forward to remote nodes that have consumers
	m.clusterAdapter.ForwardToRemoteNodes(ctx, publish, m.queueExists(ctx))

	return nil
}

// HandleQueuePublish implements cluster.QueueHandler.HandleQueuePublish.
func (m *Manager) HandleQueuePublish(ctx context.Context, publish types.PublishRequest, mode types.PublishMode) error {
	switch mode {
	case types.PublishLocal:
		return m.publishLocal(ctx, publish)
	case types.PublishForwarded:
		return m.publishLocal(ctx, publish)
	case types.PublishNormal:
		fallthrough
	default:
		return m.Publish(ctx, publish)
	}
}

func (m *Manager) publishLocal(ctx context.Context, publish types.PublishRequest) error {
	// Find all matching queues
	queues, err := m.queueStore.FindMatchingQueues(ctx, publish.Topic)
	if err != nil {
		return fmt.Errorf("failed to find matching queues: %w", err)
	}

	if len(queues) == 0 {
		m.logger.Debug("no queues match topic, creating new queue", slog.String("topic", publish.Topic))
		queueName, queuePattern := autoQueueFromTopic(publish.Topic)
		if _, err := m.GetOrCreateQueue(ctx, queueName, queuePattern); err != nil {
			m.logger.Error("failed to create ephemeral queue", slog.String("topic", publish.Topic), slog.String("error", err.Error()))
			return err
		}
		// After creating, find it again.
		queues, err = m.queueStore.FindMatchingQueues(ctx, publish.Topic)
		if err != nil {
			return fmt.Errorf("failed to find matching queues after creation: %w", err)
		}
	}

	if len(queues) == 0 {
		m.logger.Debug("no queues match topic", slog.String("topic", publish.Topic))
		return nil
	}

	// Append to each matching queue
	for _, queueName := range queues {
		queueConfig, err := m.queueStore.GetQueue(ctx, queueName)
		if err != nil {
			m.logger.Warn("failed to get queue config", slog.String("queue", queueName), slog.String("error", err.Error()))
			continue
		}

		// Create message for this queue
		msg := &types.Message{
			ID:         generateMessageID(),
			Payload:    publish.Payload,
			Topic:      publish.Topic,
			Properties: publish.Properties,
			State:      types.StateQueued,
			CreatedAt:  time.Now(),
			ExpiresAt:  time.Now().Add(queueConfig.MessageTTL),
		}

		var offset uint64
		if m.raftManager != nil && m.raftManager.IsEnabled() && m.raftManager.IsLeader() {
			offset, err = m.raftManager.ApplyAppend(ctx, queueName, msg)
		} else {
			offset, err = m.queueStore.Append(ctx, queueName, msg)
		}

		if err != nil {
			m.logger.Warn("failed to append to queue",
				slog.String("queue", queueName),
				slog.String("topic", publish.Topic),
				slog.String("error", err.Error()))
			continue
		}

		m.logger.Debug("message published",
			slog.String("queue", queueName),
			slog.String("topic", publish.Topic),
			slog.Uint64("offset", offset))

		m.scheduleQueueDelivery(queueName)
	}

	return nil
}

func autoQueueFromTopic(topic string) (queueName, pattern string) {
	if strings.HasPrefix(topic, "$queue/") {
		rest := strings.TrimPrefix(topic, "$queue/")
		if rest != "" {
			parts := strings.SplitN(rest, "/", 2)
			if parts[0] != "" {
				queueName = parts[0]
				return queueName, "$queue/" + queueName + "/#"
			}
		}
	}

	return topic, topic
}

// queueExists returns a cached closure that checks whether a queue exists in the local store.
func (m *Manager) queueExists(ctx context.Context) func(string) bool {
	cache := make(map[string]bool)
	return func(queueName string) bool {
		if exists, ok := cache[queueName]; ok {
			return exists
		}

		_, err := m.queueStore.GetQueue(ctx, queueName)
		if err == nil {
			cache[queueName] = true
			return true
		}
		if err != storage.ErrQueueNotFound {
			m.logger.Warn("failed to check queue existence for forwarding",
				slog.String("queue", queueName),
				slog.String("error", err.Error()))
		}

		cache[queueName] = false
		return false
	}
}

// matchesTopic checks if a filter pattern matches a topic using MQTT wildcard rules.
func matchesTopic(filter, topic string) bool {
	return topics.TopicMatch(filter, topic)
}

// Enqueue is an alias for Publish for backward compatibility.
func (m *Manager) Enqueue(ctx context.Context, topic string, payload []byte, properties map[string]string) error {
	return m.Publish(ctx, types.PublishRequest{
		Topic:      topic,
		Payload:    payload,
		Properties: properties,
	})
}

// --- Subscribe Operations ---

// SubscribeWithCursor adds a consumer with explicit cursor positioning.
func (m *Manager) SubscribeWithCursor(ctx context.Context, queueName, pattern string, clientID, groupID, proxyNodeID string, cursor *types.CursorOption) error {
	if proxyNodeID == "" {
		proxyNodeID = m.clusterAdapter.LocalNodeID()
	}

	mode := types.GroupModeQueue
	if cursor != nil && cursor.Mode != "" {
		mode = cursor.Mode
	}
	if cursor == nil || cursor.Position == types.CursorDefault {
		if mode != types.GroupModeStream {
			return m.Subscribe(ctx, queueName, pattern, clientID, groupID, proxyNodeID)
		}
		cursor = &types.CursorOption{Position: types.CursorLatest, Mode: mode}
	}

	// Ensure queue exists
	queueTopicPattern := "$queue/" + queueName + "/#"
	queueCfg, err := m.GetOrCreateQueue(ctx, queueName, queueTopicPattern)
	if err != nil {
		return fmt.Errorf("failed to get or create queue: %w", err)
	}
	if mode == types.GroupModeStream && queueCfg != nil && queueCfg.Type != types.QueueTypeStream {
		queueCfg.Type = types.QueueTypeStream
		if err := m.queueStore.UpdateQueue(ctx, *queueCfg); err != nil {
			m.logger.Warn("failed to update stream queue config",
				slog.String("queue", queueName),
				slog.String("error", err.Error()))
		}
	}

	if groupID == "" {
		if mode == types.GroupModeStream {
			groupID = clientID
		} else {
			groupID = extractGroupFromClientID(clientID)
		}
	}

	patternGroupID := groupID
	if pattern != "" {
		patternGroupID = fmt.Sprintf("%s@%s", groupID, pattern)
	}

	autoCommit := true
	if cursor != nil && cursor.AutoCommit != nil {
		autoCommit = *cursor.AutoCommit
	}

	group, err := m.consumerManager.GetOrCreateGroup(ctx, queueName, patternGroupID, pattern, mode, autoCommit)
	if err != nil {
		return err
	}

	// Apply cursor positioning
	switch cursor.Position {
	case types.CursorEarliest:
		head, err := m.queueStore.Head(ctx, queueName)
		if err == nil {
			m.groupStore.UpdateCursor(ctx, queueName, group.ID, head)
		}
	case types.CursorLatest:
		tail, err := m.queueStore.Tail(ctx, queueName)
		if err == nil {
			m.groupStore.UpdateCursor(ctx, queueName, group.ID, tail)
		}
	case types.CursorOffset:
		head, _ := m.queueStore.Head(ctx, queueName)
		tail, _ := m.queueStore.Tail(ctx, queueName)
		offset := cursor.Offset
		if offset < head {
			offset = head
		}
		if offset > tail {
			offset = tail
		}
		m.groupStore.UpdateCursor(ctx, queueName, group.ID, offset)
	case types.CursorTimestamp:
		if !cursor.Timestamp.IsZero() {
			if offset, err := m.offsetByTime(ctx, queueName, cursor.Timestamp); err == nil {
				m.groupStore.UpdateCursor(ctx, queueName, group.ID, offset)
			}
		}
	}

	if err := m.consumerManager.RegisterConsumer(ctx, queueName, group.ID, clientID, clientID, proxyNodeID); err != nil {
		return err
	}

	// Clear ephemeral disconnect timestamp since we now have a consumer
	m.clearEphemeralDisconnect(ctx, queueName)

	info := &cluster.QueueConsumerInfo{
		QueueName:    queueName,
		GroupID:      patternGroupID,
		ConsumerID:   clientID,
		ClientID:     clientID,
		Pattern:      pattern,
		Mode:         string(mode),
		ProxyNodeID:  proxyNodeID,
		RegisteredAt: time.Now(),
	}
	if err := m.clusterAdapter.RegisterConsumer(ctx, info); err != nil {
		m.logger.Warn("failed to register consumer in cluster",
			slog.String("error", err.Error()),
			slog.String("client", clientID))
	}

	m.trackSubscription(clientID, queueName, patternGroupID)

	m.logger.Info("consumer subscribed with cursor",
		slog.String("queue", queueName),
		slog.String("group", patternGroupID),
		slog.String("client", clientID),
		slog.String("cursor", fmt.Sprintf("%d", cursor.Position)),
		slog.String("mode", string(mode)))

	m.scheduleQueueDelivery(queueName)

	return nil
}

// Subscribe adds a consumer to a stream with optional pattern matching.
func (m *Manager) Subscribe(ctx context.Context, queueName, pattern string, clientID, groupID, proxyNodeID string) error {
	if proxyNodeID == "" {
		proxyNodeID = m.clusterAdapter.LocalNodeID()
	}

	// Ensure queue exists (auto-create if not)
	// Use $queue/<name>/# as the topic pattern so messages published to $queue/<name>/... are captured
	queueTopicPattern := "$queue/" + queueName + "/#"
	_, err := m.GetOrCreateQueue(ctx, queueName, queueTopicPattern)
	if err != nil {
		return fmt.Errorf("failed to get or create queue: %w", err)
	}

	// Default group ID to client prefix
	if groupID == "" {
		groupID = extractGroupFromClientID(clientID)
	}

	// Create unique group ID that includes the pattern
	patternGroupID := groupID
	if pattern != "" {
		patternGroupID = fmt.Sprintf("%s@%s", groupID, pattern)
	}

	// Get or create consumer group (queue mode always auto-commits)
	group, err := m.consumerManager.GetOrCreateGroup(ctx, queueName, patternGroupID, pattern, types.GroupModeQueue, true)
	if err != nil {
		return err
	}

	// Register consumer locally
	if err := m.consumerManager.RegisterConsumer(ctx, queueName, group.ID, clientID, clientID, proxyNodeID); err != nil {
		return err
	}

	// Clear ephemeral disconnect timestamp since we now have a consumer
	m.clearEphemeralDisconnect(ctx, queueName)

	// Register consumer in cluster for cross-node visibility
	info := &cluster.QueueConsumerInfo{
		QueueName:    queueName,
		GroupID:      patternGroupID,
		ConsumerID:   clientID,
		ClientID:     clientID,
		Pattern:      pattern,
		Mode:         string(types.GroupModeQueue),
		ProxyNodeID:  proxyNodeID,
		RegisteredAt: time.Now(),
	}
	if err := m.clusterAdapter.RegisterConsumer(ctx, info); err != nil {
		m.logger.Warn("failed to register consumer in cluster",
			slog.String("error", err.Error()),
			slog.String("client", clientID))
	}

	// Track subscription
	m.trackSubscription(clientID, queueName, patternGroupID)

	m.logger.Info("consumer subscribed",
		slog.String("queue", queueName),
		slog.String("group", patternGroupID),
		slog.String("client", clientID),
		slog.String("pattern", pattern))

	m.scheduleQueueDelivery(queueName)

	return nil
}

// Unsubscribe removes a consumer from a stream.
func (m *Manager) Unsubscribe(ctx context.Context, queueName, pattern string, clientID, groupID string) error {
	if groupID == "" {
		groupID = extractGroupFromClientID(clientID)
	}

	patternGroupID := groupID
	if pattern != "" {
		patternGroupID = fmt.Sprintf("%s@%s", groupID, pattern)
	}

	// Unregister consumer locally
	if err := m.consumerManager.UnregisterConsumer(ctx, queueName, patternGroupID, clientID); err != nil {
		m.logger.Warn("failed to unregister consumer, may become phantom",
			slog.String("error", err.Error()),
			slog.String("queue", queueName),
			slog.String("group", patternGroupID),
			slog.String("client", clientID))
	}

	// Unregister consumer from cluster
	if err := m.clusterAdapter.UnregisterConsumer(ctx, queueName, patternGroupID, clientID); err != nil {
		m.logger.Warn("failed to unregister consumer from cluster",
			slog.String("error", err.Error()),
			slog.String("client", clientID))
	}

	// Untrack subscription
	m.untrackSubscription(clientID, queueName, patternGroupID)

	// Track last consumer disconnect for ephemeral queues
	m.checkEphemeralDisconnect(ctx, queueName)

	m.logger.Info("consumer unsubscribed",
		slog.String("queue", queueName),
		slog.String("group", patternGroupID),
		slog.String("client", clientID))

	m.scheduleQueueDelivery(queueName)

	return nil
}

// --- Ack Operations ---

// Ack acknowledges a message.
func (m *Manager) Ack(ctx context.Context, queueName, messageID, groupID string) error {
	// Parse message ID to get offset
	offset, err := parseMessageID(messageID)
	if err != nil {
		return err
	}

	if groupID != "" {
		if group, err := m.groupStore.GetConsumerGroup(ctx, queueName, groupID); err == nil {
			if group.Mode == types.GroupModeStream {
				cursor := group.GetCursor()
				next := offset + 1
				if next > cursor.Cursor {
					if err := m.groupStore.UpdateCursor(ctx, queueName, group.ID, next); err != nil {
						m.logger.Warn("failed to update stream cursor",
							slog.String("queue", queueName),
							slog.String("group", group.ID),
							slog.String("error", err.Error()))
					}
					if err := m.groupStore.UpdateCommitted(ctx, queueName, group.ID, next); err != nil {
						m.logger.Warn("failed to update stream committed offset",
							slog.String("queue", queueName),
							slog.String("group", group.ID),
							slog.String("error", err.Error()))
					}
				}
				m.scheduleQueueDelivery(queueName)
				return nil
			}
		}
	}

	// Find the consumer that has this message pending
	groups, err := m.groupStore.ListConsumerGroups(ctx, queueName)
	if err != nil {
		return err
	}

	for _, group := range groups {
		// Check if this group matches
		if groupID != "" && group.ID != groupID {
			continue
		}
		if group.Mode == types.GroupModeStream {
			cursor := group.GetCursor()
			next := offset + 1
			if next > cursor.Cursor {
				if err := m.groupStore.UpdateCursor(ctx, queueName, group.ID, next); err != nil {
					m.logger.Warn("failed to update stream cursor",
						slog.String("queue", queueName),
						slog.String("group", group.ID),
						slog.String("error", err.Error()))
				}
				if err := m.groupStore.UpdateCommitted(ctx, queueName, group.ID, next); err != nil {
					m.logger.Warn("failed to update stream committed offset",
						slog.String("queue", queueName),
						slog.String("group", group.ID),
						slog.String("error", err.Error()))
				}
			}
			m.scheduleQueueDelivery(queueName)
			return nil
		}

		// Find and ack the message
		for consumerID := range group.PEL {
			err := m.consumerManager.Ack(ctx, queueName, group.ID, consumerID, offset)
			if err == nil {
				m.metrics.RecordAck(0)
				m.metrics.UpdatePELSize(uint64(group.PendingCount()))
				m.scheduleQueueDelivery(queueName)
				return nil
			}
		}
	}

	return consumer.ErrMessageNotPending
}

// Nack negatively acknowledges a message.
func (m *Manager) Nack(ctx context.Context, queueName, messageID, groupID string) error {
	offset, err := parseMessageID(messageID)
	if err != nil {
		return err
	}

	if groupID != "" {
		if group, err := m.groupStore.GetConsumerGroup(ctx, queueName, groupID); err == nil {
			if group.Mode == types.GroupModeStream {
				m.scheduleQueueDelivery(queueName)
				return nil
			}
		}
	}

	groups, err := m.groupStore.ListConsumerGroups(ctx, queueName)
	if err != nil {
		return err
	}

	for _, group := range groups {
		if groupID != "" && group.ID != groupID {
			continue
		}
		if group.Mode == types.GroupModeStream {
			m.scheduleQueueDelivery(queueName)
			return nil
		}

		for consumerID := range group.PEL {
			err := m.consumerManager.Nack(ctx, queueName, group.ID, consumerID, offset)
			if err == nil {
				m.metrics.RecordNack()
				m.scheduleQueueDelivery(queueName)
				return nil
			}
		}
	}

	return consumer.ErrMessageNotPending
}

// Reject rejects a message and moves it to DLQ.
func (m *Manager) Reject(ctx context.Context, queueName, messageID, groupID, reason string) error {
	offset, err := parseMessageID(messageID)
	if err != nil {
		return err
	}

	if groupID != "" {
		if group, err := m.groupStore.GetConsumerGroup(ctx, queueName, groupID); err == nil {
			if group.Mode == types.GroupModeStream {
				m.rejectStream(ctx, queueName, group, offset, reason)
				return nil
			}
		}
	}

	groups, err := m.groupStore.ListConsumerGroups(ctx, queueName)
	if err != nil {
		return err
	}

	for _, group := range groups {
		if groupID != "" && group.ID != groupID {
			continue
		}
		if group.Mode == types.GroupModeStream {
			m.rejectStream(ctx, queueName, group, offset, reason)
			return nil
		}

		for consumerID := range group.PEL {
			err := m.consumerManager.Reject(ctx, queueName, group.ID, consumerID, offset, reason)
			if err == nil {
				m.metrics.RecordReject()
				m.scheduleQueueDelivery(queueName)
				return nil
			}
		}
	}

	return consumer.ErrMessageNotPending
}

// rejectStream handles reject for stream-mode consumer groups.
// Stream queues don't have PEL, so reject advances the cursor past the
// rejected message (same as ack) to prevent infinite redelivery.
func (m *Manager) rejectStream(ctx context.Context, queueName string, group *types.ConsumerGroup, offset uint64, reason string) {
	cursor := group.GetCursor()
	next := offset + 1
	if next > cursor.Cursor {
		if err := m.groupStore.UpdateCursor(ctx, queueName, group.ID, next); err != nil {
			m.logger.Warn("failed to update stream cursor on reject",
				slog.String("queue", queueName),
				slog.String("group", group.ID),
				slog.String("error", err.Error()))
		}
		if err := m.groupStore.UpdateCommitted(ctx, queueName, group.ID, next); err != nil {
			m.logger.Warn("failed to update stream committed offset on reject",
				slog.String("queue", queueName),
				slog.String("group", group.ID),
				slog.String("error", err.Error()))
		}
	}

	m.logger.Info("stream message rejected",
		slog.String("queue", queueName),
		slog.String("group", group.ID),
		slog.Uint64("offset", offset),
		slog.String("reason", reason))
	m.metrics.RecordReject()
	m.scheduleQueueDelivery(queueName)
}

// --- Heartbeat ---

// UpdateHeartbeat updates the heartbeat for a consumer.
func (m *Manager) UpdateHeartbeat(ctx context.Context, clientID string) error {
	targets := m.getSubscriptionTargets(clientID)
	if len(targets) == 0 {
		return nil
	}

	now := time.Now()
	var staleKeys []string
	for _, target := range targets {
		err := m.consumerManager.UpdateHeartbeat(ctx, target.queueName, target.groupID, clientID)
		if err == nil {
			m.touchSubscription(clientID, target.key, now)
			continue
		}
		if err == storage.ErrConsumerNotFound || err == consumer.ErrConsumerNotFound {
			staleKeys = append(staleKeys, target.key)
		}
	}

	if len(staleKeys) > 0 {
		m.removeSubscriptionKeys(clientID, staleKeys)
	}

	return nil
}

// --- Background Workers ---

func (m *Manager) runDeliveryLoop() {
	defer m.wg.Done()

	sweepInterval := time.Second
	ticker := time.NewTicker(sweepInterval)
	defer ticker.Stop()

	for {
		select {
		case <-m.stopCh:
			return
		case queueName := <-m.deliveryQueue:
			m.markQueueDelivered(queueName)
			if m.deliverQueue(context.Background(), queueName) {
				// Continue draining while this queue still has deliverable work.
				m.scheduleQueueDelivery(queueName)
			}
		case <-ticker.C:
			m.scheduleAllQueues(context.Background())
		}
	}
}

func (m *Manager) deliverMessages() {
	ctx := context.Background()

	queues, err := m.queueStore.ListQueues(ctx)
	if err != nil {
		return
	}

	for i := range queues {
		m.deliverQueueConfig(ctx, &queues[i])
	}
}

func (m *Manager) deliverQueue(ctx context.Context, queueName string) bool {
	if queueName == "" {
		return false
	}

	queueConfig, err := m.queueStore.GetQueue(ctx, queueName)
	if err != nil {
		return false
	}

	return m.deliverQueueConfig(ctx, queueConfig)
}

func (m *Manager) deliverQueueConfig(ctx context.Context, queueConfig *types.QueueConfig) bool {
	if queueConfig == nil {
		return false
	}

	delivered := false
	primaryGroup := strings.TrimSpace(queueConfig.PrimaryGroup)
	primaryCommitted := make(map[string]uint64)
	getPrimaryCommitted := func(pattern string) (uint64, bool) {
		if primaryGroup == "" {
			return 0, false
		}

		patternGroupID := primaryGroup
		if pattern != "" {
			patternGroupID = fmt.Sprintf("%s@%s", primaryGroup, pattern)
		}

		if val, ok := primaryCommitted[patternGroupID]; ok {
			return val, true
		}

		committed, err := m.consumerManager.GetCommittedOffset(ctx, queueConfig.Name, patternGroupID)
		if err != nil {
			return 0, false
		}

		primaryCommitted[patternGroupID] = committed
		return committed, true
	}

	// Deliver to local consumer groups.
	groups, err := m.groupStore.ListConsumerGroups(ctx, queueConfig.Name)
	if err == nil {
		for _, group := range groups {
			if m.deliverToGroup(ctx, queueConfig, group, getPrimaryCommitted) {
				delivered = true
			}
		}
	}

	// Deliver to remote consumers registered in cluster.
	if m.clusterAdapter.DistributionMode() == DistributionForward {
		if m.deliverToRemoteConsumers(ctx, queueConfig) {
			delivered = true
		}
	}

	return delivered
}

func (m *Manager) forwardPublishToLeader(ctx context.Context, publish types.PublishRequest) error {
	if m.clusterAdapter.LocalNodeID() == "" {
		return fmt.Errorf("cluster not configured for leader forward")
	}

	leaderID := m.raftManager.LeaderID()
	if leaderID == "" {
		return fmt.Errorf("raft leader unavailable")
	}

	return m.clusterAdapter.ForwardPublish(ctx, leaderID, publish.Topic, publish.Payload, publish.Properties, true)
}

// deliverToRemoteConsumers delivers messages to consumers registered on remote nodes.
// This enables cross-node queue message routing.
func (m *Manager) deliverToRemoteConsumers(ctx context.Context, config *types.QueueConfig) bool {
	// Get all consumers for this queue from the cluster
	consumers, err := m.clusterAdapter.ListConsumers(ctx, config.Name)
	if err != nil {
		m.logger.Debug("failed to list cluster consumers",
			slog.String("queue", config.Name),
			slog.String("error", err.Error()))
		return false
	}

	// Group consumers by groupID for proper cursor management
	consumersByGroup := make(map[string][]*cluster.QueueConsumerInfo)
	for _, c := range consumers {
		// Only process consumers on remote nodes
		if !m.clusterAdapter.IsRemote(c.ProxyNodeID) {
			continue
		}
		consumersByGroup[c.GroupID] = append(consumersByGroup[c.GroupID], c)
	}

	delivered := false
	for groupID, groupConsumers := range consumersByGroup {
		mode := types.GroupModeQueue
		if groupConsumers[0].Mode != "" {
			mode = types.ConsumerGroupMode(groupConsumers[0].Mode)
		}
		// Get or create a local consumer group state for tracking cursor (default auto-commit for remote)
		group, err := m.consumerManager.GetOrCreateGroup(ctx, config.Name, groupID, groupConsumers[0].Pattern, mode, true)
		if err != nil {
			continue
		}

		// Create filter from group pattern
		var filter *consumer.Filter
		if group.Pattern != "" {
			filter = consumer.NewFilter(group.Pattern)
		}

		// Round-robin across remote consumers in this group
		var workCommitted uint64
		var hasWorkCommitted bool
		if group.Mode == types.GroupModeStream && config.PrimaryGroup != "" {
			patternGroupID := config.PrimaryGroup
			if group.Pattern != "" {
				patternGroupID = fmt.Sprintf("%s@%s", config.PrimaryGroup, group.Pattern)
			}
			if committed, err := m.consumerManager.GetCommittedOffset(ctx, config.Name, patternGroupID); err == nil {
				workCommitted = committed
				hasWorkCommitted = true
			}
		}

		for _, consumerInfo := range groupConsumers {
			// Claim messages for this remote consumer
			var msgs []*types.Message
			var err error
			if group.Mode == types.GroupModeStream {
				msgs, err = m.consumerManager.ClaimBatchStream(ctx, config.Name, groupID, consumerInfo.ConsumerID, filter, m.config.DeliveryBatchSize)
			} else {
				msgs, err = m.consumerManager.ClaimBatch(ctx, config.Name, groupID, consumerInfo.ConsumerID, filter, m.config.DeliveryBatchSize)
			}
			if err != nil {
				continue
			}
			if len(msgs) > 0 {
				delivered = true
			}

			// Route each message to the remote node
			for _, msg := range msgs {
				routeMsg := m.createRoutedQueueMessage(
					msg,
					groupID,
					config.Name,
					group.Mode == types.GroupModeStream,
					workCommitted,
					hasWorkCommitted,
					config.PrimaryGroup,
				)

				err := m.clusterAdapter.RouteMessage(
					ctx,
					consumerInfo.ProxyNodeID,
					consumerInfo.ClientID,
					config.Name,
					routeMsg,
				)
				if err != nil {
					m.logger.Warn("remote queue message delivery failed",
						slog.String("client", consumerInfo.ClientID),
						slog.String("node", consumerInfo.ProxyNodeID),
						slog.String("queue", config.Name),
						slog.String("error", err.Error()))
				} else {
					m.logger.Debug("routed queue message to remote consumer",
						slog.String("client", consumerInfo.ClientID),
						slog.String("node", consumerInfo.ProxyNodeID),
						slog.String("queue", config.Name),
						slog.Uint64("offset", msg.Sequence))
				}
			}
		}
	}

	return delivered
}

func (m *Manager) deliverToGroup(ctx context.Context, config *types.QueueConfig, group *types.ConsumerGroup, primaryCommitted func(pattern string) (uint64, bool)) bool {
	if group.ConsumerCount() == 0 {
		return false
	}

	// Create filter from group pattern
	var filter *consumer.Filter
	if group.Pattern != "" {
		filter = consumer.NewFilter(group.Pattern)
	}

	// Round-robin delivery across consumers
	consumers := group.ConsumerIDs()
	if len(consumers) == 0 {
		return false
	}

	delivered := false
	for _, consumerID := range consumers {
		var msgs []*types.Message
		var err error
		if group.Mode == types.GroupModeStream {
			msgs, err = m.consumerManager.ClaimBatchStream(ctx, config.Name, group.ID, consumerID, filter, m.config.DeliveryBatchSize)
		} else {
			msgs, err = m.consumerManager.ClaimBatch(ctx, config.Name, group.ID, consumerID, filter, m.config.DeliveryBatchSize)
		}
		if err != nil {
			continue
		}
		if len(msgs) > 0 {
			delivered = true
		}

		// Fetch fresh group state to get current consumer info
		freshGroup, err := m.groupStore.GetConsumerGroup(ctx, config.Name, group.ID)
		if err != nil {
			continue
		}

		consumerInfo := freshGroup.GetConsumer(consumerID)
		if consumerInfo == nil {
			continue
		}

		// Update heartbeat on delivery so active consumers are never cleaned up as stale.
		if len(msgs) > 0 {
			m.consumerManager.UpdateHeartbeat(ctx, config.Name, group.ID, consumerID)
		}

		var workCommitted uint64
		var hasWorkCommitted bool
		if group.Mode == types.GroupModeStream && primaryCommitted != nil {
			workCommitted, hasWorkCommitted = primaryCommitted(group.Pattern)
		}

		for _, msg := range msgs {
			// Check if consumer is on a remote node
			if m.clusterAdapter.IsRemote(consumerInfo.ProxyNodeID) {
				// Route to remote node
				routeMsg := m.createRoutedQueueMessage(
					msg,
					group.ID,
					config.Name,
					group.Mode == types.GroupModeStream,
					workCommitted,
					hasWorkCommitted,
					config.PrimaryGroup,
				)
				err := m.clusterAdapter.RouteMessage(
					ctx,
					consumerInfo.ProxyNodeID,
					consumerInfo.ClientID,
					config.Name,
					routeMsg,
				)
				if err != nil {
					m.logger.Warn("queue message remote routing failed",
						slog.String("client", consumerInfo.ClientID),
						slog.String("node", consumerInfo.ProxyNodeID),
						slog.String("topic", msg.Topic),
						slog.String("error", err.Error()))
				}
			} else if m.deliveryTarget != nil {
				// Local delivery
				deliveryMsg := m.createDeliveryMessage(msg, group.ID, config.Name)
				if group.Mode == types.GroupModeStream {
					m.decorateStreamDelivery(deliveryMsg, msg, group, workCommitted, hasWorkCommitted, config.PrimaryGroup)
				}

				if err := m.deliveryTarget.Deliver(ctx, consumerInfo.ClientID, deliveryMsg); err != nil {
					m.logger.Warn("queue message delivery failed",
						slog.String("client", consumerInfo.ClientID),
						slog.String("topic", msg.Topic),
						slog.String("error", err.Error()))
				}
			}
		}
	}

	return delivered
}

func (m *Manager) runStealLoop() {
	defer m.wg.Done()

	ticker := time.NewTicker(m.config.StealInterval)
	defer ticker.Stop()

	for {
		select {
		case <-m.stopCh:
			return
		case <-ticker.C:
			// Work stealing is handled internally by ClaimBatch
		}
	}
}

func (m *Manager) runCleanupLoop() {
	defer m.wg.Done()

	ticker := time.NewTicker(m.config.ConsumerTimeout)
	defer ticker.Stop()

	for {
		select {
		case <-m.stopCh:
			return
		case <-ticker.C:
			m.cleanupStaleConsumers()
			m.pruneStaleSubscriptions()
		}
	}
}

func (m *Manager) cleanupStaleConsumers() {
	ctx := context.Background()

	queues, err := m.queueStore.ListQueues(ctx)
	if err != nil {
		return
	}

	for _, queueConfig := range queues {
		groups, err := m.groupStore.ListConsumerGroups(ctx, queueConfig.Name)
		if err != nil {
			continue
		}

		for _, group := range groups {
			removed, err := m.consumerManager.CleanupStaleConsumers(ctx, queueConfig.Name, group.ID, m.config.ConsumerTimeout)
			if err == nil && len(removed) > 0 {
				m.logger.Info("cleaned up stale consumers",
					slog.Int("count", len(removed)),
					slog.String("queue", queueConfig.Name),
					slog.String("group", group.ID))
			}
		}
	}
}

func (m *Manager) runRetentionLoop() {
	defer m.wg.Done()

	ticker := time.NewTicker(m.config.RetentionCheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-m.stopCh:
			return
		case <-ticker.C:
			m.processRetention()
		}
	}
}

func (m *Manager) processRetention() {
	ctx := context.Background()

	queues, err := m.queueStore.ListQueues(ctx)
	if err != nil {
		return
	}

	for _, queueConfig := range queues {
		// Get minimum committed offset across queue-mode groups
		minCommitted, err := m.consumerManager.GetMinCommittedOffsetByMode(ctx, queueConfig.Name, types.GroupModeQueue)
		if err != nil {
			continue
		}

		truncateOffset := minCommitted
		if retentionOffset, hasRetention := m.computeRetentionOffset(ctx, &queueConfig); hasRetention {
			if retentionOffset < truncateOffset {
				truncateOffset = retentionOffset
			}
		}

		// Truncate log up to the safe offset
		if err := m.queueStore.Truncate(ctx, queueConfig.Name, truncateOffset); err != nil {
			m.logger.Debug("truncation error",
				slog.String("error", err.Error()),
				slog.String("queue", queueConfig.Name))
		}
	}
}

// --- Ephemeral Queue Lifecycle ---

// checkEphemeralDisconnect checks if an ephemeral queue has zero consumers and marks the disconnect time.
func (m *Manager) checkEphemeralDisconnect(ctx context.Context, queueName string) {
	config, err := m.queueStore.GetQueue(ctx, queueName)
	if err != nil || config.Durable || config.Reserved {
		return
	}

	if m.queueHasConsumers(ctx, queueName) {
		return
	}

	config.LastConsumerDisconnect = time.Now()
	if err := m.queueStore.UpdateQueue(ctx, *config); err != nil {
		m.logger.Warn("failed to update ephemeral queue disconnect time",
			slog.String("queue", queueName),
			slog.String("error", err.Error()))
	}
}

// clearEphemeralDisconnect clears the disconnect timestamp on an ephemeral queue.
func (m *Manager) clearEphemeralDisconnect(ctx context.Context, queueName string) {
	config, err := m.queueStore.GetQueue(ctx, queueName)
	if err != nil || config.Durable {
		return
	}

	if config.LastConsumerDisconnect.IsZero() {
		return
	}

	config.LastConsumerDisconnect = time.Time{}
	if err := m.queueStore.UpdateQueue(ctx, *config); err != nil {
		m.logger.Warn("failed to clear ephemeral queue disconnect time",
			slog.String("queue", queueName),
			slog.String("error", err.Error()))
	}
}

// queueHasConsumers returns true if any consumer group for the queue has active consumers.
func (m *Manager) queueHasConsumers(ctx context.Context, queueName string) bool {
	groups, err := m.groupStore.ListConsumerGroups(ctx, queueName)
	if err != nil {
		return false
	}

	for _, group := range groups {
		if group.ConsumerCount() > 0 {
			return true
		}
	}
	return false
}

// cleanupEphemeralQueues deletes expired ephemeral queues.
func (m *Manager) cleanupEphemeralQueues() {
	ctx := context.Background()

	queues, err := m.queueStore.ListQueues(ctx)
	if err != nil {
		return
	}

	for _, q := range queues {
		if q.Durable || q.Reserved {
			continue
		}

		if q.LastConsumerDisconnect.IsZero() {
			continue
		}

		if time.Since(q.LastConsumerDisconnect) < q.ExpiresAfter {
			continue
		}

		// Delete consumer groups first
		groups, err := m.groupStore.ListConsumerGroups(ctx, q.Name)
		if err == nil {
			for _, g := range groups {
				m.groupStore.DeleteConsumerGroup(ctx, q.Name, g.ID)
			}
		}

		if err := m.queueStore.DeleteQueue(ctx, q.Name); err != nil {
			m.logger.Warn("failed to delete expired ephemeral queue",
				slog.String("queue", q.Name),
				slog.String("error", err.Error()))
			continue
		}

		m.logger.Info("deleted expired ephemeral queue",
			slog.String("queue", q.Name),
			slog.Duration("expired_after", q.ExpiresAfter))
	}
}

func (m *Manager) runEphemeralCleanupLoop() {
	defer m.wg.Done()

	ticker := time.NewTicker(m.config.ConsumerTimeout)
	defer ticker.Stop()

	for {
		select {
		case <-m.stopCh:
			return
		case <-ticker.C:
			m.cleanupEphemeralQueues()
		}
	}
}

// --- Metrics ---

// GetMetrics returns the current metrics snapshot.
func (m *Manager) GetMetrics() consumer.Metrics {
	return m.metrics.Snapshot()
}

// GetLag returns the lag for a consumer group.
func (m *Manager) GetLag(ctx context.Context, queueName, groupID string) (uint64, error) {
	return m.consumerManager.GetLag(ctx, queueName, groupID)
}

// CommitOffset explicitly commits an offset for a stream consumer group.
// Use when AutoCommit is disabled for manual commit control.
func (m *Manager) CommitOffset(ctx context.Context, queueName, groupID string, offset uint64) error {
	return m.consumerManager.CommitOffset(ctx, queueName, groupID, offset)
}

// --- Cluster QueueHandler Implementation ---

// EnqueueLocal implements cluster.QueueHandler.EnqueueLocal.
func (m *Manager) EnqueueLocal(ctx context.Context, topic string, payload []byte, properties map[string]string) (string, error) {
	err := m.Publish(ctx, types.PublishRequest{
		Topic:      topic,
		Payload:    payload,
		Properties: properties,
	})
	if err != nil {
		return "", err
	}

	if properties != nil && properties[types.PropMessageID] != "" {
		return properties[types.PropMessageID], nil
	}

	return generateMessageID(), nil
}

// DeliverQueueMessage implements cluster.QueueHandler.DeliverQueueMessage.
func (m *Manager) DeliverQueueMessage(ctx context.Context, clientID string, msg *cluster.QueueMessage) error {
	if m.deliveryTarget == nil {
		return fmt.Errorf("no delivery function configured")
	}

	if msg == nil {
		return fmt.Errorf("queue message is nil")
	}

	queueName := msg.QueueName
	props := make(map[string]string, len(msg.UserProperties)+8)
	for k, v := range msg.UserProperties {
		props[k] = v
	}

	messageID := msg.MessageID
	if messageID == "" {
		messageID = fmt.Sprintf("%s:%d", queueName, msg.Sequence)
	}

	props[types.PropMessageID] = messageID
	props[types.PropGroupID] = msg.GroupID
	props[types.PropQueueName] = queueName
	props[types.PropOffset] = fmt.Sprintf("%d", msg.Sequence)

	if msg.Stream {
		props[types.PropStreamOffset] = fmt.Sprintf("%d", msg.StreamOffset)
		if msg.StreamTimestamp != 0 {
			props[types.PropStreamTimestamp] = fmt.Sprintf("%d", msg.StreamTimestamp)
		}
	}

	if msg.HasWorkCommitted {
		props[types.PropWorkCommittedOffset] = fmt.Sprintf("%d", msg.WorkCommittedOffset)
		props[types.PropWorkAcked] = strconv.FormatBool(msg.WorkAcked)
		if msg.WorkGroup != "" {
			props[types.PropWorkGroup] = msg.WorkGroup
		}
	}

	topic := queueName
	if topic != "" && !strings.HasPrefix(topic, "$queue/") {
		topic = "$queue/" + topic
	}

	deliveryMsg := &brokerstorage.Message{
		Topic:      topic,
		QoS:        1,
		Properties: props,
	}
	deliveryMsg.SetPayloadFromBytes(msg.Payload)

	return m.deliveryTarget.Deliver(ctx, clientID, deliveryMsg)
}
