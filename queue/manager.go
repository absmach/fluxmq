// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package queue

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/absmach/fluxmq/cluster"
	"github.com/absmach/fluxmq/queue/consumer"
	"github.com/absmach/fluxmq/queue/raft"
	"github.com/absmach/fluxmq/queue/storage"
	"github.com/absmach/fluxmq/queue/types"
	brokerstorage "github.com/absmach/fluxmq/storage"
)

// DeliverFn is the callback for delivering messages to MQTT clients.
type DeliverFn func(ctx context.Context, clientID string, msg any) error

// Manager is the queue-based queue manager.
// It uses append-only logs with cursor-based consumer groups, NATS JetQueue-style.
type Manager struct {
	queueStore      storage.QueueStore
	groupStore      storage.ConsumerGroupStore
	consumerManager *consumer.Manager
	deliverFn       DeliverFn
	logger          *slog.Logger
	config          Config

	// Raft replication manager
	raftManager *raft.Manager

	// Cluster support for cross-node message routing
	cluster     cluster.Cluster
	localNodeID string

	// Active consumer groups: queueName -> groupID -> group state
	groups sync.Map // map[string]*sync.Map

	// Subscription patterns: clientID -> []pattern
	subscriptions sync.Map // map[string][]string

	mu       sync.RWMutex
	stopCh   chan struct{}
	stopOnce sync.Once
	wg       sync.WaitGroup

	// Metrics
	metrics *consumer.Metrics
}

// Config holds configuration for the queue-based queue manager.
type Config struct {
	// Consumer configuration
	VisibilityTimeout time.Duration
	MaxDeliveryCount  int
	ClaimBatchSize    int

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

	// Retention configuration
	RetentionCheckInterval time.Duration
}

// DefaultConfig returns default configuration.
func DefaultConfig() Config {
	return Config{
		VisibilityTimeout:      30 * time.Second,
		MaxDeliveryCount:       5,
		ClaimBatchSize:         10,
		DeliveryInterval:       10 * time.Millisecond,
		DeliveryBatchSize:      100,
		HeartbeatInterval:      10 * time.Second,
		ConsumerTimeout:        30 * time.Second,
		DLQTopicPrefix:         "$dlq/",
		StealInterval:          5 * time.Second,
		StealEnabled:           true,
		RetentionCheckInterval: 5 * time.Minute,
	}
}

// NewManager creates a new queue-based queue manager.
// The cluster parameter is optional (nil for single-node mode).
func NewManager(queueStore storage.QueueStore, groupStore storage.ConsumerGroupStore, deliverFn DeliverFn, config Config, logger *slog.Logger, cl cluster.Cluster) *Manager {
	if logger == nil {
		logger = slog.Default()
	}

	metrics := consumer.NewMetrics()

	consumerCfg := consumer.Config{
		VisibilityTimeout: config.VisibilityTimeout,
		MaxDeliveryCount:  config.MaxDeliveryCount,
		ClaimBatchSize:    config.ClaimBatchSize,
		StealBatchSize:    5,
	}

	consumerMgr := consumer.NewManager(queueStore, groupStore, consumerCfg)

	var localNodeID string
	if cl != nil {
		localNodeID = cl.NodeID()
	}

	return &Manager{
		queueStore:      queueStore,
		groupStore:      groupStore,
		consumerManager: consumerMgr,
		deliverFn:       deliverFn,
		logger:          logger,
		config:          config,
		cluster:         cl,
		localNodeID:     localNodeID,
		stopCh:          make(chan struct{}),
		metrics:         metrics,
	}
}

// Start starts background workers.
func (m *Manager) Start(ctx context.Context) error {
	// Ensure reserved queues exist
	if err := m.ensureReservedQueues(ctx); err != nil {
		return fmt.Errorf("failed to create reserved queues: %w", err)
	}

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

	m.logger.Info("queue-based queue manager started")
	return nil
}

// ensureReservedQueues creates the reserved mqtt queue if it doesn't exist.
func (m *Manager) ensureReservedQueues(ctx context.Context) error {
	// Create the reserved mqtt queue that captures all MQTT messages
	mqttConfig := types.MQTTQueueConfig()

	if err := m.queueStore.CreateQueue(ctx, mqttConfig); err != nil {
		if err != storage.ErrQueueAlreadyExists {
			return err
		}
	}

	m.logger.Info("reserved mqtt queue ready",
		slog.String("queue", types.MQTTQueueName),
		slog.String("topic", types.MQTTQueueTopic))

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

	m.logger.Info("queue created",
		slog.String("queue", config.Name),
		slog.Any("topics", config.Topics))

	return nil
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

	// Create with default config
	defaultConfig := types.DefaultQueueConfig(queueName, topics...)
	if err := m.CreateQueue(ctx, defaultConfig); err != nil {
		if err != storage.ErrQueueAlreadyExists {
			return nil, err
		}
	}

	return m.queueStore.GetQueue(ctx, queueName)
}

// DeleteQueue deletes a queue.
func (m *Manager) DeleteQueue(ctx context.Context, queueName string) error {
	return m.queueStore.DeleteQueue(ctx, queueName)
}

// --- Publish Operations ---

// Publish adds a message to all queues whose topic patterns match the topic.
// This is the NATS JetQueue-style "multi-queue" routing.
func (m *Manager) Publish(ctx context.Context, topic string, payload []byte, properties map[string]string) error {
	// Find all matching queues
	queues, err := m.queueStore.FindMatchingQueues(ctx, topic)
	if err != nil {
		return fmt.Errorf("failed to find matching queues: %w", err)
	}

	if len(queues) == 0 {
		m.logger.Debug("no queues match topic", slog.String("topic", topic))
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
			Payload:    payload,
			Topic:      topic,
			Properties: properties,
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
				slog.String("topic", topic),
				slog.String("error", err.Error()))
			continue
		}

		m.logger.Debug("message published",
			slog.String("queue", queueName),
			slog.String("topic", topic),
			slog.Uint64("offset", offset))
	}

	return nil
}

// Enqueue is an alias for Publish for backward compatibility.
func (m *Manager) Enqueue(ctx context.Context, topic string, payload []byte, properties map[string]string) error {
	return m.Publish(ctx, topic, payload, properties)
}

// --- Subscribe Operations ---

// Subscribe adds a consumer to a stream with optional pattern matching.
func (m *Manager) Subscribe(ctx context.Context, queueName, pattern string, clientID, groupID, proxyNodeID string) error {
	// Ensure queue exists (auto-create if not)
	_, err := m.GetOrCreateQueue(ctx, queueName)
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

	// Get or create consumer group
	group, err := m.consumerManager.GetOrCreateGroup(ctx, queueName, patternGroupID, pattern)
	if err != nil {
		return err
	}

	// Register consumer
	if err := m.consumerManager.RegisterConsumer(ctx, queueName, group.ID, clientID, clientID, proxyNodeID); err != nil {
		return err
	}

	// Track subscription
	m.trackSubscription(clientID, fmt.Sprintf("%s/%s", queueName, pattern))

	m.logger.Info("consumer subscribed",
		slog.String("queue", queueName),
		slog.String("group", patternGroupID),
		slog.String("client", clientID),
		slog.String("pattern", pattern))

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

	// Unregister consumer
	if err := m.consumerManager.UnregisterConsumer(ctx, queueName, patternGroupID, clientID); err != nil {
		m.logger.Debug("unregister consumer error",
			slog.String("error", err.Error()),
			slog.String("client", clientID))
	}

	// Untrack subscription
	m.untrackSubscription(clientID, fmt.Sprintf("%s/%s", queueName, pattern))

	m.logger.Info("consumer unsubscribed",
		slog.String("queue", queueName),
		slog.String("group", patternGroupID),
		slog.String("client", clientID))

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

		// Find and ack the message
		for consumerID := range group.PEL {
			err := m.consumerManager.Ack(ctx, queueName, group.ID, consumerID, offset)
			if err == nil {
				m.metrics.RecordAck(0)
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

	groups, err := m.groupStore.ListConsumerGroups(ctx, queueName)
	if err != nil {
		return err
	}

	for _, group := range groups {
		if groupID != "" && group.ID != groupID {
			continue
		}

		for consumerID := range group.PEL {
			err := m.consumerManager.Nack(ctx, queueName, group.ID, consumerID, offset)
			if err == nil {
				m.metrics.RecordNack()
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

	groups, err := m.groupStore.ListConsumerGroups(ctx, queueName)
	if err != nil {
		return err
	}

	for _, group := range groups {
		if groupID != "" && group.ID != groupID {
			continue
		}

		for consumerID := range group.PEL {
			err := m.consumerManager.Reject(ctx, queueName, group.ID, consumerID, offset, reason)
			if err == nil {
				m.metrics.RecordReject()
				return nil
			}
		}
	}

	return consumer.ErrMessageNotPending
}

// --- Heartbeat ---

// UpdateHeartbeat updates the heartbeat for a consumer.
func (m *Manager) UpdateHeartbeat(ctx context.Context, clientID string) error {
	m.subscriptions.Range(func(key, value any) bool {
		if key.(string) != clientID {
			return true
		}

		patterns := value.([]string)
		for _, filter := range patterns {
			queueName, pattern := parseSubscriptionFilter(filter)
			if queueName == "" {
				continue
			}

			groupID := extractGroupFromClientID(clientID)
			if pattern != "" {
				groupID = fmt.Sprintf("%s@%s", groupID, pattern)
			}

			m.consumerManager.UpdateHeartbeat(ctx, queueName, groupID, clientID)
		}

		return true
	})
	return nil
}

// --- Background Workers ---

func (m *Manager) runDeliveryLoop() {
	defer m.wg.Done()

	ticker := time.NewTicker(m.config.DeliveryInterval)
	defer ticker.Stop()

	for {
		select {
		case <-m.stopCh:
			return
		case <-ticker.C:
			m.deliverMessages()
		}
	}
}

func (m *Manager) deliverMessages() {
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
			m.deliverToGroup(ctx, &queueConfig, group)
		}
	}
}

func (m *Manager) deliverToGroup(ctx context.Context, config *types.QueueConfig, group *types.ConsumerGroupState) {
	if group.ConsumerCount() == 0 {
		return
	}

	// Create filter from group pattern
	var filter *consumer.Filter
	if group.Pattern != "" {
		filter = consumer.NewFilter(group.Pattern)
	}

	// Round-robin delivery across consumers
	consumers := group.ConsumerIDs()
	if len(consumers) == 0 {
		return
	}

	for _, consumerID := range consumers {
		msgs, err := m.consumerManager.ClaimBatch(ctx, config.Name, group.ID, consumerID, filter, m.config.DeliveryBatchSize)
		if err != nil {
			continue
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

		for _, msg := range msgs {
			// Check if consumer is on a remote node
			if m.cluster != nil && consumerInfo.ProxyNodeID != "" && consumerInfo.ProxyNodeID != m.localNodeID {
				// Route to remote node
				err := m.cluster.RouteQueueMessage(
					ctx,
					consumerInfo.ProxyNodeID,
					consumerInfo.ClientID,
					config.Name,
					msg.ID,
					msg.GetPayload(),
					msg.Properties,
					int64(msg.Sequence),
					0, // No partition in queue model
				)
				if err != nil {
					m.logger.Warn("queue message remote routing failed",
						slog.String("client", consumerInfo.ClientID),
						slog.String("node", consumerInfo.ProxyNodeID),
						slog.String("topic", msg.Topic),
						slog.String("error", err.Error()))
				}
			} else if m.deliverFn != nil {
				// Local delivery
				deliveryMsg := m.createDeliveryMessage(msg, group.ID, config.Name)

				if err := m.deliverFn(ctx, consumerInfo.ClientID, deliveryMsg); err != nil {
					m.logger.Warn("queue message delivery failed",
						slog.String("client", consumerInfo.ClientID),
						slog.String("topic", msg.Topic),
						slog.String("error", err.Error()))
				}
			}
		}
	}
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
		// Get minimum committed offset across all groups
		minCommitted, err := m.consumerManager.GetMinCommittedOffset(ctx, queueConfig.Name)
		if err != nil {
			continue
		}

		// Truncate log up to committed offset
		if err := m.queueStore.Truncate(ctx, queueConfig.Name, minCommitted); err != nil {
			m.logger.Debug("truncation error",
				slog.String("error", err.Error()),
				slog.String("queue", queueConfig.Name))
		}
	}
}

// --- Helper Functions ---

func (m *Manager) trackSubscription(clientID, filter string) {
	val, _ := m.subscriptions.LoadOrStore(clientID, []string{})
	patterns := val.([]string)
	patterns = append(patterns, filter)
	m.subscriptions.Store(clientID, patterns)
}

func (m *Manager) untrackSubscription(clientID, filter string) {
	val, ok := m.subscriptions.Load(clientID)
	if !ok {
		return
	}

	patterns := val.([]string)
	newPatterns := make([]string, 0, len(patterns))
	for _, p := range patterns {
		if p != filter {
			newPatterns = append(newPatterns, p)
		}
	}

	if len(newPatterns) == 0 {
		m.subscriptions.Delete(clientID)
	} else {
		m.subscriptions.Store(clientID, newPatterns)
	}
}

func (m *Manager) createDeliveryMessage(msg *types.Message, groupID string, queueName string) *brokerstorage.Message {
	messageID := fmt.Sprintf("%s:%d", queueName, msg.Sequence)

	// Create properties map with ack info
	props := make(map[string]string)
	if msg.Properties != nil {
		for k, v := range msg.Properties {
			props[k] = v
		}
	}
	props["message-id"] = messageID
	props["group-id"] = groupID
	props["queue"] = queueName
	props["offset"] = fmt.Sprintf("%d", msg.Sequence)

	deliveryMsg := &brokerstorage.Message{
		Topic:      msg.Topic,
		QoS:        1, // queue messages use QoS 1 by default
		Properties: props,
	}
	deliveryMsg.SetPayloadFromBytes(msg.GetPayload())

	return deliveryMsg
}

// DeliveryMessage is the internal message format for queue delivery tracking.
type DeliveryMessage struct {
	ID          string
	Payload     []byte
	Topic       string
	Properties  map[string]string
	GroupID     string
	queueName   string
	Offset      uint64
	DeliveredAt time.Time
	AckTopic    string
	NackTopic   string
	RejectTopic string
}

func extractGroupFromClientID(clientID string) string {
	for i, c := range clientID {
		if c == '-' {
			return clientID[:i]
		}
	}
	return clientID
}

func generateMessageID() string {
	return fmt.Sprintf("%d", time.Now().UnixNano())
}

func parseMessageID(messageID string) (uint64, error) {
	var offset uint64
	// Format: queueName:offset (we only need the offset)
	for i := len(messageID) - 1; i >= 0; i-- {
		if messageID[i] == ':' {
			_, err := fmt.Sscanf(messageID[i+1:], "%d", &offset)
			return offset, err
		}
	}
	// Try parsing as just an offset
	_, err := fmt.Sscanf(messageID, "%d", &offset)
	return offset, err
}

func parseSubscriptionFilter(filter string) (queueName, pattern string) {
	for i, c := range filter {
		if c == '/' {
			return filter[:i], filter[i+1:]
		}
	}
	return filter, ""
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

// --- Cluster QueueHandler Implementation ---

// EnqueueLocal implements cluster.QueueHandler.EnqueueLocal.
func (m *Manager) EnqueueLocal(ctx context.Context, topic string, payload []byte, properties map[string]string) (string, error) {
	err := m.Publish(ctx, topic, payload, properties)
	if err != nil {
		return "", err
	}

	if properties != nil && properties["message-id"] != "" {
		return properties["message-id"], nil
	}

	return generateMessageID(), nil
}

// DeliverQueueMessage implements cluster.QueueHandler.DeliverQueueMessage.
func (m *Manager) DeliverQueueMessage(ctx context.Context, clientID string, msg any) error {
	if m.deliverFn == nil {
		return fmt.Errorf("no delivery function configured")
	}

	msgMap, ok := msg.(map[string]any)
	if !ok {
		return fmt.Errorf("invalid message type: expected map[string]any")
	}

	queueName, _ := msgMap["queueName"].(string)
	payload, _ := msgMap["payload"].([]byte)
	properties, _ := msgMap["properties"].(map[string]string)
	messageID, _ := msgMap["id"].(string)
	sequence, _ := msgMap["sequence"].(int64)

	if properties == nil {
		properties = make(map[string]string)
	}
	properties["message-id"] = messageID
	properties["queue"] = queueName
	properties["offset"] = fmt.Sprintf("%d", sequence)

	deliveryMsg := &brokerstorage.Message{
		Topic:      queueName,
		QoS:        1,
		Properties: properties,
	}
	deliveryMsg.SetPayloadFromBytes(payload)

	return m.deliverFn(ctx, clientID, deliveryMsg)
}
