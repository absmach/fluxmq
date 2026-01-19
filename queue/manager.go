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
	"github.com/absmach/fluxmq/queue/storage"
	"github.com/absmach/fluxmq/queue/types"
	brokerstorage "github.com/absmach/fluxmq/storage"
)

// DeliverFn is the callback for delivering messages to MQTT clients.
type DeliverFn func(ctx context.Context, clientID string, msg any) error

// Manager is the log-based queue manager.
// It uses append-only logs with cursor-based consumer groups.
type Manager struct {
	logStore        storage.LogStore
	groupStore      storage.ConsumerGroupStore
	consumerManager *consumer.Manager
	dlqHandler      *consumer.DLQHandler
	deliverFn       DeliverFn
	logger          *slog.Logger
	config          Config

	// Cluster support for cross-node message routing
	cluster     cluster.Cluster
	localNodeID string

	// Active consumer groups: queueName -> groupID -> group state
	groups sync.Map // map[string]*sync.Map

	// Active delivery workers: queueName -> partitionID -> worker
	workers sync.Map // map[string]*sync.Map

	// Subscription patterns: clientID -> []pattern
	subscriptions sync.Map // map[string][]string

	mu       sync.RWMutex
	stopCh   chan struct{}
	stopOnce sync.Once
	wg       sync.WaitGroup

	// Metrics
	metrics *consumer.Metrics
}

// Config holds configuration for the log-based queue manager.
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

// NewManager creates a new log-based queue manager.
// The cluster parameter is optional (nil for single-node mode).
func NewManager(logStore storage.LogStore, groupStore storage.ConsumerGroupStore, deliverFn DeliverFn, config Config, logger *slog.Logger, cl cluster.Cluster) *Manager {
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

	consumerMgr := consumer.NewManager(logStore, groupStore, consumerCfg)

	dlqCfg := consumer.DLQConfig{
		MaxDeliveryCount: config.MaxDeliveryCount,
		DLQTopicPrefix:   config.DLQTopicPrefix,
		IncludeMetadata:  true,
	}

	dlqHandler := consumer.NewDLQHandler(logStore, groupStore, dlqCfg, metrics)

	var localNodeID string
	if cl != nil {
		localNodeID = cl.NodeID()
	}

	return &Manager{
		logStore:        logStore,
		groupStore:      groupStore,
		consumerManager: consumerMgr,
		dlqHandler:      dlqHandler,
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
	// Start delivery workers for each queue
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

	m.logger.Info("log-based queue manager started")
	return nil
}

// Stop stops the manager and all workers.
func (m *Manager) Stop() error {
	m.stopOnce.Do(func() {
		close(m.stopCh)
	})

	m.wg.Wait()
	m.logger.Info("log-based queue manager stopped")
	return nil
}

// --- Queue Operations ---

// CreateQueue creates a new queue.
func (m *Manager) CreateQueue(ctx context.Context, config types.QueueConfig) error {
	if err := m.logStore.CreateQueue(ctx, config); err != nil {
		return err
	}

	m.logger.Info("queue created",
		slog.String("queue", config.Name),
		slog.Int("partitions", config.Partitions))

	return nil
}

// GetOrCreateQueue gets or creates a queue with default configuration.
func (m *Manager) GetOrCreateQueue(ctx context.Context, queueTopic string) (*types.QueueConfig, error) {
	// Extract queue root from topic
	queueRoot := types.ExtractQueueRoot(queueTopic)
	if queueRoot == "" {
		queueRoot = queueTopic
	}

	// Try to get existing
	config, err := m.logStore.GetQueue(ctx, queueRoot)
	if err == nil {
		return config, nil
	}

	if err != storage.ErrQueueNotFound {
		return nil, err
	}

	// Create with default config
	defaultConfig := types.DefaultQueueConfig(queueRoot)
	if err := m.CreateQueue(ctx, defaultConfig); err != nil {
		if err != storage.ErrQueueAlreadyExists {
			return nil, err
		}
	}

	return m.logStore.GetQueue(ctx, queueRoot)
}

// DeleteQueue deletes a queue.
func (m *Manager) DeleteQueue(ctx context.Context, queueName string) error {
	return m.logStore.DeleteQueue(ctx, queueName)
}

// --- Publish Operations ---

// Enqueue adds a message to a queue.
func (m *Manager) Enqueue(ctx context.Context, topic string, payload []byte, properties map[string]string) error {
	// Extract queue root and routing key
	queueRoot := types.ExtractQueueRoot(topic)
	if queueRoot == "" {
		return fmt.Errorf("invalid queue topic: %s", topic)
	}

	routingKey := types.ExtractRoutingKey(topic, queueRoot)

	// Get or create queue
	config, err := m.GetOrCreateQueue(ctx, queueRoot)
	if err != nil {
		return err
	}

	// Determine partition
	partitionKey := ""
	if properties != nil {
		partitionKey = properties["partition-key"]
	}
	if partitionKey == "" {
		partitionKey = routingKey
	}

	partitionID := m.getPartitionID(partitionKey, config.Partitions)

	// Create message
	msg := &types.Message{
		ID:           generateMessageID(),
		Payload:      payload,
		Topic:        topic,
		PartitionKey: partitionKey,
		PartitionID:  partitionID,
		Properties:   properties,
		State:        types.StateQueued,
		CreatedAt:    time.Now(),
		ExpiresAt:    time.Now().Add(config.MessageTTL),
	}

	// Set routing key in properties for filtering
	if msg.Properties == nil {
		msg.Properties = make(map[string]string)
	}
	msg.Properties["routing-key"] = routingKey

	// Append to log
	offset, err := m.logStore.Append(ctx, queueRoot, partitionID, msg)
	if err != nil {
		return err
	}

	m.logger.Debug("message enqueued",
		slog.String("queue", queueRoot),
		slog.Int("partition", partitionID),
		slog.Uint64("offset", offset))

	return nil
}

// --- Subscribe Operations ---

// Subscribe adds a consumer to a queue with optional pattern matching.
func (m *Manager) Subscribe(ctx context.Context, filter string, clientID, groupID, proxyNodeID string) error {
	// Parse queue subscription
	queueRoot, filterPattern := consumer.ParseQueueSubscription(filter)
	if queueRoot == "" {
		return fmt.Errorf("invalid queue subscription: %s", filter)
	}

	// Ensure queue exists
	config, err := m.GetOrCreateQueue(ctx, queueRoot)
	if err != nil {
		return err
	}

	// Default group ID to client prefix
	if groupID == "" {
		groupID = extractGroupFromClientID(clientID)
	}

	// Create unique group ID that includes the pattern
	patternGroupID := groupID
	if filterPattern != "" {
		patternGroupID = fmt.Sprintf("%s@%s", groupID, filterPattern)
	}

	// Get or create consumer group
	group, err := m.consumerManager.GetOrCreateGroup(ctx, queueRoot, patternGroupID, filterPattern)
	if err != nil {
		return err
	}

	// Register consumer
	if err := m.consumerManager.RegisterConsumer(ctx, queueRoot, group.ID, clientID, clientID, proxyNodeID); err != nil {
		return err
	}

	// Track subscription
	m.trackSubscription(clientID, filter)

	// Start delivery worker if needed
	m.ensureDeliveryWorker(config, group)

	m.logger.Info("consumer subscribed",
		slog.String("queue", queueRoot),
		slog.String("group", patternGroupID),
		slog.String("client", clientID),
		slog.String("pattern", filterPattern))

	return nil
}

// Unsubscribe removes a consumer from a queue.
func (m *Manager) Unsubscribe(ctx context.Context, filter string, clientID, groupID string) error {
	queueRoot, filterPattern := consumer.ParseQueueSubscription(filter)
	if queueRoot == "" {
		return nil
	}

	if groupID == "" {
		groupID = extractGroupFromClientID(clientID)
	}

	patternGroupID := groupID
	if filterPattern != "" {
		patternGroupID = fmt.Sprintf("%s@%s", groupID, filterPattern)
	}

	// Unregister consumer
	if err := m.consumerManager.UnregisterConsumer(ctx, queueRoot, patternGroupID, clientID); err != nil {
		m.logger.Debug("unregister consumer error",
			slog.String("error", err.Error()),
			slog.String("client", clientID))
	}

	// Untrack subscription
	m.untrackSubscription(clientID, filter)

	m.logger.Info("consumer unsubscribed",
		slog.String("queue", queueRoot),
		slog.String("group", patternGroupID),
		slog.String("client", clientID))

	return nil
}

// --- Ack Operations ---

// Ack acknowledges a message.
func (m *Manager) Ack(ctx context.Context, queueTopic, messageID, groupID string) error {
	queueRoot := types.ExtractQueueRoot(queueTopic)
	if queueRoot == "" {
		return fmt.Errorf("invalid queue topic: %s", queueTopic)
	}

	// Parse message ID to get partition and offset
	partitionID, offset, err := parseMessageID(messageID)
	if err != nil {
		return err
	}

	// Find the consumer that has this message pending
	groups, err := m.groupStore.ListConsumerGroups(ctx, queueRoot)
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
			err := m.consumerManager.Ack(ctx, queueRoot, group.ID, consumerID, partitionID, offset)
			if err == nil {
				m.metrics.RecordAck(0)
				return nil
			}
		}
	}

	return consumer.ErrMessageNotPending
}

// Nack negatively acknowledges a message.
func (m *Manager) Nack(ctx context.Context, queueTopic, messageID, groupID string) error {
	queueRoot := types.ExtractQueueRoot(queueTopic)
	if queueRoot == "" {
		return fmt.Errorf("invalid queue topic: %s", queueTopic)
	}

	partitionID, offset, err := parseMessageID(messageID)
	if err != nil {
		return err
	}

	groups, err := m.groupStore.ListConsumerGroups(ctx, queueRoot)
	if err != nil {
		return err
	}

	for _, group := range groups {
		if groupID != "" && group.ID != groupID {
			continue
		}

		for consumerID := range group.PEL {
			err := m.consumerManager.Nack(ctx, queueRoot, group.ID, consumerID, partitionID, offset)
			if err == nil {
				m.metrics.RecordNack()
				return nil
			}
		}
	}

	return consumer.ErrMessageNotPending
}

// Reject rejects a message and moves it to DLQ.
func (m *Manager) Reject(ctx context.Context, queueTopic, messageID, groupID, reason string) error {
	queueRoot := types.ExtractQueueRoot(queueTopic)
	if queueRoot == "" {
		return fmt.Errorf("invalid queue topic: %s", queueTopic)
	}

	partitionID, offset, err := parseMessageID(messageID)
	if err != nil {
		return err
	}

	groups, err := m.groupStore.ListConsumerGroups(ctx, queueRoot)
	if err != nil {
		return err
	}

	for _, group := range groups {
		if groupID != "" && group.ID != groupID {
			continue
		}

		for consumerID := range group.PEL {
			err := m.consumerManager.Reject(ctx, queueRoot, group.ID, consumerID, partitionID, offset, reason)
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
	// Find all groups this client belongs to
	m.subscriptions.Range(func(key, value interface{}) bool {
		if key.(string) != clientID {
			return true
		}

		patterns := value.([]string)
		for _, filter := range patterns {
			queueRoot, filterPattern := consumer.ParseQueueSubscription(filter)
			if queueRoot == "" {
				continue
			}

			groupID := extractGroupFromClientID(clientID)
			if filterPattern != "" {
				groupID = fmt.Sprintf("%s@%s", groupID, filterPattern)
			}

			m.consumerManager.UpdateHeartbeat(ctx, queueRoot, groupID, clientID)
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

	queues, err := m.logStore.ListQueues(ctx)
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
	if len(group.Consumers) == 0 {
		return
	}

	// Create filter from group pattern
	var filter *consumer.Filter
	if group.Pattern != "" {
		filter = consumer.NewFilter(group.Pattern)
	}

	// Round-robin delivery across consumers and partitions
	consumers := make([]string, 0, len(group.Consumers))
	for id := range group.Consumers {
		consumers = append(consumers, id)
	}

	if len(consumers) == 0 {
		return
	}

	consumerIdx := 0

	for partitionID := 0; partitionID < config.Partitions; partitionID++ {
		// Try to claim messages for each consumer
		for i := 0; i < len(consumers); i++ {
			consumerID := consumers[consumerIdx]
			consumerIdx = (consumerIdx + 1) % len(consumers)

			msgs, err := m.consumerManager.ClaimBatch(ctx, config.Name, group.ID, consumerID, partitionID, filter, m.config.DeliveryBatchSize)
			if err != nil {
				// ErrNoMessages is normal - no need to log
				continue
			}

			// Fetch fresh group state to get current consumer info
			freshGroup, err := m.groupStore.GetConsumerGroup(ctx, config.Name, group.ID)
			if err != nil {
				m.logger.Debug("failed to get fresh group",
					slog.String("queue", config.Name),
					slog.String("group", group.ID),
					slog.String("error", err.Error()))
				continue
			}

			consumerInfo := freshGroup.Consumers[consumerID]
			if consumerInfo == nil {
				m.logger.Debug("consumer not found in group",
					slog.String("consumer", consumerID),
					slog.String("group", group.ID))
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
						partitionID,
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
					deliveryMsg := m.createDeliveryMessage(msg, group.ID, partitionID)

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
			m.processWorkStealing()
		}
	}
}

func (m *Manager) processWorkStealing() {
	ctx := context.Background()

	queues, err := m.logStore.ListQueues(ctx)
	if err != nil {
		return
	}

	for _, queueConfig := range queues {
		groups, err := m.groupStore.ListConsumerGroups(ctx, queueConfig.Name)
		if err != nil {
			continue
		}

		for _, group := range groups {
			// Process DLQ for expired entries
			moved, err := m.dlqHandler.ProcessExpiredEntries(ctx, queueConfig.Name, group.ID)
			if err != nil {
				m.logger.Debug("DLQ processing error",
					slog.String("error", err.Error()))
			} else if moved > 0 {
				m.logger.Debug("moved messages to DLQ",
					slog.Int("count", moved),
					slog.String("queue", queueConfig.Name))
			}
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

	queues, err := m.logStore.ListQueues(ctx)
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

	queues, err := m.logStore.ListQueues(ctx)
	if err != nil {
		return
	}

	for _, queueConfig := range queues {
		for partitionID := 0; partitionID < queueConfig.Partitions; partitionID++ {
			// Get minimum committed offset across all groups
			minCommitted, err := m.consumerManager.GetMinCommittedOffset(ctx, queueConfig.Name, partitionID)
			if err != nil {
				continue
			}

			// Truncate log up to committed offset
			if err := m.logStore.Truncate(ctx, queueConfig.Name, partitionID, minCommitted); err != nil {
				m.logger.Debug("truncation error",
					slog.String("error", err.Error()),
					slog.String("queue", queueConfig.Name),
					slog.Int("partition", partitionID))
			}
		}
	}
}

// --- Helper Functions ---

func (m *Manager) getPartitionID(key string, partitions int) int {
	if partitions <= 0 {
		return 0
	}
	if key == "" {
		return 0
	}

	// FNV-1a hash
	var hash uint32 = 2166136261
	for i := 0; i < len(key); i++ {
		hash ^= uint32(key[i])
		hash *= 16777619
	}

	return int(hash % uint32(partitions))
}

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

func (m *Manager) ensureDeliveryWorker(config *types.QueueConfig, group *types.ConsumerGroupState) {
	// Workers are managed by the delivery loop
}

func (m *Manager) createDeliveryMessage(msg *types.Message, groupID string, partitionID int) *brokerstorage.Message {
	messageID := fmt.Sprintf("%d:%d", partitionID, msg.Sequence)

	// Create properties map with ack info
	props := make(map[string]string)
	if msg.Properties != nil {
		for k, v := range msg.Properties {
			props[k] = v
		}
	}
	props["message-id"] = messageID
	props["group-id"] = groupID
	props["partition-id"] = fmt.Sprintf("%d", partitionID)
	props["offset"] = fmt.Sprintf("%d", msg.Sequence)

	deliveryMsg := &brokerstorage.Message{
		Topic:      msg.Topic,
		QoS:        1, // Queue messages use QoS 1 by default
		Properties: props,
	}
	deliveryMsg.SetPayloadFromBytes(msg.GetPayload())

	return deliveryMsg
}

// DeliveryMessage is the internal message format for queue delivery tracking.
// Note: For actual MQTT delivery, we convert to *brokerstorage.Message.
type DeliveryMessage struct {
	ID          string
	Payload     []byte
	Topic       string
	Properties  map[string]string
	GroupID     string
	PartitionID int
	Offset      uint64
	DeliveredAt time.Time
	AckTopic    string
	NackTopic   string
	RejectTopic string
}

func extractGroupFromClientID(clientID string) string {
	// Extract prefix before first dash
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

func parseMessageID(messageID string) (partitionID int, offset uint64, err error) {
	_, err = fmt.Sscanf(messageID, "%d:%d", &partitionID, &offset)
	return
}

// --- Metrics ---

// GetMetrics returns the current metrics snapshot.
func (m *Manager) GetMetrics() consumer.Metrics {
	return m.metrics.Snapshot()
}

// GetLag returns the lag for a consumer group.
func (m *Manager) GetLag(ctx context.Context, queueName, groupID string, partitionID int) (uint64, error) {
	return m.consumerManager.GetLag(ctx, queueName, groupID, partitionID)
}

// GetDLQCount returns the number of messages in the DLQ.
func (m *Manager) GetDLQCount(ctx context.Context, queueName string) (uint64, error) {
	return m.dlqHandler.GetDLQCount(ctx, queueName)
}

// --- Cluster QueueHandler Implementation ---

// EnqueueLocal implements cluster.QueueHandler.EnqueueLocal.
// This is called by the gRPC handler when a remote node sends an enqueue request.
func (m *Manager) EnqueueLocal(ctx context.Context, queueName string, payload []byte, properties map[string]string) (string, error) {
	// Use the existing Enqueue method which handles all the logic
	err := m.Enqueue(ctx, queueName, payload, properties)
	if err != nil {
		return "", err
	}

	// Return message ID from properties if available
	if properties != nil && properties["message-id"] != "" {
		return properties["message-id"], nil
	}

	return generateMessageID(), nil
}

// DeliverQueueMessage implements cluster.QueueHandler.DeliverQueueMessage.
// This is called by the gRPC handler when a remote partition owner delivers a message.
func (m *Manager) DeliverQueueMessage(ctx context.Context, clientID string, msg any) error {
	if m.deliverFn == nil {
		return fmt.Errorf("no delivery function configured")
	}

	// The msg is a map[string]interface{} from the transport layer
	// Convert it to a proper delivery message
	msgMap, ok := msg.(map[string]interface{})
	if !ok {
		return fmt.Errorf("invalid message type: expected map[string]interface{}")
	}

	// Extract fields from the map
	queueName, _ := msgMap["queueName"].(string)
	payload, _ := msgMap["payload"].([]byte)
	properties, _ := msgMap["properties"].(map[string]string)
	messageID, _ := msgMap["id"].(string)
	sequence, _ := msgMap["sequence"].(int64)
	partitionID, _ := msgMap["partitionId"].(int32)

	// Build properties if nil
	if properties == nil {
		properties = make(map[string]string)
	}
	properties["message-id"] = messageID
	properties["partition-id"] = fmt.Sprintf("%d", partitionID)
	properties["offset"] = fmt.Sprintf("%d", sequence)

	// Create delivery message
	deliveryMsg := &brokerstorage.Message{
		Topic:      queueName,
		QoS:        1,
		Properties: properties,
	}
	deliveryMsg.SetPayloadFromBytes(payload)

	return m.deliverFn(ctx, clientID, deliveryMsg)
}
