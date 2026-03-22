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

type queueCluster interface {
	cluster.QueueConsumerDirectory
	cluster.QueueForwarder
}

type queueRaftCoordinator interface {
	raft.CoordinatorLifecycle
	raft.ReplicationInfo
	raft.QueueMapping
	raft.QueueLogReplicator
}

// Manager is the queue-based queue manager.
// It uses append-only logs with cursor-based consumer groups, NATS JetQueue-style.
type Manager struct {
	queueStore       storage.QueueStore
	groupStore       storage.ConsumerGroupStore
	raftGroupStore   *raftGroupStore
	consumerManager  *consumer.Manager
	deliveryTarget   Deliverer
	logger           *slog.Logger
	config           Config
	writePolicy      WritePolicy
	distributionMode DistributionMode

	// Raft replication coordinator (queue -> raft group routing).
	raftCoordinator queueRaftCoordinator
	// Group-state replicator for forwarded group operations.
	groupReplicator raft.GroupStateReplicator

	// Legacy access to the underlying single-group raft manager.
	// Kept for compatibility with existing call sites/tests.
	raftManager *raft.Manager

	// Cluster support for cross-node message routing
	cluster     queueCluster
	localNodeID string

	// Lightweight heartbeat index keyed by client/queue/group.
	// Stores only metadata needed to route heartbeat updates.
	subscriptionsMu sync.RWMutex
	subscriptions   map[string]map[string]*subscriptionRef // clientID -> refKey -> ref

	stopCh   chan struct{}
	stopOnce sync.Once
	wg       sync.WaitGroup

	delivery *DeliveryEngine

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

	// mgr is populated below; the DLQ closure captures it by pointer so that
	// the consumer.Manager can call back into the queue.Manager for DLQ publishing.
	var mgr *Manager

	dlqPrefix := config.DLQTopicPrefix
	if dlqPrefix == "" {
		dlqPrefix = "$dlq/"
	}

	consumerCfg := consumer.Config{
		VisibilityTimeout:  config.VisibilityTimeout,
		MaxDeliveryCount:   config.MaxDeliveryCount,
		ClaimBatchSize:     config.ClaimBatchSize,
		StealBatchSize:     5,
		AutoCommitInterval: config.AutoCommitInterval,
		MaxPELSize:         config.MaxPELSize,
		OnDLQ: func(ctx context.Context, queueName, groupID string, msg *types.Message, deliveryCount int) {
			if mgr == nil {
				return
			}
			mgr.moveToDLQ(ctx, queueName, groupID, msg, deliveryCount, dlqPrefix)
		},
	}

	raftGroupStore := newRaftGroupStore(groupStore)
	if logger != nil {
		raftGroupStore.SetLogger(logger)
	}
	if cl != nil {
		raftGroupStore.SetForwarder(cl)
	}
	consumerMgr := consumer.NewManager(queueStore, raftGroupStore, consumerCfg)

	var localNodeID string
	if cl != nil {
		localNodeID = cl.NodeID()
	}

	distMode := normalizeDistributionMode(config.DistributionMode)

	var remote RemoteRouter
	if cl != nil {
		remote = cl
	}

	engine := NewDeliveryEngine(
		queueStore, raftGroupStore, consumerMgr,
		dt,
		remote,
		localNodeID,
		distMode,
		config.DeliveryBatchSize,
		logger,
	)

	mgr = &Manager{
		queueStore:      queueStore,
		groupStore:      raftGroupStore,
		raftGroupStore:  raftGroupStore,
		consumerManager: consumerMgr,
		deliveryTarget:  dt,

		logger:           logger,
		config:           config,
		writePolicy:      normalizeWritePolicy(config.WritePolicy),
		distributionMode: distMode,
		cluster:          cl,
		localNodeID:      localNodeID,
		subscriptions:    make(map[string]map[string]*subscriptionRef),
		stopCh:           make(chan struct{}),
		delivery:         engine,
		metrics:          metrics,
	}

	return mgr
}

// Start starts background workers.
func (m *Manager) Start(ctx context.Context) error {
	if m.distributionMode == DistributionReplicate && (m.raftCoordinator == nil || !m.raftCoordinator.IsEnabled()) {
		m.logger.Warn("distribution_mode=replicate requires raft to be enabled; falling back to forward")
		m.distributionMode = DistributionForward
	}

	if err := m.syncQueueReplicationAssignments(ctx); err != nil {
		return fmt.Errorf("failed to sync queue replication assignments: %w", err)
	}

	// Ensure reserved queues exist
	if err := m.ensureReservedQueues(ctx); err != nil {
		return fmt.Errorf("failed to create reserved queues: %w", err)
	}

	// Cleanup ephemeral queues that expired while broker was down
	m.cleanupEphemeralQueues() //nolint:contextcheck // context propagation would require API changes across the call chain

	// Prime delivery for existing queues at startup.
	m.delivery.ScheduleAll(ctx)

	// Start delivery engine
	m.delivery.Start() //nolint:contextcheck // context propagation would require API changes across the call chain

	// Start work stealing if enabled
	if m.config.StealEnabled {
		m.wg.Add(1)
		go m.runStealLoop() //nolint:contextcheck // goroutine manages its own context lifecycle
	}

	// Start consumer cleanup
	m.wg.Add(1)
	go m.runCleanupLoop() //nolint:contextcheck // goroutine manages its own context lifecycle

	// Start retention
	m.wg.Add(1)
	go m.runRetentionLoop() //nolint:contextcheck // goroutine manages its own context lifecycle

	// Start ephemeral queue cleanup
	m.wg.Add(1)
	go m.runEphemeralCleanupLoop() //nolint:contextcheck // goroutine manages its own context lifecycle

	m.logger.Info("queue-based queue manager started")
	return nil
}

func (m *Manager) syncQueueReplicationAssignments(ctx context.Context) error {
	if m.raftCoordinator == nil {
		return nil
	}

	queues, err := m.queueStore.ListQueues(ctx)
	if err != nil {
		return err
	}

	for _, queueCfg := range queues {
		if err := m.raftCoordinator.EnsureQueue(ctx, queueCfg); err != nil {
			return err
		}
	}

	return nil
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
		if m.raftCoordinator != nil {
			if err := m.raftCoordinator.EnsureQueue(ctx, cfg); err != nil {
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
	m.delivery.Stop()

	m.stopOnce.Do(func() {
		close(m.stopCh)
	})

	m.wg.Wait()

	// Stop Raft manager if enabled
	if m.raftCoordinator != nil {
		if err := m.raftCoordinator.Stop(); err != nil {
			m.logger.Error("failed to stop raft manager", slog.String("error", err.Error()))
		}
	}

	m.logger.Info("queue-based queue manager stopped")
	return nil
}

// SetRaftManager sets the Raft replication manager.
func (m *Manager) SetRaftManager(rm *raft.Manager) {
	coordinator := raft.NewLogicalGroupCoordinator(rm, m.logger)
	m.raftCoordinator = coordinator
	m.groupReplicator = coordinator
	if m.raftGroupStore != nil {
		m.raftGroupStore.SetCoordinator(coordinator)
	}
	m.raftManager = rm
}

// SetRaftCoordinator sets queue-aware Raft coordinator.
func (m *Manager) SetRaftCoordinator(rc raft.QueueCoordinator) {
	m.raftCoordinator = rc
	m.groupReplicator = rc
	if m.raftGroupStore != nil {
		m.raftGroupStore.SetCoordinator(rc)
	}
}

// GetRaftManager returns the Raft replication manager.
func (m *Manager) GetRaftManager() *raft.Manager {
	return m.raftManager
}

// QueueStore returns the queue store used by the manager.
func (m *Manager) QueueStore() storage.QueueStore {
	return m.queueStore
}

// GroupStore returns the consumer group store used by the manager.
func (m *Manager) GroupStore() storage.ConsumerGroupStore {
	return m.groupStore
}

// --- Queue Operations ---

// CreateQueue creates a new queue.
func (m *Manager) CreateQueue(ctx context.Context, config types.QueueConfig) error {
	if config.Replication.Enabled && m.raftCoordinator != nil && m.raftCoordinator.IsEnabled() {
		if err := m.raftCoordinator.EnsureQueue(ctx, config); err != nil {
			return err
		}
		if err := m.raftCoordinator.ApplyCreateQueue(ctx, config); err != nil {
			return err
		}
		// Ensure local immediate visibility even with async apply/mocks.
		if err := m.queueStore.CreateQueue(ctx, config); err != nil && err != storage.ErrQueueAlreadyExists {
			return err
		}
	} else {
		if err := m.queueStore.CreateQueue(ctx, config); err != nil {
			return err
		}
		if m.raftCoordinator != nil {
			if err := m.raftCoordinator.EnsureQueue(ctx, config); err != nil {
				return err
			}
		}
	}
	m.delivery.Schedule(config.Name)

	m.logger.Info("queue created",
		slog.String("queue", config.Name),
		slog.Any("topics", config.Topics))

	return nil
}

// UpdateQueue updates an existing queue.
func (m *Manager) UpdateQueue(ctx context.Context, config types.QueueConfig) error {
	current, err := m.queueStore.GetQueue(ctx, config.Name)
	if err != nil {
		return err
	}

	replicatedNow := current.Replication.Enabled
	replicatedNext := config.Replication.Enabled

	shouldReplicate := (replicatedNow || replicatedNext) && m.raftCoordinator != nil && m.raftCoordinator.IsEnabled()
	if shouldReplicate {
		if err := m.raftCoordinator.ApplyUpdateQueue(ctx, config); err != nil {
			return err
		}
		// Keep local view in sync immediately.
		if err := m.queueStore.UpdateQueue(ctx, config); err != nil && err != storage.ErrQueueNotFound {
			return err
		}
	} else {
		if err := m.queueStore.UpdateQueue(ctx, config); err != nil {
			return err
		}
	}

	// Always sync the coordinator's queue→group mapping. UpdateQueue on the
	// coordinator captures the previous group before overwriting, so it can
	// release dynamic groups that are no longer referenced by any queue
	// (e.g. group A→B migration, or replication being disabled).
	if m.raftCoordinator != nil {
		if err := m.raftCoordinator.UpdateQueue(ctx, config); err != nil {
			return err
		}
	}

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
	queueCfg, err := m.queueStore.GetQueue(ctx, queueName)
	if err != nil {
		return err
	}

	if queueCfg.Replication.Enabled && m.raftCoordinator != nil && m.raftCoordinator.IsEnabled() {
		if err := m.raftCoordinator.ApplyDeleteQueue(ctx, queueName); err != nil {
			return err
		}
		// Ensure local deletion even with async apply/mocks.
		if err := m.queueStore.DeleteQueue(ctx, queueName); err != nil && err != storage.ErrQueueNotFound {
			return err
		}
	} else {
		if err := m.queueStore.DeleteQueue(ctx, queueName); err != nil {
			return err
		}
	}

	if m.raftCoordinator != nil {
		if err := m.raftCoordinator.DeleteQueue(ctx, queueName); err != nil {
			return err
		}
	}
	m.delivery.Unschedule(queueName)
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
	targets, err := m.resolvePublishTargets(ctx, publish)
	if err != nil {
		return err
	}
	if len(targets) == 0 {
		m.logger.Debug("no queues match topic", slog.String("topic", publish.Topic))
		return nil
	}

	allReplicated := true
	localTargets := make([]queuePublishTarget, 0, len(targets))
	forwardTargets := make(map[string][]string)
	for _, target := range targets {
		replicated := target.config != nil && target.config.Replication.Enabled
		allReplicated = allReplicated && replicated

		if !replicated || m.raftCoordinator == nil || !m.raftCoordinator.IsEnabled() {
			localTargets = append(localTargets, target)
			continue
		}

		if m.raftCoordinator.IsLeaderForQueue(target.name) {
			localTargets = append(localTargets, target)
			continue
		}

		switch m.writePolicy {
		case WritePolicyReject:
			leaderAddr := m.raftCoordinator.LeaderForQueue(target.name)
			if leaderAddr == "" {
				return fmt.Errorf("raft leader unavailable")
			}
			return fmt.Errorf("raft leader is at %s", leaderAddr)
		case WritePolicyForward:
			leaderID := m.raftCoordinator.LeaderIDForQueue(target.name)
			if leaderID == "" {
				return fmt.Errorf("raft leader unavailable")
			}
			forwardTargets[leaderID] = append(forwardTargets[leaderID], target.name)
		case WritePolicyLocal:
			localTargets = append(localTargets, target)
		default:
			// Unknown policy - default to local append for backward compatibility.
			localTargets = append(localTargets, target)
		}
	}

	// Store locally in queues handled by this node.
	if err := m.publishLocalToTargets(ctx, publish, localTargets); err != nil {
		return err
	}

	// Forward leader-owned queue targets to appropriate remote leaders.
	for leaderID, targetQueues := range forwardTargets {
		if err := m.forwardPublishToLeader(ctx, publish, leaderID, targetQueues); err != nil {
			return err
		}
	}

	// Forward to remote nodes that have consumers
	if m.cluster != nil {
		unknownOnly := allReplicated
		if m.distributionMode == DistributionReplicate {
			// Legacy explicit replicate mode remains stronger than per-queue inference.
			unknownOnly = true
		}
		m.forwardToRemoteNodes(ctx, publish, unknownOnly)
	}

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
	targets, err := m.resolvePublishTargets(ctx, publish)
	if err != nil {
		return err
	}
	return m.publishLocalToTargets(ctx, publish, targets)
}

type queuePublishTarget struct {
	name   string
	config *types.QueueConfig
}

func (m *Manager) resolvePublishTargets(ctx context.Context, publish types.PublishRequest) ([]queuePublishTarget, error) {
	forcedTargets := parseForwardTargetQueues(publish.Properties)
	if len(forcedTargets) > 0 {
		targets := make([]queuePublishTarget, 0, len(forcedTargets))
		for _, queueName := range forcedTargets {
			queueConfig, err := m.queueStore.GetQueue(ctx, queueName)
			if err != nil {
				m.logger.Warn("failed to resolve forced queue target",
					slog.String("queue", queueName),
					slog.String("error", err.Error()))
				continue
			}
			targets = append(targets, queuePublishTarget{
				name:   queueName,
				config: queueConfig,
			})
		}
		return targets, nil
	}

	// Find all matching queues
	queues, err := m.queueStore.FindMatchingQueues(ctx, publish.Topic)
	if err != nil {
		return nil, fmt.Errorf("failed to find matching queues: %w", err)
	}

	if len(queues) == 0 {
		m.logger.Debug("no queues match topic, creating new queue", slog.String("topic", publish.Topic))
		queueName, queuePattern := autoQueueFromTopic(publish.Topic)
		if _, err := m.GetOrCreateQueue(ctx, queueName, queuePattern); err != nil {
			m.logger.Error("failed to create ephemeral queue", slog.String("topic", publish.Topic), slog.String("error", err.Error()))
			return nil, err
		}
		// After creating, find it again.
		queues, err = m.queueStore.FindMatchingQueues(ctx, publish.Topic)
		if err != nil {
			return nil, fmt.Errorf("failed to find matching queues after creation: %w", err)
		}
	}

	targets := make([]queuePublishTarget, 0, len(queues))
	for _, queueName := range queues {
		queueConfig, err := m.queueStore.GetQueue(ctx, queueName)
		if err != nil {
			m.logger.Warn("failed to get queue config", slog.String("queue", queueName), slog.String("error", err.Error()))
			continue
		}
		targets = append(targets, queuePublishTarget{
			name:   queueName,
			config: queueConfig,
		})
	}

	return targets, nil
}

func (m *Manager) publishLocalToTargets(ctx context.Context, publish types.PublishRequest, targets []queuePublishTarget) error {
	cleanProps := cloneWithoutForwardingMeta(publish.Properties)

	for _, target := range targets {
		queueName := target.name
		queueConfig := target.config
		if queueConfig == nil {
			continue
		}

		// Create message for this queue
		now := time.Now()
		msg := &types.Message{
			ID:         generateMessageID(),
			Payload:    publish.Payload,
			Topic:      publish.Topic,
			Properties: cleanProps,
			State:      types.StateQueued,
			CreatedAt:  now,
		}
		if queueConfig.MessageTTL > 0 {
			msg.ExpiresAt = now.Add(queueConfig.MessageTTL)
		}

		var (
			offset uint64
			err    error
		)

		replicated := queueConfig.Replication.Enabled
		if replicated && m.raftCoordinator != nil && m.raftCoordinator.IsEnabled() {
			syncMode := queueConfig.Replication.Mode != types.ReplicationAsync
			offset, err = m.raftCoordinator.ApplyAppendWithOptions(ctx, queueName, msg, raft.ApplyOptions{
				SyncMode:   &syncMode,
				AckTimeout: queueConfig.Replication.AckTimeout,
			})
		} else {
			if replicated && (m.raftCoordinator == nil || !m.raftCoordinator.IsEnabled()) {
				m.logger.Warn("queue replication enabled but raft manager unavailable; appending locally",
					slog.String("queue", queueName))
			}
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

		m.delivery.Schedule(queueName)
	}

	return nil
}

// moveToDLQ publishes a poison message to the dead-letter queue.
// It auto-creates the DLQ queue if it doesn't exist.
func (m *Manager) moveToDLQ(ctx context.Context, queueName, groupID string, msg *types.Message, deliveryCount int, dlqPrefix string) {
	queueCfg, err := m.queueStore.GetQueue(ctx, queueName)
	if err != nil || queueCfg == nil || !queueCfg.DLQConfig.Enabled {
		return
	}

	dlqTopic := queueCfg.DLQConfig.Topic
	if dlqTopic == "" {
		dlqTopic = dlqPrefix + queueName
	}

	dlqQueueName := dlqTopic
	if _, err := m.queueStore.GetQueue(ctx, dlqQueueName); err != nil {
		dlqCfg := types.DefaultQueueConfig(dlqQueueName, dlqTopic+"/#")
		dlqCfg.DLQConfig.Enabled = false // prevent DLQ chains
		dlqCfg.MessageTTL = 0            // DLQ messages don't expire
		if createErr := m.queueStore.CreateQueue(ctx, dlqCfg); createErr != nil {
			m.logger.Warn("failed to auto-create DLQ queue",
				slog.String("dlq_queue", dlqQueueName),
				slog.String("error", createErr.Error()))
		}
	}

	props := make(map[string]string, len(msg.Properties)+6)
	for k, v := range msg.Properties {
		props[k] = v
	}
	props["_dlq_original_queue"] = queueName
	props["_dlq_original_topic"] = msg.Topic
	props["_dlq_group"] = groupID
	props["_dlq_delivery_count"] = strconv.Itoa(deliveryCount)
	props["_dlq_moved_at"] = time.Now().UTC().Format(time.RFC3339)
	if msg.ID != "" {
		props["_dlq_original_id"] = msg.ID
	}

	dlqMsg := &types.Message{
		ID:         generateMessageID(),
		Payload:    msg.GetPayload(),
		Topic:      dlqTopic,
		Properties: props,
		State:      types.StateDLQ,
		CreatedAt:  time.Now(),
	}

	if _, err := m.queueStore.Append(ctx, dlqQueueName, dlqMsg); err != nil {
		m.logger.Warn("failed to append message to DLQ",
			slog.String("queue", queueName),
			slog.String("dlq_queue", dlqQueueName),
			slog.String("message_id", msg.ID),
			slog.String("error", err.Error()))
		return
	}

	m.logger.Warn("message moved to DLQ",
		slog.String("queue", queueName),
		slog.String("group", groupID),
		slog.String("dlq_queue", dlqQueueName),
		slog.String("message_id", msg.ID),
		slog.Int("delivery_count", deliveryCount))
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

// forwardToRemoteNodes forwards a publish to nodes that have consumers for the topic.
func (m *Manager) forwardToRemoteNodes(ctx context.Context, publish types.PublishRequest, unknownOnly bool) {
	// Get all consumers from the cluster
	consumers, err := m.cluster.ListAllQueueConsumers(ctx)
	if err != nil {
		m.logger.Debug("failed to list cluster consumers for forwarding",
			slog.String("error", err.Error()))
		return
	}

	queueExistsCache := make(map[string]bool)
	queueExists := func(queueName string) bool {
		if exists, ok := queueExistsCache[queueName]; ok {
			return exists
		}

		_, err := m.queueStore.GetQueue(ctx, queueName)
		if err == nil {
			queueExistsCache[queueName] = true
			return true
		}
		if err != storage.ErrQueueNotFound {
			m.logger.Warn("failed to check queue existence for forwarding",
				slog.String("queue", queueName),
				slog.String("error", err.Error()))
		}

		queueExistsCache[queueName] = false
		return false
	}

	// Find unique remote nodes that have consumers for queues matching this topic
	remoteNodes := make(map[string]bool)
	for _, c := range consumers {
		// Skip local consumers
		if c.ProxyNodeID == m.localNodeID {
			continue
		}

		if unknownOnly && queueExists(c.QueueName) {
			continue
		}

		// Check if this consumer's queue pattern matches the topic
		queuePattern := "$queue/" + c.QueueName + "/#"
		if matchesTopic(queuePattern, publish.Topic) {
			remoteNodes[c.ProxyNodeID] = true
		}
	}

	// Forward to each unique remote node
	for nodeID := range remoteNodes {
		if err := m.cluster.ForwardQueuePublish(ctx, nodeID, publish.Topic, publish.Payload, publish.Properties, false); err != nil {
			m.logger.Warn("failed to forward publish to remote node",
				slog.String("node", nodeID),
				slog.String("topic", publish.Topic),
				slog.String("error", err.Error()))
		} else {
			m.logger.Debug("forwarded publish to remote node",
				slog.String("node", nodeID),
				slog.String("topic", publish.Topic))
		}
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
	if proxyNodeID == "" && m.localNodeID != "" {
		proxyNodeID = m.localNodeID
	}

	mode := types.GroupModeQueue
	if cursor != nil && cursor.Mode != "" {
		mode = cursor.Mode
	}
	if cursor == nil || cursor.Position == types.CursorDefault {
		if mode != types.GroupModeStream {
			return m.Subscribe(ctx, queueName, pattern, clientID, groupID, proxyNodeID)
		}
		cursor = &types.CursorOption{Position: types.CursorDefault, Mode: mode}
	}

	// Ensure queue exists
	queueTopicPattern := "$queue/" + queueName + "/#"
	queueCfg, err := m.GetOrCreateQueue(ctx, queueName, queueTopicPattern)
	if err != nil {
		return fmt.Errorf("failed to get or create queue: %w", err)
	}
	if mode == types.GroupModeStream && queueCfg != nil && queueCfg.Type != types.QueueTypeStream {
		queueCfg.Type = types.QueueTypeStream
		if err := m.UpdateQueue(ctx, *queueCfg); err != nil {
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
			m.groupStore.UpdateCursor(ctx, queueName, group.ID, head) //nolint:errcheck // cursor positioning; consumer will start from default offset on failure
		}
	case types.CursorLatest:
		tail, err := m.queueStore.Tail(ctx, queueName)
		if err == nil {
			m.groupStore.UpdateCursor(ctx, queueName, group.ID, tail) //nolint:errcheck // cursor positioning; consumer will start from default offset on failure
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
		m.groupStore.UpdateCursor(ctx, queueName, group.ID, offset) //nolint:errcheck // cursor positioning; consumer will start from default offset on failure
	case types.CursorTimestamp:
		if !cursor.Timestamp.IsZero() {
			if offset, err := m.offsetByTime(ctx, queueName, cursor.Timestamp); err == nil {
				m.groupStore.UpdateCursor(ctx, queueName, group.ID, offset) //nolint:errcheck // cursor positioning; consumer will start from default offset on failure
			}
		}
	}

	if err := m.consumerManager.RegisterConsumer(ctx, queueName, group.ID, clientID, clientID, proxyNodeID); err != nil {
		return err
	}

	// Clear ephemeral disconnect timestamp since we now have a consumer
	m.clearEphemeralDisconnect(ctx, queueName)

	if m.cluster != nil {
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
		if err := m.cluster.RegisterQueueConsumer(ctx, info); err != nil {
			m.logger.Warn("failed to register consumer in cluster",
				slog.String("error", err.Error()),
				slog.String("client", clientID))
		}
	}

	m.trackSubscription(clientID, queueName, patternGroupID)

	m.logger.Info("consumer subscribed with cursor",
		slog.String("queue", queueName),
		slog.String("group", patternGroupID),
		slog.String("client", clientID),
		slog.String("cursor", fmt.Sprintf("%d", cursor.Position)),
		slog.String("mode", string(mode)))

	m.delivery.Schedule(queueName)

	return nil
}

// Subscribe adds a consumer to a stream with optional pattern matching.
func (m *Manager) Subscribe(ctx context.Context, queueName, pattern string, clientID, groupID, proxyNodeID string) error {
	if proxyNodeID == "" && m.localNodeID != "" {
		proxyNodeID = m.localNodeID
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
	if m.cluster != nil {
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
		if err := m.cluster.RegisterQueueConsumer(ctx, info); err != nil {
			m.logger.Warn("failed to register consumer in cluster",
				slog.String("error", err.Error()),
				slog.String("client", clientID))
		}
	}

	// Track subscription
	m.trackSubscription(clientID, queueName, patternGroupID)

	m.logger.Info("consumer subscribed",
		slog.String("queue", queueName),
		slog.String("group", patternGroupID),
		slog.String("client", clientID),
		slog.String("pattern", pattern))

	m.delivery.Schedule(queueName)

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
	if m.cluster != nil {
		if err := m.cluster.UnregisterQueueConsumer(ctx, queueName, patternGroupID, clientID); err != nil {
			m.logger.Warn("failed to unregister consumer from cluster",
				slog.String("error", err.Error()),
				slog.String("client", clientID))
		}
	}

	// Untrack subscription
	m.untrackSubscription(clientID, queueName, patternGroupID)

	// Track last consumer disconnect for ephemeral queues
	m.checkEphemeralDisconnect(ctx, queueName)

	m.logger.Info("consumer unsubscribed",
		slog.String("queue", queueName),
		slog.String("group", patternGroupID),
		slog.String("client", clientID))

	m.delivery.Schedule(queueName)

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
				m.handleStreamAck(ctx, queueName, group, offset)
				m.delivery.Schedule(queueName)
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
			m.handleStreamAck(ctx, queueName, group, offset)
			m.delivery.Schedule(queueName)
			return nil
		}

		// Find and ack the message
		for consumerID := range group.PEL {
			err := m.consumerManager.Ack(ctx, queueName, group.ID, consumerID, offset)
			if err == nil {
				m.metrics.RecordAck(0)
				m.metrics.UpdatePELSize(uint64(group.PendingCount()))
				m.delivery.Schedule(queueName)
				return nil
			}
		}
	}

	return consumer.ErrMessageNotPending
}

func (m *Manager) handleStreamAck(ctx context.Context, queueName string, group *types.ConsumerGroup, offset uint64) {
	if !group.AutoCommit {
		return
	}

	cursor := group.GetCursor()
	next := offset + 1
	if next <= cursor.Committed {
		return
	}

	if err := m.groupStore.UpdateCommitted(ctx, queueName, group.ID, next); err != nil {
		m.logger.Warn("failed to update stream committed offset",
			slog.String("queue", queueName),
			slog.String("group", group.ID),
			slog.String("error", err.Error()))
	}
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
				m.delivery.Schedule(queueName)
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
			m.delivery.Schedule(queueName)
			return nil
		}

		for consumerID := range group.PEL {
			err := m.consumerManager.Nack(ctx, queueName, group.ID, consumerID, offset)
			if err == nil {
				m.metrics.RecordNack()
				m.delivery.Schedule(queueName)
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
				m.delivery.Schedule(queueName)
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
	m.delivery.Schedule(queueName)
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

// UpdateConsumerHeartbeat updates heartbeat for a specific consumer membership.
func (m *Manager) UpdateConsumerHeartbeat(ctx context.Context, queueName, groupID, consumerID string) error {
	if err := m.consumerManager.UpdateHeartbeat(ctx, queueName, groupID, consumerID); err != nil {
		return err
	}

	m.touchSubscription(consumerID, m.subscriptionRefKey(queueName, groupID), time.Now())
	return nil
}

// --- Background Workers ---

// deliverMessages is a thin forwarding method for test/bench compatibility.
func (m *Manager) deliverMessages() {
	m.delivery.DeliverAll(context.Background())
}

// deliverQueue is a thin forwarding method for test/bench compatibility.
func (m *Manager) deliverQueue(ctx context.Context, queueName string) bool {
	return m.delivery.DeliverQueue(ctx, queueName)
}

func (m *Manager) forwardPublishToLeader(ctx context.Context, publish types.PublishRequest, leaderID string, targetQueues []string) error {
	if m.cluster == nil {
		return fmt.Errorf("cluster not configured for leader forward")
	}

	if m.raftCoordinator == nil {
		return fmt.Errorf("raft coordinator unavailable")
	}

	if leaderID == "" {
		return fmt.Errorf("raft leader unavailable")
	}

	props := cloneWithoutForwardingMeta(publish.Properties)
	if len(targetQueues) > 0 {
		// Need a writable map — cloneWithoutForwardingMeta may have returned
		// the original when no forwarding key was present.
		writable := make(map[string]string, len(props)+1)
		for k, v := range props {
			writable[k] = v
		}
		writable[types.PropForwardTargetQueues] = strings.Join(targetQueues, ",")
		props = writable
	}

	return m.cluster.ForwardQueuePublish(ctx, leaderID, publish.Topic, publish.Payload, props, true)
}

func parseForwardTargetQueues(properties map[string]string) []string {
	if len(properties) == 0 {
		return nil
	}
	raw := strings.TrimSpace(properties[types.PropForwardTargetQueues])
	if raw == "" {
		return nil
	}

	seen := make(map[string]struct{})
	out := make([]string, 0, 4)
	for _, token := range strings.Split(raw, ",") {
		queueName := strings.TrimSpace(token)
		if queueName == "" {
			continue
		}
		if _, ok := seen[queueName]; ok {
			continue
		}
		seen[queueName] = struct{}{}
		out = append(out, queueName)
	}

	return out
}

func cloneWithoutForwardingMeta(properties map[string]string) map[string]string {
	if len(properties) == 0 {
		return nil
	}
	if _, has := properties[types.PropForwardTargetQueues]; !has {
		return properties
	}
	out := make(map[string]string, len(properties)-1)
	for k, v := range properties {
		if k == types.PropForwardTargetQueues {
			continue
		}
		out[k] = v
	}
	return out
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

		// Truncate log up to the safe offset.
		var truncateErr error
		if queueConfig.Replication.Enabled && m.raftCoordinator != nil && m.raftCoordinator.IsEnabled() {
			truncateErr = m.raftCoordinator.ApplyTruncate(ctx, queueConfig.Name, truncateOffset)
		} else {
			truncateErr = m.queueStore.Truncate(ctx, queueConfig.Name, truncateOffset)
		}
		if truncateErr != nil {
			m.logger.Debug("truncation error",
				slog.String("error", truncateErr.Error()),
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
	if err := m.UpdateQueue(ctx, *config); err != nil {
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
	if err := m.UpdateQueue(ctx, *config); err != nil {
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
				m.groupStore.DeleteConsumerGroup(ctx, q.Name, g.ID) //nolint:errcheck // best-effort cleanup before queue deletion
			}
		}

		if err := m.DeleteQueue(ctx, q.Name); err != nil {
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
		messageID = queueName + ":" + strconv.FormatInt(msg.Sequence, 10)
	}

	props[types.PropMessageID] = messageID
	props[types.PropGroupID] = msg.GroupID
	props[types.PropQueueName] = queueName
	props[types.PropOffset] = strconv.FormatInt(msg.Sequence, 10)

	if msg.Stream {
		props[types.PropStreamOffset] = strconv.FormatInt(msg.StreamOffset, 10)
		if msg.StreamTimestamp != 0 {
			props[types.PropStreamTimestamp] = strconv.FormatInt(msg.StreamTimestamp, 10)
		}
	}

	if msg.HasWorkCommitted {
		props[types.PropWorkCommittedOffset] = strconv.FormatInt(msg.WorkCommittedOffset, 10)
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

// HandleForwardedGroupOp implements cluster.QueueHandler.HandleForwardedGroupOp.
// It decodes a raft.Operation and applies it through the local coordinator.
func (m *Manager) HandleForwardedGroupOp(ctx context.Context, queueName string, opData []byte) error {
	if m.groupReplicator == nil {
		return fmt.Errorf("raft coordinator not available")
	}

	var op raft.Operation
	if err := raft.DecodeOperation(opData, &op); err != nil {
		return fmt.Errorf("failed to decode forwarded group op: %w", err)
	}

	if op.QueueName != queueName {
		return fmt.Errorf("queue name mismatch: request=%q op=%q", queueName, op.QueueName)
	}

	return m.applyGroupOp(ctx, &op)
}

func (m *Manager) applyGroupOp(ctx context.Context, op *raft.Operation) error {
	switch op.Type {
	case raft.OpCreateGroup:
		return m.groupReplicator.ApplyCreateGroup(ctx, op.QueueName, op.GroupState)
	case raft.OpUpdateGroup:
		return m.groupReplicator.ApplyUpdateGroup(ctx, op.QueueName, op.GroupState)
	case raft.OpDeleteGroup:
		return m.groupReplicator.ApplyDeleteGroup(ctx, op.QueueName, op.GroupID)
	case raft.OpUpdateCursor:
		return m.groupReplicator.ApplyUpdateCursor(ctx, op.QueueName, op.GroupID, op.Cursor)
	case raft.OpUpdateCommitted:
		return m.groupReplicator.ApplyUpdateCommitted(ctx, op.QueueName, op.GroupID, op.Committed)
	case raft.OpAddPending:
		return m.groupReplicator.ApplyAddPending(ctx, op.QueueName, op.GroupID, op.PendingEntry)
	case raft.OpRemovePending:
		return m.groupReplicator.ApplyRemovePending(ctx, op.QueueName, op.GroupID, op.ConsumerID, op.Offset)
	case raft.OpTransferPending:
		return m.groupReplicator.ApplyTransferPending(ctx, op.QueueName, op.GroupID, op.Offset, op.FromConsumer, op.ToConsumer)
	case raft.OpRegisterConsumer:
		return m.groupReplicator.ApplyRegisterConsumer(ctx, op.QueueName, op.GroupID, op.ConsumerInfo)
	case raft.OpUnregisterConsumer:
		return m.groupReplicator.ApplyUnregisterConsumer(ctx, op.QueueName, op.GroupID, op.ConsumerID)
	default:
		return fmt.Errorf("unsupported forwarded group op type: %d", op.Type)
	}
}
