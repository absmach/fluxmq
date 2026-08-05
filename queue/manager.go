// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package queue

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	corebroker "github.com/absmach/fluxmq/broker"
	"github.com/absmach/fluxmq/cluster"
	"github.com/absmach/fluxmq/queue/consumer"
	"github.com/absmach/fluxmq/queue/raft"
	"github.com/absmach/fluxmq/queue/storage"
	"github.com/absmach/fluxmq/queue/types"
	brokerstorage "github.com/absmach/fluxmq/storage"
	"github.com/absmach/fluxmq/topics"
)

var (
	// ErrQueueNotStream is returned when an exact stream publish targets a
	// queue that exists but is not configured as a stream.
	ErrQueueNotStream = errors.New("queue is not a stream")
	// ErrQueueNotDurable is returned when an exact durable publish targets an
	// ephemeral queue.
	ErrQueueNotDurable = errors.New("queue is not durable")
	// ErrQueueNotReserved is returned when an exact internal publish targets a
	// queue that is no longer protected as a statically reserved queue.
	ErrQueueNotReserved = errors.New("queue is not reserved")
	// ErrQueueMessageTooLarge is returned before append when an exact stream
	// publish exceeds the target queue's configured maximum message size.
	ErrQueueMessageTooLarge = errors.New("message exceeds queue maximum size")
	// ErrQueueNotProtected is returned when the exact durable stream publish
	// path is used for a queue without a registered immutable contract.
	ErrQueueNotProtected = errors.New("queue has no protected contract")
	// ErrCaptureStillRunning is returned by Stop when a capture worker is still
	// inside the queue store after the drain timeout. The manager's resources —
	// the queue store especially — must not be released while that is true.
	ErrCaptureStillRunning = errors.New("capture workers did not finish; queue resources left open")
	// ErrProtectedQueueMutation is returned when a create, update, or delete
	// would violate a registered queue contract.
	ErrProtectedQueueMutation = errors.New("protected queue mutation rejected")
	// ErrProtectedQueueContractDrift is returned when a protected queue's
	// persisted configuration no longer matches its registered contract.
	ErrProtectedQueueContractDrift = errors.New("protected queue contract drift")
	// ErrDurableSyncUnsupported is returned before append when the configured
	// queue store cannot establish a per-queue durability barrier.
	ErrDurableSyncUnsupported = errors.New("queue store does not support durable sync")
	// ErrDurableReplicatedStreamUnsupported prevents a false durability ACK in
	// clustered mode until the same barrier is carried through leader forwarding.
	ErrDurableReplicatedStreamUnsupported = errors.New("durable exact stream publish does not support replication")
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

	// protectedQueueContracts contains only queues that back exact internal
	// publishers. Its lock spans both contract checks and queue mutations so a
	// runtime contract replacement cannot race an administrative mutation.
	protectedQueuesMu       sync.RWMutex
	protectedQueueContracts map[string]types.QueueConfig
	protectedQueueConfigErr error

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

	// capture writes topic captures off the publish path.
	capture *captureDispatcher

	// shutdownComplete records that Stop finished with no capture worker left
	// inside the queue store, which is what makes the manager's resources safe
	// to release. See ShutdownComplete.
	shutdownComplete atomic.Bool

	// Metrics
	metrics *consumer.Metrics
}

var _ corebroker.ExistingQueueSubscriber = (*Manager)(nil)

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

	// Capture dispatcher configuration. Topic capture runs off the publish
	// path so a stalled queue store cannot delay subscribers; these bound how
	// much unwritten capture is held and how long shutdown waits for it.
	CaptureWorkers      int
	CaptureQueueDepth   int
	CaptureDrainTimeout time.Duration

	// Replication/distribution configuration
	WritePolicy      WritePolicy
	DistributionMode DistributionMode

	// Queue configurations from main config
	QueueConfigs []types.QueueConfig

	// ProtectedQueueContracts are immutable runtime contracts for queues used
	// by exact internal publishers. Only explicitly listed queues are protected;
	// Reserved alone does not make a queue immutable.
	ProtectedQueueContracts []types.QueueConfig

	// OnConsumerRemoved is called when stale consumers are removed during
	// heartbeat cleanup. The callback receives the queue name, group ID,
	// and the list of removed consumer IDs (prefixed client IDs).
	// Must be non-blocking.
	OnConsumerRemoved func(queueName, groupID string, consumerIDs []string)
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
		CaptureWorkers:         defaultCaptureWorkers,
		CaptureQueueDepth:      defaultCaptureQueueDepth,
		CaptureDrainTimeout:    defaultCaptureDrainTimeout,
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
	protectedQueueContracts, protectedQueueConfigErr := buildProtectedQueueContracts(config.ProtectedQueueContracts)

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
	engine.setConsumerRemovedCallback(func(ctx context.Context, queueName, groupID string, consumerIDs []string) {
		if mgr != nil {
			mgr.handleConsumersRemoved(ctx, queueName, groupID, consumerIDs)
		}
	})

	mgr = &Manager{
		queueStore:      queueStore,
		groupStore:      raftGroupStore,
		raftGroupStore:  raftGroupStore,
		consumerManager: consumerMgr,
		deliveryTarget:  dt,

		logger:                  logger,
		config:                  config,
		writePolicy:             normalizeWritePolicy(config.WritePolicy),
		distributionMode:        distMode,
		protectedQueueContracts: protectedQueueContracts,
		protectedQueueConfigErr: protectedQueueConfigErr,
		cluster:                 cl,
		localNodeID:             localNodeID,
		subscriptions:           make(map[string]map[string]*subscriptionRef),
		stopCh:                  make(chan struct{}),
		delivery:                engine,
		metrics:                 metrics,
	}

	mgr.capture = newCaptureDispatcher(
		config.CaptureWorkers,
		config.CaptureQueueDepth,
		config.CaptureDrainTimeout,
		metrics,
		logger,
		mgr.applyCaptureJob,
	)

	return mgr
}

// applyCaptureJob performs one dispatched capture off the publish path. A job
// with no target is the per-publish cluster forward.
func (m *Manager) applyCaptureJob(ctx context.Context, job captureJob) {
	if job.target == nil {
		if m.cluster != nil {
			m.forwardToRemoteNodes(ctx, job.publish)
		}
		return
	}

	// Capture is best effort: one target failing must not affect the others,
	// and they are separate jobs precisely so it cannot.
	if err := m.writeToTargets(ctx, job.publish, []queuePublishTarget{*job.target}, fanoutBestEffort); err != nil {
		m.metrics.RecordCaptureFailure()
		m.logger.Warn("capture append failed",
			slog.String("queue", job.target.name),
			slog.String("topic", job.publish.Topic),
			slog.String("error", err.Error()))
	}
}

// Start starts background workers.
func (m *Manager) Start(ctx context.Context) error {
	if m.distributionMode == DistributionReplicate && (m.raftCoordinator == nil || !m.raftCoordinator.IsEnabled()) {
		m.logger.Warn("distribution_mode=replicate requires raft to be enabled; falling back to forward")
		m.distributionMode = DistributionForward
		m.delivery.distributionMode = DistributionForward
	}

	if err := m.syncQueueReplicationAssignments(ctx); err != nil {
		return fmt.Errorf("failed to sync queue replication assignments: %w", err)
	}

	// Ensure reserved queues exist
	if err := m.ensureReservedQueues(ctx); err != nil {
		return fmt.Errorf("failed to create reserved queues: %w", err)
	}
	if err := m.ValidateProtectedQueueContracts(ctx); err != nil {
		return fmt.Errorf("invalid protected queue contract: %w", err)
	}

	// Cleanup ephemeral queues that expired while broker was down
	m.cleanupEphemeralQueues(ctx)

	// Prime delivery for existing queues at startup.
	m.delivery.ScheduleAll(ctx)

	// Start delivery engine
	m.delivery.Start(ctx)

	// Start capture workers. Uses a context detached from the caller's so a
	// cancelled start context cannot silently stop capture in a running broker;
	// Stop is what ends them.
	m.capture.Start(context.WithoutCancel(ctx))

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
// Stop shuts the manager down and reports whether that completed cleanly.
//
// Capture drains first. Its workers append through the queue store, schedule
// delivery, and on replicated queues call the Raft coordinator, so those have to
// outlive the drain rather than the other way round.
//
// A returned ErrCaptureStillRunning means a capture worker is still inside the
// queue store and could not be interrupted. Everything it touches — the store
// above all — must then be left alone: closing it underneath an in-flight append
// is a use-after-close, and leaking the handle into process exit is the cheaper
// outcome.
func (m *Manager) Stop() error {
	// Bounded: a stalled store delays shutdown by at most the drain timeout.
	quiesced := m.capture.Stop()

	m.delivery.Stop()

	m.stopOnce.Do(func() {
		close(m.stopCh)
	})

	m.wg.Wait()

	if !quiesced {
		// Deliberately skip the Raft coordinator too: a worker still running may
		// be mid-append on a replicated queue and would then be using it.
		m.logger.Error("queue manager stopped with capture still running; its resources were left open",
			slog.String("reason", "a queue store did not return within the capture drain timeout"))
		return ErrCaptureStillRunning
	}

	// Stop Raft manager if enabled
	if m.raftCoordinator != nil {
		if err := m.raftCoordinator.Stop(); err != nil {
			m.logger.Error("failed to stop raft manager", slog.String("error", err.Error()))
		}
	}

	m.shutdownComplete.Store(true)
	m.logger.Info("queue-based queue manager stopped")
	return nil
}

// ShutdownComplete reports whether Stop finished with every capture worker out
// of the queue store.
//
// It gates releasing anything the manager shares with those workers, the queue
// store above all. An append already in flight cannot be cancelled — the store
// takes no context — so Stop bounds its wait rather than hanging, and a worker
// may outlive it. Closing the store then would be a use-after-close on a segment
// that worker still holds. Callers that own such a resource must consult this
// and leak the handle instead; the process is exiting either way.
func (m *Manager) ShutdownComplete() bool {
	return m.shutdownComplete.Load()
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

// ProtectedQueueContracts returns a snapshot of the currently registered
// immutable queue contracts.
func (m *Manager) ProtectedQueueContracts() []types.QueueConfig {
	m.protectedQueuesMu.RLock()
	defer m.protectedQueuesMu.RUnlock()

	contracts := make([]types.QueueConfig, 0, len(m.protectedQueueContracts))
	for _, contract := range m.protectedQueueContracts {
		contracts = append(contracts, cloneQueueConfig(contract))
	}
	return contracts
}

// ReplaceProtectedQueueContracts validates and atomically replaces the
// immutable queue-contract registry. Queue mutations and exact publishes are
// blocked for the duration, so no operation can enter between persisted-state
// validation and the registry swap.
func (m *Manager) ReplaceProtectedQueueContracts(ctx context.Context, contracts []types.QueueConfig) error {
	next, err := buildProtectedQueueContracts(contracts)
	if err != nil {
		return err
	}

	m.protectedQueuesMu.Lock()
	defer m.protectedQueuesMu.Unlock()
	if err := m.validateProtectedQueueContractsLocked(ctx, next); err != nil {
		return err
	}
	m.protectedQueueContracts = next
	m.protectedQueueConfigErr = nil
	return nil
}

// NarrowProtectedQueueContracts replaces the registry with an exact subset of
// the already-installed contracts without reading queue storage. It is used to
// remove stale contracts after another subsystem has atomically committed its
// new authorization snapshot; unlike ReplaceProtectedQueueContracts, this
// finalization step cannot fail because of storage I/O.
func (m *Manager) NarrowProtectedQueueContracts(contracts []types.QueueConfig) error {
	next, err := buildProtectedQueueContracts(contracts)
	if err != nil {
		return err
	}

	m.protectedQueuesMu.Lock()
	defer m.protectedQueuesMu.Unlock()
	for name, contract := range next {
		installed, ok := m.protectedQueueContracts[name]
		if !ok {
			return fmt.Errorf("protected queue contract %q is not installed", name)
		}
		if err := protectedQueueContractMismatch(installed, contract); err != nil {
			return fmt.Errorf("protected queue contract %q differs from installed contract: %w", name, err)
		}
	}
	m.protectedQueueContracts = next
	m.protectedQueueConfigErr = nil
	return nil
}

// ValidateProtectedQueueContracts verifies every registered contract against
// the persisted queue configuration.
func (m *Manager) ValidateProtectedQueueContracts(ctx context.Context) error {
	m.protectedQueuesMu.RLock()
	defer m.protectedQueuesMu.RUnlock()
	if m.protectedQueueConfigErr != nil {
		return m.protectedQueueConfigErr
	}
	return m.validateProtectedQueueContractsLocked(ctx, m.protectedQueueContracts)
}

func buildProtectedQueueContracts(contracts []types.QueueConfig) (map[string]types.QueueConfig, error) {
	protected := make(map[string]types.QueueConfig, len(contracts))
	for _, contract := range contracts {
		if contract.Name == "" {
			return nil, fmt.Errorf("protected queue contract name cannot be empty")
		}
		if _, exists := protected[contract.Name]; exists {
			return nil, fmt.Errorf("protected queue contract %q is duplicated", contract.Name)
		}
		if err := protectedQueueContractMismatch(contract, contract); err != nil {
			return nil, fmt.Errorf("invalid protected queue contract %q: %w", contract.Name, err)
		}
		protected[contract.Name] = cloneQueueConfig(contract)
	}
	return protected, nil
}

func (m *Manager) validateProtectedQueueContractsLocked(ctx context.Context, contracts map[string]types.QueueConfig) error {
	if len(contracts) > 0 {
		if _, err := m.durableQueueStore(); err != nil {
			return err
		}
	}

	names := make([]string, 0, len(contracts))
	for name := range contracts {
		names = append(names, name)
	}
	sort.Strings(names)

	for _, name := range names {
		persisted, err := m.queueStore.GetQueue(ctx, name)
		if err != nil {
			return fmt.Errorf("%w: load queue %q: %v", ErrProtectedQueueContractDrift, name, err)
		}
		if persisted == nil {
			return fmt.Errorf("%w: load queue %q: %v", ErrProtectedQueueContractDrift, name, storage.ErrQueueNotFound)
		}
		if err := protectedQueueContractMismatch(contracts[name], *persisted); err != nil {
			return fmt.Errorf("%w: %v", ErrProtectedQueueContractDrift, err)
		}
	}
	return nil
}

// protectedQueueContract copies one registered contract out of the registry so
// callers can do storage work without holding protectedQueuesMu.
func (m *Manager) protectedQueueContract(queueName string) (types.QueueConfig, bool) {
	m.protectedQueuesMu.RLock()
	defer m.protectedQueuesMu.RUnlock()

	contract, protected := m.protectedQueueContracts[queueName]
	if !protected {
		return types.QueueConfig{}, false
	}
	return cloneQueueConfig(contract), true
}

// durableQueueStore returns the queue store only when it can actually make a
// single append durable. A store that implements DurableQueueStore without real
// crash durability must not back a protected queue, because publishers are
// acknowledged on the strength of that barrier.
func (m *Manager) durableQueueStore() (storage.DurableQueueStore, error) {
	durableStore, ok := m.queueStore.(storage.DurableQueueStore)
	if !ok || !durableStore.SupportsDurableSync() {
		return nil, fmt.Errorf("%w: protected queues require durable sync support with atomic append", ErrDurableSyncUnsupported)
	}
	return durableStore, nil
}

func (m *Manager) validateProtectedQueueMutationLocked(config types.QueueConfig) error {
	expected, protected := m.protectedQueueContracts[config.Name]
	if !protected {
		return nil
	}
	if err := protectedQueueContractMismatch(expected, config); err != nil {
		return fmt.Errorf("%w: %v", ErrProtectedQueueMutation, err)
	}
	return nil
}

// ValidateProtectedQueueContract compares the persisted fields that define an
// exact internal publisher's safety and replay guarantees. MaxDepth is
// deliberately excluded because stream depth is not currently enforced.
func ValidateProtectedQueueContract(expected, persisted types.QueueConfig) error {
	if err := protectedQueueContractMismatch(expected, persisted); err != nil {
		return fmt.Errorf("%w: %v", ErrProtectedQueueContractDrift, err)
	}
	return nil
}

func protectedQueueContractMismatch(expected, persisted types.QueueConfig) error {
	target := expected.Name
	switch {
	case expected.Type != types.QueueTypeStream:
		return fmt.Errorf("protected queue %q must be configured as a stream, got %q", target, expected.Type)
	case !expected.Durable:
		return fmt.Errorf("protected queue %q must be configured as durable", target)
	case !expected.Reserved:
		return fmt.Errorf("protected queue %q must be configured as reserved", target)
	case expected.Replication.Enabled:
		return fmt.Errorf("protected queue %q must not configure replication", target)
	case expected.MaxMessageSize <= 0:
		return fmt.Errorf("protected queue %q must configure a positive limits.max_message_size", target)
	case persisted.Name != expected.Name:
		return protectedQueueFieldMismatch(target, "name", persisted.Name, expected.Name)
	case !slices.Equal(persisted.Topics, expected.Topics):
		return protectedQueueFieldMismatch(target, "topics", persisted.Topics, expected.Topics)
	case persisted.Type != types.QueueTypeStream:
		return fmt.Errorf("protected queue %q must be a stream, got %q", target, persisted.Type)
	case !persisted.Durable:
		return fmt.Errorf("protected queue %q must be durable", target)
	case !persisted.Reserved:
		return fmt.Errorf("protected queue %q must be reserved", target)
	case persisted.Replication.Enabled:
		return fmt.Errorf("protected queue %q must not enable replication", target)
	case persisted.Type != expected.Type:
		return protectedQueueFieldMismatch(target, "type", persisted.Type, expected.Type)
	case persisted.Durable != expected.Durable:
		return protectedQueueFieldMismatch(target, "durable", persisted.Durable, expected.Durable)
	case persisted.Reserved != expected.Reserved:
		return protectedQueueFieldMismatch(target, "reserved", persisted.Reserved, expected.Reserved)
	case persisted.Replication.Enabled != expected.Replication.Enabled:
		return protectedQueueFieldMismatch(target, "replication.enabled", persisted.Replication.Enabled, expected.Replication.Enabled)
	case persisted.Retention.RetentionTime != expected.Retention.RetentionTime:
		return protectedQueueFieldMismatch(target, "retention.max_age", persisted.Retention.RetentionTime, expected.Retention.RetentionTime)
	case persisted.Retention.RetentionBytes != expected.Retention.RetentionBytes:
		return protectedQueueFieldMismatch(target, "retention.max_length_bytes", persisted.Retention.RetentionBytes, expected.Retention.RetentionBytes)
	case persisted.Retention.RetentionMessages != expected.Retention.RetentionMessages:
		return protectedQueueFieldMismatch(target, "retention.max_length_messages", persisted.Retention.RetentionMessages, expected.Retention.RetentionMessages)
	case persisted.MaxMessageSize != expected.MaxMessageSize:
		return protectedQueueFieldMismatch(target, "limits.max_message_size", persisted.MaxMessageSize, expected.MaxMessageSize)
	case persisted.MessageTTL != expected.MessageTTL:
		return protectedQueueFieldMismatch(target, "limits.message_ttl", persisted.MessageTTL, expected.MessageTTL)
	default:
		return nil
	}
}

func protectedQueueFieldMismatch(target, field string, got, want any) error {
	return fmt.Errorf("protected queue %q field %s is %v, want %v", target, field, got, want)
}

func cloneQueueConfig(config types.QueueConfig) types.QueueConfig {
	config.Topics = append([]string(nil), config.Topics...)
	return config
}

// GroupStore returns the consumer group store used by the manager.
func (m *Manager) GroupStore() storage.ConsumerGroupStore {
	return m.groupStore
}

// --- Queue Operations ---

// CreateQueue creates a new queue.
func (m *Manager) CreateQueue(ctx context.Context, config types.QueueConfig) error {
	// Checked here rather than only at configuration load, because this is the
	// path an admin-API creation takes. A filter that can never match would bind
	// the queue to nothing and leave it silently receiving no traffic.
	if err := types.ValidateTopicFilters(config.Topics); err != nil {
		return err
	}

	m.protectedQueuesMu.RLock()
	defer m.protectedQueuesMu.RUnlock()
	if err := m.validateProtectedQueueMutationLocked(config); err != nil {
		return err
	}

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
	// An update can introduce a filter that never matches just as a creation
	// can, silently unbinding a queue that was working.
	if err := types.ValidateTopicFilters(config.Topics); err != nil {
		return err
	}

	m.protectedQueuesMu.RLock()
	defer m.protectedQueuesMu.RUnlock()
	if err := m.validateProtectedQueueMutationLocked(config); err != nil {
		return err
	}

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
	m.protectedQueuesMu.RLock()
	defer m.protectedQueuesMu.RUnlock()
	if _, protected := m.protectedQueueContracts[queueName]; protected {
		return fmt.Errorf("%w: queue %q cannot be deleted", ErrProtectedQueueMutation, queueName)
	}

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
// The delivery engine routes appended records to remote consumers when needed.
func (m *Manager) Publish(ctx context.Context, publish types.PublishRequest) error {
	publish = normalizePublishRequest(publish)

	targets, err := m.resolvePublishTargets(ctx, publish)
	if err != nil {
		return err
	}
	if len(targets) == 0 {
		m.logger.Debug("no queues match topic", slog.String("topic", publish.Topic))
		return nil
	}

	return m.publishToTargets(ctx, publish, targets, fanoutStrict)
}

// PublishToMatchingQueues captures an ordinary pub/sub publish in existing
// queues whose configured topic patterns match it. Unlike Publish, it never
// auto-creates a queue when no pattern matches.
//
// It resolves the matching queues on the caller's goroutine — that is an
// in-memory index lookup — and then hands the storage work to the capture
// dispatcher. Enqueueing never blocks, so a queue whose store stalls can no
// longer delay the subscribers of a matching topic or the publisher's
// acknowledgement.
//
// The returned error therefore reports only what is known before the append is
// attempted: that the matching queues could not be resolved. An append that
// fails or is dropped afterwards is reported through queues.capture_failures
// and queues.capture_dropped, which is the only signal capture has.
func (m *Manager) PublishToMatchingQueues(ctx context.Context, publish types.PublishRequest) error {
	// A target dropped during resolution loses a message as surely as a failed
	// append, and each lost queue counts. A resolution error is the one coarse
	// case: the set of queues that would have matched is unknown, so it counts
	// once.
	targets, unresolved, err := m.resolveMatchingPublishTargets(ctx, publish.Topic)
	if err != nil {
		m.metrics.RecordCaptureFailure()
		return err
	}
	for range unresolved {
		m.metrics.RecordCaptureFailure()
	}
	if len(targets) == 0 {
		return nil
	}

	// Protocol brokers release or reuse their message buffers after this call,
	// and the dispatcher reads the publish long after it returns, so ownership
	// has to be taken before it is queued.
	//
	// The map is cloned whenever it exists rather than only when it holds
	// entries: an empty non-nil map is still the caller's, and
	// normalizePublishRequest writes the client ID into whatever it is given.
	// Clone leaves a nil map nil, which that call then replaces outright.
	publish.Payload = bytes.Clone(publish.Payload)
	publish.Properties = maps.Clone(publish.Properties)
	publish = normalizePublishRequest(publish)

	// One job per target, so each queue is ordered by its own lane and a
	// stalled queue cannot hold up captures into the others.
	for i := range targets {
		m.capture.enqueue(captureJob{publish: publish, target: &targets[i]})
	}
	if m.cluster != nil {
		m.capture.enqueue(captureJob{publish: publish})
	}

	return nil
}

// fanoutPolicy decides what a failing target means for the targets beside it.
type fanoutPolicy uint8

const (
	// fanoutStrict abandons the publish at the first target that cannot be
	// written. It is what an addressed publish needs: a write policy that
	// rejects a publication promises nothing was written, so the caller can
	// retry against the leader without duplicating a record this node already
	// appended. A leader rejection is therefore reported before any append runs.
	fanoutStrict fanoutPolicy = iota
	// fanoutBestEffort attempts every target and joins the failures. It is what
	// topic capture needs: the fanout is broker policy applied to whichever
	// queues happen to match, not something the publisher asked for, so one
	// unavailable queue must not suppress capture into unrelated healthy ones.
	// Nothing retries a capture, so a partial write is the best available
	// outcome rather than a duplicate risk.
	fanoutBestEffort
)

// writeToTargets routes one publish to every queue whose pattern matched it,
// without any cluster forwarding.
func (m *Manager) writeToTargets(
	ctx context.Context,
	publish types.PublishRequest,
	targets []queuePublishTarget,
	policy fanoutPolicy,
) error {
	localTargets := make([]queuePublishTarget, 0, len(targets))
	forwardTargets := make(map[string][]string)
	errs := make([]error, 0)
	for _, target := range targets {
		replicated := target.config != nil && target.config.Replication.Enabled

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
			if leaderAddr := m.raftCoordinator.LeaderForQueue(target.name); leaderAddr != "" {
				errs = append(errs, fmt.Errorf("queue %q: raft leader is at %s", target.name, leaderAddr))
				continue
			}
			errs = append(errs, fmt.Errorf("queue %q: raft leader unavailable", target.name))
		case WritePolicyForward:
			leaderID := m.raftCoordinator.LeaderIDForQueue(target.name)
			if leaderID == "" {
				errs = append(errs, fmt.Errorf("queue %q: raft leader unavailable", target.name))
				continue
			}
			forwardTargets[leaderID] = append(forwardTargets[leaderID], target.name)
		case WritePolicyLocal:
			localTargets = append(localTargets, target)
		default:
			// Unknown policy - default to local append for backward compatibility.
			localTargets = append(localTargets, target)
		}
	}

	// A classification failure is reported before anything is written, so a
	// strict caller that rejects a publication really did reject it.
	if policy == fanoutStrict && len(errs) > 0 {
		return errors.Join(errs...)
	}

	// Store locally in queues handled by this node. publishLocalToTargets
	// already attempts every target and joins their errors.
	if err := m.publishLocalToTargets(ctx, publish, localTargets); err != nil {
		errs = append(errs, err)
		if policy == fanoutStrict {
			return errors.Join(errs...)
		}
	}

	// Forward leader-owned queue targets to appropriate remote leaders.
	for leaderID, targetQueues := range forwardTargets {
		if err := m.forwardPublishToLeader(ctx, publish, leaderID, targetQueues); err != nil {
			errs = append(errs, err)
			if policy == fanoutStrict {
				return errors.Join(errs...)
			}
		}
	}

	return errors.Join(errs...)
}

// publishToTargets writes the targets and then forwards the publish to nodes
// holding queues this node does not know. The two halves are separable because
// the forward is per publish rather than per target: capture dispatches them as
// independent jobs so a queue can be ordered by name.
func (m *Manager) publishToTargets(
	ctx context.Context,
	publish types.PublishRequest,
	targets []queuePublishTarget,
	policy fanoutPolicy,
) error {
	err := m.writeToTargets(ctx, publish, targets, policy)

	// Preserve legacy forwarding for queues known only by remote nodes.
	if m.cluster != nil {
		m.forwardToRemoteNodes(ctx, publish)
	}

	return err
}

// PublishToDurableStream appends to exactly queueName and establishes a
// per-queue durability barrier before returning success. The target must be a
// reserved, durable, non-replicated stream. This method never performs topic
// fanout and never auto-creates a queue.
//
// The contract snapshot is copied out of the registry before any storage work
// starts. Holding protectedQueuesMu across the append would let one fsync block
// every contract reload, and a reload waiting for the write lock would in turn
// stall every subsequent publish.
func (m *Manager) PublishToDurableStream(ctx context.Context, queueName string, publish types.PublishRequest) error {
	expected, protected := m.protectedQueueContract(queueName)
	if !protected {
		return fmt.Errorf("%w: %s", ErrQueueNotProtected, queueName)
	}

	publish = normalizePublishRequest(publish)
	queueConfig, err := m.queueStore.GetQueue(ctx, queueName)
	if err != nil {
		return fmt.Errorf("get exact stream %q: %w", queueName, err)
	}
	if queueConfig == nil {
		return fmt.Errorf("get exact stream %q: %w", queueName, storage.ErrQueueNotFound)
	}
	if err := protectedQueueContractMismatch(expected, *queueConfig); err != nil {
		return fmt.Errorf("%w: %v", ErrProtectedQueueContractDrift, err)
	}
	if !queueConfig.Reserved {
		return fmt.Errorf("%w: %s", ErrQueueNotReserved, queueName)
	}
	if queueConfig.Type != types.QueueTypeStream {
		return fmt.Errorf("%w: %s", ErrQueueNotStream, queueName)
	}
	if !queueConfig.Durable {
		return fmt.Errorf("%w: %s", ErrQueueNotDurable, queueName)
	}
	if queueConfig.Replication.Enabled {
		return fmt.Errorf("%w: %s", ErrDurableReplicatedStreamUnsupported, queueName)
	}
	durableStore, err := m.durableQueueStore()
	if err != nil {
		return fmt.Errorf("%w: %s", err, queueName)
	}
	if queueConfig.MaxMessageSize <= 0 || int64(len(publish.Payload)) > queueConfig.MaxMessageSize {
		return fmt.Errorf(
			"%w: queue %s accepts at most %d bytes, got %d",
			ErrQueueMessageTooLarge,
			queueName,
			queueConfig.MaxMessageSize,
			len(publish.Payload),
		)
	}

	msg := newQueuedMessage(publish, queueConfig)
	offset, err := durableStore.AppendAndSync(ctx, queueName, msg)
	if err := m.completeAppend(queueName, publish.Topic, offset, err); err != nil {
		return err
	}
	m.delivery.Schedule(queueName)
	return nil
}

// HandleQueuePublish implements cluster.QueueHandler.HandleQueuePublish.
func (m *Manager) HandleQueuePublish(ctx context.Context, publish types.PublishRequest, mode types.PublishMode) error {
	publish = normalizePublishRequest(publish)

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

func normalizePublishRequest(publish types.PublishRequest) types.PublishRequest {
	publish.Properties = corebroker.AddClientIDProperty(publish.Properties, publish.ClientID)
	return publish
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

	targets, _, err := m.resolveMatchingPublishTargets(ctx, publish.Topic)
	if err != nil {
		return nil, err
	}

	if len(targets) == 0 {
		m.logger.Debug("no queues match topic, creating new queue", slog.String("topic", publish.Topic))
		queueName, queuePattern := autoQueueFromTopic(publish.Topic)
		if _, err := m.GetOrCreateQueue(ctx, queueName, queuePattern); err != nil {
			m.logger.Error("failed to create ephemeral queue", slog.String("topic", publish.Topic), slog.String("error", err.Error()))
			return nil, err
		}
		created, _, err := m.resolveMatchingPublishTargets(ctx, publish.Topic)
		return created, err
	}

	return targets, nil
}

// resolveMatchingPublishTargets returns the queues whose patterns match the
// topic. It also reports how many matched but could not be resolved: their
// configuration was unreadable, so the publish will not reach them. Dropping
// those silently is what made a capture loss invisible, so the count is returned
// rather than only logged.
func (m *Manager) resolveMatchingPublishTargets(ctx context.Context, topic string) ([]queuePublishTarget, int, error) {
	queues, err := m.queueStore.FindMatchingQueues(ctx, topic)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to find matching queues: %w", err)
	}

	unresolved := 0
	targets := make([]queuePublishTarget, 0, len(queues))
	for _, queueName := range queues {
		queueConfig, err := m.queueStore.GetQueue(ctx, queueName)
		if err != nil {
			m.logger.Warn("failed to get queue config", slog.String("queue", queueName), slog.String("error", err.Error()))
			unresolved++
			continue
		}
		targets = append(targets, queuePublishTarget{
			name:   queueName,
			config: queueConfig,
		})
	}

	return targets, unresolved, nil
}

func (m *Manager) publishLocalToTargets(ctx context.Context, publish types.PublishRequest, targets []queuePublishTarget) error {
	errs := make([]error, 0)
	for _, target := range targets {
		if err := m.appendLocalTarget(ctx, publish, target); err != nil {
			errs = append(errs, err)
			continue
		}
		m.delivery.Schedule(target.name)
	}

	return errors.Join(errs...)
}

func (m *Manager) appendLocalTarget(ctx context.Context, publish types.PublishRequest, target queuePublishTarget) error {
	queueName := target.name
	queueConfig := target.config
	if queueConfig == nil {
		return fmt.Errorf("append to queue %q: missing queue configuration", queueName)
	}

	msg := newQueuedMessage(publish, queueConfig)

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
	return m.completeAppend(queueName, publish.Topic, offset, err)
}

func newQueuedMessage(publish types.PublishRequest, queueConfig *types.QueueConfig) *types.Message {
	now := time.Now()
	msg := &types.Message{
		ID:         generateMessageID(),
		Payload:    publish.Payload,
		Topic:      publish.Topic,
		Properties: cloneWithoutForwardingMeta(publish.Properties),
		State:      types.StateQueued,
		CreatedAt:  now,
	}
	if queueConfig.MessageTTL > 0 {
		msg.ExpiresAt = now.Add(queueConfig.MessageTTL)
	}
	return msg
}

func (m *Manager) completeAppend(queueName, topic string, offset uint64, err error) error {
	if err != nil {
		m.logger.Warn("failed to append to queue",
			slog.String("queue", queueName),
			slog.String("topic", topic),
			slog.String("error", err.Error()))
		return fmt.Errorf("append to queue %q: %w", queueName, err)
	}

	m.logger.Debug("message published",
		slog.String("queue", queueName),
		slog.String("topic", topic),
		slog.Uint64("offset", offset))
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
		Payload:    msg.StablePayload(),
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

// forwardToRemoteNodes forwards a publish to nodes holding consumers for a
// queue this node does not know.
//
// A queue known here already has exactly one delivery path: the delivery engine
// routes non-replicated records to remote consumers, and Raft makes replicated
// records available on the consumer's node. Forwarding such a publish as well
// would append a second copy remotely, so only unknown queues are forwarded.
func (m *Manager) forwardToRemoteNodes(ctx context.Context, publish types.PublishRequest) {
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

		if queueExists(c.QueueName) {
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
	return m.subscribeWithCursor(ctx, queueName, pattern, clientID, groupID, proxyNodeID, cursor, true)
}

// SubscribeExistingWithCursor adds a consumer to an existing queue without
// creating the queue or changing its configured type.
func (m *Manager) SubscribeExistingWithCursor(ctx context.Context, queueName, pattern string, clientID, groupID, proxyNodeID string, cursor *types.CursorOption) error {
	return m.subscribeWithCursor(ctx, queueName, pattern, clientID, groupID, proxyNodeID, cursor, false)
}

func (m *Manager) subscribeWithCursor(ctx context.Context, queueName, pattern string, clientID, groupID, proxyNodeID string, cursor *types.CursorOption, allowQueueMutation bool) error {
	if proxyNodeID == "" && m.localNodeID != "" {
		proxyNodeID = m.localNodeID
	}

	mode := types.GroupModeQueue
	if cursor != nil && cursor.Mode != "" {
		mode = cursor.Mode
	}
	if cursor == nil || cursor.Position == types.CursorDefault {
		if mode != types.GroupModeStream {
			return m.subscribe(ctx, queueName, pattern, clientID, groupID, proxyNodeID, allowQueueMutation)
		}
		cursor = &types.CursorOption{Position: types.CursorDefault, Mode: mode}
	}

	var (
		queueCfg *types.QueueConfig
		err      error
	)
	if allowQueueMutation {
		queueTopicPattern := "$queue/" + queueName + "/#"
		queueCfg, err = m.GetOrCreateQueue(ctx, queueName, queueTopicPattern)
	} else {
		queueCfg, err = m.GetQueue(ctx, queueName)
	}
	if err != nil {
		return fmt.Errorf("failed to resolve queue for subscription: %w", err)
	}
	if queueCfg == nil {
		return fmt.Errorf("failed to resolve queue for subscription: %w", storage.ErrQueueNotFound)
	}
	if mode == types.GroupModeStream && queueCfg.Type != types.QueueTypeStream {
		if !allowQueueMutation {
			return fmt.Errorf("%w: %q has type %q", ErrQueueNotStream, queueName, queueCfg.Type)
		}
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
			groupID = DefaultConsumerGroupID(clientID)
		}
	}

	patternGroupID := corebroker.EffectiveConsumerGroupID(groupID, pattern)

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
	return m.subscribe(ctx, queueName, pattern, clientID, groupID, proxyNodeID, true)
}

// SubscribeExisting adds a consumer to an existing queue without creating it.
func (m *Manager) SubscribeExisting(ctx context.Context, queueName, pattern string, clientID, groupID, proxyNodeID string) error {
	return m.subscribe(ctx, queueName, pattern, clientID, groupID, proxyNodeID, false)
}

func (m *Manager) subscribe(ctx context.Context, queueName, pattern string, clientID, groupID, proxyNodeID string, allowQueueCreation bool) error {
	if proxyNodeID == "" && m.localNodeID != "" {
		proxyNodeID = m.localNodeID
	}

	var (
		queueCfg *types.QueueConfig
		err      error
	)
	if allowQueueCreation {
		// Use $queue/<name>/# as the topic pattern so messages published to
		// $queue/<name>/... are captured.
		queueTopicPattern := "$queue/" + queueName + "/#"
		queueCfg, err = m.GetOrCreateQueue(ctx, queueName, queueTopicPattern)
	} else {
		queueCfg, err = m.GetQueue(ctx, queueName)
	}
	if err != nil {
		return fmt.Errorf("failed to resolve queue for subscription: %w", err)
	}
	if queueCfg == nil {
		return fmt.Errorf("failed to resolve queue for subscription: %w", storage.ErrQueueNotFound)
	}

	// Default group ID to client prefix
	if groupID == "" {
		groupID = DefaultConsumerGroupID(clientID)
	}

	// Create unique group ID that includes the pattern
	patternGroupID := corebroker.EffectiveConsumerGroupID(groupID, pattern)

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
		groupID = DefaultConsumerGroupID(clientID)
	}

	patternGroupID := corebroker.EffectiveConsumerGroupID(groupID, pattern)

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
				m.handleConsumersRemoved(ctx, queueConfig.Name, group.ID, removed)
			}
		}
	}
}

func (m *Manager) handleConsumersRemoved(ctx context.Context, queueName, groupID string, consumerIDs []string) {
	if len(consumerIDs) == 0 {
		return
	}
	for _, consumerID := range consumerIDs {
		m.untrackSubscription(consumerID, queueName, groupID)
	}
	if m.config.OnConsumerRemoved != nil {
		m.config.OnConsumerRemoved(queueName, groupID, append([]string(nil), consumerIDs...))
	}
	m.checkEphemeralDisconnect(ctx, queueName)
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
func (m *Manager) cleanupEphemeralQueues(ctx context.Context) {
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
			m.cleanupEphemeralQueues(context.Background()) //nolint:contextcheck // background goroutine; no request-scoped context available
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

	// Stamped after the user properties are copied in, matching the local
	// delivery path: a publisher cannot forge queue-owned metadata.
	props[types.PropMessageID] = messageID
	props[types.PropGroupID] = msg.GroupID
	props[types.PropQueueName] = queueName
	props[types.PropOffset] = strconv.FormatInt(msg.Sequence, 10)
	if msg.SourceTopic != "" {
		props[types.PropSourceTopic] = msg.SourceTopic
	}

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

	topic := queueDeliveryTopic(queueName, msg.Topic)

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
