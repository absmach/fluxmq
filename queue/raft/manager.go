// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package raft

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/absmach/fluxmq/queue/storage"
	"github.com/absmach/fluxmq/queue/types"
	"github.com/dgraph-io/badger/v4"
	"github.com/hashicorp/raft"
)

// Manager manages Raft groups for queue partitions.
// It supports both global (all queues share partitions) and per-queue partition models.
type Manager struct {
	nodeID     string
	dataDir    string
	logStore   storage.LogStore
	groupStore storage.ConsumerGroupStore
	logger     *slog.Logger
	config     ManagerConfig

	// Raft groups: "queueName:partitionID" -> RaftGroup
	groups   map[string]*RaftGroup
	groupsMu sync.RWMutex

	// Node addresses for Raft transport
	nodeAddresses map[string]string // nodeID -> raft bind address

	stopCh chan struct{}
	wg     sync.WaitGroup
}

// ManagerConfig holds configuration for the Raft manager.
type ManagerConfig struct {
	// Replication settings
	Enabled           bool
	ReplicationFactor int
	SyncMode          bool
	MinInSyncReplicas int
	AckTimeout        time.Duration

	// Raft tuning
	HeartbeatTimeout  time.Duration
	ElectionTimeout   time.Duration
	SnapshotInterval  time.Duration
	SnapshotThreshold uint64

	// Network
	BindAddr string // Base address for Raft transport (e.g., "127.0.0.1:7100")
	DataDir  string // Directory for Raft data
}

// DefaultManagerConfig returns default manager configuration.
func DefaultManagerConfig() ManagerConfig {
	return ManagerConfig{
		Enabled:           false,
		ReplicationFactor: 3,
		SyncMode:          true,
		MinInSyncReplicas: 2,
		AckTimeout:        5 * time.Second,
		HeartbeatTimeout:  1 * time.Second,
		ElectionTimeout:   3 * time.Second,
		SnapshotInterval:  5 * time.Minute,
		SnapshotThreshold: 8192,
		BindAddr:          "127.0.0.1:7100",
		DataDir:           "/tmp/mqtt/raft",
	}
}

// NewManager creates a new Raft manager.
func NewManager(
	nodeID string,
	dataDir string,
	logStore storage.LogStore,
	groupStore storage.ConsumerGroupStore,
	nodeAddresses map[string]string,
	config ManagerConfig,
	logger *slog.Logger,
) *Manager {
	if logger == nil {
		logger = slog.Default()
	}

	return &Manager{
		nodeID:        nodeID,
		dataDir:       dataDir,
		logStore:      logStore,
		groupStore:    groupStore,
		logger:        logger,
		config:        config,
		groups:        make(map[string]*RaftGroup),
		nodeAddresses: nodeAddresses,
		stopCh:        make(chan struct{}),
	}
}

// Start initializes the Raft manager.
func (m *Manager) Start(ctx context.Context) error {
	if !m.config.Enabled {
		m.logger.Info("raft replication disabled")
		return nil
	}

	m.logger.Info("starting raft manager",
		slog.String("node_id", m.nodeID),
		slog.Int("replication_factor", m.config.ReplicationFactor),
		slog.Bool("sync_mode", m.config.SyncMode))

	return nil
}

// Stop shuts down all Raft groups.
func (m *Manager) Stop() error {
	close(m.stopCh)
	m.wg.Wait()

	m.groupsMu.Lock()
	defer m.groupsMu.Unlock()

	var errors []error
	for key, group := range m.groups {
		if err := group.Shutdown(); err != nil {
			m.logger.Error("failed to shutdown raft group",
				slog.String("group", key),
				slog.String("error", err.Error()))
			errors = append(errors, err)
		}
	}

	m.groups = make(map[string]*RaftGroup)

	if len(errors) > 0 {
		return fmt.Errorf("failed to shutdown %d raft groups", len(errors))
	}

	m.logger.Info("raft manager stopped")
	return nil
}

// IsEnabled returns true if Raft replication is enabled.
func (m *Manager) IsEnabled() bool {
	return m.config.Enabled
}

// groupKey returns the key for a Raft group.
func groupKey(queueName string, partitionID int) string {
	return fmt.Sprintf("%s:%d", queueName, partitionID)
}

// EnsurePartition ensures a Raft group exists for the given queue partition.
func (m *Manager) EnsurePartition(ctx context.Context, queueName string, partitionID int, totalPartitions int) error {
	if !m.config.Enabled {
		return nil
	}

	key := groupKey(queueName, partitionID)

	m.groupsMu.Lock()
	defer m.groupsMu.Unlock()

	if _, exists := m.groups[key]; exists {
		return nil
	}

	return m.startPartitionLocked(ctx, queueName, partitionID, totalPartitions)
}

// startPartitionLocked starts a Raft group for a partition (must hold lock).
func (m *Manager) startPartitionLocked(ctx context.Context, queueName string, partitionID int, totalPartitions int) error {
	key := groupKey(queueName, partitionID)

	// Determine replica placement using round-robin
	replicas := m.assignReplicas(partitionID, totalPartitions)

	// Create Raft group configuration
	raftConfig := RaftGroupConfig{
		QueueName:         queueName,
		PartitionID:       partitionID,
		NodeID:            m.nodeID,
		BindAddr:          m.getPartitionBindAddress(m.nodeID, partitionID),
		DataDir:           filepath.Join(m.dataDir, "raft", queueName),
		SyncMode:          m.config.SyncMode,
		AckTimeout:        m.config.AckTimeout,
		MinInSyncReplicas: m.config.MinInSyncReplicas,
		HeartbeatTimeout:  m.config.HeartbeatTimeout,
		ElectionTimeout:   m.config.ElectionTimeout,
		SnapshotInterval:  m.config.SnapshotInterval,
		SnapshotThreshold: m.config.SnapshotThreshold,
		LogStore:          m.logStore,
		GroupStore:        m.groupStore,
		Logger:            m.logger,
	}

	// Create Raft group
	group, err := NewRaftGroup(raftConfig)
	if err != nil {
		return fmt.Errorf("failed to create raft group: %w", err)
	}

	// Bootstrap the cluster
	if err := m.bootstrapPartition(group, replicas, partitionID); err != nil {
		group.Shutdown()
		return fmt.Errorf("failed to bootstrap partition: %w", err)
	}

	m.groups[key] = group

	m.logger.Info("started raft partition",
		slog.String("queue", queueName),
		slog.Int("partition", partitionID),
		slog.String("node_id", m.nodeID),
		slog.Int("replica_count", len(replicas)),
		slog.String("replicas", fmt.Sprintf("%v", replicas)))

	return nil
}

// assignReplicas returns the node IDs that should host replicas for a partition.
func (m *Manager) assignReplicas(partitionID, totalPartitions int) []string {
	nodeIDs := make([]string, 0, len(m.nodeAddresses))
	for nodeID := range m.nodeAddresses {
		nodeIDs = append(nodeIDs, nodeID)
	}
	sort.Strings(nodeIDs) // Deterministic ordering

	replicationFactor := m.config.ReplicationFactor
	if replicationFactor > len(nodeIDs) {
		replicationFactor = len(nodeIDs)
	}

	replicas := make([]string, replicationFactor)
	for i := 0; i < replicationFactor; i++ {
		nodeIdx := (partitionID + i) % len(nodeIDs)
		replicas[i] = nodeIDs[nodeIdx]
	}

	return replicas
}

// bootstrapPartition bootstraps the Raft cluster for a partition.
func (m *Manager) bootstrapPartition(group *RaftGroup, replicas []string, partitionID int) error {
	var servers []raft.Server
	for _, nodeID := range replicas {
		addr := m.getPartitionBindAddress(nodeID, partitionID)
		servers = append(servers, raft.Server{
			ID:      raft.ServerID(nodeID),
			Address: raft.ServerAddress(addr),
		})
	}

	return group.Bootstrap(servers)
}

// getPartitionBindAddress returns the Raft bind address for a specific partition on a node.
func (m *Manager) getPartitionBindAddress(nodeID string, partitionID int) string {
	baseAddr, ok := m.nodeAddresses[nodeID]
	if !ok {
		// Use this node's bind address
		baseAddr = m.config.BindAddr
	}

	host, portStr, err := net.SplitHostPort(baseAddr)
	if err != nil {
		return fmt.Sprintf("%s:%d", baseAddr, 7100+partitionID)
	}

	basePort, err := strconv.Atoi(portStr)
	if err != nil {
		basePort = 7100
	}

	return fmt.Sprintf("%s:%d", host, basePort+partitionID)
}

// GetGroup returns the Raft group for a partition.
func (m *Manager) GetGroup(queueName string, partitionID int) (*RaftGroup, bool) {
	m.groupsMu.RLock()
	defer m.groupsMu.RUnlock()

	group, ok := m.groups[groupKey(queueName, partitionID)]
	return group, ok
}

// IsLeader returns true if this node is the Raft leader for the partition.
func (m *Manager) IsLeader(queueName string, partitionID int) bool {
	group, ok := m.GetGroup(queueName, partitionID)
	if !ok {
		return false
	}
	return group.IsLeader()
}

// Apply submits an operation to Raft for replication.
// Returns the result after the operation is committed (in sync mode).
func (m *Manager) Apply(ctx context.Context, queueName string, partitionID int, op *Operation) (*ApplyResult, error) {
	if !m.config.Enabled {
		// Replication disabled - execute directly
		return nil, nil
	}

	group, ok := m.GetGroup(queueName, partitionID)
	if !ok {
		return nil, fmt.Errorf("no raft group for %s partition %d", queueName, partitionID)
	}

	op.Timestamp = time.Now()
	op.QueueName = queueName
	op.PartitionID = partitionID

	data, err := json.Marshal(op)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal operation: %w", err)
	}

	result, err := group.Apply(data, m.config.AckTimeout)
	if err != nil {
		return nil, err
	}

	return result, nil
}

// ApplyAppend submits an append operation to Raft.
func (m *Manager) ApplyAppend(ctx context.Context, queueName string, partitionID int, msg *types.Message) (uint64, error) {
	if !m.config.Enabled {
		return 0, nil
	}

	op := &Operation{
		Type:    OpAppend,
		Message: msg,
	}

	result, err := m.Apply(ctx, queueName, partitionID, op)
	if err != nil {
		return 0, err
	}

	if result != nil && result.Error != nil {
		return 0, result.Error
	}

	if result != nil {
		return result.Offset, nil
	}

	return 0, nil
}

// ApplyTruncate submits a truncate operation to Raft.
func (m *Manager) ApplyTruncate(ctx context.Context, queueName string, partitionID int, minOffset uint64) error {
	if !m.config.Enabled {
		return nil
	}

	op := &Operation{
		Type:      OpTruncate,
		MinOffset: minOffset,
	}

	result, err := m.Apply(ctx, queueName, partitionID, op)
	if err != nil {
		return err
	}

	if result != nil && result.Error != nil {
		return result.Error
	}

	return nil
}

// ApplyCreateGroup submits a create consumer group operation to Raft.
func (m *Manager) ApplyCreateGroup(ctx context.Context, queueName string, partitionID int, group *types.ConsumerGroupState) error {
	if !m.config.Enabled {
		return nil
	}

	op := &Operation{
		Type:       OpCreateGroup,
		GroupID:    group.ID,
		GroupState: group,
	}

	result, err := m.Apply(ctx, queueName, partitionID, op)
	if err != nil {
		return err
	}

	if result != nil && result.Error != nil {
		return result.Error
	}

	return nil
}

// ApplyUpdateCursor submits a cursor update operation to Raft.
func (m *Manager) ApplyUpdateCursor(ctx context.Context, queueName string, partitionID int, groupID string, cursor uint64) error {
	if !m.config.Enabled {
		return nil
	}

	op := &Operation{
		Type:    OpUpdateCursor,
		GroupID: groupID,
		Cursor:  cursor,
	}

	result, err := m.Apply(ctx, queueName, partitionID, op)
	if err != nil {
		return err
	}

	if result != nil && result.Error != nil {
		return result.Error
	}

	return nil
}

// ApplyAddPending submits an add pending entry operation to Raft.
func (m *Manager) ApplyAddPending(ctx context.Context, queueName string, partitionID int, groupID string, entry *types.PendingEntry) error {
	if !m.config.Enabled {
		return nil
	}

	op := &Operation{
		Type:         OpAddPending,
		GroupID:      groupID,
		PendingEntry: entry,
	}

	result, err := m.Apply(ctx, queueName, partitionID, op)
	if err != nil {
		return err
	}

	if result != nil && result.Error != nil {
		return result.Error
	}

	return nil
}

// ApplyRemovePending submits a remove pending entry operation to Raft (ACK).
func (m *Manager) ApplyRemovePending(ctx context.Context, queueName string, partitionID int, groupID, consumerID string, offset uint64) error {
	if !m.config.Enabled {
		return nil
	}

	op := &Operation{
		Type:       OpRemovePending,
		GroupID:    groupID,
		ConsumerID: consumerID,
		Offset:     offset,
	}

	result, err := m.Apply(ctx, queueName, partitionID, op)
	if err != nil {
		return err
	}

	if result != nil && result.Error != nil {
		return result.Error
	}

	return nil
}

// ApplyRegisterConsumer submits a register consumer operation to Raft.
func (m *Manager) ApplyRegisterConsumer(ctx context.Context, queueName string, partitionID int, groupID string, consumer *types.ConsumerInfo) error {
	if !m.config.Enabled {
		return nil
	}

	op := &Operation{
		Type:         OpRegisterConsumer,
		GroupID:      groupID,
		ConsumerInfo: consumer,
	}

	result, err := m.Apply(ctx, queueName, partitionID, op)
	if err != nil {
		return err
	}

	if result != nil && result.Error != nil {
		return result.Error
	}

	return nil
}

// ApplyUnregisterConsumer submits an unregister consumer operation to Raft.
func (m *Manager) ApplyUnregisterConsumer(ctx context.Context, queueName string, partitionID int, groupID, consumerID string) error {
	if !m.config.Enabled {
		return nil
	}

	op := &Operation{
		Type:       OpUnregisterConsumer,
		GroupID:    groupID,
		ConsumerID: consumerID,
	}

	result, err := m.Apply(ctx, queueName, partitionID, op)
	if err != nil {
		return err
	}

	if result != nil && result.Error != nil {
		return result.Error
	}

	return nil
}

// GetStats returns statistics for all Raft groups.
func (m *Manager) GetStats() map[string]map[string]string {
	m.groupsMu.RLock()
	defer m.groupsMu.RUnlock()

	stats := make(map[string]map[string]string)
	for key, group := range m.groups {
		stats[key] = group.GetStats()
	}

	return stats
}

// WaitForLeader waits for a leader to be elected for the partition.
func (m *Manager) WaitForLeader(ctx context.Context, queueName string, partitionID int) error {
	group, ok := m.GetGroup(queueName, partitionID)
	if !ok {
		return fmt.Errorf("no raft group for %s partition %d", queueName, partitionID)
	}

	return group.WaitForLeader(ctx, m.config.AckTimeout)
}

// RaftGroup manages a single Raft consensus group for one queue partition.
type RaftGroup struct {
	queueName   string
	partitionID int
	nodeID      string
	bindAddr    string

	raft *raft.Raft
	fsm  *LogFSM

	logStore      *BadgerLogStore
	stableStore   *BadgerStableStore
	snapshotStore raft.SnapshotStore
	transport     raft.Transport

	raftDB *badger.DB

	syncMode          bool
	ackTimeout        time.Duration
	minInSyncReplicas int

	logger *slog.Logger
}

// RaftGroupConfig holds configuration for a Raft group.
type RaftGroupConfig struct {
	QueueName   string
	PartitionID int
	NodeID      string
	BindAddr    string
	DataDir     string

	SyncMode          bool
	AckTimeout        time.Duration
	MinInSyncReplicas int

	HeartbeatTimeout  time.Duration
	ElectionTimeout   time.Duration
	SnapshotInterval  time.Duration
	SnapshotThreshold uint64

	LogStore   storage.LogStore
	GroupStore storage.ConsumerGroupStore
	Logger     *slog.Logger
}

// NewRaftGroup creates a new Raft group for a queue partition.
func NewRaftGroup(cfg RaftGroupConfig) (*RaftGroup, error) {
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}

	// Set defaults
	if cfg.HeartbeatTimeout == 0 {
		cfg.HeartbeatTimeout = 1 * time.Second
	}
	if cfg.ElectionTimeout == 0 {
		cfg.ElectionTimeout = 3 * time.Second
	}
	if cfg.SnapshotInterval == 0 {
		cfg.SnapshotInterval = 5 * time.Minute
	}
	if cfg.SnapshotThreshold == 0 {
		cfg.SnapshotThreshold = 8192
	}
	if cfg.AckTimeout == 0 {
		cfg.AckTimeout = 5 * time.Second
	}

	rg := &RaftGroup{
		queueName:         cfg.QueueName,
		partitionID:       cfg.PartitionID,
		nodeID:            cfg.NodeID,
		bindAddr:          cfg.BindAddr,
		syncMode:          cfg.SyncMode,
		ackTimeout:        cfg.AckTimeout,
		minInSyncReplicas: cfg.MinInSyncReplicas,
		logger:            cfg.Logger,
	}

	// Create data directory for this partition
	partitionDir := filepath.Join(cfg.DataDir, fmt.Sprintf("partition-%d", cfg.PartitionID))
	raftDir := filepath.Join(partitionDir, "raft")
	if err := os.MkdirAll(raftDir, 0o755); err != nil {
		return nil, fmt.Errorf("failed to create raft directory: %w", err)
	}

	// Open BadgerDB for Raft log and metadata
	opts := badger.DefaultOptions(raftDir)
	opts.Logger = nil
	raftDB, err := badger.Open(opts)
	if err != nil {
		return nil, fmt.Errorf("failed to open raft badger db: %w", err)
	}
	rg.raftDB = raftDB

	// Create storage backends
	rg.logStore = NewBadgerLogStore(raftDB, cfg.QueueName, cfg.PartitionID)
	rg.stableStore = NewBadgerStableStore(raftDB, cfg.QueueName, cfg.PartitionID)

	// Create snapshot store
	snapshotDir := filepath.Join(raftDir, "snapshots")
	snapStore, err := raft.NewFileSnapshotStore(snapshotDir, 3, os.Stderr)
	if err != nil {
		raftDB.Close()
		return nil, fmt.Errorf("failed to create snapshot store: %w", err)
	}
	rg.snapshotStore = snapStore

	// Create FSM
	rg.fsm = NewLogFSM(cfg.QueueName, cfg.PartitionID, cfg.LogStore, cfg.GroupStore, cfg.Logger)

	// Create transport
	addr, err := net.ResolveTCPAddr("tcp", cfg.BindAddr)
	if err != nil {
		raftDB.Close()
		return nil, fmt.Errorf("failed to resolve bind address: %w", err)
	}

	transport, err := raft.NewTCPTransport(cfg.BindAddr, addr, 3, 10*time.Second, os.Stderr)
	if err != nil {
		raftDB.Close()
		return nil, fmt.Errorf("failed to create raft transport: %w", err)
	}
	rg.transport = transport

	// Create Raft configuration
	raftCfg := raft.DefaultConfig()
	raftCfg.LocalID = raft.ServerID(cfg.NodeID)
	raftCfg.HeartbeatTimeout = cfg.HeartbeatTimeout
	raftCfg.ElectionTimeout = cfg.ElectionTimeout
	raftCfg.SnapshotInterval = cfg.SnapshotInterval
	raftCfg.SnapshotThreshold = cfg.SnapshotThreshold
	raftCfg.LogLevel = "WARN"

	// Create Raft instance
	r, err := raft.NewRaft(raftCfg, rg.fsm, rg.logStore, rg.stableStore, rg.snapshotStore, rg.transport)
	if err != nil {
		transport.Close()
		raftDB.Close()
		return nil, fmt.Errorf("failed to create raft: %w", err)
	}
	rg.raft = r

	rg.logger.Info("raft group created",
		slog.String("queue", cfg.QueueName),
		slog.Int("partition", cfg.PartitionID),
		slog.String("node_id", cfg.NodeID),
		slog.String("bind_addr", cfg.BindAddr))

	return rg, nil
}

// Bootstrap initializes the Raft cluster with the initial set of peers.
func (rg *RaftGroup) Bootstrap(peers []raft.Server) error {
	hasState, err := raft.HasExistingState(rg.logStore, rg.stableStore, rg.snapshotStore)
	if err != nil {
		return fmt.Errorf("failed to check existing state: %w", err)
	}

	if hasState {
		rg.logger.Info("raft already bootstrapped, skipping",
			slog.String("queue", rg.queueName),
			slog.Int("partition", rg.partitionID))
		return nil
	}

	cfg := raft.Configuration{Servers: peers}
	future := rg.raft.BootstrapCluster(cfg)
	if err := future.Error(); err != nil {
		return fmt.Errorf("failed to bootstrap raft: %w", err)
	}

	rg.logger.Info("raft bootstrapped",
		slog.String("queue", rg.queueName),
		slog.Int("partition", rg.partitionID),
		slog.Int("peer_count", len(peers)))

	return nil
}

// Apply submits an operation to the Raft log.
func (rg *RaftGroup) Apply(data []byte, timeout time.Duration) (*ApplyResult, error) {
	if rg.raft.State() != raft.Leader {
		return nil, fmt.Errorf("not leader")
	}

	future := rg.raft.Apply(data, timeout)

	if rg.syncMode {
		if err := future.Error(); err != nil {
			return nil, fmt.Errorf("raft apply failed: %w", err)
		}

		if result := future.Response(); result != nil {
			if applyResult, ok := result.(*ApplyResult); ok {
				return applyResult, nil
			}
		}

		return &ApplyResult{}, nil
	}

	// Async mode - return immediately
	go func() {
		if err := future.Error(); err != nil {
			rg.logger.Error("async raft apply failed",
				slog.String("queue", rg.queueName),
				slog.Int("partition", rg.partitionID),
				slog.String("error", err.Error()))
		}
	}()

	return &ApplyResult{}, nil
}

// IsLeader returns true if this node is the Raft leader.
func (rg *RaftGroup) IsLeader() bool {
	return rg.raft.State() == raft.Leader
}

// Leader returns the current leader's address.
func (rg *RaftGroup) Leader() string {
	addr, _ := rg.raft.LeaderWithID()
	return string(addr)
}

// WaitForLeader blocks until a leader is elected or the context is cancelled.
func (rg *RaftGroup) WaitForLeader(ctx context.Context, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("timeout waiting for leader")
		case <-ticker.C:
			if rg.Leader() != "" {
				return nil
			}
		}
	}
}

// GetStats returns Raft stats for monitoring.
func (rg *RaftGroup) GetStats() map[string]string {
	return rg.raft.Stats()
}

// Shutdown gracefully shuts down the Raft group.
func (rg *RaftGroup) Shutdown() error {
	rg.logger.Info("shutting down raft group",
		slog.String("queue", rg.queueName),
		slog.Int("partition", rg.partitionID))

	if rg.raft != nil {
		future := rg.raft.Shutdown()
		if err := future.Error(); err != nil {
			rg.logger.Error("raft shutdown error",
				slog.String("queue", rg.queueName),
				slog.Int("partition", rg.partitionID),
				slog.String("error", err.Error()))
		}
	}

	if rg.raftDB != nil {
		if err := rg.raftDB.Close(); err != nil {
			rg.logger.Error("raft db close error",
				slog.String("error", err.Error()))
		}
	}

	return nil
}
