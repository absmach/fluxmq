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
	"sync"
	"time"

	"github.com/absmach/fluxmq/queue/storage"
	"github.com/absmach/fluxmq/queue/types"
	"github.com/dgraph-io/badger/v4"
	"github.com/hashicorp/raft"
)

// Manager manages a single Raft group for all queue operations.
// All queues share one Raft consensus group per node.
type Manager struct {
	nodeID     string
	bindAddr   string
	dataDir    string
	queueStore storage.QueueStore
	groupStore storage.ConsumerGroupStore
	logger     *slog.Logger
	config     ManagerConfig

	// Single Raft group for all queues
	raft          *raft.Raft
	fsm           *LogFSM
	raftLogStore  *BadgerLogStore
	stableStore   *BadgerStableStore
	snapshotStore raft.SnapshotStore
	transport     raft.Transport
	raftDB        *badger.DB

	// Peer addresses: nodeID -> raft address
	peers map[string]string

	mu     sync.RWMutex
	stopCh chan struct{}
}

// ManagerConfig holds configuration for the Raft manager.
type ManagerConfig struct {
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
	}
}

// NewManager creates a new Raft manager with a single Raft group.
func NewManager(
	nodeID string,
	bindAddr string,
	dataDir string,
	queueStore storage.QueueStore,
	groupStore storage.ConsumerGroupStore,
	peers map[string]string,
	config ManagerConfig,
	logger *slog.Logger,
) *Manager {
	if logger == nil {
		logger = slog.Default()
	}

	return &Manager{
		nodeID:     nodeID,
		bindAddr:   bindAddr,
		dataDir:    dataDir,
		queueStore: queueStore,
		groupStore: groupStore,
		peers:      peers,
		config:     config,
		logger:     logger,
		stopCh:     make(chan struct{}),
	}
}

// Start initializes and starts the Raft group.
func (m *Manager) Start(ctx context.Context) error {
	if !m.config.Enabled {
		m.logger.Info("raft replication disabled")
		return nil
	}

	m.logger.Info("starting raft manager",
		slog.String("node_id", m.nodeID),
		slog.String("bind_addr", m.bindAddr),
		slog.Int("replication_factor", m.config.ReplicationFactor),
		slog.Bool("sync_mode", m.config.SyncMode),
		slog.Int("peer_count", len(m.peers)))

	// Create data directory
	raftDir := filepath.Join(m.dataDir, "raft")
	if err := os.MkdirAll(raftDir, 0o755); err != nil {
		return fmt.Errorf("failed to create raft directory: %w", err)
	}

	// Open BadgerDB for Raft log and metadata
	opts := badger.DefaultOptions(raftDir)
	opts.Logger = nil
	raftDB, err := badger.Open(opts)
	if err != nil {
		return fmt.Errorf("failed to open raft badger db: %w", err)
	}
	m.raftDB = raftDB

	// Create storage backends
	m.raftLogStore = NewBadgerLogStore(raftDB, "_raft", 0)
	m.stableStore = NewBadgerStableStore(raftDB, "_raft", 0)

	// Create snapshot store
	snapshotDir := filepath.Join(raftDir, "snapshots")
	snapStore, err := raft.NewFileSnapshotStore(snapshotDir, 3, os.Stderr)
	if err != nil {
		raftDB.Close()
		return fmt.Errorf("failed to create snapshot store: %w", err)
	}
	m.snapshotStore = snapStore

	// Create FSM that handles all queues
	m.fsm = NewLogFSM(m.queueStore, m.groupStore, m.logger)

	// Create transport
	addr, err := net.ResolveTCPAddr("tcp", m.bindAddr)
	if err != nil {
		raftDB.Close()
		return fmt.Errorf("failed to resolve bind address: %w", err)
	}

	transport, err := raft.NewTCPTransport(m.bindAddr, addr, 3, 10*time.Second, os.Stderr)
	if err != nil {
		raftDB.Close()
		return fmt.Errorf("failed to create raft transport: %w", err)
	}
	m.transport = transport

	// Create Raft configuration
	raftCfg := raft.DefaultConfig()
	raftCfg.LocalID = raft.ServerID(m.nodeID)
	raftCfg.HeartbeatTimeout = m.config.HeartbeatTimeout
	raftCfg.ElectionTimeout = m.config.ElectionTimeout
	raftCfg.SnapshotInterval = m.config.SnapshotInterval
	raftCfg.SnapshotThreshold = m.config.SnapshotThreshold
	raftCfg.LogLevel = "WARN"

	// Create Raft instance
	r, err := raft.NewRaft(raftCfg, m.fsm, m.raftLogStore, m.stableStore, m.snapshotStore, m.transport)
	if err != nil {
		transport.Close()
		raftDB.Close()
		return fmt.Errorf("failed to create raft: %w", err)
	}
	m.raft = r

	// Bootstrap cluster if needed
	if err := m.bootstrap(); err != nil {
		m.logger.Warn("bootstrap returned error (may be expected if already bootstrapped)",
			slog.String("error", err.Error()))
	}

	m.logger.Info("raft manager started",
		slog.String("node_id", m.nodeID),
		slog.String("bind_addr", m.bindAddr))

	return nil
}

// bootstrap initializes the Raft cluster with all configured peers.
func (m *Manager) bootstrap() error {
	hasState, err := raft.HasExistingState(m.raftLogStore, m.stableStore, m.snapshotStore)
	if err != nil {
		return fmt.Errorf("failed to check existing state: %w", err)
	}

	if hasState {
		m.logger.Info("raft already has existing state, skipping bootstrap")
		return nil
	}

	// Wait for peers to be reachable before bootstrapping
	m.waitForPeers()

	// Build server list: self + all peers
	var servers []raft.Server

	// Add self
	servers = append(servers, raft.Server{
		ID:      raft.ServerID(m.nodeID),
		Address: raft.ServerAddress(m.bindAddr),
	})

	// Add peers
	for nodeID, addr := range m.peers {
		servers = append(servers, raft.Server{
			ID:      raft.ServerID(nodeID),
			Address: raft.ServerAddress(addr),
		})
	}

	m.logger.Info("bootstrapping raft cluster",
		slog.Int("server_count", len(servers)))

	cfg := raft.Configuration{Servers: servers}
	future := m.raft.BootstrapCluster(cfg)
	if err := future.Error(); err != nil {
		return fmt.Errorf("failed to bootstrap raft: %w", err)
	}

	m.logger.Info("raft cluster bootstrapped successfully")
	return nil
}

// waitForPeers waits for peer nodes to be reachable.
func (m *Manager) waitForPeers() {
	if len(m.peers) == 0 {
		return
	}

	m.logger.Info("waiting for raft peers to be reachable",
		slog.Int("peer_count", len(m.peers)))

	maxWait := 30 * time.Second
	checkInterval := 500 * time.Millisecond
	deadline := time.Now().Add(maxWait)

	for time.Now().Before(deadline) {
		allReachable := true
		for nodeID, addr := range m.peers {
			conn, err := net.DialTimeout("tcp", addr, 1*time.Second)
			if err != nil {
				allReachable = false
				m.logger.Debug("peer not reachable yet",
					slog.String("peer", nodeID),
					slog.String("addr", addr))
			} else {
				conn.Close()
			}
		}

		if allReachable {
			m.logger.Info("all raft peers are reachable")
			return
		}

		time.Sleep(checkInterval)
	}

	m.logger.Warn("timeout waiting for all peers, proceeding with bootstrap anyway",
		slog.Int("peer_count", len(m.peers)))
}

// Stop shuts down the Raft group.
func (m *Manager) Stop() error {
	close(m.stopCh)

	m.mu.Lock()
	defer m.mu.Unlock()

	if m.raft != nil {
		future := m.raft.Shutdown()
		if err := future.Error(); err != nil {
			m.logger.Error("raft shutdown error", slog.String("error", err.Error()))
		}
	}

	if m.raftDB != nil {
		if err := m.raftDB.Close(); err != nil {
			m.logger.Error("raft db close error", slog.String("error", err.Error()))
		}
	}

	m.logger.Info("raft manager stopped")
	return nil
}

// IsEnabled returns true if Raft replication is enabled.
func (m *Manager) IsEnabled() bool {
	return m.config.Enabled && m.raft != nil
}

// IsLeader returns true if this node is the Raft leader.
func (m *Manager) IsLeader() bool {
	if m.raft == nil {
		return false
	}
	return m.raft.State() == raft.Leader
}

// Leader returns the current leader's address.
func (m *Manager) Leader() string {
	if m.raft == nil {
		return ""
	}
	addr, _ := m.raft.LeaderWithID()
	return string(addr)
}

// LeaderID returns the current leader's node ID.
func (m *Manager) LeaderID() string {
	if m.raft == nil {
		return ""
	}
	_, id := m.raft.LeaderWithID()
	return string(id)
}

// Apply submits an operation to Raft for replication.
func (m *Manager) Apply(ctx context.Context, op *Operation) (*ApplyResult, error) {
	if !m.IsEnabled() {
		return nil, nil
	}

	if m.raft.State() != raft.Leader {
		leaderAddr := m.Leader()
		if leaderAddr == "" {
			return nil, fmt.Errorf("no leader available")
		}
		return nil, fmt.Errorf("not leader, leader is at %s", leaderAddr)
	}

	op.Timestamp = time.Now()

	data, err := json.Marshal(op)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal operation: %w", err)
	}

	future := m.raft.Apply(data, m.config.AckTimeout)

	if m.config.SyncMode {
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
			m.logger.Error("async raft apply failed",
				slog.String("error", err.Error()))
		}
	}()

	return &ApplyResult{}, nil
}

// ApplyAppend submits an append operation to Raft.
func (m *Manager) ApplyAppend(ctx context.Context, queueName string, msg *types.Message) (uint64, error) {
	if !m.IsEnabled() {
		return 0, nil
	}

	op := &Operation{
		Type:      OpAppend,
		QueueName: queueName,
		Message:   msg,
	}

	result, err := m.Apply(ctx, op)
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
func (m *Manager) ApplyTruncate(ctx context.Context, queueName string, minOffset uint64) error {
	if !m.IsEnabled() {
		return nil
	}

	op := &Operation{
		Type:      OpTruncate,
		QueueName: queueName,
		MinOffset: minOffset,
	}

	result, err := m.Apply(ctx, op)
	if err != nil {
		return err
	}

	if result != nil && result.Error != nil {
		return result.Error
	}

	return nil
}

// ApplyCreateGroup submits a create consumer group operation to Raft.
func (m *Manager) ApplyCreateGroup(ctx context.Context, queueName string, group *types.ConsumerGroup) error {
	if !m.IsEnabled() {
		return nil
	}

	op := &Operation{
		Type:       OpCreateGroup,
		QueueName:  queueName,
		GroupID:    group.ID,
		GroupState: group,
	}

	result, err := m.Apply(ctx, op)
	if err != nil {
		return err
	}

	if result != nil && result.Error != nil {
		return result.Error
	}

	return nil
}

// ApplyUpdateCursor submits a cursor update operation to Raft.
func (m *Manager) ApplyUpdateCursor(ctx context.Context, queueName, groupID string, cursor uint64) error {
	if !m.IsEnabled() {
		return nil
	}

	op := &Operation{
		Type:      OpUpdateCursor,
		QueueName: queueName,
		GroupID:   groupID,
		Cursor:    cursor,
	}

	result, err := m.Apply(ctx, op)
	if err != nil {
		return err
	}

	if result != nil && result.Error != nil {
		return result.Error
	}

	return nil
}

// ApplyAddPending submits an add pending entry operation to Raft.
func (m *Manager) ApplyAddPending(ctx context.Context, queueName, groupID string, entry *types.PendingEntry) error {
	if !m.IsEnabled() {
		return nil
	}

	op := &Operation{
		Type:         OpAddPending,
		QueueName:    queueName,
		GroupID:      groupID,
		PendingEntry: entry,
	}

	result, err := m.Apply(ctx, op)
	if err != nil {
		return err
	}

	if result != nil && result.Error != nil {
		return result.Error
	}

	return nil
}

// ApplyRemovePending submits a remove pending entry operation to Raft (ACK).
func (m *Manager) ApplyRemovePending(ctx context.Context, queueName, groupID, consumerID string, offset uint64) error {
	if !m.IsEnabled() {
		return nil
	}

	op := &Operation{
		Type:       OpRemovePending,
		QueueName:  queueName,
		GroupID:    groupID,
		ConsumerID: consumerID,
		Offset:     offset,
	}

	result, err := m.Apply(ctx, op)
	if err != nil {
		return err
	}

	if result != nil && result.Error != nil {
		return result.Error
	}

	return nil
}

// ApplyRegisterConsumer submits a register consumer operation to Raft.
func (m *Manager) ApplyRegisterConsumer(ctx context.Context, queueName string, groupID string, consumer *types.ConsumerInfo) error {
	if !m.IsEnabled() {
		return nil
	}

	op := &Operation{
		Type:         OpRegisterConsumer,
		QueueName:    queueName,
		GroupID:      groupID,
		ConsumerInfo: consumer,
	}

	result, err := m.Apply(ctx, op)
	if err != nil {
		return err
	}

	if result != nil && result.Error != nil {
		return result.Error
	}

	return nil
}

// ApplyUnregisterConsumer submits an unregister consumer operation to Raft.
func (m *Manager) ApplyUnregisterConsumer(ctx context.Context, queueName string, groupID, consumerID string) error {
	if !m.IsEnabled() {
		return nil
	}

	op := &Operation{
		Type:       OpUnregisterConsumer,
		QueueName:  queueName,
		GroupID:    groupID,
		ConsumerID: consumerID,
	}

	result, err := m.Apply(ctx, op)
	if err != nil {
		return err
	}

	if result != nil && result.Error != nil {
		return result.Error
	}

	return nil
}

// GetStats returns Raft statistics.
func (m *Manager) GetStats() map[string]string {
	if m.raft == nil {
		return nil
	}
	return m.raft.Stats()
}

// WaitForLeader waits for a leader to be elected.
func (m *Manager) WaitForLeader(ctx context.Context, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("timeout waiting for leader")
		case <-ticker.C:
			if m.Leader() != "" {
				return nil
			}
		}
	}
}
