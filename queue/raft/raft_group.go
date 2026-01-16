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
	"sync/atomic"
	"time"

	"github.com/absmach/fluxmq/queue/storage"
	"github.com/dgraph-io/badger/v4"
	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
)

// RaftGroup manages a single Raft group for one queue partition.
// Each partition has its own independent Raft group with a leader and followers.
type RaftGroup struct {
	queueName   string
	partitionID int
	nodeID      string // This node's Raft ID
	bindAddr    string // Raft transport bind address

	// Raft core
	raft *raft.Raft
	fsm  *PartitionFSM

	// Storage backends
	logStore      *BadgerLogStore
	stableStore   *BadgerStableStore
	snapshotStore raft.SnapshotStore
	transport     raft.Transport

	// BadgerDB instances
	raftDB *badger.DB // Raft log and metadata

	// Configuration
	syncMode   bool          // sync=wait for quorum, async=leader returns immediately
	ackTimeout time.Duration // Timeout for sync operations

	// State tracking
	isLeader atomic.Bool
	leaderCh chan bool // Leadership change notifications

	logger *slog.Logger
}

// RaftGroupConfig contains configuration for a Raft group.
type RaftGroupConfig struct {
	QueueName   string
	PartitionID int
	NodeID      string
	BindAddr    string
	DataDir     string

	// Replication settings
	SyncMode   bool
	AckTimeout time.Duration

	// Raft tuning
	HeartbeatTimeout  time.Duration
	ElectionTimeout   time.Duration
	SnapshotInterval  time.Duration
	SnapshotThreshold uint64

	MessageStore storage.MessageStore
	Logger       *slog.Logger
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
		queueName:   cfg.QueueName,
		partitionID: cfg.PartitionID,
		nodeID:      cfg.NodeID,
		bindAddr:    cfg.BindAddr,
		syncMode:    cfg.SyncMode,
		ackTimeout:  cfg.AckTimeout,
		leaderCh:    make(chan bool, 10),
		logger:      cfg.Logger,
	}

	// Create data directory for this partition
	partitionDir := filepath.Join(cfg.DataDir, fmt.Sprintf("partition-%d", cfg.PartitionID))
	raftDir := filepath.Join(partitionDir, "raft")
	if err := os.MkdirAll(raftDir, 0o755); err != nil {
		return nil, fmt.Errorf("failed to create raft directory: %w", err)
	}

	// Open BadgerDB for Raft log and metadata
	opts := badger.DefaultOptions(raftDir)
	opts.Logger = nil // Disable BadgerDB logging
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
	rg.fsm = NewPartitionFSM(cfg.QueueName, cfg.PartitionID, cfg.MessageStore, cfg.Logger)

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

	// Use hclog with our slog logger as backend
	raftCfg.Logger = hclog.New(&hclog.LoggerOptions{
		Name:   fmt.Sprintf("%s-p%d", cfg.QueueName, cfg.PartitionID),
		Level:  hclog.Info,
		Output: os.Stderr,
	})

	// Create Raft instance
	r, err := raft.NewRaft(raftCfg, rg.fsm, rg.logStore, rg.stableStore, rg.snapshotStore, rg.transport)
	if err != nil {
		transport.Close()
		raftDB.Close()
		return nil, fmt.Errorf("failed to create raft: %w", err)
	}
	rg.raft = r

	// Monitor leadership changes
	go rg.monitorLeadership()

	rg.logger.Info("raft group created",
		slog.String("queue", cfg.QueueName),
		slog.Int("partition", cfg.PartitionID),
		slog.String("node_id", cfg.NodeID),
		slog.String("bind_addr", cfg.BindAddr))

	return rg, nil
}

// Bootstrap initializes the Raft cluster with the initial set of peers.
// This should only be called once when creating a new cluster.
func (rg *RaftGroup) Bootstrap(peers []raft.Server) error {
	// Check if already bootstrapped
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

	// Bootstrap with initial configuration
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
// In sync mode, waits for quorum ACK. In async mode, returns immediately after leader accepts.
func (rg *RaftGroup) Apply(op *Operation, timeout time.Duration) error {
	if !rg.IsLeader() {
		return fmt.Errorf("not leader")
	}

	op.Timestamp = time.Now()
	data, err := json.Marshal(op)
	if err != nil {
		return fmt.Errorf("failed to marshal operation: %w", err)
	}

	future := rg.raft.Apply(data, timeout)

	if rg.syncMode {
		// Sync mode: wait for commit
		if err := future.Error(); err != nil {
			return fmt.Errorf("raft apply failed: %w", err)
		}

		// Check FSM result
		if result := future.Response(); result != nil {
			if err, ok := result.(error); ok {
				return err
			}
		}

		return nil
	}

	// Async mode: return immediately
	// The apply will continue in background
	go func() {
		if err := future.Error(); err != nil {
			rg.logger.Error("async raft apply failed",
				slog.String("queue", rg.queueName),
				slog.Int("partition", rg.partitionID),
				slog.String("error", err.Error()))
		}
	}()

	return nil
}

// IsLeader returns true if this node is the Raft leader for this partition.
func (rg *RaftGroup) IsLeader() bool {
	return rg.isLeader.Load()
}

// Leader returns the current leader's node ID.
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

	// Shutdown Raft
	if rg.raft != nil {
		future := rg.raft.Shutdown()
		if err := future.Error(); err != nil {
			rg.logger.Error("raft shutdown error",
				slog.String("queue", rg.queueName),
				slog.Int("partition", rg.partitionID),
				slog.String("error", err.Error()))
		}
	}

	// Transport is closed by Raft.Shutdown()

	// Close BadgerDB
	if rg.raftDB != nil {
		if err := rg.raftDB.Close(); err != nil {
			rg.logger.Error("raft db close error",
				slog.String("error", err.Error()))
		}
	}

	close(rg.leaderCh)

	return nil
}

// monitorLeadership watches for leadership changes.
func (rg *RaftGroup) monitorLeadership() {
	for {
		select {
		case isLeader := <-rg.raft.LeaderCh():
			rg.isLeader.Store(isLeader)

			if isLeader {
				rg.logger.Info("became leader",
					slog.String("queue", rg.queueName),
					slog.Int("partition", rg.partitionID))
			} else {
				rg.logger.Info("lost leadership",
					slog.String("queue", rg.queueName),
					slog.Int("partition", rg.partitionID))
			}

			select {
			case rg.leaderCh <- isLeader:
			default:
			}
		}
	}
}
