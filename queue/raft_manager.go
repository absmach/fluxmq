// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package queue

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"path/filepath"
	"sort"
	"strconv"
	"sync"

	"github.com/absmach/fluxmq/queue/raft"
	"github.com/absmach/fluxmq/queue/storage"
	"github.com/absmach/fluxmq/queue/types"
	raftlib "github.com/hashicorp/raft"
)

// RaftManager manages Raft groups for all partitions of a queue.
type RaftManager struct {
	queueName    string
	config       types.ReplicationConfig
	nodeID       string
	dataDir      string
	messageStore storage.MessageStore
	logger       *slog.Logger

	// Raft groups per partition
	groups   map[int]*raft.RaftGroup
	groupsMu sync.RWMutex

	// Placement strategy
	placement PlacementStrategy

	// Cluster node addresses for Raft transport
	nodeAddresses map[string]string // nodeID -> raft bind address
}

// RaftManagerConfig contains configuration for creating a RaftManager.
type RaftManagerConfig struct {
	QueueName     string
	Config        types.ReplicationConfig
	NodeID        string
	DataDir       string
	MessageStore  storage.MessageStore
	NodeAddresses map[string]string // nodeID -> raft bind address
	Logger        *slog.Logger
}

// NewRaftManager creates a new Raft manager for a queue.
func NewRaftManager(cfg RaftManagerConfig) (*RaftManager, error) {
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}

	if !cfg.Config.Enabled {
		return nil, fmt.Errorf("replication not enabled for queue %s", cfg.QueueName)
	}

	// Create placement strategy
	var placement PlacementStrategy
	switch cfg.Config.Placement {
	case types.PlacementRoundRobin:
		placement = NewRoundRobinPlacement(cfg.NodeAddresses)
	case types.PlacementManual:
		placement = NewManualPlacement(cfg.Config.ManualReplicas)
	default:
		return nil, fmt.Errorf("unknown placement strategy: %s", cfg.Config.Placement)
	}

	rm := &RaftManager{
		queueName:     cfg.QueueName,
		config:        cfg.Config,
		nodeID:        cfg.NodeID,
		dataDir:       cfg.DataDir,
		messageStore:  cfg.MessageStore,
		logger:        cfg.Logger,
		groups:        make(map[int]*raft.RaftGroup),
		placement:     placement,
		nodeAddresses: cfg.NodeAddresses,
	}

	return rm, nil
}

// StartPartition creates and starts a Raft group for the given partition.
func (rm *RaftManager) StartPartition(ctx context.Context, partitionID int, partitionCount int) error {
	rm.groupsMu.Lock()
	defer rm.groupsMu.Unlock()

	if _, exists := rm.groups[partitionID]; exists {
		return fmt.Errorf("partition %d already started", partitionID)
	}

	// Get replica assignments for this partition
	replicas := rm.placement.AssignReplicas(partitionID, partitionCount, rm.config.ReplicationFactor)

	// Create Raft group configuration
	raftConfig := raft.RaftGroupConfig{
		QueueName:         rm.queueName,
		PartitionID:       partitionID,
		NodeID:            rm.nodeID,
		BindAddr:          rm.getPartitionBindAddress(rm.nodeID, partitionID),
		DataDir:           filepath.Join(rm.dataDir, rm.queueName),
		SyncMode:          rm.config.Mode == types.ReplicationSync,
		AckTimeout:        rm.config.AckTimeout,
		MinInSyncReplicas: rm.config.MinInSyncReplicas,

		HeartbeatTimeout:  rm.config.HeartbeatTimeout,
		ElectionTimeout:   rm.config.ElectionTimeout,
		SnapshotInterval:  rm.config.SnapshotInterval,
		SnapshotThreshold: rm.config.SnapshotThreshold,

		MessageStore: rm.messageStore,
		Logger:       rm.logger,
	}

	// Create Raft group
	group, err := raft.NewRaftGroup(raftConfig)
	if err != nil {
		return fmt.Errorf("failed to create raft group: %w", err)
	}

	// Bootstrap if this is the initial cluster setup
	if err := rm.bootstrapPartition(group, replicas, partitionID); err != nil {
		group.Shutdown()
		return fmt.Errorf("failed to bootstrap partition: %w", err)
	}

	rm.groups[partitionID] = group

	rm.logger.Info("started raft partition",
		slog.String("queue", rm.queueName),
		slog.Int("partition", partitionID),
		slog.String("node_id", rm.nodeID),
		slog.Int("replica_count", len(replicas)))

	return nil
}

// bootstrapPartition bootstraps the Raft cluster for a partition.
func (rm *RaftManager) bootstrapPartition(group *raft.RaftGroup, replicas []string, partitionID int) error {
	// Build server list for bootstrap
	var servers []raftlib.Server
	for _, nodeID := range replicas {
		addr := rm.getPartitionBindAddress(nodeID, partitionID)
		servers = append(servers, raftlib.Server{
			ID:      raftlib.ServerID(nodeID),
			Address: raftlib.ServerAddress(addr),
		})
	}

	return group.Bootstrap(servers)
}

// getBindAddress returns the base Raft bind address for a node.
func (rm *RaftManager) getBindAddress(nodeID string) string {
	if addr, ok := rm.nodeAddresses[nodeID]; ok {
		return addr
	}
	// Fallback: derive from node ID (this should not happen in production)
	return fmt.Sprintf("%s:7946", nodeID)
}

// getPartitionBindAddress returns the Raft bind address for a specific partition on a node.
// Each partition needs its own unique address to avoid port conflicts.
func (rm *RaftManager) getPartitionBindAddress(nodeID string, partitionID int) string {
	baseAddr := rm.getBindAddress(nodeID)

	// Parse the base address to extract host and port
	// Format: "host:port" -> "host:(port + partitionOffset)"
	// Example: node1 base=127.0.0.1:7000, partition 0->7000, partition 1->7100, partition 2->7200

	// For testing with multiple partitions, offset by 100 per partition to avoid conflicts
	// In production, you'd use a multiplexed transport instead
	host, portStr, err := net.SplitHostPort(baseAddr)
	if err != nil {
		// Fallback if parsing fails
		return fmt.Sprintf("%s:%d", baseAddr, 7000+(partitionID*100))
	}

	basePort, err := strconv.Atoi(portStr)
	if err != nil {
		basePort = 7000
	}

	partitionPort := basePort + (partitionID * 100)
	return fmt.Sprintf("%s:%d", host, partitionPort)
}

// StopPartition stops the Raft group for the given partition.
func (rm *RaftManager) StopPartition(partitionID int) error {
	rm.groupsMu.Lock()
	defer rm.groupsMu.Unlock()

	group, exists := rm.groups[partitionID]
	if !exists {
		return fmt.Errorf("partition %d not found", partitionID)
	}

	if err := group.Shutdown(); err != nil {
		rm.logger.Error("failed to shutdown raft group",
			slog.String("queue", rm.queueName),
			slog.Int("partition", partitionID),
			slog.String("error", err.Error()))
		return err
	}

	delete(rm.groups, partitionID)

	rm.logger.Info("stopped raft partition",
		slog.String("queue", rm.queueName),
		slog.Int("partition", partitionID))

	return nil
}

// GetPartitionGroup returns the Raft group for a partition.
func (rm *RaftManager) GetPartitionGroup(partitionID int) (*raft.RaftGroup, error) {
	rm.groupsMu.RLock()
	defer rm.groupsMu.RUnlock()

	group, exists := rm.groups[partitionID]
	if !exists {
		return nil, fmt.Errorf("partition %d not found", partitionID)
	}

	return group, nil
}

// IsLeader returns true if this node is the Raft leader for the partition.
func (rm *RaftManager) IsLeader(partitionID int) bool {
	rm.groupsMu.RLock()
	defer rm.groupsMu.RUnlock()

	group, exists := rm.groups[partitionID]
	if !exists {
		return false
	}

	return group.IsLeader()
}

// ApplyEnqueue replicates an enqueue operation via Raft.
func (rm *RaftManager) ApplyEnqueue(ctx context.Context, partitionID int, msg *types.Message) error {
	group, err := rm.GetPartitionGroup(partitionID)
	if err != nil {
		return err
	}

	op := &raft.Operation{
		Type:    raft.OpEnqueue,
		Message: msg,
	}

	return group.Apply(op, rm.config.AckTimeout)
}

// ApplyAck replicates an ACK operation via Raft.
func (rm *RaftManager) ApplyAck(ctx context.Context, partitionID int, messageID, groupID string) error {
	group, err := rm.GetPartitionGroup(partitionID)
	if err != nil {
		return err
	}

	op := &raft.Operation{
		Type:      raft.OpAck,
		MessageID: messageID,
		GroupID:   groupID,
	}

	return group.Apply(op, rm.config.AckTimeout)
}

// ApplyNack replicates a NACK operation via Raft.
func (rm *RaftManager) ApplyNack(ctx context.Context, partitionID int, messageID, groupID, reason string) error {
	group, err := rm.GetPartitionGroup(partitionID)
	if err != nil {
		return err
	}

	op := &raft.Operation{
		Type:      raft.OpNack,
		MessageID: messageID,
		GroupID:   groupID,
		Reason:    reason,
	}

	return group.Apply(op, rm.config.AckTimeout)
}

// ApplyReject replicates a REJECT operation via Raft.
func (rm *RaftManager) ApplyReject(ctx context.Context, partitionID int, messageID, groupID, reason string) error {
	group, err := rm.GetPartitionGroup(partitionID)
	if err != nil {
		return err
	}

	op := &raft.Operation{
		Type:      raft.OpReject,
		MessageID: messageID,
		GroupID:   groupID,
		Reason:    reason,
	}

	return group.Apply(op, rm.config.AckTimeout)
}

// ApplyUpdateMessage replicates a message update operation via Raft.
// Used by retry manager to update message state to retry.
func (rm *RaftManager) ApplyUpdateMessage(ctx context.Context, partitionID int, msg *types.Message) error {
	group, err := rm.GetPartitionGroup(partitionID)
	if err != nil {
		return err
	}

	op := &raft.Operation{
		Type:    raft.OpUpdateMessage,
		Message: msg,
	}

	return group.Apply(op, rm.config.AckTimeout)
}

// ApplyRetentionDelete replicates a batch retention delete operation via Raft.
func (rm *RaftManager) ApplyRetentionDelete(ctx context.Context, partitionID int, messageIDs []string) error {
	group, err := rm.GetPartitionGroup(partitionID)
	if err != nil {
		return err
	}

	op := &raft.Operation{
		Type:       raft.OpRetentionDelete,
		MessageIDs: messageIDs,
	}

	return group.Apply(op, rm.config.AckTimeout)
}

// ApplyMoveToDLQ replicates a move to DLQ operation via Raft.
// Used by retry manager when max retries are exceeded or timeout occurs.
func (rm *RaftManager) ApplyMoveToDLQ(ctx context.Context, partitionID int, msg *types.Message, dlqTopic string) error {
	group, err := rm.GetPartitionGroup(partitionID)
	if err != nil {
		return err
	}

	op := &raft.Operation{
		Type:     raft.OpMoveToDLQ,
		Message:  msg,
		DLQTopic: dlqTopic,
	}

	return group.Apply(op, rm.config.AckTimeout)
}

// WaitForLeader blocks until the partition has a leader or timeout.
func (rm *RaftManager) WaitForLeader(ctx context.Context, partitionID int) error {
	group, err := rm.GetPartitionGroup(partitionID)
	if err != nil {
		return err
	}

	return group.WaitForLeader(ctx, rm.config.AckTimeout)
}

// Shutdown stops all Raft groups.
func (rm *RaftManager) Shutdown() error {
	rm.groupsMu.Lock()
	defer rm.groupsMu.Unlock()

	var errors []error
	for partitionID, group := range rm.groups {
		if err := group.Shutdown(); err != nil {
			rm.logger.Error("failed to shutdown raft group",
				slog.String("queue", rm.queueName),
				slog.Int("partition", partitionID),
				slog.String("error", err.Error()))
			errors = append(errors, err)
		}
	}

	rm.groups = make(map[int]*raft.RaftGroup)

	if len(errors) > 0 {
		return fmt.Errorf("failed to shutdown %d raft groups", len(errors))
	}

	return nil
}

// GetStats returns statistics for all Raft groups.
func (rm *RaftManager) GetStats() map[int]map[string]string {
	rm.groupsMu.RLock()
	defer rm.groupsMu.RUnlock()

	stats := make(map[int]map[string]string)
	for partitionID, group := range rm.groups {
		stats[partitionID] = group.GetStats()
	}

	return stats
}

// PlacementStrategy determines how replicas are assigned to nodes.
type PlacementStrategy interface {
	// AssignReplicas returns the node IDs that should host replicas for a partition.
	AssignReplicas(partitionID, partitionCount, replicationFactor int) []string
}

// RoundRobinPlacement distributes replicas evenly across nodes.
type RoundRobinPlacement struct {
	nodeIDs []string
}

// NewRoundRobinPlacement creates a round-robin placement strategy.
func NewRoundRobinPlacement(nodeAddresses map[string]string) *RoundRobinPlacement {
	nodeIDs := make([]string, 0, len(nodeAddresses))
	for nodeID := range nodeAddresses {
		nodeIDs = append(nodeIDs, nodeID)
	}
	// Sort node IDs for deterministic placement across all cluster nodes
	sort.Strings(nodeIDs)

	return &RoundRobinPlacement{
		nodeIDs: nodeIDs,
	}
}

// AssignReplicas implements PlacementStrategy.
func (p *RoundRobinPlacement) AssignReplicas(partitionID, partitionCount, replicationFactor int) []string {
	if replicationFactor > len(p.nodeIDs) {
		replicationFactor = len(p.nodeIDs)
	}

	replicas := make([]string, replicationFactor)
	for i := 0; i < replicationFactor; i++ {
		nodeIdx := (partitionID + i) % len(p.nodeIDs)
		replicas[i] = p.nodeIDs[nodeIdx]
	}

	return replicas
}

// ManualPlacement uses operator-specified replica assignments.
type ManualPlacement struct {
	assignments map[int][]string // partitionID -> []nodeID
}

// NewManualPlacement creates a manual placement strategy.
func NewManualPlacement(assignments map[int][]string) *ManualPlacement {
	return &ManualPlacement{
		assignments: assignments,
	}
}

// AssignReplicas implements PlacementStrategy.
func (p *ManualPlacement) AssignReplicas(partitionID, partitionCount, replicationFactor int) []string {
	if replicas, ok := p.assignments[partitionID]; ok {
		return replicas
	}

	return nil
}
