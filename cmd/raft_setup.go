// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"fmt"
	"hash/fnv"
	"log/slog"
	"net"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/absmach/fluxmq/config"
	qraft "github.com/absmach/fluxmq/queue/raft"
	qstorage "github.com/absmach/fluxmq/queue/storage"
)

type raftGroupRuntime struct {
	GroupID string

	BindAddr string
	DataDir  string
	Peers    map[string]string

	ManagerConfig qraft.ManagerConfig
}

func startQueueRaftCoordinator(
	nodeID string,
	raftCfg config.RaftConfig,
	queueStore qstorage.QueueStore,
	groupStore qstorage.ConsumerGroupStore,
	logger *slog.Logger,
) (qraft.QueueCoordinator, *qraft.Manager, []raftGroupRuntime, error) {
	if logger == nil {
		logger = slog.Default()
	}

	runtimes, err := buildRaftGroupRuntimes(raftCfg)
	if err != nil {
		return nil, nil, nil, err
	}
	if len(runtimes) == 0 {
		return nil, nil, nil, nil
	}

	started := make(map[string]*qraft.Manager, len(runtimes))
	startedList := make([]*qraft.Manager, 0, len(runtimes))
	stopStarted := func() {
		for _, rm := range startedList {
			_ = rm.Stop()
		}
	}

	for _, runtime := range runtimes {
		groupLogger := logger.With(slog.String("raft_group", runtime.GroupID))
		rm := qraft.NewManager(
			nodeID,
			runtime.BindAddr,
			runtime.DataDir,
			queueStore,
			groupStore,
			runtime.Peers,
			runtime.ManagerConfig,
			groupLogger,
		)

		if err := rm.Start(context.Background()); err != nil {
			stopStarted()
			return nil, nil, nil, fmt.Errorf("failed to start raft group %q: %w", runtime.GroupID, err)
		}

		started[runtime.GroupID] = rm
		startedList = append(startedList, rm)
	}

	defaultManager := started[qraft.DefaultGroupID]
	if defaultManager == nil {
		stopStarted()
		return nil, nil, nil, fmt.Errorf("default raft group %q must be configured", qraft.DefaultGroupID)
	}

	provisioner := newRaftGroupProvisioner(nodeID, raftCfg, queueStore, groupStore, started, runtimes, logger)
	coordinator := qraft.NewLogicalGroupCoordinatorWithProvisioner(defaultManager, provisioner, logger)
	for groupID, manager := range started {
		if groupID == qraft.DefaultGroupID {
			continue
		}
		coordinator.RegisterGroup(groupID, manager)
	}

	return coordinator, defaultManager, runtimes, nil
}

func buildRaftGroupRuntimes(raftCfg config.RaftConfig) ([]raftGroupRuntime, error) {
	if !raftCfg.Enabled {
		return nil, nil
	}

	defaultOverride, hasDefaultOverride := raftCfg.Groups[qraft.DefaultGroupID]
	if hasDefaultOverride && defaultOverride.Enabled != nil && !*defaultOverride.Enabled {
		return nil, fmt.Errorf("cluster.raft.groups.%s.enabled cannot be false when raft is enabled", qraft.DefaultGroupID)
	}

	runtimes := make([]raftGroupRuntime, 0, len(raftCfg.Groups)+1)

	defaultRuntime, err := resolveRaftGroupRuntime(qraft.DefaultGroupID, raftCfg, defaultOverride, true)
	if err != nil {
		return nil, err
	}
	runtimes = append(runtimes, defaultRuntime)

	groupIDs := make([]string, 0, len(raftCfg.Groups))
	for groupID := range raftCfg.Groups {
		if groupID == qraft.DefaultGroupID {
			continue
		}
		groupIDs = append(groupIDs, groupID)
	}
	sort.Strings(groupIDs)

	for _, groupID := range groupIDs {
		groupCfg := raftCfg.Groups[groupID]
		if groupCfg.Enabled != nil && !*groupCfg.Enabled {
			continue
		}

		runtime, err := resolveRaftGroupRuntime(groupID, raftCfg, groupCfg, false)
		if err != nil {
			return nil, err
		}
		runtimes = append(runtimes, runtime)
	}

	seenBind := map[string]string{}
	for _, runtime := range runtimes {
		if existing, ok := seenBind[runtime.BindAddr]; ok {
			return nil, fmt.Errorf("raft groups %q and %q share bind_addr %q", existing, runtime.GroupID, runtime.BindAddr)
		}
		seenBind[runtime.BindAddr] = runtime.GroupID
	}

	return runtimes, nil
}

func resolveRaftGroupRuntime(
	groupID string,
	base config.RaftConfig,
	override config.RaftGroupConfig,
	allowBaseEndpoints bool,
) (raftGroupRuntime, error) {
	normalizedGroupID := strings.TrimSpace(groupID)
	if normalizedGroupID == "" {
		return raftGroupRuntime{}, fmt.Errorf("raft group id cannot be empty")
	}

	bindAddr := strings.TrimSpace(override.BindAddr)
	if bindAddr == "" && allowBaseEndpoints {
		bindAddr = strings.TrimSpace(base.BindAddr)
	}
	if bindAddr == "" {
		return raftGroupRuntime{}, fmt.Errorf("cluster.raft.groups.%s.bind_addr is required", normalizedGroupID)
	}

	dataDir := strings.TrimSpace(override.DataDir)
	if dataDir == "" {
		baseDir := strings.TrimSpace(base.DataDir)
		if allowBaseEndpoints {
			dataDir = baseDir
		} else {
			dataDir = filepath.Join(baseDir, "groups", normalizedGroupID)
		}
	}
	if dataDir == "" {
		return raftGroupRuntime{}, fmt.Errorf("cluster.raft.groups.%s.data_dir is required", normalizedGroupID)
	}

	peers := copyPeers(override.Peers)
	if len(peers) == 0 && allowBaseEndpoints {
		peers = copyPeers(base.Peers)
	}
	if len(peers) == 0 && !allowBaseEndpoints {
		return raftGroupRuntime{}, fmt.Errorf("cluster.raft.groups.%s.peers is required", normalizedGroupID)
	}

	syncMode := base.SyncMode
	if override.SyncMode != nil {
		syncMode = *override.SyncMode
	}

	runtime := raftGroupRuntime{
		GroupID:  normalizedGroupID,
		BindAddr: bindAddr,
		DataDir:  dataDir,
		Peers:    peers,
		ManagerConfig: qraft.ManagerConfig{
			Enabled:           true,
			ReplicationFactor: coalesceInt(override.ReplicationFactor, base.ReplicationFactor, 3),
			SyncMode:          syncMode,
			MinInSyncReplicas: coalesceInt(override.MinInSyncReplicas, base.MinInSyncReplicas, 2),
			AckTimeout:        coalesceDuration(override.AckTimeout, base.AckTimeout, 5*time.Second),
			HeartbeatTimeout:  coalesceDuration(override.HeartbeatTimeout, base.HeartbeatTimeout, time.Second),
			ElectionTimeout:   coalesceDuration(override.ElectionTimeout, base.ElectionTimeout, 3*time.Second),
			SnapshotInterval:  coalesceDuration(override.SnapshotInterval, base.SnapshotInterval, 5*time.Minute),
			SnapshotThreshold: coalesceUint64(override.SnapshotThreshold, base.SnapshotThreshold, 8192),
		},
	}

	return runtime, nil
}

type raftGroupProvisioner struct {
	nodeID     string
	raftCfg    config.RaftConfig
	queueStore qstorage.QueueStore
	groupStore qstorage.ConsumerGroupStore
	logger     *slog.Logger

	mu       sync.Mutex
	managers map[string]*qraft.Manager
	usedBind map[string]string
}

func newRaftGroupProvisioner(
	nodeID string,
	raftCfg config.RaftConfig,
	queueStore qstorage.QueueStore,
	groupStore qstorage.ConsumerGroupStore,
	started map[string]*qraft.Manager,
	runtimes []raftGroupRuntime,
	logger *slog.Logger,
) *raftGroupProvisioner {
	if logger == nil {
		logger = slog.Default()
	}

	usedBind := make(map[string]string, len(runtimes))
	for _, rt := range runtimes {
		usedBind[rt.BindAddr] = rt.GroupID
	}

	managers := make(map[string]*qraft.Manager, len(started))
	for groupID, manager := range started {
		managers[groupID] = manager
	}

	return &raftGroupProvisioner{
		nodeID:     nodeID,
		raftCfg:    raftCfg,
		queueStore: queueStore,
		groupStore: groupStore,
		logger:     logger,
		managers:   managers,
		usedBind:   usedBind,
	}
}

func (p *raftGroupProvisioner) GetOrCreateGroup(ctx context.Context, groupID string) (qraft.GroupReplicator, error) {
	gid := strings.TrimSpace(groupID)
	if gid == "" {
		gid = qraft.DefaultGroupID
	}

	p.mu.Lock()
	if manager, ok := p.managers[gid]; ok && manager != nil {
		p.mu.Unlock()
		return manager, nil
	}
	p.mu.Unlock()

	groupCfg, configured := p.raftCfg.Groups[gid]
	if configured && groupCfg.Enabled != nil && !*groupCfg.Enabled {
		return nil, fmt.Errorf("raft group %q is disabled", gid)
	}
	if !configured && !p.raftCfg.AutoProvisionGroups {
		return nil, fmt.Errorf("raft group %q is not configured and auto_provision_groups is disabled", gid)
	}

	if !configured {
		derived, err := p.deriveAutoGroupConfig(gid)
		if err != nil {
			return nil, err
		}
		groupCfg = derived
	}

	runtime, err := resolveRaftGroupRuntime(gid, p.raftCfg, groupCfg, false)
	if err != nil {
		return nil, err
	}

	p.mu.Lock()
	if manager, ok := p.managers[gid]; ok && manager != nil {
		p.mu.Unlock()
		return manager, nil
	}
	if existingGroup, ok := p.usedBind[runtime.BindAddr]; ok && existingGroup != gid {
		p.mu.Unlock()
		return nil, fmt.Errorf("raft group %q bind_addr %q collides with group %q", gid, runtime.BindAddr, existingGroup)
	}
	p.mu.Unlock()

	groupLogger := p.logger.With(slog.String("raft_group", gid))
	if p.queueStore == nil || p.groupStore == nil {
		return nil, fmt.Errorf("cannot provision raft group %q: queue/group stores are not configured", gid)
	}
	manager := qraft.NewManager(
		p.nodeID,
		runtime.BindAddr,
		runtime.DataDir,
		p.queueStore,
		p.groupStore,
		runtime.Peers,
		runtime.ManagerConfig,
		groupLogger,
	)

	if err := manager.Start(ctx); err != nil {
		return nil, fmt.Errorf("failed to start raft group %q: %w", gid, err)
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	if existing, ok := p.managers[gid]; ok && existing != nil {
		_ = manager.Stop()
		return existing, nil
	}
	p.managers[gid] = manager
	p.usedBind[runtime.BindAddr] = gid

	p.logger.Info("dynamically provisioned raft group",
		slog.String("group", gid),
		slog.String("bind_addr", runtime.BindAddr),
		slog.String("data_dir", runtime.DataDir))

	return manager, nil
}

func (p *raftGroupProvisioner) deriveAutoGroupConfig(groupID string) (config.RaftGroupConfig, error) {
	baseBind := strings.TrimSpace(p.raftCfg.BindAddr)
	if baseBind == "" {
		return config.RaftGroupConfig{}, fmt.Errorf("cannot auto-provision raft group %q: base bind_addr is empty", groupID)
	}

	const maxAttempts = 32
	for i := 0; i < maxAttempts; i++ {
		tag := groupID
		if i > 0 {
			tag = fmt.Sprintf("%s-%d", groupID, i)
		}

		offset := portOffsetForGroup(tag)
		bindAddr, err := addPortOffset(baseBind, offset)
		if err != nil {
			return config.RaftGroupConfig{}, fmt.Errorf("cannot derive bind_addr for group %q: %w", groupID, err)
		}
		peers, err := addPortOffsetToPeers(p.raftCfg.Peers, offset)
		if err != nil {
			return config.RaftGroupConfig{}, fmt.Errorf("cannot derive peers for group %q: %w", groupID, err)
		}

		p.mu.Lock()
		_, used := p.usedBind[bindAddr]
		p.mu.Unlock()
		if used {
			continue
		}

		return config.RaftGroupConfig{
			BindAddr: bindAddr,
			Peers:    peers,
		}, nil
	}

	return config.RaftGroupConfig{}, fmt.Errorf("unable to auto-provision raft group %q without bind address collisions", groupID)
}

func portOffsetForGroup(groupID string) int {
	hasher := fnv.New32a()
	_, _ = hasher.Write([]byte(groupID))
	return int(hasher.Sum32()%20000) + 1000
}

func addPortOffset(addr string, offset int) (string, error) {
	host, portStr, err := net.SplitHostPort(strings.TrimSpace(addr))
	if err != nil {
		return "", err
	}

	port, err := strconv.Atoi(portStr)
	if err != nil {
		return "", err
	}
	shifted := port + offset
	if shifted <= 0 || shifted > 65535 {
		return "", fmt.Errorf("derived port out of range: %d", shifted)
	}

	return net.JoinHostPort(host, strconv.Itoa(shifted)), nil
}

func addPortOffsetToPeers(peers map[string]string, offset int) (map[string]string, error) {
	if len(peers) == 0 {
		return nil, nil
	}

	out := make(map[string]string, len(peers))
	for nodeID, addr := range peers {
		shifted, err := addPortOffset(addr, offset)
		if err != nil {
			return nil, err
		}
		out[nodeID] = shifted
	}

	return out, nil
}

func copyPeers(peers map[string]string) map[string]string {
	if len(peers) == 0 {
		return nil
	}
	out := make(map[string]string, len(peers))
	for nodeID, addr := range peers {
		out[nodeID] = addr
	}
	return out
}

func coalesceInt(value, fallback, defaultValue int) int {
	switch {
	case value > 0:
		return value
	case fallback > 0:
		return fallback
	default:
		return defaultValue
	}
}

func coalesceDuration(value, fallback, defaultValue time.Duration) time.Duration {
	switch {
	case value > 0:
		return value
	case fallback > 0:
		return fallback
	default:
		return defaultValue
	}
}

func coalesceUint64(value, fallback, defaultValue uint64) uint64 {
	switch {
	case value > 0:
		return value
	case fallback > 0:
		return fallback
	default:
		return defaultValue
	}
}
