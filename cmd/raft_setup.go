// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"fmt"
	"log/slog"
	"path/filepath"
	"sort"
	"strings"
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

	coordinator := qraft.NewLogicalGroupCoordinator(defaultManager, logger)
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
