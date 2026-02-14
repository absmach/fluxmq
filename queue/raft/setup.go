// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package raft

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
	qstorage "github.com/absmach/fluxmq/queue/storage"
)

type RaftGroupRuntime struct {
	GroupID string

	BindAddr string
	DataDir  string
	Peers    map[string]string

	ManagerConfig ManagerConfig
}

func StartQueueCoordinator(
	nodeID string,
	raftCfg config.RaftConfig,
	queueStore qstorage.QueueStore,
	groupStore qstorage.ConsumerGroupStore,
	logger *slog.Logger,
) (QueueCoordinator, *Manager, []RaftGroupRuntime, error) {
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

	started := make(map[string]*Manager, len(runtimes))
	startedList := make([]*Manager, 0, len(runtimes))
	stopStarted := func() {
		for _, rm := range startedList {
			_ = rm.Stop()
		}
	}

	for _, runtime := range runtimes {
		groupLogger := logger.With(slog.String("raft_group", runtime.GroupID))
		rm := NewManager(
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

	defaultManager := started[DefaultGroupID]
	if defaultManager == nil {
		stopStarted()
		return nil, nil, nil, fmt.Errorf("default raft group %q must be configured", DefaultGroupID)
	}

	provisioner := newRaftGroupProvisioner(nodeID, raftCfg, queueStore, groupStore, started, runtimes, logger)
	coordinator := NewLogicalGroupCoordinatorWithProvisioner(defaultManager, provisioner, logger)
	for groupID, manager := range started {
		if groupID == DefaultGroupID {
			continue
		}
		coordinator.RegisterGroup(groupID, manager)
	}

	return coordinator, defaultManager, runtimes, nil
}

func buildRaftGroupRuntimes(raftCfg config.RaftConfig) ([]RaftGroupRuntime, error) {
	if !raftCfg.Enabled {
		return nil, nil
	}

	defaultOverride, hasDefaultOverride := raftCfg.Groups[DefaultGroupID]
	if hasDefaultOverride && defaultOverride.Enabled != nil && !*defaultOverride.Enabled {
		return nil, fmt.Errorf("cluster.raft.groups.%s.enabled cannot be false when raft is enabled", DefaultGroupID)
	}

	runtimes := make([]RaftGroupRuntime, 0, len(raftCfg.Groups)+1)

	defaultRuntime, err := resolveRaftGroupRuntime(DefaultGroupID, raftCfg, defaultOverride, true)
	if err != nil {
		return nil, err
	}
	runtimes = append(runtimes, defaultRuntime)

	groupIDs := make([]string, 0, len(raftCfg.Groups))
	for groupID := range raftCfg.Groups {
		if groupID == DefaultGroupID {
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
) (RaftGroupRuntime, error) {
	normalizedGroupID := strings.TrimSpace(groupID)
	if normalizedGroupID == "" {
		return RaftGroupRuntime{}, fmt.Errorf("raft group id cannot be empty")
	}

	bindAddr := strings.TrimSpace(override.BindAddr)
	if bindAddr == "" && allowBaseEndpoints {
		bindAddr = strings.TrimSpace(base.BindAddr)
	}
	if bindAddr == "" {
		return RaftGroupRuntime{}, fmt.Errorf("cluster.raft.groups.%s.bind_addr is required", normalizedGroupID)
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
		return RaftGroupRuntime{}, fmt.Errorf("cluster.raft.groups.%s.data_dir is required", normalizedGroupID)
	}

	peers := copyPeers(override.Peers)
	if len(peers) == 0 && allowBaseEndpoints {
		peers = copyPeers(base.Peers)
	}
	if len(peers) == 0 && !allowBaseEndpoints {
		return RaftGroupRuntime{}, fmt.Errorf("cluster.raft.groups.%s.peers is required", normalizedGroupID)
	}

	syncMode := base.SyncMode
	if override.SyncMode != nil {
		syncMode = *override.SyncMode
	}

	runtime := RaftGroupRuntime{
		GroupID:  normalizedGroupID,
		BindAddr: bindAddr,
		DataDir:  dataDir,
		Peers:    peers,
		ManagerConfig: ManagerConfig{
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

	mu           sync.Mutex
	managers     map[string]*Manager
	inflight     map[string]chan struct{} // closed when provisioning completes (success or failure)
	usedBind     map[string]string
	bindByGroup  map[string]string
	staticGroups map[string]struct{}
}

func newRaftGroupProvisioner(
	nodeID string,
	raftCfg config.RaftConfig,
	queueStore qstorage.QueueStore,
	groupStore qstorage.ConsumerGroupStore,
	started map[string]*Manager,
	runtimes []RaftGroupRuntime,
	logger *slog.Logger,
) *raftGroupProvisioner {
	if logger == nil {
		logger = slog.Default()
	}

	usedBind := make(map[string]string, len(runtimes))
	bindByGroup := make(map[string]string, len(runtimes))
	staticGroups := make(map[string]struct{}, len(runtimes)+len(started))
	for _, rt := range runtimes {
		usedBind[rt.BindAddr] = rt.GroupID
		bindByGroup[rt.GroupID] = rt.BindAddr
		staticGroups[rt.GroupID] = struct{}{}
	}

	managers := make(map[string]*Manager, len(started))
	for groupID, manager := range started {
		managers[groupID] = manager
		staticGroups[groupID] = struct{}{}
	}

	return &raftGroupProvisioner{
		nodeID:       nodeID,
		raftCfg:      raftCfg,
		queueStore:   queueStore,
		groupStore:   groupStore,
		logger:       logger,
		managers:     managers,
		inflight:     make(map[string]chan struct{}),
		usedBind:     usedBind,
		bindByGroup:  bindByGroup,
		staticGroups: staticGroups,
	}
}

func (p *raftGroupProvisioner) GetOrCreateGroup(ctx context.Context, groupID string) (GroupReplicator, error) {
	gid := strings.TrimSpace(groupID)
	if gid == "" {
		gid = DefaultGroupID
	}

	for {
		p.mu.Lock()

		// Fast path: already provisioned.
		if manager, ok := p.managers[gid]; ok && manager != nil {
			p.mu.Unlock()
			return manager, nil
		}

		// Another goroutine is provisioning this group — wait for it.
		if ch, ok := p.inflight[gid]; ok {
			p.mu.Unlock()
			<-ch
			continue // re-check managers after wakeup
		}

		// This goroutine wins: resolve config and reserve the slot.
		groupCfg, configured := p.raftCfg.Groups[gid]
		if configured && groupCfg.Enabled != nil && !*groupCfg.Enabled {
			p.mu.Unlock()
			return nil, fmt.Errorf("raft group %q is disabled", gid)
		}
		if !configured && !p.raftCfg.AutoProvisionGroups {
			p.mu.Unlock()
			return nil, fmt.Errorf("raft group %q is not configured and auto_provision_groups is disabled", gid)
		}

		if !configured {
			derived, err := p.deriveAutoGroupConfig(gid)
			if err != nil {
				p.mu.Unlock()
				return nil, err
			}
			groupCfg = derived
		}

		runtime, err := resolveRaftGroupRuntime(gid, p.raftCfg, groupCfg, false)
		if err != nil {
			p.mu.Unlock()
			return nil, err
		}

		if existingGroup, ok := p.usedBind[runtime.BindAddr]; ok && existingGroup != gid {
			p.mu.Unlock()
			return nil, fmt.Errorf("raft group %q bind_addr %q collides with group %q", gid, runtime.BindAddr, existingGroup)
		}

		// Reserve bind address and mark provisioning in-flight.
		done := make(chan struct{})
		p.inflight[gid] = done
		p.usedBind[runtime.BindAddr] = gid
		p.bindByGroup[gid] = runtime.BindAddr
		p.mu.Unlock()

		manager, startErr := p.startManager(ctx, gid, runtime)

		p.mu.Lock()
		delete(p.inflight, gid)
		if startErr != nil {
			// Roll back bind reservation only — no manager was stored.
			if p.usedBind[runtime.BindAddr] == gid {
				delete(p.usedBind, runtime.BindAddr)
			}
			delete(p.bindByGroup, gid)
			close(done)
			p.mu.Unlock()
			return nil, startErr
		}
		p.managers[gid] = manager
		close(done)
		p.mu.Unlock()

		p.logger.Info("dynamically provisioned raft group",
			slog.String("group", gid),
			slog.String("bind_addr", runtime.BindAddr),
			slog.String("data_dir", runtime.DataDir))

		return manager, nil
	}
}

func (p *raftGroupProvisioner) startManager(ctx context.Context, gid string, runtime RaftGroupRuntime) (*Manager, error) {
	groupLogger := p.logger.With(slog.String("raft_group", gid))
	if p.queueStore == nil || p.groupStore == nil {
		return nil, fmt.Errorf("cannot provision raft group %q: queue/group stores are not configured", gid)
	}
	manager := NewManager(
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

	return manager, nil
}

func (p *raftGroupProvisioner) TryReleaseGroup(_ context.Context, groupID string) (bool, error) {
	gid := strings.TrimSpace(groupID)
	if gid == "" {
		gid = DefaultGroupID
	}

	p.mu.Lock()
	if _, static := p.staticGroups[gid]; static {
		p.mu.Unlock()
		return false, nil
	}
	manager, exists := p.managers[gid]
	if !exists {
		p.mu.Unlock()
		return false, nil
	}
	bindAddr := p.bindByGroup[gid]
	p.mu.Unlock()

	if manager != nil {
		if err := manager.Stop(); err != nil {
			return false, fmt.Errorf("failed to stop raft group %q: %w", gid, err)
		}
	}

	p.mu.Lock()
	current, ok := p.managers[gid]
	if !ok || current != manager {
		p.mu.Unlock()
		return false, nil
	}
	delete(p.managers, gid)
	delete(p.bindByGroup, gid)
	if bindAddr != "" && p.usedBind[bindAddr] == gid {
		delete(p.usedBind, bindAddr)
	}
	p.mu.Unlock()

	p.logger.Info("released raft group",
		slog.String("group", gid),
		slog.String("bind_addr", bindAddr))

	return true, nil
}

// deriveAutoGroupConfig derives a RaftGroupConfig by hashing the group ID to
// produce a deterministic port offset. Caller must hold p.mu.
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

		if _, used := p.usedBind[bindAddr]; used {
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
	// Range [10000, 20000) keeps derived ports well above typical static
	// group ports (8xxx) and below 65535 for base ports up to ~45000.
	return int(hasher.Sum32()%10000) + 10000
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
