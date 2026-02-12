// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package raft

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"

	"github.com/absmach/fluxmq/queue/types"
)

const (
	// DefaultGroupID is used when a queue does not explicitly set a Raft group.
	DefaultGroupID = "default"
)

// QueueCoordinator routes queue operations to Raft replication domains.
//
// The current implementation can map queues to logical groups and delegate all
// groups to one underlying replicator. Future work can register dedicated
// replicators per group without changing queue manager call sites.
type QueueCoordinator interface {
	Stop() error
	IsEnabled() bool
	IsLeaderForQueue(queueName string) bool
	LeaderForQueue(queueName string) string
	LeaderIDForQueue(queueName string) string
	ApplyAppendWithOptions(ctx context.Context, queueName string, msg *types.Message, opts ApplyOptions) (uint64, error)
	EnsureQueue(ctx context.Context, cfg types.QueueConfig) error
	UpdateQueue(ctx context.Context, cfg types.QueueConfig) error
	DeleteQueue(ctx context.Context, queueName string) error
}

// GroupReplicator is a low-level Raft apply/leader API for one replication
// domain. The existing single-group Manager already satisfies this interface.
type GroupReplicator interface {
	Stop() error
	IsEnabled() bool
	IsLeader() bool
	Leader() string
	LeaderID() string
	ApplyAppendWithOptions(ctx context.Context, queueName string, msg *types.Message, opts ApplyOptions) (uint64, error)
}

// LogicalGroupCoordinator maps queues to logical Raft groups.
//
// Unknown groups fall back to the default replicator so callers can assign
// group IDs now and wire dedicated group replicators incrementally.
type LogicalGroupCoordinator struct {
	logger *slog.Logger

	defaultGroup      string
	defaultReplicator GroupReplicator

	mu           sync.RWMutex
	groupMembers map[string]GroupReplicator
	queueGroups  map[string]string
}

// NewLogicalGroupCoordinator creates a queue coordinator with one default
// underlying replicator.
func NewLogicalGroupCoordinator(defaultReplicator GroupReplicator, logger *slog.Logger) *LogicalGroupCoordinator {
	if logger == nil {
		logger = slog.Default()
	}

	c := &LogicalGroupCoordinator{
		logger:            logger,
		defaultGroup:      DefaultGroupID,
		defaultReplicator: defaultReplicator,
		groupMembers:      make(map[string]GroupReplicator),
		queueGroups:       make(map[string]string),
	}
	if defaultReplicator != nil {
		c.groupMembers[c.defaultGroup] = defaultReplicator
	}

	return c
}

// RegisterGroup wires a dedicated replicator for a logical group.
func (c *LogicalGroupCoordinator) RegisterGroup(groupID string, replicator GroupReplicator) {
	if replicator == nil {
		return
	}

	gid := normalizeGroupID(groupID)

	c.mu.Lock()
	c.groupMembers[gid] = replicator
	c.mu.Unlock()
}

// GroupForQueue returns the logical group assigned to a queue.
func (c *LogicalGroupCoordinator) GroupForQueue(queueName string) string {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if gid, ok := c.queueGroups[queueName]; ok && gid != "" {
		return gid
	}

	return c.defaultGroup
}

func (c *LogicalGroupCoordinator) ensureQueueAssignment(cfg types.QueueConfig) {
	if !cfg.Replication.Enabled {
		delete(c.queueGroups, cfg.Name)
		return
	}

	c.queueGroups[cfg.Name] = normalizeGroupID(cfg.Replication.Group)
}

// EnsureQueue syncs queue->group mapping on queue create/bootstrap.
func (c *LogicalGroupCoordinator) EnsureQueue(_ context.Context, cfg types.QueueConfig) error {
	c.mu.Lock()
	c.ensureQueueAssignment(cfg)
	c.mu.Unlock()

	return nil
}

// UpdateQueue syncs queue->group mapping on queue updates.
func (c *LogicalGroupCoordinator) UpdateQueue(_ context.Context, cfg types.QueueConfig) error {
	c.mu.Lock()
	c.ensureQueueAssignment(cfg)
	c.mu.Unlock()

	return nil
}

// DeleteQueue removes queue->group mapping on queue deletion.
func (c *LogicalGroupCoordinator) DeleteQueue(_ context.Context, queueName string) error {
	c.mu.Lock()
	delete(c.queueGroups, queueName)
	c.mu.Unlock()
	return nil
}

func (c *LogicalGroupCoordinator) replicatorForQueue(queueName string) GroupReplicator {
	c.mu.RLock()
	defer c.mu.RUnlock()

	gid := c.defaultGroup
	if qgid, ok := c.queueGroups[queueName]; ok && qgid != "" {
		gid = qgid
	}

	if replicator, ok := c.groupMembers[gid]; ok && replicator != nil {
		return replicator
	}

	return c.defaultReplicator
}

// IsEnabled reports whether any configured replicator is enabled.
func (c *LogicalGroupCoordinator) IsEnabled() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()

	for _, replicator := range c.groupMembers {
		if replicator != nil && replicator.IsEnabled() {
			return true
		}
	}

	return false
}

// IsLeaderForQueue reports leader status of the queue's assigned group.
func (c *LogicalGroupCoordinator) IsLeaderForQueue(queueName string) bool {
	replicator := c.replicatorForQueue(queueName)
	if replicator == nil {
		return false
	}
	return replicator.IsLeader()
}

// LeaderForQueue returns the leader address for the queue's assigned group.
func (c *LogicalGroupCoordinator) LeaderForQueue(queueName string) string {
	replicator := c.replicatorForQueue(queueName)
	if replicator == nil {
		return ""
	}
	return replicator.Leader()
}

// LeaderIDForQueue returns the leader node ID for the queue's assigned group.
func (c *LogicalGroupCoordinator) LeaderIDForQueue(queueName string) string {
	replicator := c.replicatorForQueue(queueName)
	if replicator == nil {
		return ""
	}
	return replicator.LeaderID()
}

// ApplyAppendWithOptions replicates append operation in the queue's group.
func (c *LogicalGroupCoordinator) ApplyAppendWithOptions(ctx context.Context, queueName string, msg *types.Message, opts ApplyOptions) (uint64, error) {
	replicator := c.replicatorForQueue(queueName)
	if replicator == nil {
		return 0, fmt.Errorf("no raft replicator configured for queue %q", queueName)
	}
	return replicator.ApplyAppendWithOptions(ctx, queueName, msg, opts)
}

// Stop shuts down all unique registered replicators.
func (c *LogicalGroupCoordinator) Stop() error {
	c.mu.RLock()
	replicators := make(map[GroupReplicator]struct{}, len(c.groupMembers))
	for _, replicator := range c.groupMembers {
		if replicator != nil {
			replicators[replicator] = struct{}{}
		}
	}
	c.mu.RUnlock()

	var firstErr error
	for replicator := range replicators {
		if err := replicator.Stop(); err != nil && firstErr == nil {
			firstErr = err
		}
	}

	return firstErr
}

func normalizeGroupID(groupID string) string {
	if strings.TrimSpace(groupID) == "" {
		return DefaultGroupID
	}
	return strings.TrimSpace(groupID)
}
