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
	IsQueueReplicated(queueName string) bool
	IsLeaderForQueue(queueName string) bool
	LeaderForQueue(queueName string) string
	LeaderIDForQueue(queueName string) string
	ApplyCreateQueue(ctx context.Context, cfg types.QueueConfig) error
	ApplyUpdateQueue(ctx context.Context, cfg types.QueueConfig) error
	ApplyDeleteQueue(ctx context.Context, queueName string) error
	ApplyAppendWithOptions(ctx context.Context, queueName string, msg *types.Message, opts ApplyOptions) (uint64, error)
	ApplyTruncate(ctx context.Context, queueName string, minOffset uint64) error
	ApplyCreateGroup(ctx context.Context, queueName string, group *types.ConsumerGroup) error
	ApplyUpdateGroup(ctx context.Context, queueName string, group *types.ConsumerGroup) error
	ApplyDeleteGroup(ctx context.Context, queueName, groupID string) error
	ApplyUpdateCursor(ctx context.Context, queueName, groupID string, cursor uint64) error
	ApplyUpdateCommitted(ctx context.Context, queueName, groupID string, committed uint64) error
	ApplyAddPending(ctx context.Context, queueName, groupID string, entry *types.PendingEntry) error
	ApplyRemovePending(ctx context.Context, queueName, groupID, consumerID string, offset uint64) error
	ApplyTransferPending(ctx context.Context, queueName, groupID string, offset uint64, fromConsumer, toConsumer string) error
	ApplyRegisterConsumer(ctx context.Context, queueName, groupID string, consumer *types.ConsumerInfo) error
	ApplyUnregisterConsumer(ctx context.Context, queueName, groupID, consumerID string) error
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
	ApplyCreateQueue(ctx context.Context, cfg types.QueueConfig) error
	ApplyUpdateQueue(ctx context.Context, cfg types.QueueConfig) error
	ApplyDeleteQueue(ctx context.Context, queueName string) error
	ApplyAppendWithOptions(ctx context.Context, queueName string, msg *types.Message, opts ApplyOptions) (uint64, error)
	ApplyTruncate(ctx context.Context, queueName string, minOffset uint64) error
	ApplyCreateGroup(ctx context.Context, queueName string, group *types.ConsumerGroup) error
	ApplyUpdateGroup(ctx context.Context, queueName string, group *types.ConsumerGroup) error
	ApplyDeleteGroup(ctx context.Context, queueName, groupID string) error
	ApplyUpdateCursor(ctx context.Context, queueName, groupID string, cursor uint64) error
	ApplyUpdateCommitted(ctx context.Context, queueName, groupID string, committed uint64) error
	ApplyAddPending(ctx context.Context, queueName, groupID string, entry *types.PendingEntry) error
	ApplyRemovePending(ctx context.Context, queueName, groupID, consumerID string, offset uint64) error
	ApplyTransferPending(ctx context.Context, queueName, groupID string, offset uint64, fromConsumer, toConsumer string) error
	ApplyRegisterConsumer(ctx context.Context, queueName, groupID string, consumer *types.ConsumerInfo) error
	ApplyUnregisterConsumer(ctx context.Context, queueName, groupID, consumerID string) error
}

// GroupProvisioner can lazily create/register group replicators on demand.
type GroupProvisioner interface {
	GetOrCreateGroup(ctx context.Context, groupID string) (GroupReplicator, error)
	TryReleaseGroup(ctx context.Context, groupID string) (bool, error)
}

// LogicalGroupCoordinator maps queues to logical Raft groups.
//
// Unknown groups fall back to the default replicator so callers can assign
// group IDs now and wire dedicated group replicators incrementally.
type LogicalGroupCoordinator struct {
	logger *slog.Logger

	defaultGroup      string
	defaultReplicator GroupReplicator
	provisioner       GroupProvisioner

	mu           sync.RWMutex
	groupMembers map[string]GroupReplicator
	queueGroups  map[string]string
}

// NewLogicalGroupCoordinator creates a queue coordinator with one default
// underlying replicator.
func NewLogicalGroupCoordinator(defaultReplicator GroupReplicator, logger *slog.Logger) *LogicalGroupCoordinator {
	return NewLogicalGroupCoordinatorWithProvisioner(defaultReplicator, nil, logger)
}

// NewLogicalGroupCoordinatorWithProvisioner creates a queue coordinator that
// can lazily provision group replicators when queues reference unknown groups.
func NewLogicalGroupCoordinatorWithProvisioner(defaultReplicator GroupReplicator, provisioner GroupProvisioner, logger *slog.Logger) *LogicalGroupCoordinator {
	if logger == nil {
		logger = slog.Default()
	}

	c := &LogicalGroupCoordinator{
		logger:            logger,
		defaultGroup:      DefaultGroupID,
		defaultReplicator: defaultReplicator,
		provisioner:       provisioner,
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

// IsQueueReplicated reports whether queue currently has replication enabled.
func (c *LogicalGroupCoordinator) IsQueueReplicated(queueName string) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()

	_, ok := c.queueGroups[queueName]
	return ok
}

func (c *LogicalGroupCoordinator) ensureQueueAssignment(cfg types.QueueConfig) {
	if !cfg.Replication.Enabled {
		delete(c.queueGroups, cfg.Name)
		return
	}

	c.queueGroups[cfg.Name] = normalizeGroupID(cfg.Replication.Group)
}

func (c *LogicalGroupCoordinator) ensureGroup(ctx context.Context, groupID string) (GroupReplicator, error) {
	gid := normalizeGroupID(groupID)

	c.mu.RLock()
	if replicator, ok := c.groupMembers[gid]; ok && replicator != nil {
		c.mu.RUnlock()
		return replicator, nil
	}
	c.mu.RUnlock()

	if c.provisioner == nil {
		return nil, fmt.Errorf("raft group %q is not configured", gid)
	}

	replicator, err := c.provisioner.GetOrCreateGroup(ctx, gid)
	if err != nil {
		return nil, err
	}
	if replicator == nil {
		return nil, fmt.Errorf("raft group %q provisioner returned nil replicator", gid)
	}

	c.mu.Lock()
	if existing, ok := c.groupMembers[gid]; ok && existing != nil {
		c.mu.Unlock()
		return existing, nil
	}
	c.groupMembers[gid] = replicator
	c.mu.Unlock()

	return replicator, nil
}

// EnsureQueue syncs queue->group mapping on queue create/bootstrap.
func (c *LogicalGroupCoordinator) EnsureQueue(ctx context.Context, cfg types.QueueConfig) error {
	if cfg.Replication.Enabled {
		if _, err := c.ensureGroup(ctx, cfg.Replication.Group); err != nil {
			return err
		}
	}

	c.mu.Lock()
	c.ensureQueueAssignment(cfg)
	c.mu.Unlock()

	return nil
}

// UpdateQueue syncs queue->group mapping on queue updates.
func (c *LogicalGroupCoordinator) UpdateQueue(ctx context.Context, cfg types.QueueConfig) error {
	if cfg.Replication.Enabled {
		if _, err := c.ensureGroup(ctx, cfg.Replication.Group); err != nil {
			return err
		}
	}

	var previousGroup string
	var hadPrevious bool
	var newGroup string
	var hasNew bool

	c.mu.Lock()
	previousGroup, hadPrevious = c.queueGroups[cfg.Name]
	c.ensureQueueAssignment(cfg)
	newGroup, hasNew = c.queueGroups[cfg.Name]
	c.mu.Unlock()

	if hadPrevious && (!hasNew || normalizeGroupID(previousGroup) != normalizeGroupID(newGroup)) {
		if err := c.maybeReleaseGroup(ctx, previousGroup); err != nil {
			return err
		}
	}

	return nil
}

// DeleteQueue removes queue->group mapping on queue deletion.
func (c *LogicalGroupCoordinator) DeleteQueue(ctx context.Context, queueName string) error {
	var previousGroup string
	var hadPrevious bool

	c.mu.Lock()
	previousGroup, hadPrevious = c.queueGroups[queueName]
	delete(c.queueGroups, queueName)
	c.mu.Unlock()

	if hadPrevious {
		if err := c.maybeReleaseGroup(ctx, previousGroup); err != nil {
			return err
		}
	}

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

func (c *LogicalGroupCoordinator) ApplyCreateQueue(ctx context.Context, cfg types.QueueConfig) error {
	replicator, err := c.ensureGroup(ctx, cfg.Replication.Group)
	if err != nil {
		return err
	}
	return replicator.ApplyCreateQueue(ctx, cfg)
}

func (c *LogicalGroupCoordinator) ApplyUpdateQueue(ctx context.Context, cfg types.QueueConfig) error {
	if cfg.Replication.Enabled {
		replicator, err := c.ensureGroup(ctx, cfg.Replication.Group)
		if err != nil {
			return err
		}
		return replicator.ApplyUpdateQueue(ctx, cfg)
	}

	replicator := c.replicatorForQueue(cfg.Name)
	if replicator == nil {
		return fmt.Errorf("no raft replicator configured for queue %q", cfg.Name)
	}
	return replicator.ApplyUpdateQueue(ctx, cfg)
}

func (c *LogicalGroupCoordinator) ApplyDeleteQueue(ctx context.Context, queueName string) error {
	replicator := c.replicatorForQueue(queueName)
	if replicator == nil {
		return fmt.Errorf("no raft replicator configured for queue %q", queueName)
	}
	return replicator.ApplyDeleteQueue(ctx, queueName)
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

func (c *LogicalGroupCoordinator) ApplyTruncate(ctx context.Context, queueName string, minOffset uint64) error {
	replicator := c.replicatorForQueue(queueName)
	if replicator == nil {
		return fmt.Errorf("no raft replicator configured for queue %q", queueName)
	}
	return replicator.ApplyTruncate(ctx, queueName, minOffset)
}

func (c *LogicalGroupCoordinator) ApplyCreateGroup(ctx context.Context, queueName string, group *types.ConsumerGroup) error {
	replicator := c.replicatorForQueue(queueName)
	if replicator == nil {
		return fmt.Errorf("no raft replicator configured for queue %q", queueName)
	}
	return replicator.ApplyCreateGroup(ctx, queueName, group)
}

func (c *LogicalGroupCoordinator) ApplyUpdateGroup(ctx context.Context, queueName string, group *types.ConsumerGroup) error {
	replicator := c.replicatorForQueue(queueName)
	if replicator == nil {
		return fmt.Errorf("no raft replicator configured for queue %q", queueName)
	}
	return replicator.ApplyUpdateGroup(ctx, queueName, group)
}

func (c *LogicalGroupCoordinator) ApplyDeleteGroup(ctx context.Context, queueName, groupID string) error {
	replicator := c.replicatorForQueue(queueName)
	if replicator == nil {
		return fmt.Errorf("no raft replicator configured for queue %q", queueName)
	}
	return replicator.ApplyDeleteGroup(ctx, queueName, groupID)
}

func (c *LogicalGroupCoordinator) ApplyUpdateCursor(ctx context.Context, queueName, groupID string, cursor uint64) error {
	replicator := c.replicatorForQueue(queueName)
	if replicator == nil {
		return fmt.Errorf("no raft replicator configured for queue %q", queueName)
	}
	return replicator.ApplyUpdateCursor(ctx, queueName, groupID, cursor)
}

func (c *LogicalGroupCoordinator) ApplyUpdateCommitted(ctx context.Context, queueName, groupID string, committed uint64) error {
	replicator := c.replicatorForQueue(queueName)
	if replicator == nil {
		return fmt.Errorf("no raft replicator configured for queue %q", queueName)
	}
	return replicator.ApplyUpdateCommitted(ctx, queueName, groupID, committed)
}

func (c *LogicalGroupCoordinator) ApplyAddPending(ctx context.Context, queueName, groupID string, entry *types.PendingEntry) error {
	replicator := c.replicatorForQueue(queueName)
	if replicator == nil {
		return fmt.Errorf("no raft replicator configured for queue %q", queueName)
	}
	return replicator.ApplyAddPending(ctx, queueName, groupID, entry)
}

func (c *LogicalGroupCoordinator) ApplyRemovePending(ctx context.Context, queueName, groupID, consumerID string, offset uint64) error {
	replicator := c.replicatorForQueue(queueName)
	if replicator == nil {
		return fmt.Errorf("no raft replicator configured for queue %q", queueName)
	}
	return replicator.ApplyRemovePending(ctx, queueName, groupID, consumerID, offset)
}

func (c *LogicalGroupCoordinator) ApplyTransferPending(ctx context.Context, queueName, groupID string, offset uint64, fromConsumer, toConsumer string) error {
	replicator := c.replicatorForQueue(queueName)
	if replicator == nil {
		return fmt.Errorf("no raft replicator configured for queue %q", queueName)
	}
	return replicator.ApplyTransferPending(ctx, queueName, groupID, offset, fromConsumer, toConsumer)
}

func (c *LogicalGroupCoordinator) ApplyRegisterConsumer(ctx context.Context, queueName, groupID string, consumer *types.ConsumerInfo) error {
	replicator := c.replicatorForQueue(queueName)
	if replicator == nil {
		return fmt.Errorf("no raft replicator configured for queue %q", queueName)
	}
	return replicator.ApplyRegisterConsumer(ctx, queueName, groupID, consumer)
}

func (c *LogicalGroupCoordinator) ApplyUnregisterConsumer(ctx context.Context, queueName, groupID, consumerID string) error {
	replicator := c.replicatorForQueue(queueName)
	if replicator == nil {
		return fmt.Errorf("no raft replicator configured for queue %q", queueName)
	}
	return replicator.ApplyUnregisterConsumer(ctx, queueName, groupID, consumerID)
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

func (c *LogicalGroupCoordinator) maybeReleaseGroup(ctx context.Context, groupID string) error {
	gid := normalizeGroupID(groupID)
	if gid == c.defaultGroup {
		return nil
	}
	if c.provisioner == nil {
		return nil
	}

	c.mu.RLock()
	for _, qgid := range c.queueGroups {
		if normalizeGroupID(qgid) == gid {
			c.mu.RUnlock()
			return nil
		}
	}
	_, exists := c.groupMembers[gid]
	c.mu.RUnlock()
	if !exists {
		return nil
	}

	released, err := c.provisioner.TryReleaseGroup(ctx, gid)
	if err != nil {
		return err
	}
	if !released {
		return nil
	}

	c.mu.Lock()
	for _, qgid := range c.queueGroups {
		if normalizeGroupID(qgid) == gid {
			c.mu.Unlock()
			return nil
		}
	}
	delete(c.groupMembers, gid)
	c.mu.Unlock()

	return nil
}
