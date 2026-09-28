// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package queue

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"

	"github.com/absmach/fluxmq/queue/raft"
	"github.com/absmach/fluxmq/queue/storage"
	"github.com/absmach/fluxmq/queue/types"
)

// protectedQueueRegistry owns the mutable protected-contract snapshot. Its lock
// spans both contract validation and queue mutations, so a reload cannot race a
// create, update or delete that was validated against the previous snapshot.
// It is shared by the facade and record services, so the core never has to
// reach back into Manager to observe a reload.
type protectedQueueRegistry struct {
	mu        sync.RWMutex
	contracts map[string]types.QueueConfig
	configErr error
}

func (r *protectedQueueRegistry) contract(queueName string) (types.QueueConfig, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	contract, protected := r.contracts[queueName]
	if !protected {
		return types.QueueConfig{}, false
	}
	return cloneQueueConfig(contract), true
}

func (r *protectedQueueRegistry) validateMutationLocked(config types.QueueConfig) error {
	expected, protected := r.contracts[config.Name]
	if !protected {
		return nil
	}
	if err := protectedQueueContractMismatch(expected, config); err != nil {
		return fmt.Errorf("%w: %v", ErrProtectedQueueMutation, err)
	}
	return nil
}

// replicationRuntime is the installable replication capability. The manager
// and record services share this slot, allowing a coordinator to be installed
// after construction without a back-reference from either core component.
type replicationRuntime struct {
	mu          sync.RWMutex
	coordinator queueRaftCoordinator
}

func (r *replicationRuntime) get() queueRaftCoordinator {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.coordinator
}

func (r *replicationRuntime) set(coordinator queueRaftCoordinator) {
	r.mu.Lock()
	r.coordinator = coordinator
	r.mu.Unlock()
}

// queueControl owns queue-creation policy and the registries record semantics
// consult. It is constructed independently, then shared by Manager and
// recordCore; no method calls back into the facade.
type queueControl struct {
	queueStore           storage.QueueStore
	delivery             deliveryScheduler
	logger               *slog.Logger
	protected            *protectedQueueRegistry
	replication          *replicationRuntime
	writePolicy          WritePolicy
	defaultAckDurability AckDurability

	// configuredReplicated names the replicated queues declared in the
	// broker's configuration. Set at construction and never changed, so it is
	// read without a lock.
	configuredReplicated map[string]struct{}
}

var _ recordServices = (*queueControl)(nil)

func (c *queueControl) protectedQueueContract(queueName string) (types.QueueConfig, bool) {
	return c.protected.contract(queueName)
}

func (c *queueControl) replicationCoordinator() queueRaftCoordinator {
	return c.replication.get()
}

// replicationWriteReadiness reports whether a write to a replicated queue may
// proceed from this node.
//
// Beyond the group being usable, the leader refuses writes to a configured
// queue until its settings are Raft state. Its local copy is not: an append
// committed ahead of the settings is replayed, after a crash or onto a stale
// snapshot, into a queue rebuilt with default settings.
func (c *queueControl) replicationWriteReadiness(queueName string) error {
	if err := c.replicationReadiness(queueName); err != nil {
		return err
	}
	return c.configuredQueueReadiness(queueName)
}

func (c *queueControl) configuredQueueReadiness(queueName string) error {
	if _, configured := c.configuredReplicated[queueName]; !configured {
		return nil
	}
	coordinator := c.replication.get()
	if !coordinator.IsLeaderForQueue(queueName) {
		// The leader applies the same check to anything forwarded to it.
		return nil
	}
	recorder, ok := coordinator.(raft.QueueConfigRecorder)
	if !ok || recorder.IsQueueConfigRecorded(queueName) {
		return nil
	}
	return WithFailure(
		fmt.Errorf("%w: queue %q settings are not yet recorded in its raft log", ErrReplicationUnavailable, queueName),
		Failure{
			Code:       ErrorCodeUnavailable,
			Retryable:  true,
			Durability: DurabilityNotAttempted,
		},
	)
}

// replicationReadiness reports whether the queue's Raft group can take a
// write from this node, directly or by forwarding to its leader.
func (c *queueControl) replicationReadiness(queueName string) error {
	coordinator := c.replication.get()
	if coordinator == nil || !coordinator.IsEnabled() {
		return fmt.Errorf("%w: queue %q has no enabled raft coordinator", ErrReplicationUnavailable, queueName)
	}
	if !coordinator.IsQueueReplicated(queueName) {
		return fmt.Errorf("%w: queue %q has no usable raft group", ErrReplicationUnavailable, queueName)
	}
	if coordinator.IsLeaderForQueue(queueName) {
		return nil
	}
	leaderID := coordinator.LeaderIDForQueue(queueName)
	leaderAddr := coordinator.LeaderForQueue(queueName)
	if leaderID == "" && leaderAddr == "" {
		return fmt.Errorf("%w: queue %q has no raft leader", ErrReplicationUnavailable, queueName)
	}
	if c.writePolicy == WritePolicyForward && leaderID == "" {
		return fmt.Errorf("%w: queue %q leader has no routable node ID", ErrReplicationUnavailable, queueName)
	}
	return nil
}

func (c *queueControl) validateQueueReplication(ctx context.Context, queueCfg types.QueueConfig) error {
	if err := queueCfg.Validate(); err != nil {
		return fmt.Errorf("queue %q configuration: %w", queueCfg.Name, err)
	}
	if !queueCfg.Replication.Enabled {
		return nil
	}
	if c.writePolicy != WritePolicyReject && c.writePolicy != WritePolicyForward {
		return fmt.Errorf("%w: queue %q requires reject or forward, got %q", ErrReplicationWritePolicy, queueCfg.Name, c.writePolicy)
	}
	coordinator := c.replication.get()
	if coordinator == nil || !coordinator.IsEnabled() {
		return fmt.Errorf("%w: queue %q requires enabled raft", ErrReplicationUnavailable, queueCfg.Name)
	}
	if validator, ok := coordinator.(queueReplicationValidator); ok {
		if err := validator.ValidateQueueReplication(ctx, queueCfg); err != nil {
			return fmt.Errorf("%w: queue %q: %v", ErrReplicationUnavailable, queueCfg.Name, err)
		}
	}
	if err := coordinator.EnsureQueue(ctx, queueCfg); err != nil {
		return fmt.Errorf("%w: queue %q group: %v", ErrReplicationUnavailable, queueCfg.Name, err)
	}
	// Validation precedes recording a configured queue's settings, so it
	// cannot wait for them.
	return c.replicationReadiness(queueCfg.Name)
}

func (c *queueControl) validateQueueAckDurability(queueConfig types.QueueConfig) error {
	configured := strings.ToLower(strings.TrimSpace(queueConfig.AckDurability))
	switch configured {
	case "", string(AckDurabilityFsync), string(AckDurabilityBuffered):
	default:
		return fmt.Errorf("queue %q ack_durability must be one of: %s, %s",
			queueConfig.Name, AckDurabilityFsync, AckDurabilityBuffered)
	}

	effective := AckDurability(configured)
	if configured == "" {
		effective = NormalizeAckDurability(c.defaultAckDurability)
	}
	if queueConfig.Durable && queueConfig.Replication.Enabled && effective == AckDurabilityFsync {
		return fmt.Errorf("%w: %s", ErrFsyncReplicatedQueueUnsupported, queueConfig.Name)
	}
	return nil
}

func (c *queueControl) CreateQueue(ctx context.Context, config types.QueueConfig) error {
	if err := types.ValidateTopicFilters(config.Topics); err != nil {
		return err
	}
	if err := c.validateQueueAckDurability(config); err != nil {
		return err
	}
	c.protected.mu.RLock()
	defer c.protected.mu.RUnlock()
	if err := c.protected.validateMutationLocked(config); err != nil {
		return err
	}
	if err := c.validateQueueReplication(ctx, config); err != nil {
		return err
	}

	coordinator := c.replication.get()
	if config.Replication.Enabled {
		if err := coordinator.ApplyCreateQueue(ctx, config); err != nil {
			return err
		}
		if err := c.queueStore.CreateQueue(ctx, config); err != nil && !errors.Is(err, storage.ErrQueueAlreadyExists) {
			return err
		}
	} else {
		if err := c.queueStore.CreateQueue(ctx, config); err != nil {
			return err
		}
		if coordinator != nil {
			if err := coordinator.EnsureQueue(ctx, config); err != nil {
				return err
			}
		}
	}
	c.delivery.Schedule(config.Name)
	c.logger.Info("queue created",
		slog.String("queue", config.Name),
		slog.Any("topics", config.Topics))
	return nil
}
