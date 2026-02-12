// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package queue

import (
	"context"
	"sync"

	"github.com/absmach/fluxmq/queue/raft"
	"github.com/absmach/fluxmq/queue/storage"
	"github.com/absmach/fluxmq/queue/types"
)

// raftGroupStore routes mutating consumer-group operations for replicated
// queues through the Raft coordinator on leader nodes while preserving direct
// local reads. On follower nodes, mutations fall back to the local store so
// that consumer operations don't fail — the leader's FSM apply will eventually
// propagate the authoritative state.
type raftGroupStore struct {
	base        storage.ConsumerGroupStore
	coordinator raft.QueueCoordinator

	mu sync.RWMutex
}

func newRaftGroupStore(base storage.ConsumerGroupStore) *raftGroupStore {
	return &raftGroupStore{
		base: base,
	}
}

func (s *raftGroupStore) SetCoordinator(coordinator raft.QueueCoordinator) {
	s.mu.Lock()
	s.coordinator = coordinator
	s.mu.Unlock()
}

// leaderCoordinatorForQueue returns the coordinator only when the queue is
// replicated AND this node is the leader for the queue's Raft group.
// Returns nil otherwise, causing callers to fall back to the local store.
func (s *raftGroupStore) leaderCoordinatorForQueue(queueName string) raft.QueueCoordinator {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.coordinator == nil || !s.coordinator.IsEnabled() {
		return nil
	}
	if !s.coordinator.IsQueueReplicated(queueName) {
		return nil
	}
	if !s.coordinator.IsLeaderForQueue(queueName) {
		return nil
	}

	return s.coordinator
}

func (s *raftGroupStore) CreateConsumerGroup(ctx context.Context, group *types.ConsumerGroup) error {
	if group == nil {
		return s.base.CreateConsumerGroup(ctx, group)
	}

	if coordinator := s.leaderCoordinatorForQueue(group.QueueName); coordinator != nil {
		return coordinator.ApplyCreateGroup(ctx, group.QueueName, group)
	}

	return s.base.CreateConsumerGroup(ctx, group)
}

func (s *raftGroupStore) GetConsumerGroup(ctx context.Context, queueName, groupID string) (*types.ConsumerGroup, error) {
	return s.base.GetConsumerGroup(ctx, queueName, groupID)
}

func (s *raftGroupStore) UpdateConsumerGroup(ctx context.Context, group *types.ConsumerGroup) error {
	// UpdateConsumerGroup writes the full group struct. Individual mutations
	// (cursor, pending, consumer registration) are already replicated via
	// dedicated Raft ops. Full-group updates are bulk metadata writes
	// (rebalance state, mode changes) that are safe to apply locally — the
	// granular ops are the source of truth for replicated fields.
	return s.base.UpdateConsumerGroup(ctx, group)
}

func (s *raftGroupStore) DeleteConsumerGroup(ctx context.Context, queueName, groupID string) error {
	if coordinator := s.leaderCoordinatorForQueue(queueName); coordinator != nil {
		return coordinator.ApplyDeleteGroup(ctx, queueName, groupID)
	}

	return s.base.DeleteConsumerGroup(ctx, queueName, groupID)
}

func (s *raftGroupStore) ListConsumerGroups(ctx context.Context, queueName string) ([]*types.ConsumerGroup, error) {
	return s.base.ListConsumerGroups(ctx, queueName)
}

func (s *raftGroupStore) AddPendingEntry(ctx context.Context, queueName, groupID string, entry *types.PendingEntry) error {
	if coordinator := s.leaderCoordinatorForQueue(queueName); coordinator != nil {
		return coordinator.ApplyAddPending(ctx, queueName, groupID, entry)
	}

	return s.base.AddPendingEntry(ctx, queueName, groupID, entry)
}

func (s *raftGroupStore) RemovePendingEntry(ctx context.Context, queueName, groupID, consumerID string, offset uint64) error {
	if coordinator := s.leaderCoordinatorForQueue(queueName); coordinator != nil {
		return coordinator.ApplyRemovePending(ctx, queueName, groupID, consumerID, offset)
	}

	return s.base.RemovePendingEntry(ctx, queueName, groupID, consumerID, offset)
}

func (s *raftGroupStore) GetPendingEntries(ctx context.Context, queueName, groupID, consumerID string) ([]*types.PendingEntry, error) {
	return s.base.GetPendingEntries(ctx, queueName, groupID, consumerID)
}

func (s *raftGroupStore) GetAllPendingEntries(ctx context.Context, queueName, groupID string) ([]*types.PendingEntry, error) {
	return s.base.GetAllPendingEntries(ctx, queueName, groupID)
}

func (s *raftGroupStore) TransferPendingEntry(ctx context.Context, queueName, groupID string, offset uint64, fromConsumer, toConsumer string) error {
	if coordinator := s.leaderCoordinatorForQueue(queueName); coordinator != nil {
		return coordinator.ApplyTransferPending(ctx, queueName, groupID, offset, fromConsumer, toConsumer)
	}

	return s.base.TransferPendingEntry(ctx, queueName, groupID, offset, fromConsumer, toConsumer)
}

func (s *raftGroupStore) UpdateCursor(ctx context.Context, queueName, groupID string, cursor uint64) error {
	if coordinator := s.leaderCoordinatorForQueue(queueName); coordinator != nil {
		return coordinator.ApplyUpdateCursor(ctx, queueName, groupID, cursor)
	}

	return s.base.UpdateCursor(ctx, queueName, groupID, cursor)
}

func (s *raftGroupStore) UpdateCommitted(ctx context.Context, queueName, groupID string, committed uint64) error {
	if coordinator := s.leaderCoordinatorForQueue(queueName); coordinator != nil {
		return coordinator.ApplyUpdateCommitted(ctx, queueName, groupID, committed)
	}

	return s.base.UpdateCommitted(ctx, queueName, groupID, committed)
}

func (s *raftGroupStore) RegisterConsumer(ctx context.Context, queueName, groupID string, consumer *types.ConsumerInfo) error {
	if coordinator := s.leaderCoordinatorForQueue(queueName); coordinator != nil {
		return coordinator.ApplyRegisterConsumer(ctx, queueName, groupID, consumer)
	}

	return s.base.RegisterConsumer(ctx, queueName, groupID, consumer)
}

func (s *raftGroupStore) UnregisterConsumer(ctx context.Context, queueName, groupID, consumerID string) error {
	if coordinator := s.leaderCoordinatorForQueue(queueName); coordinator != nil {
		return coordinator.ApplyUnregisterConsumer(ctx, queueName, groupID, consumerID)
	}

	return s.base.UnregisterConsumer(ctx, queueName, groupID, consumerID)
}

func (s *raftGroupStore) ListConsumers(ctx context.Context, queueName, groupID string) ([]*types.ConsumerInfo, error) {
	return s.base.ListConsumers(ctx, queueName, groupID)
}
