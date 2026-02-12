// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package queue

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"

	"github.com/absmach/fluxmq/queue/raft"
	"github.com/absmach/fluxmq/queue/storage"
	"github.com/absmach/fluxmq/queue/types"
)

// raftGroupStore routes mutating consumer-group operations for replicated
// queues through the Raft coordinator on the leader node. On follower nodes
// mutations fall back to the local store so that consumer operations don't
// hard-fail.
//
// KNOWN LIMITATION: follower-side mutations are local-only and NOT replicated.
// If a client connected to a follower acks messages or updates cursors, those
// changes exist only in that follower's local store. On failover or leadership
// change, the new leader's replicated state will NOT include those mutations
// and messages may be redelivered. Correct fix requires cluster-level
// forwarding of consumer group operations to the Raft leader, similar to how
// ForwardQueuePublish works for message append. Until then, for strongest
// consistency guarantees, clients should connect to the leader node for
// replicated queues.
type raftGroupStore struct {
	base        storage.ConsumerGroupStore
	coordinator raft.QueueCoordinator
	logger      *slog.Logger

	mu sync.RWMutex

	// Throttle follower-fallback warnings to avoid log spam.
	followerWarned atomic.Bool
}

func newRaftGroupStore(base storage.ConsumerGroupStore) *raftGroupStore {
	return &raftGroupStore{
		base:   base,
		logger: slog.Default(),
	}
}

func (s *raftGroupStore) SetCoordinator(coordinator raft.QueueCoordinator) {
	s.mu.Lock()
	s.coordinator = coordinator
	s.mu.Unlock()
}

func (s *raftGroupStore) SetLogger(logger *slog.Logger) {
	if logger != nil {
		s.logger = logger
	}
}

// leaderCoordinatorForQueue returns the coordinator only when the queue is
// replicated AND this node is the leader for the queue's Raft group.
// Returns nil otherwise — callers fall back to the local store.
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

// isReplicatedFollower reports whether the queue is replicated but this node
// is NOT the leader. Used to emit a warning on the first follower-side
// mutation.
func (s *raftGroupStore) isReplicatedFollower(queueName string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.coordinator == nil || !s.coordinator.IsEnabled() {
		return false
	}
	return s.coordinator.IsQueueReplicated(queueName) && !s.coordinator.IsLeaderForQueue(queueName)
}

func (s *raftGroupStore) warnFollowerFallback(queueName, op string) {
	if !s.followerWarned.Swap(true) {
		s.logger.Warn("consumer group mutation on follower applied locally only — not replicated (this warning is logged once)",
			slog.String("queue", queueName),
			slog.String("op", op))
	}
}

func (s *raftGroupStore) CreateConsumerGroup(ctx context.Context, group *types.ConsumerGroup) error {
	if group == nil {
		return s.base.CreateConsumerGroup(ctx, group)
	}

	if coordinator := s.leaderCoordinatorForQueue(group.QueueName); coordinator != nil {
		return coordinator.ApplyCreateGroup(ctx, group.QueueName, group)
	}

	if s.isReplicatedFollower(group.QueueName) {
		s.warnFollowerFallback(group.QueueName, "CreateConsumerGroup")
	}
	return s.base.CreateConsumerGroup(ctx, group)
}

func (s *raftGroupStore) GetConsumerGroup(ctx context.Context, queueName, groupID string) (*types.ConsumerGroup, error) {
	return s.base.GetConsumerGroup(ctx, queueName, groupID)
}

func (s *raftGroupStore) UpdateConsumerGroup(ctx context.Context, group *types.ConsumerGroup) error {
	// UpdateConsumerGroup writes the full group struct. Individual mutations
	// (cursor, pending, consumer registration) are already replicated via
	// dedicated Raft ops on the leader. Full-group updates are bulk metadata
	// writes (rebalance state, mode changes) applied locally on all nodes.
	return s.base.UpdateConsumerGroup(ctx, group)
}

func (s *raftGroupStore) DeleteConsumerGroup(ctx context.Context, queueName, groupID string) error {
	if coordinator := s.leaderCoordinatorForQueue(queueName); coordinator != nil {
		return coordinator.ApplyDeleteGroup(ctx, queueName, groupID)
	}

	if s.isReplicatedFollower(queueName) {
		s.warnFollowerFallback(queueName, "DeleteConsumerGroup")
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

	if s.isReplicatedFollower(queueName) {
		s.warnFollowerFallback(queueName, "AddPendingEntry")
	}
	return s.base.AddPendingEntry(ctx, queueName, groupID, entry)
}

func (s *raftGroupStore) RemovePendingEntry(ctx context.Context, queueName, groupID, consumerID string, offset uint64) error {
	if coordinator := s.leaderCoordinatorForQueue(queueName); coordinator != nil {
		return coordinator.ApplyRemovePending(ctx, queueName, groupID, consumerID, offset)
	}

	if s.isReplicatedFollower(queueName) {
		s.warnFollowerFallback(queueName, "RemovePendingEntry")
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

	if s.isReplicatedFollower(queueName) {
		s.warnFollowerFallback(queueName, "TransferPendingEntry")
	}
	return s.base.TransferPendingEntry(ctx, queueName, groupID, offset, fromConsumer, toConsumer)
}

func (s *raftGroupStore) UpdateCursor(ctx context.Context, queueName, groupID string, cursor uint64) error {
	if coordinator := s.leaderCoordinatorForQueue(queueName); coordinator != nil {
		return coordinator.ApplyUpdateCursor(ctx, queueName, groupID, cursor)
	}

	if s.isReplicatedFollower(queueName) {
		s.warnFollowerFallback(queueName, "UpdateCursor")
	}
	return s.base.UpdateCursor(ctx, queueName, groupID, cursor)
}

func (s *raftGroupStore) UpdateCommitted(ctx context.Context, queueName, groupID string, committed uint64) error {
	if coordinator := s.leaderCoordinatorForQueue(queueName); coordinator != nil {
		return coordinator.ApplyUpdateCommitted(ctx, queueName, groupID, committed)
	}

	if s.isReplicatedFollower(queueName) {
		s.warnFollowerFallback(queueName, "UpdateCommitted")
	}
	return s.base.UpdateCommitted(ctx, queueName, groupID, committed)
}

func (s *raftGroupStore) RegisterConsumer(ctx context.Context, queueName, groupID string, consumer *types.ConsumerInfo) error {
	if coordinator := s.leaderCoordinatorForQueue(queueName); coordinator != nil {
		return coordinator.ApplyRegisterConsumer(ctx, queueName, groupID, consumer)
	}

	if s.isReplicatedFollower(queueName) {
		s.warnFollowerFallback(queueName, "RegisterConsumer")
	}
	return s.base.RegisterConsumer(ctx, queueName, groupID, consumer)
}

func (s *raftGroupStore) UnregisterConsumer(ctx context.Context, queueName, groupID, consumerID string) error {
	if coordinator := s.leaderCoordinatorForQueue(queueName); coordinator != nil {
		return coordinator.ApplyUnregisterConsumer(ctx, queueName, groupID, consumerID)
	}

	if s.isReplicatedFollower(queueName) {
		s.warnFollowerFallback(queueName, "UnregisterConsumer")
	}
	return s.base.UnregisterConsumer(ctx, queueName, groupID, consumerID)
}

func (s *raftGroupStore) ListConsumers(ctx context.Context, queueName, groupID string) ([]*types.ConsumerInfo, error) {
	return s.base.ListConsumers(ctx, queueName, groupID)
}
