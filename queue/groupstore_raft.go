// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package queue

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/absmach/fluxmq/queue/raft"
	"github.com/absmach/fluxmq/queue/storage"
	"github.com/absmach/fluxmq/queue/types"
)

// GroupOpForwarder forwards consumer group operations to a remote node.
type GroupOpForwarder interface {
	ForwardGroupOp(ctx context.Context, nodeID, queueName string, opData []byte) error
}

// raftGroupStore routes mutating consumer-group operations for replicated
// queues through the Raft coordinator. On the leader node, mutations go
// directly through the coordinator's Apply* methods. On follower nodes,
// mutations are forwarded to the leader via the cluster transport so they
// go through Raft consensus and get replicated to all nodes.
type raftGroupStore struct {
	base        storage.ConsumerGroupStore
	coordinator raft.QueueCoordinator
	forwarder   GroupOpForwarder
	logger      *slog.Logger

	mu sync.RWMutex
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

func (s *raftGroupStore) SetForwarder(forwarder GroupOpForwarder) {
	s.mu.Lock()
	s.forwarder = forwarder
	s.mu.Unlock()
}

func (s *raftGroupStore) SetLogger(logger *slog.Logger) {
	if logger != nil {
		s.logger = logger
	}
}

// leaderCoordinatorForQueue returns the coordinator only when the queue is
// replicated AND this node is the leader for the queue's Raft group.
// Returns nil otherwise.
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

// forwardToLeader encodes op and forwards it to the leader node via the
// cluster transport. Returns nil error only if forwarding succeeds. Returns
// a non-nil error if the leader is unknown or the forward fails.
func (s *raftGroupStore) forwardToLeader(ctx context.Context, queueName string, op *raft.Operation) error {
	s.mu.RLock()
	coordinator := s.coordinator
	forwarder := s.forwarder
	s.mu.RUnlock()

	if coordinator == nil || forwarder == nil {
		return fmt.Errorf("no forwarder available")
	}

	leaderID := coordinator.LeaderIDForQueue(queueName)
	if leaderID == "" {
		return fmt.Errorf("leader unknown for queue %q", queueName)
	}

	data, err := raft.EncodeOperation(op)
	if err != nil {
		return fmt.Errorf("failed to encode operation: %w", err)
	}

	return forwarder.ForwardGroupOp(ctx, leaderID, queueName, data)
}

// applyOrForward applies the mutation on the leader, forwards to the leader
// for replicated follower queues, or falls back to local store for
// non-replicated queues.
func (s *raftGroupStore) applyOrForward(ctx context.Context, queueName string, applyOnLeader func(raft.QueueCoordinator) error, op *raft.Operation, localFallback func() error) error {
	if coordinator := s.leaderCoordinatorForQueue(queueName); coordinator != nil {
		return applyOnLeader(coordinator)
	}

	if s.isReplicatedFollower(queueName) {
		if err := s.forwardToLeader(ctx, queueName, op); err != nil {
			s.logger.Warn("failed to forward replicated group op to leader",
				slog.String("queue", queueName),
				slog.Int("op", int(op.Type)),
				slog.String("error", err.Error()))
			return fmt.Errorf("failed to forward replicated group op to leader: %w", err)
		}
		return nil
	}

	return localFallback()
}

func (s *raftGroupStore) isReplicatedFollower(queueName string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.coordinator == nil {
		return false
	}
	return s.coordinator.IsQueueReplicated(queueName) && !s.coordinator.IsLeaderForQueue(queueName)
}

func (s *raftGroupStore) CreateConsumerGroup(ctx context.Context, group *types.ConsumerGroup) error {
	if group == nil {
		return s.base.CreateConsumerGroup(ctx, group)
	}

	op := &raft.Operation{
		Type:       raft.OpCreateGroup,
		Timestamp:  time.Now(),
		QueueName:  group.QueueName,
		GroupState: group,
	}

	return s.applyOrForward(ctx, group.QueueName,
		func(c raft.QueueCoordinator) error {
			return c.ApplyCreateGroup(ctx, group.QueueName, group)
		},
		op,
		func() error { return s.base.CreateConsumerGroup(ctx, group) },
	)
}

func (s *raftGroupStore) GetConsumerGroup(ctx context.Context, queueName, groupID string) (*types.ConsumerGroup, error) {
	return s.base.GetConsumerGroup(ctx, queueName, groupID)
}

func (s *raftGroupStore) UpdateConsumerGroup(ctx context.Context, group *types.ConsumerGroup) error {
	if group == nil {
		return s.base.UpdateConsumerGroup(ctx, group)
	}

	op := &raft.Operation{
		Type:       raft.OpUpdateGroup,
		Timestamp:  time.Now(),
		QueueName:  group.QueueName,
		GroupID:    group.ID,
		GroupState: group,
	}

	return s.applyOrForward(ctx, group.QueueName,
		func(c raft.QueueCoordinator) error {
			return c.ApplyUpdateGroup(ctx, group.QueueName, group)
		},
		op,
		func() error { return s.base.UpdateConsumerGroup(ctx, group) },
	)
}

func (s *raftGroupStore) DeleteConsumerGroup(ctx context.Context, queueName, groupID string) error {
	op := &raft.Operation{
		Type:      raft.OpDeleteGroup,
		Timestamp: time.Now(),
		QueueName: queueName,
		GroupID:   groupID,
	}

	return s.applyOrForward(ctx, queueName,
		func(c raft.QueueCoordinator) error {
			return c.ApplyDeleteGroup(ctx, queueName, groupID)
		},
		op,
		func() error { return s.base.DeleteConsumerGroup(ctx, queueName, groupID) },
	)
}

func (s *raftGroupStore) ListConsumerGroups(ctx context.Context, queueName string) ([]*types.ConsumerGroup, error) {
	return s.base.ListConsumerGroups(ctx, queueName)
}

func (s *raftGroupStore) AddPendingEntry(ctx context.Context, queueName, groupID string, entry *types.PendingEntry) error {
	op := &raft.Operation{
		Type:         raft.OpAddPending,
		Timestamp:    time.Now(),
		QueueName:    queueName,
		GroupID:      groupID,
		PendingEntry: entry,
	}

	return s.applyOrForward(ctx, queueName,
		func(c raft.QueueCoordinator) error {
			return c.ApplyAddPending(ctx, queueName, groupID, entry)
		},
		op,
		func() error { return s.base.AddPendingEntry(ctx, queueName, groupID, entry) },
	)
}

func (s *raftGroupStore) RemovePendingEntry(ctx context.Context, queueName, groupID, consumerID string, offset uint64) error {
	op := &raft.Operation{
		Type:       raft.OpRemovePending,
		Timestamp:  time.Now(),
		QueueName:  queueName,
		GroupID:    groupID,
		ConsumerID: consumerID,
		Offset:     offset,
	}

	return s.applyOrForward(ctx, queueName,
		func(c raft.QueueCoordinator) error {
			return c.ApplyRemovePending(ctx, queueName, groupID, consumerID, offset)
		},
		op,
		func() error { return s.base.RemovePendingEntry(ctx, queueName, groupID, consumerID, offset) },
	)
}

func (s *raftGroupStore) GetPendingEntries(ctx context.Context, queueName, groupID, consumerID string) ([]*types.PendingEntry, error) {
	return s.base.GetPendingEntries(ctx, queueName, groupID, consumerID)
}

func (s *raftGroupStore) GetAllPendingEntries(ctx context.Context, queueName, groupID string) ([]*types.PendingEntry, error) {
	return s.base.GetAllPendingEntries(ctx, queueName, groupID)
}

func (s *raftGroupStore) TransferPendingEntry(ctx context.Context, queueName, groupID string, offset uint64, fromConsumer, toConsumer string) error {
	op := &raft.Operation{
		Type:         raft.OpTransferPending,
		Timestamp:    time.Now(),
		QueueName:    queueName,
		GroupID:      groupID,
		Offset:       offset,
		FromConsumer: fromConsumer,
		ToConsumer:   toConsumer,
	}

	return s.applyOrForward(ctx, queueName,
		func(c raft.QueueCoordinator) error {
			return c.ApplyTransferPending(ctx, queueName, groupID, offset, fromConsumer, toConsumer)
		},
		op,
		func() error {
			return s.base.TransferPendingEntry(ctx, queueName, groupID, offset, fromConsumer, toConsumer)
		},
	)
}

func (s *raftGroupStore) UpdateCursor(ctx context.Context, queueName, groupID string, cursor uint64) error {
	op := &raft.Operation{
		Type:      raft.OpUpdateCursor,
		Timestamp: time.Now(),
		QueueName: queueName,
		GroupID:   groupID,
		Cursor:    cursor,
	}

	return s.applyOrForward(ctx, queueName,
		func(c raft.QueueCoordinator) error {
			return c.ApplyUpdateCursor(ctx, queueName, groupID, cursor)
		},
		op,
		func() error { return s.base.UpdateCursor(ctx, queueName, groupID, cursor) },
	)
}

func (s *raftGroupStore) UpdateCommitted(ctx context.Context, queueName, groupID string, committed uint64) error {
	op := &raft.Operation{
		Type:      raft.OpUpdateCommitted,
		Timestamp: time.Now(),
		QueueName: queueName,
		GroupID:   groupID,
		Committed: committed,
	}

	return s.applyOrForward(ctx, queueName,
		func(c raft.QueueCoordinator) error {
			return c.ApplyUpdateCommitted(ctx, queueName, groupID, committed)
		},
		op,
		func() error { return s.base.UpdateCommitted(ctx, queueName, groupID, committed) },
	)
}

func (s *raftGroupStore) RegisterConsumer(ctx context.Context, queueName, groupID string, consumer *types.ConsumerInfo) error {
	op := &raft.Operation{
		Type:         raft.OpRegisterConsumer,
		Timestamp:    time.Now(),
		QueueName:    queueName,
		GroupID:      groupID,
		ConsumerInfo: consumer,
	}

	return s.applyOrForward(ctx, queueName,
		func(c raft.QueueCoordinator) error {
			return c.ApplyRegisterConsumer(ctx, queueName, groupID, consumer)
		},
		op,
		func() error { return s.base.RegisterConsumer(ctx, queueName, groupID, consumer) },
	)
}

func (s *raftGroupStore) UnregisterConsumer(ctx context.Context, queueName, groupID, consumerID string) error {
	op := &raft.Operation{
		Type:       raft.OpUnregisterConsumer,
		Timestamp:  time.Now(),
		QueueName:  queueName,
		GroupID:    groupID,
		ConsumerID: consumerID,
	}

	return s.applyOrForward(ctx, queueName,
		func(c raft.QueueCoordinator) error {
			return c.ApplyUnregisterConsumer(ctx, queueName, groupID, consumerID)
		},
		op,
		func() error { return s.base.UnregisterConsumer(ctx, queueName, groupID, consumerID) },
	)
}

func (s *raftGroupStore) ListConsumers(ctx context.Context, queueName, groupID string) ([]*types.ConsumerInfo, error) {
	return s.base.ListConsumers(ctx, queueName, groupID)
}
