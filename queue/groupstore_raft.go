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

// raftAwareGroupStore routes mutating consumer-group operations for replicated
// queues through queue Raft coordinator while preserving direct local reads.
type raftAwareGroupStore struct {
	base        storage.ConsumerGroupStore
	coordinator raft.QueueCoordinator

	mu sync.RWMutex
}

func newRaftAwareGroupStore(base storage.ConsumerGroupStore) *raftAwareGroupStore {
	return &raftAwareGroupStore{
		base: base,
	}
}

func (s *raftAwareGroupStore) SetCoordinator(coordinator raft.QueueCoordinator) {
	s.mu.Lock()
	s.coordinator = coordinator
	s.mu.Unlock()
}

func (s *raftAwareGroupStore) coordinatorForQueue(queueName string) raft.QueueCoordinator {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.coordinator == nil || !s.coordinator.IsEnabled() {
		return nil
	}
	if !s.coordinator.IsQueueReplicated(queueName) {
		return nil
	}

	return s.coordinator
}

func (s *raftAwareGroupStore) CreateConsumerGroup(ctx context.Context, group *types.ConsumerGroup) error {
	if group == nil {
		return s.base.CreateConsumerGroup(ctx, group)
	}

	if coordinator := s.coordinatorForQueue(group.QueueName); coordinator != nil {
		return coordinator.ApplyCreateGroup(ctx, group.QueueName, group)
	}

	return s.base.CreateConsumerGroup(ctx, group)
}

func (s *raftAwareGroupStore) GetConsumerGroup(ctx context.Context, queueName, groupID string) (*types.ConsumerGroup, error) {
	return s.base.GetConsumerGroup(ctx, queueName, groupID)
}

func (s *raftAwareGroupStore) UpdateConsumerGroup(ctx context.Context, group *types.ConsumerGroup) error {
	// Full group-state updates are currently local metadata updates.
	return s.base.UpdateConsumerGroup(ctx, group)
}

func (s *raftAwareGroupStore) DeleteConsumerGroup(ctx context.Context, queueName, groupID string) error {
	if coordinator := s.coordinatorForQueue(queueName); coordinator != nil {
		return coordinator.ApplyDeleteGroup(ctx, queueName, groupID)
	}

	return s.base.DeleteConsumerGroup(ctx, queueName, groupID)
}

func (s *raftAwareGroupStore) ListConsumerGroups(ctx context.Context, queueName string) ([]*types.ConsumerGroup, error) {
	return s.base.ListConsumerGroups(ctx, queueName)
}

func (s *raftAwareGroupStore) AddPendingEntry(ctx context.Context, queueName, groupID string, entry *types.PendingEntry) error {
	if coordinator := s.coordinatorForQueue(queueName); coordinator != nil {
		return coordinator.ApplyAddPending(ctx, queueName, groupID, entry)
	}

	return s.base.AddPendingEntry(ctx, queueName, groupID, entry)
}

func (s *raftAwareGroupStore) RemovePendingEntry(ctx context.Context, queueName, groupID, consumerID string, offset uint64) error {
	if coordinator := s.coordinatorForQueue(queueName); coordinator != nil {
		return coordinator.ApplyRemovePending(ctx, queueName, groupID, consumerID, offset)
	}

	return s.base.RemovePendingEntry(ctx, queueName, groupID, consumerID, offset)
}

func (s *raftAwareGroupStore) GetPendingEntries(ctx context.Context, queueName, groupID, consumerID string) ([]*types.PendingEntry, error) {
	return s.base.GetPendingEntries(ctx, queueName, groupID, consumerID)
}

func (s *raftAwareGroupStore) GetAllPendingEntries(ctx context.Context, queueName, groupID string) ([]*types.PendingEntry, error) {
	return s.base.GetAllPendingEntries(ctx, queueName, groupID)
}

func (s *raftAwareGroupStore) TransferPendingEntry(ctx context.Context, queueName, groupID string, offset uint64, fromConsumer, toConsumer string) error {
	if coordinator := s.coordinatorForQueue(queueName); coordinator != nil {
		return coordinator.ApplyTransferPending(ctx, queueName, groupID, offset, fromConsumer, toConsumer)
	}

	return s.base.TransferPendingEntry(ctx, queueName, groupID, offset, fromConsumer, toConsumer)
}

func (s *raftAwareGroupStore) UpdateCursor(ctx context.Context, queueName, groupID string, cursor uint64) error {
	if coordinator := s.coordinatorForQueue(queueName); coordinator != nil {
		return coordinator.ApplyUpdateCursor(ctx, queueName, groupID, cursor)
	}

	return s.base.UpdateCursor(ctx, queueName, groupID, cursor)
}

func (s *raftAwareGroupStore) UpdateCommitted(ctx context.Context, queueName, groupID string, committed uint64) error {
	if coordinator := s.coordinatorForQueue(queueName); coordinator != nil {
		return coordinator.ApplyUpdateCommitted(ctx, queueName, groupID, committed)
	}

	return s.base.UpdateCommitted(ctx, queueName, groupID, committed)
}

func (s *raftAwareGroupStore) RegisterConsumer(ctx context.Context, queueName, groupID string, consumer *types.ConsumerInfo) error {
	if coordinator := s.coordinatorForQueue(queueName); coordinator != nil {
		return coordinator.ApplyRegisterConsumer(ctx, queueName, groupID, consumer)
	}

	return s.base.RegisterConsumer(ctx, queueName, groupID, consumer)
}

func (s *raftAwareGroupStore) UnregisterConsumer(ctx context.Context, queueName, groupID, consumerID string) error {
	if coordinator := s.coordinatorForQueue(queueName); coordinator != nil {
		return coordinator.ApplyUnregisterConsumer(ctx, queueName, groupID, consumerID)
	}

	return s.base.UnregisterConsumer(ctx, queueName, groupID, consumerID)
}

func (s *raftAwareGroupStore) ListConsumers(ctx context.Context, queueName, groupID string) ([]*types.ConsumerInfo, error) {
	return s.base.ListConsumers(ctx, queueName, groupID)
}
