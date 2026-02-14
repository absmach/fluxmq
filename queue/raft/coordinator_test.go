// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package raft

import (
	"context"
	"testing"

	"github.com/absmach/fluxmq/queue/types"
)

type mockReplicator struct {
	enabled    bool
	leader     bool
	leaderAddr string
	leaderID   string
	offset     uint64

	stopCalls    int
	appendQueues []string
}

type mockProvisioner struct {
	replicators  map[string]GroupReplicator
	keepGroups   map[string]bool
	releaseCalls []string
	releaseErr   error
}

func (m *mockProvisioner) GetOrCreateGroup(_ context.Context, groupID string) (GroupReplicator, error) {
	if replicator, ok := m.replicators[groupID]; ok {
		return replicator, nil
	}
	return nil, nil
}

func (m *mockProvisioner) TryReleaseGroup(_ context.Context, groupID string) (bool, error) {
	m.releaseCalls = append(m.releaseCalls, groupID)
	if m.releaseErr != nil {
		return false, m.releaseErr
	}
	if m.keepGroups != nil && m.keepGroups[groupID] {
		return false, nil
	}
	delete(m.replicators, groupID)
	return true, nil
}

func (m *mockReplicator) Stop() error {
	m.stopCalls++
	return nil
}

func (m *mockReplicator) IsEnabled() bool { return m.enabled }
func (m *mockReplicator) IsLeader() bool  { return m.leader }
func (m *mockReplicator) Leader() string  { return m.leaderAddr }
func (m *mockReplicator) LeaderID() string {
	return m.leaderID
}
func (m *mockReplicator) ApplyCreateQueue(context.Context, types.QueueConfig) error { return nil }
func (m *mockReplicator) ApplyUpdateQueue(context.Context, types.QueueConfig) error { return nil }
func (m *mockReplicator) ApplyDeleteQueue(context.Context, string) error            { return nil }

func (m *mockReplicator) ApplyAppendWithOptions(_ context.Context, queueName string, _ *types.Message, _ ApplyOptions) (uint64, error) {
	m.appendQueues = append(m.appendQueues, queueName)
	return m.offset, nil
}
func (m *mockReplicator) ApplyTruncate(context.Context, string, uint64) error { return nil }
func (m *mockReplicator) ApplyCreateGroup(context.Context, string, *types.ConsumerGroup) error {
	return nil
}
func (m *mockReplicator) ApplyUpdateGroup(context.Context, string, *types.ConsumerGroup) error {
	return nil
}
func (m *mockReplicator) ApplyDeleteGroup(context.Context, string, string) error { return nil }
func (m *mockReplicator) ApplyUpdateCursor(context.Context, string, string, uint64) error {
	return nil
}
func (m *mockReplicator) ApplyUpdateCommitted(context.Context, string, string, uint64) error {
	return nil
}
func (m *mockReplicator) ApplyAddPending(context.Context, string, string, *types.PendingEntry) error {
	return nil
}
func (m *mockReplicator) ApplyRemovePending(context.Context, string, string, string, uint64) error {
	return nil
}
func (m *mockReplicator) ApplyTransferPending(context.Context, string, string, uint64, string, string) error {
	return nil
}
func (m *mockReplicator) ApplyRegisterConsumer(context.Context, string, string, *types.ConsumerInfo) error {
	return nil
}
func (m *mockReplicator) ApplyUnregisterConsumer(context.Context, string, string, string) error {
	return nil
}

func TestLogicalGroupCoordinatorQueueAssignments(t *testing.T) {
	coordinator := NewLogicalGroupCoordinator(&mockReplicator{enabled: true}, nil)
	ctx := context.Background()
	coordinator.RegisterGroup("hot", &mockReplicator{enabled: true})

	cfg := types.DefaultQueueConfig("orders", "$queue/orders/#")
	cfg.Replication.Enabled = true
	cfg.Replication.Group = "hot"

	if err := coordinator.EnsureQueue(ctx, cfg); err != nil {
		t.Fatalf("EnsureQueue failed: %v", err)
	}
	if !coordinator.IsQueueReplicated("orders") {
		t.Fatalf("expected queue to be marked replicated")
	}
	if got := coordinator.GroupForQueue("orders"); got != "hot" {
		t.Fatalf("unexpected queue group after ensure: got %q", got)
	}

	cfg.Replication.Enabled = false
	if err := coordinator.UpdateQueue(ctx, cfg); err != nil {
		t.Fatalf("UpdateQueue failed: %v", err)
	}
	if coordinator.IsQueueReplicated("orders") {
		t.Fatalf("expected queue replication marker to be removed")
	}
	if got := coordinator.GroupForQueue("orders"); got != DefaultGroupID {
		t.Fatalf("unexpected queue group after disabling replication: got %q", got)
	}

	if err := coordinator.DeleteQueue(ctx, "orders"); err != nil {
		t.Fatalf("DeleteQueue failed: %v", err)
	}
	if got := coordinator.GroupForQueue("orders"); got != DefaultGroupID {
		t.Fatalf("unexpected queue group after delete: got %q", got)
	}
}

func TestLogicalGroupCoordinatorRejectsUnknownGroupWithoutProvisioner(t *testing.T) {
	coordinator := NewLogicalGroupCoordinator(&mockReplicator{enabled: true}, nil)

	cfg := types.DefaultQueueConfig("orders", "$queue/orders/#")
	cfg.Replication.Enabled = true
	cfg.Replication.Group = "hot"

	if err := coordinator.EnsureQueue(context.Background(), cfg); err == nil {
		t.Fatalf("expected unknown group error")
	}
}

func TestLogicalGroupCoordinatorProvisionerCreatesMissingGroup(t *testing.T) {
	hotReplicator := &mockReplicator{enabled: true, leader: true}
	provisioner := &mockProvisioner{
		replicators: map[string]GroupReplicator{
			"hot": hotReplicator,
		},
	}
	coordinator := NewLogicalGroupCoordinatorWithProvisioner(&mockReplicator{enabled: true}, provisioner, nil)

	cfg := types.DefaultQueueConfig("orders", "$queue/orders/#")
	cfg.Replication.Enabled = true
	cfg.Replication.Group = "hot"

	if err := coordinator.EnsureQueue(context.Background(), cfg); err != nil {
		t.Fatalf("EnsureQueue failed: %v", err)
	}
	if !coordinator.IsQueueReplicated("orders") {
		t.Fatalf("expected queue replication marker")
	}
	if !coordinator.IsLeaderForQueue("orders") {
		t.Fatalf("expected provisioned hot group to be leader")
	}
}

func TestLogicalGroupCoordinatorRoutesByQueueGroup(t *testing.T) {
	defaultReplicator := &mockReplicator{
		enabled:    true,
		leader:     false,
		leaderAddr: "default-leader:7000",
		leaderID:   "default-node",
		offset:     11,
	}
	hotReplicator := &mockReplicator{
		enabled:    true,
		leader:     true,
		leaderAddr: "hot-leader:7100",
		leaderID:   "hot-node",
		offset:     42,
	}

	coordinator := NewLogicalGroupCoordinator(defaultReplicator, nil)
	coordinator.RegisterGroup("hot", hotReplicator)

	cfg := types.DefaultQueueConfig("jobs", "$queue/jobs/#")
	cfg.Replication.Enabled = true
	cfg.Replication.Group = "hot"
	if err := coordinator.EnsureQueue(context.Background(), cfg); err != nil {
		t.Fatalf("EnsureQueue failed: %v", err)
	}

	if !coordinator.IsLeaderForQueue("jobs") {
		t.Fatalf("expected hot group leader for jobs queue")
	}
	if got := coordinator.LeaderForQueue("jobs"); got != "hot-leader:7100" {
		t.Fatalf("unexpected leader addr: got %q", got)
	}
	if got := coordinator.LeaderIDForQueue("jobs"); got != "hot-node" {
		t.Fatalf("unexpected leader id: got %q", got)
	}

	offset, err := coordinator.ApplyAppendWithOptions(context.Background(), "jobs", &types.Message{}, ApplyOptions{})
	if err != nil {
		t.Fatalf("ApplyAppendWithOptions failed: %v", err)
	}
	if offset != 42 {
		t.Fatalf("unexpected offset from hot group: got %d", offset)
	}

	if coordinator.IsLeaderForQueue("other") {
		t.Fatalf("expected default group non-leader for unmatched queue")
	}
}

func TestLogicalGroupCoordinatorReleasesDynamicGroupWhenLastQueueDeleted(t *testing.T) {
	provisioner := &mockProvisioner{
		replicators: map[string]GroupReplicator{
			"hot": &mockReplicator{enabled: true},
		},
	}
	coordinator := NewLogicalGroupCoordinatorWithProvisioner(&mockReplicator{enabled: true}, provisioner, nil)
	ctx := context.Background()

	queueA := types.DefaultQueueConfig("orders-a", "$queue/orders-a/#")
	queueA.Replication.Enabled = true
	queueA.Replication.Group = "hot"
	if err := coordinator.EnsureQueue(ctx, queueA); err != nil {
		t.Fatalf("EnsureQueue orders-a failed: %v", err)
	}

	queueB := types.DefaultQueueConfig("orders-b", "$queue/orders-b/#")
	queueB.Replication.Enabled = true
	queueB.Replication.Group = "hot"
	if err := coordinator.EnsureQueue(ctx, queueB); err != nil {
		t.Fatalf("EnsureQueue orders-b failed: %v", err)
	}

	if err := coordinator.DeleteQueue(ctx, "orders-a"); err != nil {
		t.Fatalf("DeleteQueue orders-a failed: %v", err)
	}
	if len(provisioner.releaseCalls) != 0 {
		t.Fatalf("expected no release while one queue still uses the group")
	}

	if err := coordinator.DeleteQueue(ctx, "orders-b"); err != nil {
		t.Fatalf("DeleteQueue orders-b failed: %v", err)
	}
	if len(provisioner.releaseCalls) != 1 || provisioner.releaseCalls[0] != "hot" {
		t.Fatalf("expected one release call for hot group, got %v", provisioner.releaseCalls)
	}
	if _, ok := coordinator.groupMembers["hot"]; ok {
		t.Fatalf("expected hot group member to be removed after release")
	}
}

func TestLogicalGroupCoordinatorKeepsStaticGroupRegisteredOnQueueRemoval(t *testing.T) {
	provisioner := &mockProvisioner{
		replicators: map[string]GroupReplicator{
			"hot": &mockReplicator{enabled: true},
		},
		keepGroups: map[string]bool{
			"hot": true,
		},
	}
	coordinator := NewLogicalGroupCoordinatorWithProvisioner(&mockReplicator{enabled: true}, provisioner, nil)
	ctx := context.Background()

	cfg := types.DefaultQueueConfig("orders", "$queue/orders/#")
	cfg.Replication.Enabled = true
	cfg.Replication.Group = "hot"
	if err := coordinator.EnsureQueue(ctx, cfg); err != nil {
		t.Fatalf("EnsureQueue failed: %v", err)
	}

	if err := coordinator.DeleteQueue(ctx, "orders"); err != nil {
		t.Fatalf("DeleteQueue failed: %v", err)
	}
	if len(provisioner.releaseCalls) != 1 || provisioner.releaseCalls[0] != "hot" {
		t.Fatalf("expected release attempt for hot group, got %v", provisioner.releaseCalls)
	}
	if _, ok := coordinator.groupMembers["hot"]; !ok {
		t.Fatalf("expected hot group member to remain for non-releasable group")
	}
}
