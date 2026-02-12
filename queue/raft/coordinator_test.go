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

func (m *mockReplicator) ApplyAppendWithOptions(_ context.Context, queueName string, _ *types.Message, _ ApplyOptions) (uint64, error) {
	m.appendQueues = append(m.appendQueues, queueName)
	return m.offset, nil
}

func TestLogicalGroupCoordinatorQueueAssignments(t *testing.T) {
	coordinator := NewLogicalGroupCoordinator(&mockReplicator{enabled: true}, nil)
	ctx := context.Background()

	cfg := types.DefaultQueueConfig("orders", "$queue/orders/#")
	cfg.Replication.Enabled = true
	cfg.Replication.Group = "hot"

	if err := coordinator.EnsureQueue(ctx, cfg); err != nil {
		t.Fatalf("EnsureQueue failed: %v", err)
	}
	if got := coordinator.GroupForQueue("orders"); got != "hot" {
		t.Fatalf("unexpected queue group after ensure: got %q", got)
	}

	cfg.Replication.Enabled = false
	if err := coordinator.UpdateQueue(ctx, cfg); err != nil {
		t.Fatalf("UpdateQueue failed: %v", err)
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
