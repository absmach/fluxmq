// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package queue

import (
	"context"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/absmach/fluxmq/queue/raft"
	memlog "github.com/absmach/fluxmq/queue/storage/memory/log"
	"github.com/absmach/fluxmq/queue/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ensureRecorder records which placements reach the coordinator, which is the
// side effect a refused update must not have.
type ensureRecorder struct {
	*mockQueueCoordinator
	mu      sync.Mutex
	ensured []types.QueueConfig
}

func (r *ensureRecorder) EnsureQueue(_ context.Context, cfg types.QueueConfig) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.ensured = append(r.ensured, cfg)
	return nil
}

func (r *ensureRecorder) ensuredPlacements() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	placements := make([]string, 0, len(r.ensured))
	for _, cfg := range r.ensured {
		placements = append(placements, replicationPlacement(cfg))
	}
	return placements
}

func TestUpdateQueueKeepsReplicationPlacement(t *testing.T) {
	const (
		localQueue      = "local"
		replicatedQueue = "replicated"
	)

	cases := []struct {
		name    string
		queue   string
		edit    func(*types.QueueConfig)
		wantErr error
	}{
		{
			name:    "local/enable replication",
			queue:   localQueue,
			edit:    func(c *types.QueueConfig) { c.Replication.Enabled = true },
			wantErr: ErrReplicationPlacementImmutable,
		},
		{
			name:    "replicated/disable replication",
			queue:   replicatedQueue,
			edit:    func(c *types.QueueConfig) { c.Replication.Enabled = false },
			wantErr: ErrReplicationPlacementImmutable,
		},
		{
			name:    "replicated/move to another group",
			queue:   replicatedQueue,
			edit:    func(c *types.QueueConfig) { c.Replication.Group = "other" },
			wantErr: ErrReplicationPlacementImmutable,
		},
		{
			name:  "replicated/naming the default group is no move",
			queue: replicatedQueue,
			edit: func(c *types.QueueConfig) {
				c.Replication.Group = " default "
				c.MessageTTL = time.Hour
			},
		},
		{
			name:  "local/other settings still update",
			queue: localQueue,
			edit:  func(c *types.QueueConfig) { c.MessageTTL = time.Hour },
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			coordinator := &ensureRecorder{mockQueueCoordinator: &mockQueueCoordinator{
				enabled:           true,
				replicatedByQueue: map[string]bool{replicatedQueue: true},
				leaderByQueue:     map[string]bool{replicatedQueue: true},
				leaderIDByQueue:   map[string]string{replicatedQueue: node1},
			}}
			config := DefaultConfig()
			config.WritePolicy = WritePolicyReject
			m := NewManager(memlog.New(), newMockGroupStore(), nil, config,
				slog.New(slog.NewTextHandler(io.Discard, nil)), nil)
			m.SetRaftCoordinator(coordinator)

			require.NoError(t, m.CreateQueue(ctx, types.DefaultQueueConfig(localQueue, "local/#")))
			replicated := types.DefaultQueueConfig(replicatedQueue, "replicated/#")
			replicated.Replication.Enabled = true
			require.NoError(t, m.CreateQueue(ctx, replicated))

			before, err := m.GetQueue(ctx, tc.queue)
			require.NoError(t, err)
			ensuredBefore := len(coordinator.ensuredPlacements())

			updated := before.Clone()
			tc.edit(&updated)
			err = m.UpdateQueue(ctx, updated)

			stored, getErr := m.GetQueue(ctx, tc.queue)
			require.NoError(t, getErr)
			assert.Equal(t, replicationPlacement(*before), replicationPlacement(*stored),
				"the stored placement never changes")
			for _, placement := range coordinator.ensuredPlacements()[ensuredBefore:] {
				assert.Equal(t, replicationPlacement(*before), placement,
					"the coordinator is never handed another placement")
			}

			if tc.wantErr != nil {
				require.ErrorIs(t, err, tc.wantErr)
				assert.Equal(t, before.MessageTTL, stored.MessageTTL)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, time.Hour, stored.MessageTTL)
		})
	}
}

func TestReplicatedQueueMessageSizeFitsSnapshot(t *testing.T) {
	const bigQueue = "big"
	cases := []struct {
		name           string
		maxMessageSize int64
		wantErr        bool
	}{
		{name: "default", maxMessageSize: types.DefaultQueueConfig("q").MaxMessageSize},
		{name: "at limit", maxMessageSize: raft.MaxReplicatedMessageSize},
		{name: "over limit", maxMessageSize: raft.MaxReplicatedMessageSize + 1, wantErr: true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			config := DefaultConfig()
			config.WritePolicy = WritePolicyReject
			m := NewManager(memlog.New(), newMockGroupStore(), nil, config,
				slog.New(slog.NewTextHandler(io.Discard, nil)), nil)
			m.SetRaftCoordinator(&mockQueueCoordinator{
				enabled:           true,
				replicatedByQueue: map[string]bool{bigQueue: true},
				leaderByQueue:     map[string]bool{bigQueue: true},
				leaderIDByQueue:   map[string]string{bigQueue: node1},
			})

			cfg := types.DefaultQueueConfig(bigQueue, "big/#")
			cfg.Replication.Enabled = true
			cfg.MaxMessageSize = tc.maxMessageSize
			err := m.CreateQueue(ctx, cfg)
			if tc.wantErr {
				require.ErrorIs(t, err, types.ErrInvalidConfig)
				return
			}
			require.NoError(t, err)

			// A local queue is not bounded by snapshots.
			local := types.DefaultQueueConfig("local-big", "local-big/#")
			local.MaxMessageSize = raft.MaxReplicatedMessageSize * 4
			require.NoError(t, m.CreateQueue(ctx, local))
		})
	}
}
