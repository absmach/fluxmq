// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package queue

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"

	memlog "github.com/absmach/fluxmq/queue/storage/memory/log"
	"github.com/absmach/fluxmq/queue/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A configured replicated queue exists in each node's store but not in Raft
// state until its leader records it; without that, snapshot restore or log
// replay can rebuild it with ephemeral defaults.
func TestStartRecordsConfiguredReplicatedQueueInRaftLog(t *testing.T) {
	const queueName = "configured-jobs"

	cases := []struct {
		name       string
		replicated bool
		leader     bool
		createErr  error
		wantCalls  []string
	}{
		{
			name:       "replicated/leader/records-create-then-update",
			replicated: true,
			leader:     true,
			wantCalls:  []string{"create:" + queueName, "update:" + queueName},
		},
		{
			name:       "replicated/follower/leaves-it-to-the-leader",
			replicated: true,
			leader:     false,
		},
		{
			name:       "local/leader/not-raft-state",
			replicated: false,
			leader:     true,
		},
		{
			name:       "replicated/leader/create-fails/start-continues",
			replicated: true,
			leader:     true,
			createErr:  errors.New("leadership lost"),
			wantCalls:  []string{"create:" + queueName},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			configured := types.DefaultQueueConfig(queueName, "configured/#")
			configured.Replication.Enabled = tc.replicated

			config := DefaultConfig()
			config.WritePolicy = WritePolicyReject
			config.QueueConfigs = []types.QueueConfig{configured}
			logStore := memlog.New()
			manager := NewManager(logStore, newMockGroupStore(), nil, config, slog.New(slog.NewTextHandler(io.Discard, nil)), nil)
			coordinator := &mockQueueCoordinator{
				enabled:           true,
				replicatedByQueue: map[string]bool{queueName: tc.replicated},
				leaderByQueue:     map[string]bool{queueName: tc.leader},
				leaderIDByQueue:   map[string]string{queueName: "node-2"},
				createQueueErr:    tc.createErr,
			}
			manager.SetRaftCoordinator(coordinator)

			require.NoError(t, manager.Start(context.Background()))
			t.Cleanup(func() {
				if err := manager.Stop(); err != nil {
					t.Errorf("stop manager: %v", err)
				}
			})

			assert.Equal(t, tc.wantCalls, coordinator.queueCalls)
			stored, err := logStore.GetQueue(context.Background(), queueName)
			require.NoError(t, err, "the local copy is created regardless")
			assert.Equal(t, []string{"configured/#"}, stored.Topics)
		})
	}
}
