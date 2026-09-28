// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package queue

import (
	"context"
	"io"
	"log/slog"
	"slices"
	"testing"
	"time"

	memlog "github.com/absmach/fluxmq/queue/storage/memory/log"
	"github.com/absmach/fluxmq/queue/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	configuredTestQueue = "configured-jobs"
	configuredTestTopic = "configured/#"
)

var recordedConfiguredQueue = []string{"create:" + configuredTestQueue, "update:" + configuredTestQueue}

// startConfiguredQueueManager starts a manager whose configuration declares
// one queue, with a retry backoff short enough to poll for.
func startConfiguredQueueManager(t *testing.T, replicated bool, coordinator *mockQueueCoordinator) (*Manager, *memlog.Store) {
	t.Helper()
	configured := types.DefaultQueueConfig(configuredTestQueue, configuredTestTopic)
	configured.Replication.Enabled = replicated

	config := DefaultConfig()
	config.WritePolicy = WritePolicyReject
	config.QueueConfigs = []types.QueueConfig{configured}
	config.ConfiguredQueueRetryInterval = time.Millisecond
	config.ConfiguredQueueRetryMaxInterval = 5 * time.Millisecond

	coordinator.enabled = true
	coordinator.replicatedByQueue = map[string]bool{configuredTestQueue: replicated}
	coordinator.leaderIDByQueue = map[string]string{configuredTestQueue: "node-2"}

	logStore := memlog.New()
	manager := NewManager(logStore, newMockGroupStore(), nil, config, slog.New(slog.NewTextHandler(io.Discard, nil)), nil)
	manager.SetRaftCoordinator(coordinator)
	require.NoError(t, manager.Start(context.Background()))
	return manager, logStore
}

func stopManagerOnCleanup(t *testing.T, manager *Manager) {
	t.Helper()
	t.Cleanup(func() {
		if err := manager.Stop(); err != nil {
			t.Errorf("stop manager: %v", err)
		}
	})
}

// A configured replicated queue exists in each node's store but not in Raft
// state until its leader records it; without that, snapshot restore or log
// replay can rebuild it with ephemeral defaults.
func TestStartRecordsConfiguredReplicatedQueueInRaftLog(t *testing.T) {
	cases := []struct {
		name       string
		replicated bool
		leader     bool
		wantCalls  []string
	}{
		{name: "replicated/leader/records-create-then-update", replicated: true, leader: true, wantCalls: recordedConfiguredQueue},
		{name: "replicated/follower/leaves-it-to-the-leader", replicated: true, leader: false},
		{name: "local/leader/not-raft-state", replicated: false, leader: true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			coordinator := &mockQueueCoordinator{}
			coordinator.setLeader(configuredTestQueue, tc.leader)
			manager, logStore := startConfiguredQueueManager(t, tc.replicated, coordinator)
			stopManagerOnCleanup(t, manager)

			assert.Equal(t, tc.wantCalls, coordinator.recordedQueueCalls())
			stored, err := logStore.GetQueue(context.Background(), configuredTestQueue)
			require.NoError(t, err, "the local copy is created regardless")
			assert.Equal(t, []string{configuredTestTopic}, stored.Topics)
		})
	}
}

func TestConfiguredQueueRecorderRetriesUntilRecorded(t *testing.T) {
	t.Run("write-fails/retries-until-it-succeeds", func(t *testing.T) {
		coordinator := &mockQueueCoordinator{createQueueFailures: 2}
		coordinator.setLeader(configuredTestQueue, true)
		manager, _ := startConfiguredQueueManager(t, true, coordinator)

		want := []string{"create:" + configuredTestQueue, "create:" + configuredTestQueue, "create:" + configuredTestQueue, "update:" + configuredTestQueue}
		require.Eventually(t, func() bool {
			return slices.Equal(coordinator.recordedQueueCalls(), want)
		}, 5*time.Second, time.Millisecond, "recorder should retry the failed create")

		require.NoError(t, manager.Stop())
		assert.Equal(t, want, coordinator.recordedQueueCalls(), "a recorded queue must not be written again")
	})

	t.Run("follower/records-after-becoming-leader", func(t *testing.T) {
		coordinator := &mockQueueCoordinator{}
		coordinator.setLeader(configuredTestQueue, false)
		manager, _ := startConfiguredQueueManager(t, true, coordinator)
		stopManagerOnCleanup(t, manager)
		require.Empty(t, coordinator.recordedQueueCalls())

		coordinator.setLeader(configuredTestQueue, true)
		require.Eventually(t, func() bool {
			return slices.Equal(coordinator.recordedQueueCalls(), recordedConfiguredQueue)
		}, 5*time.Second, time.Millisecond, "new leader should record the queue")
	})

	t.Run("follower/stop-ends-pending-retries", func(t *testing.T) {
		coordinator := &mockQueueCoordinator{}
		coordinator.setLeader(configuredTestQueue, false)
		manager, _ := startConfiguredQueueManager(t, true, coordinator)

		stopped := make(chan error, 1)
		go func() { stopped <- manager.Stop() }()
		select {
		case err := <-stopped:
			require.NoError(t, err)
		case <-time.After(5 * time.Second):
			t.Fatal("Stop did not return while a configured queue was still pending")
		}
		assert.Empty(t, coordinator.recordedQueueCalls())
	})
}
