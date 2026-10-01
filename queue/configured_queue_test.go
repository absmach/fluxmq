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

	"github.com/absmach/fluxmq/queue/storage"
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

	t.Run("follower/settings-arrive-then-leads/does-not-revert-them", func(t *testing.T) {
		coordinator := &mockQueueCoordinator{}
		coordinator.setLeader(configuredTestQueue, false)
		manager, _ := startConfiguredQueueManager(t, true, coordinator)
		stopManagerOnCleanup(t, manager)

		// Newer settings reach the follower through Raft, then it wins an election.
		coordinator.configRecorded.Store(true)
		coordinator.setLeader(configuredTestQueue, true)

		seen := coordinator.recordedChecks.Load()
		require.Eventually(t, func() bool {
			return coordinator.recordedChecks.Load() > seen
		}, 5*time.Second, time.Millisecond, "recorder should look at the pending queue")
		assert.Empty(t, coordinator.recordedQueueCalls(), "startup settings must not overwrite committed ones")
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

// deleteThroughRaft applies a delete of the configured queue the way a node
// receives one committed by a leader whose configuration does not declare it.
// This node refuses the delete itself; see TestConfiguredReplicatedQueueRuntimeDelete.
func deleteThroughRaft(t *testing.T, mock *mockQueueCoordinator, logStore *memlog.Store) {
	t.Helper()
	ctx := context.Background()
	require.NoError(t, mock.ApplyDeleteQueue(ctx, configuredTestQueue))
	require.NoError(t, logStore.DeleteQueue(ctx, configuredTestQueue))
}

// A node that declares a replicated queue records it again whenever it starts
// or takes over the queue's Raft group, so a runtime delete would last only
// until then, and whether it survived would depend on recorder timing.
func TestConfiguredReplicatedQueueRuntimeDelete(t *testing.T) {
	cases := []struct {
		name       string
		replicated bool
		leader     bool
		wantErr    error
	}{
		{name: "replicated/leader/refused", replicated: true, leader: true, wantErr: ErrConfiguredQueueDeletion},
		{name: "replicated/follower/refused", replicated: true, leader: false, wantErr: ErrConfiguredQueueDeletion},
		{name: "local/leader/deleted", replicated: false, leader: true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			mock := &mockQueueCoordinator{applyLikeFSM: true}
			mock.setLeader(configuredTestQueue, tc.leader)
			manager, logStore := startConfiguredQueueManager(t, tc.replicated, mock)
			stopManagerOnCleanup(t, manager)
			callsBefore := len(mock.recordedQueueCalls())

			ctx := context.Background()
			err := manager.DeleteQueue(ctx, configuredTestQueue)
			_, getErr := logStore.GetQueue(ctx, configuredTestQueue)
			if tc.wantErr == nil {
				require.NoError(t, err)
				assert.ErrorIs(t, getErr, storage.ErrQueueNotFound)
				return
			}

			require.ErrorIs(t, err, tc.wantErr)
			assert.Equal(t, ErrorCodeFailedPrecondition, ClassifyError(err).Code)
			assert.NoError(t, getErr, "a refused delete must leave the queue in place")
			assert.Len(t, mock.recordedQueueCalls(), callsBefore, "a refused delete must not reach raft")
		})
	}
}

// Until a configured queue's settings are raft state, an append committed on
// its leader could be replayed into a queue rebuilt with default settings.
func TestConfiguredQueueWritesWaitForRecordedSettings(t *testing.T) {
	publish := func(t *testing.T, manager *Manager) error {
		t.Helper()
		return manager.Publish(context.Background(), publishEnvelope(t, "configured/ready", []byte("job")))
	}

	t.Run("leader/unrecorded/rejects-then-accepts-once-recorded", func(t *testing.T) {
		mock := &mockQueueCoordinator{}
		mock.setLeader(configuredTestQueue, true)
		manager, _ := startConfiguredQueueManager(t, true, mock)
		stopManagerOnCleanup(t, manager)

		err := publish(t, manager)
		require.ErrorIs(t, err, ErrReplicationUnavailable)
		failure := ClassifyError(err)
		assert.Equal(t, ErrorCodeUnavailable, failure.Code)
		assert.True(t, failure.Retryable)
		assert.Equal(t, DurabilityNotAttempted, failure.Durability)
		assert.Empty(t, mock.appendCalls, "no append may reach raft before the settings")

		mock.configRecorded.Store(true)
		require.NoError(t, publish(t, manager))
		assert.Equal(t, []string{configuredTestQueue}, mock.appendCalls)
	})

	t.Run("follower/not-gated-here/leader-decides", func(t *testing.T) {
		mock := &mockQueueCoordinator{}
		mock.setLeader(configuredTestQueue, false)
		manager, _ := startConfiguredQueueManager(t, true, mock)
		stopManagerOnCleanup(t, manager)

		assert.NoError(t, manager.replicationWriteReadiness(configuredTestQueue))
	})

	t.Run("leader/queue-not-in-configuration/not-gated", func(t *testing.T) {
		mock := &mockQueueCoordinator{}
		mock.setLeader(configuredTestQueue, true)
		manager, _ := startConfiguredQueueManager(t, true, mock)
		stopManagerOnCleanup(t, manager)

		const apiQueue = "api-jobs"
		mock.setLeader(apiQueue, true)
		mock.replicatedByQueue[apiQueue] = true // no recorder goroutine runs: Start recorded the configured queue
		created := types.DefaultQueueConfig(apiQueue, "api/#")
		created.Replication.Enabled = true
		require.NoError(t, manager.CreateQueue(context.Background(), created))

		assert.NoError(t, manager.replicationWriteReadiness(apiQueue),
			"a queue created through raft has its settings in the log already")
	})

	t.Run("leader/deleted-and-recreated/settings-recorded-again", func(t *testing.T) {
		mock := &mockQueueCoordinator{applyLikeFSM: true}
		mock.setLeader(configuredTestQueue, true)
		manager, logStore := startConfiguredQueueManager(t, true, mock)
		stopManagerOnCleanup(t, manager)
		require.NoError(t, publish(t, manager), "Start recorded the settings")

		ctx := context.Background()
		deleteThroughRaft(t, mock, logStore)

		recreated := types.DefaultQueueConfig(configuredTestQueue, configuredTestTopic)
		recreated.Replication.Enabled = true
		require.NoError(t, manager.CreateQueue(ctx, recreated))

		assert.NoError(t, publish(t, manager), "the recreated queue's settings must be Raft state before writes resume")
	})

	t.Run("leader/duplicate-create/keeps-existing-settings", func(t *testing.T) {
		mock := &mockQueueCoordinator{applyLikeFSM: true}
		mock.setLeader(configuredTestQueue, true)
		manager, logStore := startConfiguredQueueManager(t, true, mock)
		stopManagerOnCleanup(t, manager)
		callsBefore := len(mock.recordedQueueCalls())

		replacement := types.DefaultQueueConfig(configuredTestQueue, "replacement/#")
		replacement.Replication.Enabled = true
		require.NoError(t, manager.CreateQueue(context.Background(), replacement))

		stored, err := logStore.GetQueue(context.Background(), configuredTestQueue)
		require.NoError(t, err)
		assert.Equal(t, []string{configuredTestTopic}, stored.Topics)
		assert.NotContains(t, mock.recordedQueueCalls()[callsBefore:], "update:"+configuredTestQueue,
			"a create that found the queue must not submit its settings")
	})

	t.Run("leader/recreate-update-failed/retry-repairs-with-existing-settings", func(t *testing.T) {
		mock := &mockQueueCoordinator{applyLikeFSM: true}
		mock.setLeader(configuredTestQueue, true)
		manager, logStore := startConfiguredQueueManager(t, true, mock)
		stopManagerOnCleanup(t, manager)
		mock.fsmStore = logStore

		ctx := context.Background()
		deleteThroughRaft(t, mock, logStore)

		recreated := types.DefaultQueueConfig(configuredTestQueue, configuredTestTopic)
		recreated.Replication.Enabled = true
		mock.updateQueueFailures = 1
		require.Error(t, manager.CreateQueue(ctx, recreated), "the create committed, its settings did not")
		require.ErrorIs(t, publish(t, manager), ErrReplicationUnavailable)

		retry := types.DefaultQueueConfig(configuredTestQueue, "replacement/#")
		retry.Replication.Enabled = true
		require.NoError(t, manager.CreateQueue(ctx, retry))

		assert.NoError(t, publish(t, manager), "the retry must record the settings and reopen writes")
		assert.Equal(t, []string{configuredTestTopic}, mock.lastUpdate.Topics,
			"the repair records the queue's existing settings, not the retry's")
		stored, err := logStore.GetQueue(ctx, configuredTestQueue)
		require.NoError(t, err)
		assert.Equal(t, []string{configuredTestTopic}, stored.Topics)
	})
}
