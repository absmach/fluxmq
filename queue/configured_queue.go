// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package queue

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/absmach/fluxmq/queue/types"
)

const (
	defaultConfiguredQueueRetryInterval    = time.Second
	defaultConfiguredQueueRetryMaxInterval = 30 * time.Second

	// configuredQueueErrorAttempts is the failed attempt from which a queue
	// still missing from its Raft log is logged as an error, not a warning.
	configuredQueueErrorAttempts = 5
)

// errNotConfiguredQueueLeader means this node cannot record the queue because
// another node leads its Raft group. It is a reason to wait, not a failure.
var errNotConfiguredQueueLeader = errors.New("node does not lead the queue's raft group")

// recordConfiguredQueue writes a configured replicated queue's settings into
// its Raft log. Only the leader of the queue's group can.
//
// Each node also creates the queue in its own store, but that copy is not Raft
// state: restoring a snapshot taken before the queue existed, or replaying the
// log after a recovery lost the queue, rebuilds it from appends alone, with
// ephemeral defaults. The create covers a replay that reaches this point
// without the queue; the update then overrides whatever an earlier replayed
// append invented, since an existing queue ignores the create. Both are
// written by every process that leads the group because the log's older
// entries cannot be rewritten, only followed.
func (m *Manager) recordConfiguredQueue(ctx context.Context, cfg types.QueueConfig) error {
	coordinator := m.coordinator()
	if coordinator == nil || !coordinator.IsLeaderForQueue(cfg.Name) {
		return errNotConfiguredQueueLeader
	}
	if err := coordinator.ApplyCreateQueue(ctx, cfg); err != nil {
		return fmt.Errorf("record queue creation: %w", err)
	}
	if err := coordinator.ApplyUpdateQueue(ctx, cfg); err != nil {
		return fmt.Errorf("record queue settings: %w", err)
	}
	return nil
}

// tryRecordConfiguredQueue makes one attempt and logs its outcome. It reports
// whether the settings are now in the log.
func (m *Manager) tryRecordConfiguredQueue(ctx context.Context, cfg types.QueueConfig, attempt int) bool {
	err := m.recordConfiguredQueue(ctx, cfg)
	switch {
	case err == nil:
		if attempt > 1 {
			m.logger.Info("recorded configured queue in raft log",
				slog.String("queue", cfg.Name),
				slog.Int("attempts", attempt))
		}
		return true
	case errors.Is(err, errNotConfiguredQueueLeader):
		m.logger.Debug("configured queue awaits its raft leader",
			slog.String("queue", cfg.Name))
		return false
	default:
		level := slog.LevelWarn
		if attempt >= configuredQueueErrorAttempts {
			level = slog.LevelError
		}
		m.logger.LogAttrs(ctx, level, "failed to record configured queue in raft log; its settings are not yet recoverable",
			slog.String("queue", cfg.Name),
			slog.Int("attempts", attempt),
			slog.String("error", err.Error()))
		return false
	}
}

// runConfiguredQueueRecorder retries queues whose settings Start could not
// record, until each is recorded or the manager stops.
//
// Until the settings are Raft state, the leader refuses writes to the queue
// (see queueControl.configuredQueueReadiness), so no append can be committed
// ahead of them. A queue waits here while this node is a follower and is
// recorded if the node becomes leader, unless settings from Raft reach it first:
// those are authoritative and are never overwritten with the startup copy. If a leader holding an older configuration
// never gives up leadership, nothing on this node can record the queue.
func (m *Manager) runConfiguredQueueRecorder(ctx context.Context, pending []types.QueueConfig) {
	defer m.wg.Done()

	delay, maxDelay := m.configuredQueueRetryIntervals()
	attempts := make(map[string]int, len(pending))
	for _, cfg := range pending {
		attempts[cfg.Name] = 1 // Start made the first attempt.
	}

	timer := time.NewTimer(delay)
	defer timer.Stop()
	for len(pending) > 0 {
		select {
		case <-m.stopCh:
			return
		case <-ctx.Done():
			return
		case <-timer.C:
		}

		remaining := pending[:0]
		for _, cfg := range pending {
			if m.configuredQueueSettled(cfg.Name) {
				continue
			}
			attempts[cfg.Name]++
			if !m.tryRecordConfiguredQueue(ctx, cfg, attempts[cfg.Name]) {
				remaining = append(remaining, cfg)
			}
		}
		pending = remaining

		delay = min(delay*2, maxDelay)
		timer.Reset(delay)
	}
}

// configuredQueueSettled reports whether settings for the queue have reached
// this node through Raft since Start. What a follower keeps pending is its
// startup configuration; once authoritative settings arrive, possibly newer
// ones from a runtime update, writing it back after a failover would revert
// them.
func (m *Manager) configuredQueueSettled(name string) bool {
	coordinator := m.coordinator()
	return coordinator != nil && coordinator.IsQueueConfigRecorded(name)
}

func configuredReplicatedQueues(configs []types.QueueConfig) map[string]struct{} {
	names := make(map[string]struct{})
	for _, cfg := range configs {
		if cfg.Replication.Enabled {
			names[cfg.Name] = struct{}{}
		}
	}
	return names
}

func (m *Manager) configuredQueueRetryIntervals() (time.Duration, time.Duration) {
	delay := m.config.ConfiguredQueueRetryInterval
	if delay <= 0 {
		delay = defaultConfiguredQueueRetryInterval
	}
	maxDelay := m.config.ConfiguredQueueRetryMaxInterval
	if maxDelay <= 0 {
		maxDelay = defaultConfiguredQueueRetryMaxInterval
	}
	return delay, max(delay, maxDelay)
}
