// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package queue

import (
	"context"
	"io"
	"log/slog"
	"sync/atomic"
	"testing"

	"github.com/absmach/fluxmq/queue/storage"
	memlog "github.com/absmach/fluxmq/queue/storage/memory/log"
	"github.com/absmach/fluxmq/queue/types"
	"github.com/stretchr/testify/require"
)

// syncRecordingStore reports durable-sync support and records which append path
// each publish took. The in-memory log store does not claim durable sync, so a
// double is the only way to exercise the fsync policy in a unit test.
type syncRecordingStore struct {
	storage.QueueStore
	appends atomic.Int64
	synced  atomic.Int64
}

func (s *syncRecordingStore) Append(ctx context.Context, queueName string, msg *types.Message) (uint64, error) {
	s.appends.Add(1)
	return s.QueueStore.Append(ctx, queueName, msg)
}

func (s *syncRecordingStore) AppendAndSync(ctx context.Context, queueName string, msg *types.Message) (uint64, error) {
	s.synced.Add(1)
	return s.QueueStore.Append(ctx, queueName, msg)
}

func (s *syncRecordingStore) SupportsDurableSync() bool { return true }

func newDurabilityManager(t *testing.T, store storage.QueueStore, policy AckDurability) *Manager {
	t.Helper()
	cfg := DefaultConfig()
	cfg.AckDurability = policy
	return NewManager(store, newMockGroupStore(), nil, cfg, slog.New(slog.NewTextHandler(io.Discard, nil)), nil)
}

func publishTo(t *testing.T, mgr *Manager, queueName string) {
	t.Helper()
	require.NoError(t, mgr.Publish(context.Background(), types.PublishRequest{
		Topic:   "$queue/" + queueName,
		Payload: []byte("payload"),
	}))
}

// TestAckDurabilityFsyncSyncsDurableQueuePublish is the guarantee the default
// buys: the publisher is not told a durable queue accepted the message until
// the append is on disk. Before this, only the protected internal stream took
// that path and every ordinary publish acknowledged from the page cache.
func TestAckDurabilityFsyncSyncsDurableQueuePublish(t *testing.T) {
	store := &syncRecordingStore{QueueStore: memlog.New()}
	mgr := newDurabilityManager(t, store, AckDurabilityFsync)

	require.NoError(t, mgr.CreateQueue(context.Background(), types.DefaultQueueConfig("durable", "$queue/durable")))
	publishTo(t, mgr, "durable")

	require.Equal(t, int64(1), store.synced.Load(), "durable publish did not sync before acknowledgement")
	require.Equal(t, int64(0), store.appends.Load(), "durable publish took the buffered path")
}

func TestDLQTransferUsesConfiguredDurabilityPath(t *testing.T) {
	store := &syncRecordingStore{QueueStore: memlog.New()}
	mgr := newDurabilityManager(t, store, AckDurabilityFsync)
	ctx := context.Background()

	require.NoError(t, mgr.CreateQueue(ctx, types.DefaultQueueConfig("tasks", "$queue/tasks/#")))
	require.NoError(t, mgr.moveToDLQ(ctx, "tasks", "workers", &types.Message{
		ID: "poison", Topic: "$queue/tasks/job", Payload: []byte("bad"),
	}, 7, 5, "decode failed", "$dlq/"))

	require.Equal(t, int64(1), store.synced.Load(), "DLQ append did not use the durable path")
	require.Equal(t, int64(0), store.appends.Load(), "DLQ append bypassed fsync policy")
}

// TestAckDurabilityBufferedSkipsTheSync covers the opt-out: a deployment that
// chooses throughput gets the unsynced path, and the loss window is the store's
// sync interval.
func TestAckDurabilityBufferedSkipsTheSync(t *testing.T) {
	store := &syncRecordingStore{QueueStore: memlog.New()}
	mgr := newDurabilityManager(t, store, AckDurabilityBuffered)

	require.NoError(t, mgr.CreateQueue(context.Background(), types.DefaultQueueConfig("durable", "$queue/durable")))
	publishTo(t, mgr, "durable")

	require.Equal(t, int64(1), store.appends.Load(), "buffered publish should not sync")
	require.Equal(t, int64(0), store.synced.Load())
}

// TestAckDurabilityFsyncSkipsEphemeralQueues keeps the cost proportional: an
// ephemeral queue does not survive a restart in the first place, so paying for
// a barrier on it buys nothing.
func TestAckDurabilityFsyncSkipsEphemeralQueues(t *testing.T) {
	store := &syncRecordingStore{QueueStore: memlog.New()}
	mgr := newDurabilityManager(t, store, AckDurabilityFsync)

	require.NoError(t, mgr.CreateQueue(context.Background(), types.DefaultEphemeralQueueConfig("ephemeral", "$queue/ephemeral")))
	publishTo(t, mgr, "ephemeral")

	require.Equal(t, int64(1), store.appends.Load())
	require.Equal(t, int64(0), store.synced.Load(), "ephemeral queues have nothing to make durable")
}

// TestAckDurabilityFallsBackWhenStoreCannotSync documents the one downgrade:
// a store with no durable-sync support cannot honour fsync, so the manager says
// so and keeps working. cmd refuses to start in that state, so production never
// reaches it.
func TestAckDurabilityFallsBackWhenStoreCannotSync(t *testing.T) {
	mgr := newDurabilityManager(t, memlog.New(), AckDurabilityFsync)

	require.Equal(t, AckDurabilityBuffered, mgr.ackDurability)
	require.NoError(t, mgr.CreateQueue(context.Background(), types.DefaultQueueConfig("durable", "$queue/durable")))
	publishTo(t, mgr, "durable")
}

// TestQueueAckDurabilityOverridesBrokerDefault is why the policy lives on the
// queue as well as the broker: one audit queue can demand the barrier without
// imposing ~5ms per publish on the telemetry queues beside it.
func TestQueueAckDurabilityOverridesBrokerDefault(t *testing.T) {
	store := &syncRecordingStore{QueueStore: memlog.New()}
	mgr := newDurabilityManager(t, store, AckDurabilityBuffered)
	ctx := context.Background()

	audit := types.DefaultQueueConfig("audit", "$queue/audit")
	audit.AckDurability = string(AckDurabilityFsync)
	require.NoError(t, mgr.CreateQueue(ctx, audit))
	require.NoError(t, mgr.CreateQueue(ctx, types.DefaultQueueConfig("telemetry", "$queue/telemetry")))

	publishTo(t, mgr, "audit")
	require.Equal(t, int64(1), store.synced.Load(), "queue asking for fsync did not sync")
	require.Equal(t, int64(0), store.appends.Load())

	publishTo(t, mgr, "telemetry")
	require.Equal(t, int64(1), store.synced.Load(), "a queue on the default should not sync")
	require.Equal(t, int64(1), store.appends.Load())
}

// TestQueueBufferedOverridesFsyncDefault covers the other direction: a broker
// that syncs by default still lets one high-volume queue opt out.
func TestQueueBufferedOverridesFsyncDefault(t *testing.T) {
	store := &syncRecordingStore{QueueStore: memlog.New()}
	mgr := newDurabilityManager(t, store, AckDurabilityFsync)
	ctx := context.Background()

	telemetry := types.DefaultQueueConfig("telemetry", "$queue/telemetry")
	telemetry.AckDurability = string(AckDurabilityBuffered)
	require.NoError(t, mgr.CreateQueue(ctx, telemetry))

	publishTo(t, mgr, "telemetry")
	require.Equal(t, int64(1), store.appends.Load())
	require.Equal(t, int64(0), store.synced.Load(), "queue asking for buffered was synced anyway")
}

func TestQueueBlankAckDurabilityUsesBrokerDefault(t *testing.T) {
	store := &syncRecordingStore{QueueStore: memlog.New()}
	mgr := newDurabilityManager(t, store, AckDurabilityFsync)

	audit := types.DefaultQueueConfig("audit", "$queue/audit")
	audit.AckDurability = " \t"
	require.NoError(t, mgr.CreateQueue(context.Background(), audit))
	publishTo(t, mgr, "audit")

	require.Equal(t, int64(1), store.synced.Load(), "a blank override must inherit the fsync default")
	require.Equal(t, int64(0), store.appends.Load())
}

func TestReplicatedQueueRejectsFsyncAckDurability(t *testing.T) {
	for _, tc := range []struct {
		name          string
		brokerPolicy  AckDurability
		queueOverride string
	}{
		{name: "inherited", brokerPolicy: AckDurabilityFsync},
		{name: "queue override", brokerPolicy: AckDurabilityBuffered, queueOverride: string(AckDurabilityFsync)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := &syncRecordingStore{QueueStore: memlog.New()}
			mgr := newDurabilityManager(t, store, tc.brokerPolicy)

			cfg := types.DefaultQueueConfig("replicated", "$queue/replicated")
			cfg.AckDurability = tc.queueOverride
			cfg.Replication.Enabled = true
			err := mgr.CreateQueue(context.Background(), cfg)

			require.ErrorIs(t, err, ErrFsyncReplicatedQueueUnsupported)
		})
	}
}

func TestReplicatedEphemeralQueueIgnoresFsyncAckDurability(t *testing.T) {
	store := &syncRecordingStore{QueueStore: memlog.New()}
	managerConfig := DefaultConfig()
	managerConfig.AckDurability = AckDurabilityFsync
	managerConfig.WritePolicy = WritePolicyReject
	mgr := NewManager(store, newMockGroupStore(), nil, managerConfig, slog.New(slog.NewTextHandler(io.Discard, nil)), nil)
	mgr.SetRaftCoordinator(&mockQueueCoordinator{
		enabled:           true,
		replicatedByQueue: map[string]bool{"replicated": true},
		leaderByQueue:     map[string]bool{"replicated": true},
	})

	cfg := types.DefaultEphemeralQueueConfig("replicated", "$queue/replicated")
	cfg.Replication.Enabled = true
	require.NoError(t, mgr.CreateQueue(context.Background(), cfg))
}

func TestCreateQueueRejectsUnknownAckDurability(t *testing.T) {
	store := &syncRecordingStore{QueueStore: memlog.New()}
	mgr := newDurabilityManager(t, store, AckDurabilityBuffered)

	cfg := types.DefaultQueueConfig("audit", "$queue/audit")
	cfg.AckDurability = "sometimes"
	err := mgr.CreateQueue(context.Background(), cfg)

	require.Error(t, err)
	require.Contains(t, err.Error(), "ack_durability must be one of")
}

func TestNormalizeAckDurability(t *testing.T) {
	for _, tc := range []struct {
		name string
		in   AckDurability
		want AckDurability
	}{
		{name: "empty defaults to buffered", in: "", want: AckDurabilityBuffered},
		{name: "fsync", in: "fsync", want: AckDurabilityFsync},
		{name: "buffered", in: "buffered", want: AckDurabilityBuffered},
		{name: "mixed case", in: "Buffered", want: AckDurabilityBuffered},
		{name: "unknown defaults to buffered", in: "sometimes", want: AckDurabilityBuffered},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, NormalizeAckDurability(tc.in))
		})
	}
}
