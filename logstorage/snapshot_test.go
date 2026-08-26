// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package logstorage

import (
	"context"
	"testing"
	"time"

	"github.com/absmach/fluxmq/message"
	"github.com/absmach/fluxmq/queue/storage"
	"github.com/absmach/fluxmq/queue/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newSnapshotAdapter(t *testing.T) *Adapter {
	t.Helper()

	adapter, err := NewAdapter(t.TempDir(), DefaultAdapterConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = adapter.Close() })
	return adapter
}

// smallSegmentAdapter rolls a segment per record, which is what lets a test
// retain a prefix away: the log truncates whole segments, so a head only moves
// when the records below it fill segments of their own.
func smallSegmentAdapter(t *testing.T) *Adapter {
	t.Helper()

	cfg := DefaultAdapterConfig()
	cfg.StoreConfig.ManagerConfig.MaxSegmentSize = 1
	adapter, err := NewAdapter(t.TempDir(), cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = adapter.Close() })
	return adapter
}

func snapshotQueueConfig(name string) types.QueueConfig {
	return types.DefaultQueueConfig(name, name+"/#")
}

func TestAdapterSnapshotRoundTripsRecords(t *testing.T) {
	ctx := context.Background()
	source := smallSegmentAdapter(t)
	config := snapshotQueueConfig("orders")
	require.NoError(t, source.CreateQueue(ctx, config))

	payloads := []string{"first", "second", "third", "fourth"}
	for _, payload := range payloads {
		_, err := source.Append(ctx, config.Name, message.New(config.Name+"/x", []byte(payload)))
		require.NoError(t, err)
	}
	// Retention has taken the first two, so the log no longer starts at zero.
	require.NoError(t, source.Truncate(ctx, config.Name, 2))

	head, records := drainQueueSnapshot(t, source, config.Name)
	assert.Equal(t, uint64(2), head)
	require.Len(t, records, 2)

	target := smallSegmentAdapter(t)
	require.NoError(t, target.RestoreQueue(ctx, config, head))
	for i, record := range records {
		require.NoError(t, target.RestoreRecord(ctx, config.Name, head+uint64(i), record))
	}

	restoredHead, err := target.Head(ctx, config.Name)
	require.NoError(t, err)
	assert.Equal(t, uint64(2), restoredHead, "a retained prefix must not be rebuilt from zero")
	restoredTail, err := target.Tail(ctx, config.Name)
	require.NoError(t, err)
	assert.Equal(t, uint64(4), restoredTail)

	for offset, want := range map[uint64]string{2: "third", 3: "fourth"} {
		got, readErr := target.Read(ctx, config.Name, offset)
		require.NoError(t, readErr, "offset %d must survive", offset)
		assert.Equal(t, want, string(got.PayloadBytes()), "offset %d", offset)
		message.Release(got)
	}
}

// The deduplication index is durable derived state that a plain append does not
// write. A queue restored without it would accept a transfer it already holds.
func TestAdapterRestoreRebuildsDeduplicationIndex(t *testing.T) {
	ctx := context.Background()
	source := newSnapshotAdapter(t)
	config := snapshotQueueConfig("transfers")
	require.NoError(t, source.CreateQueue(ctx, config))

	const key = "transfer-1"
	offset, deduplicated, err := source.AppendOnce(ctx, config.Name, key, message.New(config.Name+"/x", []byte("payload")))
	require.NoError(t, err)
	require.False(t, deduplicated)

	head, records := drainQueueSnapshot(t, source, config.Name)
	require.Len(t, records, 1)

	target := newSnapshotAdapter(t)
	require.NoError(t, target.RestoreQueue(ctx, config, head))
	require.NoError(t, target.RestoreRecord(ctx, config.Name, head, records[0]))

	// The same transfer retried against the restored queue must be recognised.
	gotOffset, gotDeduplicated, err := target.AppendOnce(ctx, config.Name, key, message.New(config.Name+"/x", []byte("payload")))
	require.NoError(t, err)
	assert.True(t, gotDeduplicated, "a restored queue must recognise a key it already holds")
	assert.Equal(t, offset, gotOffset)

	count, err := target.Count(ctx, config.Name)
	require.NoError(t, err)
	assert.Equal(t, uint64(1), count, "a recognised retry must not append a second record")
}

// Raft reports a failed install without moving lastApplied, so a record written
// before the rejection is left in a store the FSM believes it never touched.
// Refusing has to happen before anything is durable.
func TestAdapterRestoreRecordRejectsOffsetGapWithoutWriting(t *testing.T) {
	ctx := context.Background()
	adapter := newSnapshotAdapter(t)
	config := snapshotQueueConfig("gapped")
	require.NoError(t, adapter.RestoreQueue(ctx, config, 10))

	err := adapter.RestoreRecord(ctx, config.Name, 11, message.New(config.Name+"/x", []byte("payload")))
	assert.ErrorIs(t, err, storage.ErrOffsetOutOfRange, "a record that does not continue the log must be refused")

	count, err := adapter.Count(ctx, config.Name)
	require.NoError(t, err)
	assert.Zero(t, count, "a refused record must not be written")

	tail, err := adapter.Tail(ctx, config.Name)
	require.NoError(t, err)
	assert.Equal(t, uint64(10), tail, "a refused record must not move the tail")

	_, err = adapter.Read(ctx, config.Name, 10)
	assert.Error(t, err, "a refused record must not be readable")
}

// The operational cursor and pending list live in the log store, not beside the
// group metadata: delivery, acknowledgement and redelivery all read them there.
// A restored group written only to the metadata store reads back with a cursor
// of zero and a pending list no settlement can act on.
func TestAdapterCreateConsumerGroupSeedsOperationalState(t *testing.T) {
	ctx := context.Background()
	adapter := newSnapshotAdapter(t)
	config := snapshotQueueConfig("jobs")
	require.NoError(t, adapter.CreateQueue(ctx, config))

	// Committed is the oldest offset still unacknowledged, so the entry pending
	// at that offset is exactly the one a restore must not settle. A fixture
	// that puts the pending entry below Committed contradicts itself and would
	// hide that.
	claimedAt := time.Now().Add(-time.Minute).Truncate(time.Millisecond)
	group := &types.ConsumerGroup{
		ID: "workers", QueueName: config.Name, Mode: types.GroupModeQueue,
		Cursor: &types.QueueCursor{Cursor: 8, Committed: 7},
		PEL: map[string][]*types.PendingEntry{
			"c1": {{Offset: 7, ConsumerID: "c1", ClaimedAt: claimedAt, DeliveryCount: 3}},
		},
		Consumers: map[string]*types.ConsumerInfo{},
	}
	require.NoError(t, adapter.CreateConsumerGroup(ctx, group))

	got, err := adapter.GetConsumerGroup(ctx, config.Name, "workers")
	require.NoError(t, err)
	snapshot := got.Snapshot()
	assert.Equal(t, uint64(8), snapshot.Cursor.Cursor, "the cursor must survive the restore")
	require.Len(t, snapshot.PEL["c1"], 1, "the entry at the committed offset must survive the restore")
	assert.Equal(t, uint64(7), snapshot.PEL["c1"][0].Offset)
	assert.Equal(t, 3, snapshot.PEL["c1"][0].DeliveryCount,
		"the attempt count decides redelivery and dead-lettering, so it must survive too")

	committed, err := adapter.store.GetCommittedOffset(config.Name, "workers")
	require.NoError(t, err)
	assert.Equal(t, uint64(7), committed, "the committed position must survive the restore")

	// The entry has to be actionable, not merely visible.
	assert.NoError(t, adapter.RemovePendingEntry(ctx, config.Name, "workers", "c1", 7),
		"a restored pending entry must be settleable")
}

// Raft applies entries while Persist runs. A view that resolves its queue by
// name would follow a delete-and-recreate onto a different log, and the snapshot
// would pair one queue's records with another's configuration and groups.
func TestAdapterQueueSnapshotFailsWhenQueueIsReplaced(t *testing.T) {
	ctx := context.Background()
	adapter := newSnapshotAdapter(t)
	config := snapshotQueueConfig("replaced-under-capture")
	require.NoError(t, adapter.CreateQueue(ctx, config))
	_, err := adapter.Append(ctx, config.Name, message.New(config.Name+"/x", []byte("old")))
	require.NoError(t, err)

	reader, err := adapter.OpenQueueSnapshot(ctx, config.Name)
	require.NoError(t, err)
	t.Cleanup(func() { _ = reader.Close() })

	require.NoError(t, adapter.DeleteQueue(ctx, config.Name))
	require.NoError(t, adapter.CreateQueue(ctx, config))
	_, err = adapter.Append(ctx, config.Name, message.New(config.Name+"/x", []byte("new")))
	require.NoError(t, err)

	_, envelope, ok, err := reader.Next(ctx)
	if envelope != nil {
		message.Release(envelope)
	}
	assert.False(t, ok, "a replaced queue must not read back as the captured one")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "replaced")
}

// RestoreQueue replaces what is already there: a snapshot is the group's state,
// not a change to merge into local state.
func TestAdapterRestoreQueueReplacesExistingRecords(t *testing.T) {
	ctx := context.Background()
	adapter := newSnapshotAdapter(t)
	config := snapshotQueueConfig("replaced")
	require.NoError(t, adapter.CreateQueue(ctx, config))
	_, err := adapter.Append(ctx, config.Name, message.New(config.Name+"/x", []byte("stale")))
	require.NoError(t, err)

	require.NoError(t, adapter.RestoreQueue(ctx, config, 0))

	count, err := adapter.Count(ctx, config.Name)
	require.NoError(t, err)
	assert.Zero(t, count, "a record absent from the snapshot must not survive")
}

func TestAdapterResetForRestoreClearsOnlyNamedQueues(t *testing.T) {
	ctx := context.Background()
	adapter := newSnapshotAdapter(t)

	mine := snapshotQueueConfig("mine")
	theirs := snapshotQueueConfig("theirs")
	for _, config := range []types.QueueConfig{mine, theirs} {
		require.NoError(t, adapter.CreateQueue(ctx, config))
		_, err := adapter.Append(ctx, config.Name, message.New(config.Name+"/x", []byte("payload")))
		require.NoError(t, err)
	}

	require.NoError(t, adapter.ResetForRestore(ctx, []string{mine.Name}))

	_, err := adapter.GetQueue(ctx, mine.Name)
	assert.Error(t, err, "a named queue must be cleared")

	count, err := adapter.Count(ctx, theirs.Name)
	require.NoError(t, err)
	assert.Equal(t, uint64(1), count, "a queue another raft group owns must be untouched")
}

// A restored queue has to survive the process, not just the call: the point of
// the durable store is that recovery finds what was written.
func TestAdapterRestoreSurvivesReopen(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	config := snapshotQueueConfig("durable")

	adapter, err := NewAdapter(dir, DefaultAdapterConfig())
	require.NoError(t, err)
	require.NoError(t, adapter.RestoreQueue(ctx, config, 7))
	require.NoError(t, adapter.RestoreRecord(ctx, config.Name, 7, message.New(config.Name+"/x", []byte("restored"))))
	require.NoError(t, adapter.Close())

	reopened, err := NewAdapter(dir, DefaultAdapterConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = reopened.Close() })

	head, err := reopened.Head(ctx, config.Name)
	require.NoError(t, err)
	assert.Equal(t, uint64(7), head)

	got, err := reopened.Read(ctx, config.Name, 7)
	require.NoError(t, err)
	assert.Equal(t, "restored", string(got.PayloadBytes()))
	message.Release(got)
}

// drainQueueSnapshot reads a captured view to the end, the way Persist does.
func drainQueueSnapshot(t *testing.T, adapter *Adapter, queueName string) (uint64, []*message.Envelope) {
	t.Helper()

	reader, err := adapter.OpenQueueSnapshot(context.Background(), queueName)
	require.NoError(t, err)
	t.Cleanup(func() { _ = reader.Close() })

	var records []*message.Envelope
	for {
		_, envelope, ok, nextErr := reader.Next(context.Background())
		require.NoError(t, nextErr)
		if !ok {
			return reader.Head(), records
		}
		records = append(records, envelope)
	}
}

// A snapshot is taken on the raft goroutine, which cannot apply entries while it
// runs. Opening the view must therefore not read the queue.
func TestAdapterOpenQueueSnapshotDefersReads(t *testing.T) {
	ctx := context.Background()
	adapter := newSnapshotAdapter(t)
	config := snapshotQueueConfig("deferred")
	require.NoError(t, adapter.CreateQueue(ctx, config))

	for range 4 {
		_, err := adapter.Append(ctx, config.Name, message.New(config.Name+"/x", []byte("payload")))
		require.NoError(t, err)
	}

	reader, err := adapter.OpenQueueSnapshot(ctx, config.Name)
	require.NoError(t, err)
	t.Cleanup(func() { _ = reader.Close() })

	assert.Equal(t, uint64(0), reader.Head())
	assert.Equal(t, uint64(4), reader.Tail(), "the range is captured up front")

	view, ok := reader.(*adapterQueueSnapshot)
	require.True(t, ok)
	assert.Empty(t, view.buffered, "opening the view must not have read any record")

	_, envelope, ok, err := reader.Next(ctx)
	require.NoError(t, err)
	require.True(t, ok)
	message.Release(envelope)
	assert.NotEmpty(t, view.buffered, "records are read only once they are asked for")
}
