// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package logstorage

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/absmach/fluxmq/message"
	"github.com/absmach/fluxmq/queue/storage"
	"github.com/absmach/fluxmq/queue/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func queueEnvelope(id, topic string, data []byte) *message.Envelope {
	envelope := message.New(topic, data)
	envelope.Broker.Queue.MessageID = id
	return envelope
}

func TestAdapter_ReadBatch(t *testing.T) {
	dir := t.TempDir()
	adapter, err := NewAdapter(dir, DefaultAdapterConfig())
	require.NoError(t, err)
	defer adapter.Close()

	ctx := context.Background()
	cfg := types.DefaultQueueConfig("q1", "$queue/q1/#")
	require.NoError(t, adapter.CreateQueue(ctx, cfg))

	msgs := []*message.Envelope{
		queueEnvelope("1", "t", []byte("a")),
		queueEnvelope("2", "t", []byte("b")),
		queueEnvelope("3", "t", []byte("c")),
		queueEnvelope("4", "t", []byte("d")),
	}

	_, err = adapter.AppendBatch(ctx, "q1", msgs[:3])
	require.NoError(t, err)
	_, err = adapter.Append(ctx, "q1", msgs[3])
	require.NoError(t, err)

	got, err := adapter.ReadBatch(ctx, "q1", 1, 2)
	require.NoError(t, err)
	assert.Len(t, got, 2)
	assert.Equal(t, []byte("b"), got[0].PayloadBytes())
	assert.Equal(t, []byte("c"), got[1].PayloadBytes())

	got, err = adapter.ReadBatch(ctx, "q1", 2, 10)
	require.NoError(t, err)
	assert.Len(t, got, 2)
	assert.Equal(t, []byte("c"), got[0].PayloadBytes())
	assert.Equal(t, []byte("d"), got[1].PayloadBytes())

	got, err = adapter.ReadBatch(ctx, "q1", 10, 10)
	require.NoError(t, err)
	assert.Len(t, got, 0)
}

func TestAdapter_AppendRequiresQueueConfig(t *testing.T) {
	dir := t.TempDir()
	adapter, err := NewAdapter(dir, DefaultAdapterConfig())
	require.NoError(t, err)
	defer adapter.Close()

	ctx := context.Background()
	_, err = adapter.Append(ctx, "missing", queueEnvelope("1", "$queue/missing", []byte("x")))
	require.ErrorIs(t, err, storage.ErrQueueNotFound)
}

func TestAdapter_StreamCursorAndCommitDoNotRegress(t *testing.T) {
	dir := t.TempDir()
	adapter, err := NewAdapter(dir, DefaultAdapterConfig())
	require.NoError(t, err)
	defer adapter.Close()

	ctx := context.Background()

	cfg := types.DefaultQueueConfig("events", "$queue/events/#")
	cfg.Type = types.QueueTypeStream
	require.NoError(t, adapter.CreateQueue(ctx, cfg))

	group := types.NewConsumerGroupState("events", "streamers", "")
	group.Mode = types.GroupModeStream
	group.AutoCommit = true
	require.NoError(t, adapter.CreateConsumerGroup(ctx, group))

	require.NoError(t, adapter.UpdateCursor(ctx, "events", "streamers", 7))

	got, err := adapter.GetConsumerGroup(ctx, "events", "streamers")
	require.NoError(t, err)
	assert.Equal(t, uint64(7), got.CursorView().Cursor)

	require.NoError(t, adapter.UpdateCommitted(ctx, "events", "streamers", 7))

	got, err = adapter.GetConsumerGroup(ctx, "events", "streamers")
	require.NoError(t, err)
	assert.Equal(t, uint64(7), got.CursorView().Cursor)
	assert.Equal(t, uint64(7), got.CursorView().Committed)
}

func TestAdapter_UpdateQueueRefreshesTopicIndex(t *testing.T) {
	dir := t.TempDir()
	adapter, err := NewAdapter(dir, DefaultAdapterConfig())
	require.NoError(t, err)
	defer adapter.Close()

	ctx := context.Background()

	cfg := types.DefaultQueueConfig("orders", "$queue/orders/#")
	require.NoError(t, adapter.CreateQueue(ctx, cfg))

	matches, err := adapter.FindMatchingQueues(ctx, "$queue/orders/new")
	require.NoError(t, err)
	require.Len(t, matches, 1)
	assert.Equal(t, "orders", matches[0])

	cfg.Topics = []string{"$queue/payments/#"}
	require.NoError(t, adapter.UpdateQueue(ctx, cfg))

	matches, err = adapter.FindMatchingQueues(ctx, "$queue/orders/new")
	require.NoError(t, err)
	assert.Len(t, matches, 0)

	matches, err = adapter.FindMatchingQueues(ctx, "$queue/payments/new")
	require.NoError(t, err)
	require.Len(t, matches, 1)
	assert.Equal(t, "orders", matches[0])
}

func TestAdapter_ExpiresAtRoundtrip(t *testing.T) {
	dir := t.TempDir()
	adapter, err := NewAdapter(dir, DefaultAdapterConfig())
	require.NoError(t, err)
	defer adapter.Close()

	ctx := context.Background()
	cfg := types.DefaultQueueConfig("q1", "$queue/q1/#")
	require.NoError(t, adapter.CreateQueue(ctx, cfg))

	expiry := time.Now().Add(5 * time.Minute).Truncate(time.Millisecond)
	msg := queueEnvelope("1", "$queue/q1/test", []byte("data"))
	msg.Broker.Queue.ExpiresAt = expiry

	offset, err := adapter.Append(ctx, "q1", msg)
	require.NoError(t, err)

	got, err := adapter.Read(ctx, "q1", offset)
	require.NoError(t, err)
	assert.True(t, expiry.Equal(got.Broker.Queue.ExpiresAt))
}

func TestAdapter_ExpiresAtZeroNotPersisted(t *testing.T) {
	dir := t.TempDir()
	adapter, err := NewAdapter(dir, DefaultAdapterConfig())
	require.NoError(t, err)
	defer adapter.Close()

	ctx := context.Background()
	cfg := types.DefaultQueueConfig("q1", "$queue/q1/#")
	require.NoError(t, adapter.CreateQueue(ctx, cfg))

	msg := queueEnvelope("1", "$queue/q1/test", []byte("data"))

	offset, err := adapter.Append(ctx, "q1", msg)
	require.NoError(t, err)

	got, err := adapter.Read(ctx, "q1", offset)
	require.NoError(t, err)
	assert.True(t, got.Broker.Queue.ExpiresAt.IsZero())
}

func TestAdapter_ExpiresAtBatchRoundtrip(t *testing.T) {
	dir := t.TempDir()
	adapter, err := NewAdapter(dir, DefaultAdapterConfig())
	require.NoError(t, err)
	defer adapter.Close()

	ctx := context.Background()
	cfg := types.DefaultQueueConfig("q1", "$queue/q1/#")
	require.NoError(t, adapter.CreateQueue(ctx, cfg))

	expiry := time.Now().Add(10 * time.Minute).Truncate(time.Millisecond)
	first := queueEnvelope("1", "$queue/q1/a", []byte("a"))
	first.Broker.Queue.ExpiresAt = expiry
	msgs := []*message.Envelope{first, queueEnvelope("2", "$queue/q1/b", []byte("b"))}

	_, err = adapter.AppendBatch(ctx, "q1", msgs)
	require.NoError(t, err)

	got0, err := adapter.Read(ctx, "q1", 0)
	require.NoError(t, err)
	assert.True(t, expiry.Equal(got0.Broker.Queue.ExpiresAt))

	got1, err := adapter.Read(ctx, "q1", 1)
	require.NoError(t, err)
	assert.True(t, got1.Broker.Queue.ExpiresAt.IsZero())
}

func TestAdapter_QueueAPIKeyAndBinaryHeadersRoundTrip(t *testing.T) {
	dir := t.TempDir()
	adapter, err := NewAdapter(dir, DefaultAdapterConfig())
	require.NoError(t, err)
	defer adapter.Close()

	ctx := context.Background()
	cfg := types.DefaultQueueConfig("api", "$queue/api/#")
	require.NoError(t, adapter.CreateQueue(ctx, cfg))

	firstMessage := queueEnvelope("1", "$queue/api/one", []byte("one"))
	firstKey := []byte{0x00, 0xff}
	firstHeaders := map[string][]byte{"binary": {0x00, 0xff}}
	firstMessage.User.Key = firstKey
	firstMessage.User.Headers = firstHeaders
	secondMessage := queueEnvelope("2", "$queue/api/two", []byte("two"))
	secondKey := []byte("key-2")
	secondHeaders := map[string][]byte{"text": []byte("value")}
	secondMessage.User.Key = secondKey
	secondMessage.User.Headers = secondHeaders
	messages := []*message.Envelope{firstMessage, secondMessage}

	_, err = adapter.AppendBatch(ctx, "api", messages)
	require.NoError(t, err)

	got, err := adapter.ReadBatch(ctx, "api", 0, 2)
	require.NoError(t, err)
	require.Len(t, got, 2)
	defer message.Release(got[0])
	defer message.Release(got[1])
	assert.Equal(t, firstKey, got[0].User.Key)
	assert.Equal(t, firstHeaders, got[0].User.Headers)
	assert.Equal(t, secondKey, got[1].User.Key)
	assert.Equal(t, secondHeaders, got[1].User.Headers)
}

func TestAdapterAppendOwnershipContract(t *testing.T) {
	dir := t.TempDir()
	adapter, err := NewAdapter(dir, DefaultAdapterConfig())
	require.NoError(t, err)
	defer adapter.Close()

	ctx := context.Background()
	require.NoError(t, adapter.CreateQueue(ctx, types.DefaultQueueConfig("owned", "$queue/owned/#")))

	stored := queueEnvelope("1", "$queue/owned/one", []byte("stored"))
	storedPayload := stored.Payload
	_, err = adapter.Append(ctx, "owned", stored)
	require.NoError(t, err)
	require.Equal(t, int32(0), storedPayload.RefCount(), "successful append must consume the envelope")

	rejected := queueEnvelope("2", "$queue/missing/two", []byte("rejected"))
	rejectedPayload := rejected.Payload
	_, err = adapter.Append(ctx, "missing", rejected)
	require.Error(t, err)
	require.Equal(t, int32(1), rejectedPayload.RefCount(), "failed append must leave ownership with the caller")
	message.Release(rejected)
}

func TestAdapter_AppendAndSyncHonorsContextAndReportsDurability(t *testing.T) {
	dir := t.TempDir()
	adapter, err := NewAdapter(dir, DefaultAdapterConfig())
	require.NoError(t, err)
	defer adapter.Close()

	assert.True(t, adapter.SupportsDurableSync())

	ctx := context.Background()
	cfg := types.DefaultQueueConfig("audit", "$queue/audit/#")
	require.NoError(t, adapter.CreateQueue(ctx, cfg))

	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	_, err = adapter.AppendAndSync(cancelled, "audit", queueEnvelope("1", "t", []byte("a")))
	require.ErrorIs(t, err, context.Canceled)

	count, err := adapter.Count(ctx, "audit")
	require.NoError(t, err)
	assert.Zero(t, count, "cancelled publish must not reach the log")

	_, err = adapter.AppendAndSync(ctx, "audit", queueEnvelope("2", "t", []byte("b")))
	require.NoError(t, err)
	count, err = adapter.Count(ctx, "audit")
	require.NoError(t, err)
	assert.Equal(t, uint64(1), count)
}

func TestSyncDirRejectsMissingDirectory(t *testing.T) {
	require.NoError(t, SyncDir(t.TempDir()))
	require.Error(t, SyncDir(filepath.Join(t.TempDir(), "absent")))
}

func TestAdapter_CreateQueueRestoresMetadataLostAfterCrash(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	cfg := types.DefaultQueueConfig("audit", "$queue/audit/#")
	cfg.Type = types.QueueTypeStream
	cfg.Reserved = true

	adapter, err := NewAdapter(dir, DefaultAdapterConfig())
	require.NoError(t, err)
	require.NoError(t, adapter.CreateQueue(ctx, cfg))
	_, err = adapter.AppendAndSync(ctx, "audit", queueEnvelope("1", "t", []byte("a")))
	require.NoError(t, err)
	require.NoError(t, adapter.Close())

	// A crash between the log directory reaching disk and its metadata doing so
	// leaves the acknowledged record present but the queue invisible.
	require.NoError(t, os.Remove(filepath.Join(dir, "config", "queues.json")))

	reopened, err := NewAdapter(dir, DefaultAdapterConfig())
	require.NoError(t, err)
	t.Cleanup(func() { reopened.Close() })

	_, err = reopened.GetQueue(ctx, "audit")
	require.ErrorIs(t, err, storage.ErrQueueNotFound, "precondition: metadata is gone")

	require.NoError(t, reopened.CreateQueue(ctx, cfg), "recreating the queue must repair the lost metadata")

	restored, err := reopened.GetQueue(ctx, "audit")
	require.NoError(t, err)
	assert.Equal(t, cfg.Name, restored.Name)

	msg, err := reopened.Read(ctx, "audit", 0)
	require.NoError(t, err)
	assert.Equal(t, []byte("a"), msg.PayloadBytes())

	// A queue whose metadata is intact is still reported as already existing.
	assert.ErrorIs(t, reopened.CreateQueue(ctx, cfg), storage.ErrQueueAlreadyExists)
}

func TestMkdirAllSyncedCreatesNestedDirectories(t *testing.T) {
	root := t.TempDir()
	nested := filepath.Join(root, "queues", "audit", "segments")
	require.NoError(t, MkdirAllSynced(nested, 0o755))

	info, err := os.Stat(nested)
	require.NoError(t, err)
	assert.True(t, info.IsDir())

	require.NoError(t, MkdirAllSynced(nested, 0o755), "existing directories are accepted")

	file := filepath.Join(root, "file")
	require.NoError(t, os.WriteFile(file, []byte("x"), 0o600))
	require.Error(t, MkdirAllSynced(file, 0o755), "a file in the path must not be reported as a directory")
}

// The disk-backed adapter is the production store, so it must refuse a binding
// that can never match rather than persist one.
func TestAdapterRejectsFiltersThatCannotMatch(t *testing.T) {
	adapter, err := NewAdapter(t.TempDir(), DefaultAdapterConfig())
	if err != nil {
		t.Fatalf("NewAdapter failed: %v", err)
	}
	t.Cleanup(func() { _ = adapter.Close() })

	ctx := context.Background()
	if err := adapter.CreateQueue(ctx, types.DefaultQueueConfig("black-holed", "#/events")); err == nil {
		t.Fatal("CreateQueue persisted a filter that can never match")
	}

	if err := adapter.CreateQueue(ctx, types.DefaultQueueConfig("working", "m/#")); err != nil {
		t.Fatalf("CreateQueue failed: %v", err)
	}
	if err := adapter.UpdateQueue(ctx, types.DefaultQueueConfig("working", "m/#/events")); err == nil {
		t.Fatal("UpdateQueue persisted a filter that can never match")
	}
}

// A queue persisted before filters were validated must not be bound silently.
// Startup cannot refuse it — the data is already on disk — so it is reported,
// and the queue keeps working through whichever of its filters are valid.
func TestAdapterReportsPersistedFiltersThatCannotMatch(t *testing.T) {
	dir := t.TempDir()

	// Write the bad filter behind the adapter's validation, as a release that
	// did not validate would have left it.
	queueStore, err := NewQueueConfigStore(dir)
	if err != nil {
		t.Fatalf("NewQueueConfigStore failed: %v", err)
	}
	legacy := types.DefaultQueueConfig("legacy", "#/events", "m/#")
	if err := queueStore.Save(legacy); err != nil {
		t.Fatalf("Save failed: %v", err)
	}
	if err := queueStore.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	var reported []string
	config := DefaultAdapterConfig()
	config.RecoveryLogger = func(msg string, args ...any) {
		reported = append(reported, msg)
	}

	adapter, err := NewAdapter(dir, config)
	if err != nil {
		t.Fatalf("NewAdapter failed: %v", err)
	}
	t.Cleanup(func() { _ = adapter.Close() })

	if len(reported) != 1 {
		t.Fatalf("reported %d malformed filters, want 1: %v", len(reported), reported)
	}

	// The queue still matches through its valid filter.
	matched, err := adapter.FindMatchingQueues(context.Background(), "m/acme")
	if err != nil {
		t.Fatalf("FindMatchingQueues failed: %v", err)
	}
	if len(matched) != 1 || matched[0] != "legacy" {
		t.Fatalf("FindMatchingQueues = %v, want [legacy]", matched)
	}
}
