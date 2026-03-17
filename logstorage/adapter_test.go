// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package logstorage

import (
	"context"
	"testing"
	"time"

	"github.com/absmach/fluxmq/queue/storage"
	"github.com/absmach/fluxmq/queue/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAdapter_ReadBatch(t *testing.T) {
	dir := t.TempDir()
	adapter, err := NewAdapter(dir, DefaultAdapterConfig())
	require.NoError(t, err)
	defer adapter.Close()

	ctx := context.Background()
	cfg := types.DefaultQueueConfig("q1", "$queue/q1/#")
	require.NoError(t, adapter.CreateQueue(ctx, cfg))

	msgs := []*types.Message{
		{ID: "1", Topic: "t", Payload: []byte("a")},
		{ID: "2", Topic: "t", Payload: []byte("b")},
		{ID: "3", Topic: "t", Payload: []byte("c")},
		{ID: "4", Topic: "t", Payload: []byte("d")},
	}

	_, err = adapter.AppendBatch(ctx, "q1", msgs[:3])
	require.NoError(t, err)
	_, err = adapter.Append(ctx, "q1", msgs[3])
	require.NoError(t, err)

	got, err := adapter.ReadBatch(ctx, "q1", 1, 2)
	require.NoError(t, err)
	assert.Len(t, got, 2)
	assert.Equal(t, []byte("b"), got[0].Payload)
	assert.Equal(t, []byte("c"), got[1].Payload)

	got, err = adapter.ReadBatch(ctx, "q1", 2, 10)
	require.NoError(t, err)
	assert.Len(t, got, 2)
	assert.Equal(t, []byte("c"), got[0].Payload)
	assert.Equal(t, []byte("d"), got[1].Payload)

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
	_, err = adapter.Append(ctx, "missing", &types.Message{ID: "1", Topic: "$queue/missing", Payload: []byte("x")})
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
	assert.Equal(t, uint64(7), got.GetCursor().Cursor)

	require.NoError(t, adapter.UpdateCommitted(ctx, "events", "streamers", 7))

	got, err = adapter.GetConsumerGroup(ctx, "events", "streamers")
	require.NoError(t, err)
	assert.Equal(t, uint64(7), got.GetCursor().Cursor)
	assert.Equal(t, uint64(7), got.GetCursor().Committed)
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
	msg := &types.Message{
		ID:        "1",
		Topic:     "$queue/q1/test",
		Payload:   []byte("data"),
		ExpiresAt: expiry,
	}

	offset, err := adapter.Append(ctx, "q1", msg)
	require.NoError(t, err)

	got, err := adapter.Read(ctx, "q1", offset)
	require.NoError(t, err)
	assert.Equal(t, expiry, got.ExpiresAt)
}

func TestAdapter_ExpiresAtZeroNotPersisted(t *testing.T) {
	dir := t.TempDir()
	adapter, err := NewAdapter(dir, DefaultAdapterConfig())
	require.NoError(t, err)
	defer adapter.Close()

	ctx := context.Background()
	cfg := types.DefaultQueueConfig("q1", "$queue/q1/#")
	require.NoError(t, adapter.CreateQueue(ctx, cfg))

	msg := &types.Message{
		ID:      "1",
		Topic:   "$queue/q1/test",
		Payload: []byte("data"),
	}

	offset, err := adapter.Append(ctx, "q1", msg)
	require.NoError(t, err)

	got, err := adapter.Read(ctx, "q1", offset)
	require.NoError(t, err)
	assert.True(t, got.ExpiresAt.IsZero())
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
	msgs := []*types.Message{
		{ID: "1", Topic: "$queue/q1/a", Payload: []byte("a"), ExpiresAt: expiry},
		{ID: "2", Topic: "$queue/q1/b", Payload: []byte("b")},
	}

	_, err = adapter.AppendBatch(ctx, "q1", msgs)
	require.NoError(t, err)

	got0, err := adapter.Read(ctx, "q1", 0)
	require.NoError(t, err)
	assert.Equal(t, expiry, got0.ExpiresAt)

	got1, err := adapter.Read(ctx, "q1", 1)
	require.NoError(t, err)
	assert.True(t, got1.ExpiresAt.IsZero())
}
