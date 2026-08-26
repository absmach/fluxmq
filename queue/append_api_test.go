// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package queue

import (
	"context"
	"testing"

	memlog "github.com/absmach/fluxmq/queue/storage/memory/log"
	"github.com/absmach/fluxmq/queue/types"
	"github.com/stretchr/testify/require"
)

// appendOne and appendBatch express what the deleted Manager wrappers used to:
// the vocabulary now lives on the command surface, and these keep the tests
// readable without reinstating a facade nothing else calls.
func appendOne(ctx context.Context, mgr *Manager, queueName string, publish types.PublishRequest) (uint64, error) {
	outcome, err := mgr.StateMachine().Append(ctx, AppendCommand{
		QueueName: queueName,
		Messages:  []types.PublishRequest{publish},
	})
	return outcome.FirstOffset, err
}

func appendBatch(ctx context.Context, mgr *Manager, queueName string, publishes []types.PublishRequest) (uint64, uint32, error) {
	outcome, err := mgr.StateMachine().Append(ctx, AppendCommand{
		QueueName:   queueName,
		Messages:    publishes,
		AtomicBatch: true,
	})
	return outcome.FirstOffset, outcome.Count, err
}

func TestAppendToQueueReturnsAssignedOffsetAndTargetsExactlyOneQueue(t *testing.T) {
	ctx := context.Background()
	store := memlog.New()
	mgr := newDurabilityManager(t, store, AckDurabilityBuffered)

	require.NoError(t, mgr.CreateQueue(ctx, types.DefaultQueueConfig("primary", "shared/#")))
	require.NoError(t, mgr.CreateQueue(ctx, types.DefaultQueueConfig("secondary", "shared/#")))

	offset, err := appendOne(ctx, mgr, "primary", types.PublishRequest{
		Topic:   "shared/value",
		Payload: []byte("payload"),
		Key:     []byte{0x00, 0xff},
		Headers: map[string][]byte{"binary": {0x00, 0xff}},
	})
	require.NoError(t, err)
	require.Equal(t, uint64(0), offset)

	msg, err := store.Read(ctx, "primary", offset)
	require.NoError(t, err)
	require.Equal(t, []byte{0x00, 0xff}, msg.User.Key)
	require.Equal(t, []byte{0x00, 0xff}, msg.User.Headers["binary"])

	count, err := store.Count(ctx, "secondary")
	require.NoError(t, err)
	require.Zero(t, count, "exact append fanned out through topic matching")
}

func TestAppendBatchToQueueReturnsContiguousOffsets(t *testing.T) {
	ctx := context.Background()
	store := memlog.New()
	mgr := newDurabilityManager(t, store, AckDurabilityBuffered)
	require.NoError(t, mgr.CreateQueue(ctx, types.DefaultQueueConfig("batch", "batch/#")))

	first, count, err := appendBatch(ctx, mgr, "batch", []types.PublishRequest{
		{Topic: "batch/1", Payload: []byte("one"), Key: []byte("k1")},
		{Topic: "batch/2", Payload: []byte("two"), Key: []byte("k2")},
	})
	require.NoError(t, err)
	require.Equal(t, uint64(0), first)
	require.Equal(t, uint32(2), count)

	firstMessage, err := store.Read(ctx, "batch", first)
	require.NoError(t, err)
	lastMessage, err := store.Read(ctx, "batch", first+uint64(count)-1)
	require.NoError(t, err)
	require.Equal(t, []byte("k1"), firstMessage.User.Key)
	require.Equal(t, []byte("k2"), lastMessage.User.Key)
}

func TestAppendBatchToQueueRejectsUnsupportedAtomicContracts(t *testing.T) {
	ctx := context.Background()

	t.Run("fsync", func(t *testing.T) {
		store := &syncRecordingStore{Store: memlog.New()}
		mgr := newDurabilityManager(t, store, AckDurabilityFsync)
		require.NoError(t, mgr.CreateQueue(ctx, types.DefaultQueueConfig("fsync", "fsync/#")))

		_, _, err := appendBatch(ctx, mgr, "fsync", []types.PublishRequest{{Payload: []byte("value")}})
		require.ErrorIs(t, err, ErrAtomicBatchDurabilityUnsupported)
		failure := ClassifyError(err)
		require.Equal(t, ErrorCodeFailedPrecondition, failure.Code)
		require.Equal(t, DurabilityUnsupported, failure.Durability)
	})

	t.Run("replication", func(t *testing.T) {
		store := memlog.New()
		cfg := types.DefaultQueueConfig("replicated", "replicated/#")
		cfg.Replication.Enabled = true
		require.NoError(t, store.CreateQueue(ctx, cfg))
		mgr := newDurabilityManager(t, store, AckDurabilityBuffered)

		_, _, err := appendBatch(ctx, mgr, "replicated", []types.PublishRequest{{Payload: []byte("value")}})
		require.ErrorIs(t, err, ErrAtomicBatchReplicationUnsupported)
		require.Equal(t, ErrorCodeFailedPrecondition, ClassifyError(err).Code)
	})
}
