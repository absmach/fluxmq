// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package log

import (
	"context"
	"testing"

	"github.com/absmach/fluxmq/message"
	"github.com/absmach/fluxmq/payload"
	"github.com/absmach/fluxmq/queue/types"
	"github.com/stretchr/testify/require"
)

func newTestStore(t *testing.T, queueName string) *Store {
	t.Helper()
	store := New()
	require.NoError(t, store.CreateQueue(context.Background(), types.DefaultQueueConfig(queueName, "$queue/"+queueName+"/#")))
	return store
}

func TestStore_AppendTakesEnvelopeOwnershipWithoutCopy(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t, "buffered")

	pool := payload.NewPool()
	buf := pool.FromBytes([]byte("remote-payload"))
	msg := message.NewWithBuffer("$queue/buffered", buf)
	msg.PublisherMeta.MessageID = "append-buf"

	_, err := store.Append(ctx, "buffered", msg)
	require.NoError(t, err)

	got, err := store.Read(ctx, "buffered", 0)
	require.NoError(t, err)
	defer message.Release(got)
	require.Equal(t, int32(2), buf.RefCount(), "read must retain the stored payload instead of copying it")
	require.Equal(t, "remote-payload", string(got.PayloadBytes()))
}

func TestStore_AppendBatchTakesEnvelopeOwnershipWithoutCopy(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t, "buffered-batch")

	pool := payload.NewPool()
	buf := pool.FromBytes([]byte("remote-payload"))
	msg := message.NewWithBuffer("$queue/buffered-batch", buf)
	msg.PublisherMeta.MessageID = "batch-buf"

	_, err := store.AppendBatch(ctx, "buffered-batch", []*message.Envelope{msg})
	require.NoError(t, err)

	got, err := store.Read(ctx, "buffered-batch", 0)
	require.NoError(t, err)
	defer message.Release(got)
	require.Equal(t, int32(2), buf.RefCount(), "batch read must retain the stored payload instead of copying it")
	require.Equal(t, "remote-payload", string(got.PayloadBytes()))
}

func TestStore_AppendPlainPayloadNotCopied(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t, "plain")

	data := []byte("plain-payload")
	msg := message.New("$queue/plain", data)
	buf := msg.RetainPayload()
	defer buf.Release()
	msg.PublisherMeta.MessageID = "plain-1"

	_, err := store.Append(ctx, "plain", msg)
	require.NoError(t, err)

	got, err := store.Read(ctx, "plain", 0)
	require.NoError(t, err)
	defer message.Release(got)
	require.Equal(t, int32(3), buf.RefCount(), "append and read must share the immutable payload")
}

// The memory store implements the same deduplication contract as the persistent
// one, so a queue backed by it behaves the same way for a retried transfer.
// It needs no recovery: the index is lost with the records it describes.
func TestAppendOnceDeduplicatesAndPrunes(t *testing.T) {
	ctx := context.Background()
	store := New()
	require.NoError(t, store.CreateQueue(ctx, types.DefaultQueueConfig("q", "q/#")))

	first, duplicated, err := store.AppendOnce(ctx, "q", "key", message.New("q", []byte("one")))
	require.NoError(t, err)
	require.False(t, duplicated)

	second, duplicated, err := store.AppendOnce(ctx, "q", "key", message.New("q", []byte("two")))
	require.NoError(t, err)
	require.True(t, duplicated, "a repeated key must be recognised")
	require.Equal(t, first, second)

	count, err := store.Count(ctx, "q")
	require.NoError(t, err)
	require.Equal(t, uint64(1), count)

	// Once the record is truncated away the key must be forgotten: reporting a
	// duplicate for a record the caller can no longer read would be a lie.
	require.NoError(t, store.Truncate(ctx, "q", first+1))
	_, duplicated, err = store.AppendOnce(ctx, "q", "key", message.New("q", []byte("three")))
	require.NoError(t, err)
	require.False(t, duplicated, "a key whose record was truncated must not deduplicate")

	_, _, err = store.AppendOnce(ctx, "q", "", message.New("q", []byte("four")))
	require.Error(t, err, "an append that cannot be deduplicated must be refused")
}
