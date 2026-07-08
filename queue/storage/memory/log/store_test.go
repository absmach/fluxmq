// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package log

import (
	"context"
	"testing"

	core "github.com/absmach/fluxmq/mqtt"
	"github.com/absmach/fluxmq/queue/types"
	"github.com/stretchr/testify/require"
)

func newTestStore(t *testing.T, queueName string) *Store {
	t.Helper()
	store := New()
	require.NoError(t, store.CreateQueue(context.Background(), types.DefaultQueueConfig(queueName, "$queue/"+queueName+"/#")))
	return store
}

func TestStore_AppendCopiesPayloadBuffer(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t, "buffered")

	// Cap-1 pool: releasing then re-acquiring returns the same underlying
	// buffer, so a reused Get overwrites the bytes the store may alias.
	pool := core.NewBufferPoolWithCapacity(1, 0, 0)
	msg := &types.Message{ID: "append-buf", Topic: "$queue/buffered"}
	msg.SetPayloadFromBuffer(pool.GetWithData([]byte("remote-payload")))

	_, err := store.Append(ctx, "buffered", msg)
	require.NoError(t, err)

	msg.ReleasePayload()
	reused := pool.GetWithData([]byte("overwritten!!!"))
	defer reused.Release()

	got, err := store.Read(ctx, "buffered", 0)
	require.NoError(t, err)
	require.Equal(t, "remote-payload", string(got.GetPayload()))
}

func TestStore_AppendBatchCopiesPayloadBuffers(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t, "buffered-batch")

	pool := core.NewBufferPoolWithCapacity(1, 0, 0)
	msg := &types.Message{ID: "batch-buf", Topic: "$queue/buffered-batch"}
	msg.SetPayloadFromBuffer(pool.GetWithData([]byte("remote-payload")))

	_, err := store.AppendBatch(ctx, "buffered-batch", []*types.Message{msg})
	require.NoError(t, err)

	msg.ReleasePayload()
	reused := pool.GetWithData([]byte("overwritten!!!"))
	defer reused.Release()

	got, err := store.Read(ctx, "buffered-batch", 0)
	require.NoError(t, err)
	require.Equal(t, "remote-payload", string(got.GetPayload()))
}

func TestStore_AppendPlainPayloadNotCopied(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t, "plain")

	payload := []byte("plain-payload")
	msg := &types.Message{ID: "plain-1", Topic: "$queue/plain", Payload: payload}

	_, err := store.Append(ctx, "plain", msg)
	require.NoError(t, err)

	got, err := store.Read(ctx, "plain", 0)
	require.NoError(t, err)
	require.Equal(t, &payload[0], &got.GetPayload()[0], "plain payload must be retained without copying")
}
