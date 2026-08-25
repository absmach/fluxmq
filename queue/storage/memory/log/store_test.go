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

	pool := payload.NewPoolWithCapacity(1, 0, 0)
	buf := pool.FromBytes([]byte("remote-payload"))
	msg := message.NewWithBuffer("$queue/buffered", buf)
	msg.Broker.Queue.MessageID = "append-buf"

	_, err := store.Append(ctx, "buffered", msg)
	require.NoError(t, err)

	got, err := store.Read(ctx, "buffered", 0)
	require.NoError(t, err)
	require.Same(t, buf, got.Payload)
	require.Equal(t, "remote-payload", string(got.PayloadBytes()))
}

func TestStore_AppendBatchTakesEnvelopeOwnershipWithoutCopy(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t, "buffered-batch")

	pool := payload.NewPoolWithCapacity(1, 0, 0)
	buf := pool.FromBytes([]byte("remote-payload"))
	msg := message.NewWithBuffer("$queue/buffered-batch", buf)
	msg.Broker.Queue.MessageID = "batch-buf"

	_, err := store.AppendBatch(ctx, "buffered-batch", []*message.Envelope{msg})
	require.NoError(t, err)

	got, err := store.Read(ctx, "buffered-batch", 0)
	require.NoError(t, err)
	require.Same(t, buf, got.Payload)
	require.Equal(t, "remote-payload", string(got.PayloadBytes()))
}

func TestStore_AppendPlainPayloadNotCopied(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t, "plain")

	data := []byte("plain-payload")
	msg := message.New("$queue/plain", data)
	msg.Broker.Queue.MessageID = "plain-1"

	_, err := store.Append(ctx, "plain", msg)
	require.NoError(t, err)

	got, err := store.Read(ctx, "plain", 0)
	require.NoError(t, err)
	require.Same(t, msg.Payload, got.Payload, "append must retain the envelope payload without copying")
}
