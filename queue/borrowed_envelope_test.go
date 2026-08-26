// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package queue

import (
	"context"
	"testing"
	"time"

	"github.com/absmach/fluxmq/message"
	memlog "github.com/absmach/fluxmq/queue/storage/memory/log"
	"github.com/absmach/fluxmq/queue/types"
	"github.com/stretchr/testify/require"
)

// The command surface borrows: the caller releases its envelope the moment the
// call returns, and the payload buffer goes back to the pool with it. The
// record the queue stored has to survive that, which it does by holding a
// reference of its own rather than a copy of the bytes. Releasing the caller's
// envelope and then reading the record is the whole contract in one assertion —
// an implementation that aliased the caller's buffer reads back corrupted here
// once the pool hands it out again.
func TestPublishedRecordSurvivesTheCallersRelease(t *testing.T) {
	ctx := context.Background()
	store := memlog.New()
	mgr := newDurabilityManager(t, store, AckDurabilityBuffered)
	require.NoError(t, mgr.CreateQueue(ctx, types.DefaultQueueConfig("orders", "orders/#")))

	published := message.New("orders/new", []byte("the-payload"))
	published.PublisherMeta.Properties = map[string]string{"tenant": "acme"}
	published.PublisherMeta.Headers = map[string][]byte{"unit": []byte("celsius")}

	require.NoError(t, mgr.Publish(ctx, published))

	// The caller's envelope dies here, exactly as every protocol ingress does it.
	message.Release(published)
	churnThePool(t)

	stored, err := store.Read(ctx, "orders", 0)
	require.NoError(t, err)
	require.Equal(t, "the-payload", string(stored.PayloadBytes()))
	require.Equal(t, "orders/new", stored.Topic)
	require.Equal(t, "acme", stored.PublisherMeta.Properties["tenant"])
	require.Equal(t, "celsius", string(stored.PublisherMeta.Headers["unit"]))
}

// The same contract on the capture path, which is harder: the dispatcher runs
// on another goroutine long after the caller has let go.
func TestCapturedRecordSurvivesTheCallersRelease(t *testing.T) {
	ctx := context.Background()
	store := memlog.New()
	mgr := newDurabilityManager(t, store, AckDurabilityBuffered)
	require.NoError(t, mgr.Start(ctx))
	t.Cleanup(func() { _ = mgr.Stop() })
	require.NoError(t, mgr.CreateQueue(ctx, types.DefaultQueueConfig("captured", "sensors/#")))

	published := message.New("sensors/temperature", []byte("21.5"))
	published.PublisherMeta.Properties = map[string]string{"tenant": "acme"}

	require.NoError(t, mgr.PublishToMatchingQueues(ctx, published))
	message.Release(published)
	churnThePool(t)

	require.Eventually(t, func() bool {
		count, err := store.Count(ctx, "captured")
		return err == nil && count == 1
	}, 2*time.Second, 5*time.Millisecond, "capture never reached the store")

	stored, err := store.Read(ctx, "captured", 0)
	require.NoError(t, err)
	require.Equal(t, "21.5", string(stored.PayloadBytes()))
	require.Equal(t, "acme", stored.PublisherMeta.Properties["tenant"])
}

// churnThePool hands out and returns buffers of the same size class, so a
// record that aliased a released buffer reads back someone else's bytes rather
// than getting away with it.
func churnThePool(t *testing.T) {
	t.Helper()
	for range 64 {
		scratch := message.New("scratch", []byte("xxxxxxxxxxxxxxxxxxxxxxxx"))
		copy(scratch.PayloadBytes(), "########################")
		message.Release(scratch)
	}
}
