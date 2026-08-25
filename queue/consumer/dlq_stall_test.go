// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package consumer

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/absmach/fluxmq/message"
	memlog "github.com/absmach/fluxmq/queue/storage/memory/log"
	"github.com/absmach/fluxmq/queue/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	stallOwner   = "consumer-owner"
	stallGroup   = "workers"
	stallHeldFor = 300 * time.Millisecond
)

type stallFixture struct {
	manager  *Manager
	entered  chan struct{}
	release  chan struct{}
	poisonAt uint64
}

// newStallFixture builds two independent queues, each with one group. The first
// holds a pending entry that Reject will dead-letter through a handler that
// blocks, so the lock held across the transfer can be observed.
func newStallFixture(t *testing.T, queues ...string) stallFixture {
	t.Helper()

	ctx := context.Background()
	store := &groupStore{Store: memlog.New()}

	var poisonAt uint64
	for i, name := range queues {
		require.NoError(t, store.CreateQueue(ctx, types.DefaultQueueConfig(name, name+"/#")))
		offset, err := store.Append(ctx, name, message.New(name, []byte("payload")))
		require.NoError(t, err)

		group := types.NewConsumerGroupState(name, stallGroup, "")
		group.SetConsumer(stallOwner, &types.ConsumerInfo{ID: stallOwner, ClientID: stallOwner})
		if i == 0 {
			poisonAt = offset
			group.ReplacePEL(map[string][]*types.PendingEntry{
				stallOwner: {{
					Offset:        offset,
					ConsumerID:    stallOwner,
					ClaimedAt:     time.Now(),
					DeliveryCount: 1,
				}},
			})
		}
		require.NoError(t, store.CreateConsumerGroup(ctx, group))
	}

	entered := make(chan struct{})
	release := make(chan struct{})
	blocking := func(context.Context, string, string, *message.Envelope, uint64, int, string) error {
		close(entered)
		<-release
		return nil
	}

	manager := NewManager(store, store, Config{
		VisibilityTimeout: time.Minute,
		MaxDeliveryCount:  5,
		ClaimBatchSize:    10,
		StealBatchSize:    5,
		MaxPELSize:        1000,
		Logger:            slog.New(slog.NewTextHandler(io.Discard, nil)),
		OnDLQ:             blocking,
	})

	return stallFixture{manager: manager, entered: entered, release: release, poisonAt: poisonAt}
}

// waitOn reports how long an operation on one group waited.
func waitOn(f stallFixture, queueName string) time.Duration {
	start := time.Now()
	_ = f.manager.UpdateHeartbeat(context.Background(), queueName, stallGroup, stallOwner)
	return time.Since(start)
}

// A dead-letter transfer holds the owning group's lock for as long as the
// destination write takes. Groups that share nothing with it must not wait:
// that cross-group stall is what scoping the lock per group removed, and this
// pins it.
func TestDLQTransferDoesNotStallUnrelatedGroups(t *testing.T) {
	f := newStallFixture(t, "poisoned", "unrelated")

	done := make(chan error, 1)
	go func() {
		done <- f.manager.Reject(context.Background(), "poisoned", stallGroup, stallOwner, f.poisonAt, "poison")
	}()

	<-f.entered
	waited := waitOn(f, "unrelated")
	close(f.release)
	require.NoError(t, <-done)

	assert.Less(t, waited, stallHeldFor/3,
		"an unrelated group waited %s on a dead-letter transfer", waited)
}

// The same group does wait: the transfer runs under its lock. This records the
// cost rather than asserting it away, so the decision to unlock during the
// transfer rests on a measurement.
func TestDLQTransferStallsItsOwnGroup(t *testing.T) {
	f := newStallFixture(t, "poisoned")

	done := make(chan error, 1)
	go func() {
		done <- f.manager.Reject(context.Background(), "poisoned", stallGroup, stallOwner, f.poisonAt, "poison")
	}()

	<-f.entered
	go func() {
		time.Sleep(stallHeldFor)
		close(f.release)
	}()
	waited := waitOn(f, "poisoned")
	require.NoError(t, <-done)

	t.Logf("same-group operation waited %s while the transfer held the lock", waited)
	assert.GreaterOrEqual(t, waited, stallHeldFor/2,
		"the transfer is expected to hold its own group's lock")
}
