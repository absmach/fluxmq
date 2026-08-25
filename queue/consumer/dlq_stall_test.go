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
func newStallFixture(ctx context.Context, t *testing.T, queues ...string) stallFixture {
	t.Helper()

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
	ctx := context.Background()
	f := newStallFixture(ctx, t, "poisoned", "unrelated")

	done := make(chan error, 1)
	go func() {
		done <- f.manager.Reject(ctx, "poisoned", stallGroup, stallOwner, f.poisonAt, "poison")
	}()

	<-f.entered
	waited := waitOn(f, "unrelated")
	close(f.release)
	require.NoError(t, <-done)

	assert.Less(t, waited, stallHeldFor/3,
		"an unrelated group waited %s on a dead-letter transfer", waited)
}

// The owning group must not wait either. The destination write runs with the
// group lock released, so a consumer acking or heartbeating on the same group
// proceeds while the transfer is in flight. Holding the lock across that write
// stalled the group for its full duration - a Raft round trip for a replicated
// destination, bounded only by AckTimeout.
func TestDLQTransferDoesNotStallItsOwnGroup(t *testing.T) {
	ctx := context.Background()
	f := newStallFixture(ctx, t, "poisoned")

	done := make(chan error, 1)
	go func() {
		done <- f.manager.Reject(ctx, "poisoned", stallGroup, stallOwner, f.poisonAt, "poison")
	}()

	<-f.entered
	waited := waitOn(f, "poisoned")
	close(f.release)
	require.NoError(t, <-done)

	assert.Less(t, waited, stallHeldFor/3,
		"the owning group waited %s on its own dead-letter transfer", waited)
}

// The entry stays reserved while the write is in flight. Settling it would race
// the write and leave a message both acked and dead-lettered.
func TestSettlingDuringTransferIsRefused(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name    string
		settle  func(*Manager, uint64) error
		wantErr error
	}{
		{
			name:    "ack",
			settle:  func(m *Manager, offset uint64) error { return m.Ack(ctx, "poisoned", stallGroup, stallOwner, offset) },
			wantErr: ErrTransferInProgress,
		},
		{
			name:    "nack",
			settle:  func(m *Manager, offset uint64) error { return m.Nack(ctx, "poisoned", stallGroup, stallOwner, offset) },
			wantErr: ErrTransferInProgress,
		},
		{
			name: "second reject",
			settle: func(m *Manager, offset uint64) error {
				return m.Reject(ctx, "poisoned", stallGroup, stallOwner, offset, "poison")
			},
			wantErr: ErrTransferInProgress,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f := newStallFixture(ctx, t, "poisoned")

			done := make(chan error, 1)
			go func() {
				done <- f.manager.Reject(ctx, "poisoned", stallGroup, stallOwner, f.poisonAt, "poison")
			}()

			<-f.entered
			err := tc.settle(f.manager, f.poisonAt)
			close(f.release)
			require.NoError(t, <-done)

			assert.ErrorIs(t, err, tc.wantErr)
		})
	}
}

// Once the transfer resolves the reservation is gone, so the entry behaves
// normally again - including after a failure, where it stays pending.
func TestReservationIsReleasedAfterTransfer(t *testing.T) {
	ctx := context.Background()
	f := newStallFixture(ctx, t, "poisoned")

	done := make(chan error, 1)
	go func() {
		done <- f.manager.Reject(ctx, "poisoned", stallGroup, stallOwner, f.poisonAt, "poison")
	}()

	<-f.entered
	require.True(t, f.manager.transferring("poisoned", stallGroup, f.poisonAt))
	close(f.release)
	require.NoError(t, <-done)

	assert.False(t, f.manager.transferring("poisoned", stallGroup, f.poisonAt),
		"a resolved transfer must not leave the entry reserved")
}

// A failed destination write must leave the entry pending and unreserved, so a
// later attempt can retry it. The destination deduplicates, so the retry cannot
// produce a second record.
func TestFailedTransferReleasesReservationAndKeepsEntryPending(t *testing.T) {
	ctx := context.Background()
	f := newStallFixture(ctx, t, "poisoned")
	f.manager.config.OnDLQ = func(context.Context, string, string, *message.Envelope, uint64, int, string) error {
		return errDLQGone
	}

	err := f.manager.Reject(ctx, "poisoned", stallGroup, stallOwner, f.poisonAt, "poison")
	require.ErrorIs(t, err, errDLQGone)

	assert.False(t, f.manager.transferring("poisoned", stallGroup, f.poisonAt))

	group, err := f.manager.groupStore.GetConsumerGroup(ctx, "poisoned", stallGroup)
	require.NoError(t, err)
	entry, _ := group.FindPending(f.poisonAt)
	assert.NotNil(t, entry, "a failed transfer must leave the source pending")
}
