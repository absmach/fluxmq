// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package consumer

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/absmach/fluxmq/message"
	"github.com/absmach/fluxmq/queue/storage"
	memlog "github.com/absmach/fluxmq/queue/storage/memory/log"
	"github.com/absmach/fluxmq/queue/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	testPoisonQueue    = "poison-queue"
	testPoisonGroup    = "workers"
	testPoisonOwner    = "consumer-stuck"
	testPoisonThief    = "consumer-thief"
	testMaxDeliveries  = 3
	testPoisonPatterns = "poison/#"
)

var errDLQGone = errors.New("dead-letter queue is disabled")

// groupStore adapts the in-memory log store to storage.ConsumerGroupStore. The
// two differ only in the element type of ListConsumers; shadowing it here avoids
// a hand-written fake for fifteen methods.
//
// The in-memory store is used rather than the log adapter because the adapter
// stamps its own ClaimedAt and DeliveryCount when an entry is added, so an
// exhausted, long-idle entry cannot be expressed through it.
type groupStore struct {
	*memlog.Store
}

// RegisterConsumer and UnregisterConsumer differ in shape, and they record
// membership on the group itself the way logstorage.Adapter does. Stubbing them
// out left group.Consumers empty, so every pending entry looked orphaned and
// StealableEntries handed back deliveries whose visibility lease was still held.
func (s *groupStore) RegisterConsumer(ctx context.Context, queueName, groupID string, consumer *types.ConsumerInfo) error {
	group, err := s.Store.GetConsumerGroup(ctx, queueName, groupID)
	if err != nil {
		return err
	}
	group.SetConsumer(consumer.ID, consumer)

	return s.Store.UpdateConsumerGroup(ctx, group)
}

func (s *groupStore) UnregisterConsumer(ctx context.Context, queueName, groupID, consumerID string) error {
	group, err := s.Store.GetConsumerGroup(ctx, queueName, groupID)
	if err != nil {
		return err
	}
	group.DeleteConsumer(consumerID)

	return s.Store.UpdateConsumerGroup(ctx, group)
}

// RequeuePendingEntry is the optional capability nack uses to release a
// delivery ahead of its visibility lease. The in-memory log
// store does not provide it, so the double supplies it over the group state.
func (s *groupStore) RequeuePendingEntry(ctx context.Context, queueName, groupID, consumerID string, offset uint64, attemptedAt time.Time) error {
	group, err := s.Store.GetConsumerGroup(ctx, queueName, groupID)
	if err != nil {
		return err
	}
	if !group.RequeuePending(offset, consumerID, attemptedAt) {
		return storage.ErrPendingEntryNotFound
	}

	return s.Store.UpdateConsumerGroup(ctx, group)
}

func (s *groupStore) ListConsumers(ctx context.Context, queueName, groupID string) ([]*types.ConsumerInfo, error) {
	consumers, err := s.Store.ListConsumers(ctx, queueName, groupID)
	if err != nil {
		return nil, err
	}
	infos := make([]*types.ConsumerInfo, 0, len(consumers))
	for _, c := range consumers {
		infos = append(infos, &types.ConsumerInfo{ID: c.ID, ClientID: c.ClientID})
	}
	return infos, nil
}

type poisonFixture struct {
	manager *Manager
	group   *types.ConsumerGroup
	store   *groupStore
	calls   *int
}

// newPoisonFixture builds a group holding one entry that has already exhausted
// its delivery budget, claimed long enough ago to be stealable.
func newPoisonFixture(t *testing.T, onDLQ DLQHandler, unavailable func(error) bool, metrics *Metrics) poisonFixture {
	t.Helper()

	ctx := context.Background()
	store := &groupStore{Store: memlog.New()}
	require.NoError(t, store.CreateQueue(ctx, types.DefaultQueueConfig(testPoisonQueue, testPoisonPatterns)))

	envelope := message.New(testPoisonQueue, []byte("poison"))
	offset, err := store.Append(ctx, testPoisonQueue, envelope)
	require.NoError(t, err)

	group := types.NewConsumerGroupState(testPoisonQueue, testPoisonGroup, "")
	group.ReplacePEL(map[string][]*types.PendingEntry{
		testPoisonOwner: {{
			Offset:        offset,
			ConsumerID:    testPoisonOwner,
			ClaimedAt:     time.Now().Add(-time.Hour),
			DeliveryCount: testMaxDeliveries,
		}},
	})
	require.NoError(t, store.CreateConsumerGroup(ctx, group))
	require.Len(t, group.StealableEntries(time.Millisecond, testPoisonThief), 1,
		"fixture must present exactly one stealable exhausted entry")

	// A nil handler must stay nil: that is the "no dead-letter destination
	// configured at all" case the branch has to distinguish.
	calls := 0
	var counted DLQHandler
	if onDLQ != nil {
		counted = func(ctx context.Context, queueName, groupID string, msg *message.Envelope, offset uint64, deliveryCount int, reason string) error {
			calls++
			return onDLQ(ctx, queueName, groupID, msg, offset, deliveryCount, reason)
		}
	}

	manager := NewManager(store, store, Config{
		VisibilityTimeout: time.Millisecond,
		MaxDeliveryCount:  testMaxDeliveries,
		ClaimBatchSize:    10,
		StealBatchSize:    5,
		DLQRetryBackoff:   time.Hour,
		DLQUnavailable:    unavailable,
		Metrics:           metrics,
		Logger:            slog.New(slog.NewTextHandler(io.Discard, nil)),
		OnDLQ:             counted,
	})

	return poisonFixture{manager: manager, group: group, store: store, calls: &calls}
}

// A poison message whose queue has no dead-letter destination must keep being
// redelivered. Holding it pending forever occupies a slot for a transfer that
// can never happen, and eventually stalls the group on MaxPELSize.
func TestPoisonWithoutDLQIsRedeliveredNotBlocked(t *testing.T) {
	tests := []struct {
		name        string
		onDLQ       DLQHandler
		unavailable func(error) bool
	}{
		{
			name:  "no handler configured",
			onDLQ: nil,
		},
		{
			name: "queue reports no destination",
			onDLQ: func(context.Context, string, string, *message.Envelope, uint64, int, string) error {
				return errDLQGone
			},
			unavailable: func(err error) bool { return errors.Is(err, errDLQGone) },
		},
		{
			name: "handler reports itself unavailable",
			onDLQ: func(context.Context, string, string, *message.Envelope, uint64, int, string) error {
				return ErrDLQHandlerUnavailable
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			metrics := NewMetrics()
			fixture := newPoisonFixture(t, tt.onDLQ, tt.unavailable, metrics)

			ctx := context.Background()

			// With no handler at all the claim path knows immediately. With one
			// configured, the first sweep is what discovers it has nowhere to
			// send the message, and the claim after that redelivers.
			stolen, err := fixture.manager.stealWork(ctx, fixture.group, testPoisonThief, nil)
			if err != nil {
				require.ErrorIs(t, err, ErrNoMessages, "the entry waits for the sweeper, not for a consumer")
				fixture.manager.SweepPoison(ctx)
				stolen, err = fixture.manager.stealWork(ctx, fixture.group, testPoisonThief, nil)
			}

			require.NoError(t, err, "an undead-letterable poison message must still be delivered")
			require.NotNil(t, stolen)
			message.Release(stolen)

			assert.Positive(t, metrics.PoisonWithoutDLQ,
				"redelivering a poison message without a destination must be counted")
			assert.Zero(t, metrics.DLQTransferFailures,
				"an absent destination is not a transfer failure")
		})
	}
}

// A transient failure is different: a destination exists, so the entry stays
// pending and the transfer is retried. Redelivering now would duplicate a
// message the transfer may still deliver.
func TestTransientDLQFailureKeepsEntryPending(t *testing.T) {
	metrics := NewMetrics()
	fixture := newPoisonFixture(t, func(context.Context, string, string, *message.Envelope, uint64, int, string) error {
		return errors.New("storage temporarily unavailable")
	}, nil, metrics)

	ctx := context.Background()

	stolen, err := fixture.manager.stealWork(ctx, fixture.group, testPoisonThief, nil)
	require.ErrorIs(t, err, ErrNoMessages, "a retryable poison entry must not be redelivered")
	require.Nil(t, stolen)

	// The transfer runs off the claim path now.
	fixture.manager.SweepPoison(ctx)

	assert.Equal(t, uint64(1), metrics.DLQTransferFailures)
	assert.Zero(t, metrics.PoisonWithoutDLQ)
	assert.Equal(t, 1, *fixture.calls)
}

// A persistently failing transfer must not consume a steal slot on every cycle.
func TestFailingDLQTransferIsRateLimited(t *testing.T) {
	metrics := NewMetrics()
	fixture := newPoisonFixture(t, func(context.Context, string, string, *message.Envelope, uint64, int, string) error {
		return errors.New("storage temporarily unavailable")
	}, nil, metrics)

	ctx := context.Background()
	_, err := fixture.manager.stealWork(ctx, fixture.group, testPoisonThief, nil)
	require.ErrorIs(t, err, ErrNoMessages)

	for range 5 {
		fixture.manager.SweepPoison(ctx)
	}

	assert.Equal(t, 1, *fixture.calls,
		"the backoff must throttle retries; the handler ran on every sweep")
	assert.Equal(t, uint64(1), metrics.DLQTransferFailures)
}

// The happy path still settles the entry and records the transfer.
func TestSuccessfulDLQTransferSettlesEntry(t *testing.T) {
	metrics := NewMetrics()
	fixture := newPoisonFixture(t, func(context.Context, string, string, *message.Envelope, uint64, int, string) error {
		return nil
	}, nil, metrics)

	ctx := context.Background()
	_, err := fixture.manager.stealWork(ctx, fixture.group, testPoisonThief, nil)
	require.ErrorIs(t, err, ErrNoMessages, "the entry was not redelivered while awaiting transfer")

	fixture.manager.SweepPoison(ctx)

	assert.Equal(t, uint64(1), metrics.DLQCount)
	assert.Zero(t, metrics.DLQTransferFailures)

	fresh, err := fixture.store.GetConsumerGroup(context.Background(), testPoisonQueue, testPoisonGroup)
	require.NoError(t, err)
	assert.Zero(t, fresh.PendingCount(), "a transferred entry must leave the pending list")
}

// The poison counters have to survive the trip through Snapshot, because that
// is the only way an operator sees them. Incrementing a field that Snapshot
// omits produces a counter that is always zero from outside the process, which
// is indistinguishable from nothing going wrong.
func TestPoisonCountersReachTheSnapshot(t *testing.T) {
	metrics := NewMetrics()
	metrics.RecordDLQTransferFailure()
	metrics.RecordDLQTransferFailure()
	metrics.RecordPoisonWithoutDLQ()

	snapshot := metrics.Snapshot()
	assert.Equal(t, uint64(2), snapshot.DLQTransferFailures,
		"dead-letter transfer failures must be observable outside the process")
	assert.Equal(t, uint64(1), snapshot.PoisonWithoutDLQ,
		"poison messages without a destination must be observable outside the process")

	metrics.Reset()
	cleared := metrics.Snapshot()
	assert.Zero(t, cleared.DLQTransferFailures, "reset must clear what snapshot reports")
	assert.Zero(t, cleared.PoisonWithoutDLQ)
}

// The gauge answers "how many messages are stuck right now", which is the
// question worth alerting on. A counter cannot: one permanently stuck message
// increments it forever, so its rate tracks how often consumers looked rather
// than how bad things are.
func TestPoisonGaugeReflectsCurrentlyStuckEntries(t *testing.T) {
	ctx := context.Background()
	metrics := NewMetrics()
	fixture := newPoisonFixture(t, nil, nil, metrics)

	// The queue has no dead-letter destination, so the entry falls through to
	// redelivery and stays stuck.
	stolen, err := fixture.manager.stealWork(ctx, fixture.group, testPoisonThief, nil)
	require.NoError(t, err)
	message.Release(stolen)

	snapshot := metrics.Snapshot()
	assert.Equal(t, uint64(1), snapshot.PoisonPending, "the stuck entry must show in the gauge")
	assert.Equal(t, uint64(1), snapshot.PoisonPendingNoDestination,
		"an entry with nowhere to go must be distinguishable from a failing transfer")
	assert.Equal(t, uint64(1), snapshot.PoisonWithoutDLQ)

	// Make the entry stale again so a second consumer can examine it. Examining
	// it must not inflate either number: it is the same entry, and the counter
	// records entering the state rather than noticing it.
	require.True(t, fixture.group.RequeuePending(0, testPoisonThief, time.Now().Add(-time.Hour)))

	stolen, err = fixture.manager.stealWork(ctx, fixture.group, "consumer-other", nil)
	require.NoError(t, err)
	message.Release(stolen)

	repeated := metrics.Snapshot()
	assert.Equal(t, uint64(1), repeated.PoisonPending, "one entry must count once however often it is examined")
	assert.Equal(t, uint64(1), repeated.PoisonWithoutDLQ,
		"the counter must record the transition, not every observation")
}

// A gauge that only counts up is worse than none. An entry settled by an
// ordinary ack leaves no dead-letter signal, so the sweep has to notice it is
// gone.
func TestPoisonGaugeFallsWhenTheEntryIsSettled(t *testing.T) {
	ctx := context.Background()
	metrics := NewMetrics()
	fixture := newPoisonFixture(t, nil, nil, metrics)

	stolen, err := fixture.manager.stealWork(ctx, fixture.group, testPoisonThief, nil)
	require.NoError(t, err)
	message.Release(stolen)
	require.Equal(t, uint64(1), metrics.Snapshot().PoisonPending)

	// The steal transferred ownership, so the thief is the one that settles it.
	require.NoError(t, fixture.manager.Ack(ctx, testPoisonQueue, testPoisonGroup, testPoisonThief, 0))

	// The next sweep is where a settled entry leaves the population.
	_, err = fixture.manager.stealWork(ctx, fixture.group, testPoisonThief, nil)
	require.ErrorIs(t, err, ErrNoMessages)

	assert.Zero(t, metrics.Snapshot().PoisonPending, "a settled entry must leave the gauge")
	assert.Zero(t, metrics.Snapshot().PoisonPendingNoDestination)
}
