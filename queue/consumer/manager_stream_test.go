// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package consumer

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/absmach/fluxmq/message"
	memlog "github.com/absmach/fluxmq/queue/storage/memory/log"
	"github.com/absmach/fluxmq/queue/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	testStreamQueue    = "stream-queue"
	testStreamGroup    = "readers"
	testStreamConsumer = "consumer-1"
)

func newStreamFixture(t *testing.T, records int) (*Manager, *types.ConsumerGroup) {
	t.Helper()

	ctx := context.Background()
	store := &groupStore{Store: memlog.New()}
	require.NoError(t, store.CreateQueue(ctx, types.DefaultQueueConfig(testStreamQueue, testStreamQueue+"/#")))
	for range records {
		_, err := store.Append(ctx, testStreamQueue, message.New(testStreamQueue, []byte("payload")))
		require.NoError(t, err)
	}

	group := types.NewConsumerGroupState(testStreamQueue, testStreamGroup, "")
	group.Mode = types.GroupModeStream
	group.SetCursor(uint64(records), 0)
	require.NoError(t, store.CreateConsumerGroup(ctx, group))

	return NewManager(store, store, Config{
		VisibilityTimeout: time.Minute,
		MaxDeliveryCount:  5,
		ClaimBatchSize:    10,
		MaxPELSize:        1000,
	}), group
}

// newStreamFixtureStore is newStreamFixture with the store exposed, for tests
// that assert on what was persisted rather than on the in-memory group.
func newStreamFixtureStore(t *testing.T, records int) (*Manager, *groupStore, *types.ConsumerGroup) {
	t.Helper()

	ctx := context.Background()
	store := &groupStore{Store: memlog.New()}
	require.NoError(t, store.CreateQueue(ctx, types.DefaultQueueConfig(testStreamQueue, testStreamQueue+"/#")))
	for range records {
		_, err := store.Append(ctx, testStreamQueue, message.New(testStreamQueue, []byte("payload")))
		require.NoError(t, err)
	}

	group := types.NewConsumerGroupState(testStreamQueue, testStreamGroup, "")
	group.Mode = types.GroupModeStream
	group.SetCursor(uint64(records), 0)
	require.NoError(t, store.CreateConsumerGroup(ctx, group))

	return NewManager(store, store, Config{
		VisibilityTimeout: time.Minute,
		MaxDeliveryCount:  5,
		ClaimBatchSize:    10,
		MaxPELSize:        1000,
	}), store, group
}

// The committed position must never move backwards.
//
// Read and write have to be one operation: split, two acknowledgements both
// read the old position and the lower one writes last, so a stream group is
// redelivered records it already settled. Both stores assign unconditionally,
// so nothing downstream catches it.
func TestAdvanceCommittedIsMonotonicUnderConcurrency(t *testing.T) {
	ctx := context.Background()
	const records = 256
	manager, group := newStreamFixture(t, records)

	var wg sync.WaitGroup
	for offset := range records {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = manager.AdvanceCommitted(ctx, testStreamQueue, testStreamGroup, uint64(offset)+1)
		}()
	}
	wg.Wait()

	assert.Equal(t, uint64(records), group.CursorView().Committed,
		"the highest acknowledgement must win regardless of completion order")
}

// A commit behind the safe point is a rewind, not a commit, and silently
// redelivers settled records.
func TestCommitOffsetRejectsRegression(t *testing.T) {
	ctx := context.Background()
	manager, _ := newStreamFixture(t, 10)

	require.NoError(t, manager.CommitOffset(ctx, testStreamQueue, testStreamGroup, 8))
	err := manager.CommitOffset(ctx, testStreamQueue, testStreamGroup, 3)
	assert.ErrorIs(t, err, ErrCommitOffsetNotMonotonic)
}

func TestCommitOffsetSettlesManualStreamPendingEntries(t *testing.T) {
	ctx := context.Background()
	manager, group := newStreamFixture(t, 10)
	group.SetAutoCommit(false)
	group.AddPending(testStreamConsumer, &types.PendingEntry{Offset: 2, ConsumerID: testStreamConsumer, ClaimedAt: time.Now(), DeliveryCount: 1})
	group.AddPending(testStreamConsumer, &types.PendingEntry{Offset: 7, ConsumerID: testStreamConsumer, ClaimedAt: time.Now(), DeliveryCount: 1})
	group.AddPending(testStreamConsumer, &types.PendingEntry{Offset: 9, ConsumerID: testStreamConsumer, ClaimedAt: time.Now(), DeliveryCount: 1})

	require.NoError(t, manager.CommitOffset(ctx, testStreamQueue, testStreamGroup, 8))
	assert.Equal(t, uint64(8), group.CursorView().Committed)
	assert.Equal(t, 1, group.PendingCount())
	_, owner := group.FindPending(9)
	assert.Equal(t, testStreamConsumer, owner)
}

func newManualStreamFixture(t *testing.T, records int) (*Manager, *groupStore, *types.ConsumerGroup) {
	t.Helper()

	ctx := context.Background()
	store := &groupStore{Store: memlog.New()}
	require.NoError(t, store.CreateQueue(ctx, types.DefaultQueueConfig(testStreamQueue, testStreamQueue+"/#")))
	for range records {
		_, err := store.Append(ctx, testStreamQueue, message.New(testStreamQueue, []byte("payload")))
		require.NoError(t, err)
	}

	group := types.NewConsumerGroupState(testStreamQueue, testStreamGroup, "")
	group.Mode = types.GroupModeStream
	group.AutoCommit = false
	require.NoError(t, store.CreateConsumerGroup(ctx, group))

	manager := NewManager(store, store, Config{
		VisibilityTimeout: time.Minute,
		MaxDeliveryCount:  5,
		ClaimBatchSize:    10,
		MaxPELSize:        1000,
	})

	return manager, store, group
}

// A consumer asking for its own unsettled entry back must not wait out the
// visibility timeout. That timeout stops one consumer stealing from another
// that is still working; applied to the owner it stalled every reconnect for
// its full duration.
func TestClaimManualStreamRedeliversOwnEntryWithoutWaitingVisibility(t *testing.T) {
	ctx := context.Background()
	manager, store, _ := newManualStreamFixture(t, 2)

	first, err := manager.ClaimManualStream(ctx, testStreamQueue, testStreamGroup, testStreamConsumer, nil)
	require.NoError(t, err)
	require.Equal(t, uint64(0), first.BrokerMeta.Queue.Offset)

	// Well inside the one-minute visibility timeout.
	again, err := manager.ClaimManualStream(ctx, testStreamQueue, testStreamGroup, testStreamConsumer, nil)
	require.NoError(t, err)
	assert.Equal(t, uint64(0), again.BrokerMeta.Queue.Offset, "unsettled entry must be redelivered before the next record")

	group, err := store.GetConsumerGroup(ctx, testStreamQueue, testStreamGroup)
	require.NoError(t, err)
	assert.Equal(t, 1, group.PendingCountFor(testStreamConsumer), "redelivery must not add a second pending entry")
}

// Reconnect cycles must not walk an entry to the dead-letter queue on their
// own. Only deliveries that actually reached the consumer count as attempts.
func TestClaimManualStreamCountsOneAttemptPerRedelivery(t *testing.T) {
	ctx := context.Background()
	manager, store, _ := newManualStreamFixture(t, 1)

	for range 3 {
		require.NoError(t, manager.UnregisterConsumer(ctx, testStreamQueue, testStreamGroup, testStreamConsumer))
		msg, err := manager.ClaimManualStream(ctx, testStreamQueue, testStreamGroup, testStreamConsumer, nil)
		require.NoError(t, err)
		require.Equal(t, uint64(0), msg.BrokerMeta.Queue.Offset)
	}

	group, err := store.GetConsumerGroup(ctx, testStreamQueue, testStreamGroup)
	require.NoError(t, err)
	entry, owner := group.FindPending(0)
	require.Equal(t, testStreamConsumer, owner)
	assert.Equal(t, 3, entry.DeliveryCount, "three deliveries must cost three attempts")
}

// The ordering contract: a manual-commit stream hands out one unsettled
// delivery at a time so a nack can redeliver ahead of the next record.
func TestClaimManualStreamHoldsOneUnsettledDelivery(t *testing.T) {
	ctx := context.Background()
	manager, store, _ := newManualStreamFixture(t, 3)

	first, err := manager.ClaimManualStream(ctx, testStreamQueue, testStreamGroup, testStreamConsumer, nil)
	require.NoError(t, err)
	require.Equal(t, uint64(0), first.BrokerMeta.Queue.Offset)

	require.NoError(t, manager.Ack(ctx, testStreamQueue, testStreamGroup, testStreamConsumer, 0))

	second, err := manager.ClaimManualStream(ctx, testStreamQueue, testStreamGroup, testStreamConsumer, nil)
	require.NoError(t, err)
	assert.Equal(t, uint64(1), second.BrokerMeta.Queue.Offset, "settling releases the next record")

	group, err := store.GetConsumerGroup(ctx, testStreamQueue, testStreamGroup)
	require.NoError(t, err)
	assert.Equal(t, 1, group.PendingCountFor(testStreamConsumer))
}

// An auto-commit group refuses the manual claim path outright rather than
// quietly delivering under the wrong settlement contract.
func TestClaimManualStreamRejectsAutoCommitGroup(t *testing.T) {
	ctx := context.Background()
	manager, _ := newStreamFixture(t, 1)

	_, err := manager.ClaimManualStream(ctx, testStreamQueue, testStreamGroup, testStreamConsumer, nil)
	assert.ErrorIs(t, err, ErrGroupModeMismatch)
}

// Switching an existing group to auto-commit must not leave its pending list
// behind: nothing settles a pending entry once delivery is itself the commit,
// so the entries would be unreachable state inflating the group forever.
func TestGetOrCreateConfiguredGroupSwitchToAutoCommitClearsPending(t *testing.T) {
	ctx := context.Background()
	manager, store, group := newManualStreamFixture(t, 4)

	group.SetCursor(3, 1)
	group.AddPending(testStreamConsumer, &types.PendingEntry{Offset: 1, ConsumerID: testStreamConsumer, ClaimedAt: time.Now(), DeliveryCount: 1})
	group.AddPending(testStreamConsumer, &types.PendingEntry{Offset: 2, ConsumerID: testStreamConsumer, ClaimedAt: time.Now(), DeliveryCount: 1})
	require.NoError(t, store.UpdateConsumerGroup(ctx, group))

	switched, created, err := manager.GetOrCreateConfiguredGroup(ctx, testStreamQueue, testStreamGroup, "", types.GroupModeStream, true, true)
	require.NoError(t, err)
	assert.False(t, created)
	assert.True(t, switched.AutoCommitEnabled())

	stored, err := store.GetConsumerGroup(ctx, testStreamQueue, testStreamGroup)
	require.NoError(t, err)
	assert.Zero(t, stored.PendingCount(), "unsettled entries cannot survive the switch to auto-commit")
	assert.Equal(t, uint64(3), stored.CursorView().Committed, "everything already read is committed by the switch")
}

// The reverse direction keeps the boundary the consumer was already shown:
// records read under auto-commit are settled, only later ones are pending.
func TestGetOrCreateConfiguredGroupSwitchToManualCommitsWhatWasRead(t *testing.T) {
	ctx := context.Background()
	manager, store, group := newStreamFixtureStore(t, 4)

	group.SetCursor(3, 1)
	require.NoError(t, store.UpdateConsumerGroup(ctx, group))

	switched, _, err := manager.GetOrCreateConfiguredGroup(ctx, testStreamQueue, testStreamGroup, "", types.GroupModeStream, false, true)
	require.NoError(t, err)
	assert.False(t, switched.AutoCommitEnabled())

	stored, err := store.GetConsumerGroup(ctx, testStreamQueue, testStreamGroup)
	require.NoError(t, err)
	assert.Equal(t, uint64(3), stored.CursorView().Committed)
}

// An explicit auto-commit policy is a property of the subscription. Leaving the
// mode to the stored group must not silently discard it and hand the caller the
// opposite settlement contract.
func TestGetOrCreateConfiguredGroupAppliesPolicyWithoutMode(t *testing.T) {
	ctx := context.Background()
	manager, store, _ := newStreamFixtureStore(t, 1)

	switched, _, err := manager.GetOrCreateConfiguredGroup(ctx, testStreamQueue, testStreamGroup, "", "", false, true)
	require.NoError(t, err)
	assert.False(t, switched.AutoCommitEnabled())

	stored, err := store.GetConsumerGroup(ctx, testStreamQueue, testStreamGroup)
	require.NoError(t, err)
	assert.False(t, stored.AutoCommitEnabled())
}

// Without an explicit request the stored policy stands: a plain reconnect must
// not reset a group to the auto-commit default.
func TestGetOrCreateGroupLeavesStoredPolicyAlone(t *testing.T) {
	ctx := context.Background()
	manager, store, _ := newManualStreamFixture(t, 1)

	group, err := manager.GetOrCreateGroup(ctx, testStreamQueue, testStreamGroup, "", types.GroupModeStream, true)
	require.NoError(t, err)
	assert.False(t, group.AutoCommitEnabled())

	stored, err := store.GetConsumerGroup(ctx, testStreamQueue, testStreamGroup)
	require.NoError(t, err)
	assert.False(t, stored.AutoCommitEnabled())
}

// Auto-commit describes a stream cursor. A queue group settles through its
// pending list whatever the flag says, so configuring it must not drop entries
// its consumers still hold.
func TestGetOrCreateConfiguredGroupKeepsQueueGroupPending(t *testing.T) {
	ctx := context.Background()
	store := &groupStore{Store: memlog.New()}
	require.NoError(t, store.CreateQueue(ctx, types.DefaultQueueConfig(testStreamQueue, testStreamQueue+"/#")))

	group := types.NewConsumerGroupState(testStreamQueue, testStreamGroup, "")
	group.Mode = types.GroupModeQueue
	group.AutoCommit = false
	group.SetCursor(3, 1)
	group.AddPending(testStreamConsumer, &types.PendingEntry{Offset: 1, ConsumerID: testStreamConsumer, ClaimedAt: time.Now(), DeliveryCount: 1})
	require.NoError(t, store.CreateConsumerGroup(ctx, group))

	manager := NewManager(store, store, Config{
		VisibilityTimeout: time.Minute,
		MaxDeliveryCount:  5,
		ClaimBatchSize:    10,
		MaxPELSize:        1000,
	})

	_, _, err := manager.GetOrCreateConfiguredGroup(ctx, testStreamQueue, testStreamGroup, "", types.GroupModeQueue, true, true)
	require.NoError(t, err)

	stored, err := store.GetConsumerGroup(ctx, testStreamQueue, testStreamGroup)
	require.NoError(t, err)
	assert.Equal(t, 1, stored.PendingCount(), "a queue group keeps the entries its consumers hold")
	assert.Equal(t, uint64(1), stored.CursorView().Committed, "a queue group's committed position is not migrated")
}
