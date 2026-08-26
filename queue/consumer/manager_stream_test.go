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
	testStreamQueue = "stream-queue"
	testStreamGroup = "readers"
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
