// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package logstorage

import (
	"sync"
	"testing"
	"time"

	"github.com/absmach/fluxmq/queue/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newGroupStateStore(t *testing.T) *ConsumerGroupStateStore {
	t.Helper()

	store, err := NewConsumerGroupStateStore(t.TempDir())
	require.NoError(t, err)
	return store
}

// Persisting a group encodes its PEL, consumer membership and cursor. Consumers
// mutate all three through the group's own lock, so the encode has to take it
// too — without that this is a data race on the maps, which is fatal rather
// than merely wrong.
func TestSaveDoesNotRaceGroupMutations(t *testing.T) {
	store := newGroupStateStore(t)
	group := types.NewConsumerGroupState("orders", "workers", "orders/#")
	require.NoError(t, store.Save(group))

	var wg sync.WaitGroup
	stop := make(chan struct{})

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := range 200 {
			group.AddPending("c1", &types.PendingEntry{Offset: uint64(i), ConsumerID: "c1", ClaimedAt: time.Now()})
			group.SetConsumer("c1", &types.ConsumerInfo{ID: "c1", ClientID: "client"})
			group.AdvanceCommitted(uint64(i))
			group.RemovePending("c1", uint64(i))
		}
		close(stop)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				if err := store.Save(group); err != nil {
					t.Errorf("save: %v", err)
					return
				}
			}
		}
	}()

	wg.Wait()
}

// The dirty set names the queue and group as separate fields, so a queue name
// containing a slash cannot be confused with a group name. Joining the two with
// "/" made ("$dlq/tasks", "workers") and ("$dlq", "tasks/workers") one entry,
// so a flush of either cleared the other's dirty flag and lost its state.
//
// Note this covers the dirty set only: groupPath still joins the two into a
// filesystem path, where the same pair still collides. That is a separate
// defect in the on-disk layout, not something this key change fixes.
func TestDirtyKeysDistinguishSlashesInQueueNames(t *testing.T) {
	store := newGroupStateStore(t)

	first := types.NewConsumerGroupState("$dlq/tasks", "workers", "#")
	second := types.NewConsumerGroupState("$dlq", "tasks/workers", "#")
	require.NoError(t, store.Save(first))
	require.NoError(t, store.Save(second))

	require.NoError(t, store.UpdateCursor("$dlq/tasks", "workers", 4, 4))
	require.NoError(t, store.UpdateCursor("$dlq", "tasks/workers", 9, 9))

	store.mu.RLock()
	dirty := len(store.dirty)
	store.mu.RUnlock()
	assert.Equal(t, 2, dirty, "two distinct groups must occupy two dirty entries")

	assert.Equal(t, uint64(4), first.GetCursor().Committed)
	assert.Equal(t, uint64(9), second.GetCursor().Committed)
}

// Sync must write the groups the dirty set names and clear them, rather than
// scanning every group in the process to find them.
func TestSyncWritesDirtyGroupsAndClearsThem(t *testing.T) {
	store := newGroupStateStore(t)

	for _, name := range []string{"alpha", "beta", "gamma"} {
		require.NoError(t, store.Save(types.NewConsumerGroupState(name, "workers", "#")))
	}

	require.NoError(t, store.UpdateCursor("beta", "workers", 12, 12))

	store.mu.RLock()
	dirty := len(store.dirty)
	store.mu.RUnlock()
	require.Equal(t, 1, dirty, "only the updated group may be dirty")

	require.NoError(t, store.Sync())

	store.mu.RLock()
	remaining := len(store.dirty)
	store.mu.RUnlock()
	assert.Zero(t, remaining, "a successful sync clears what it wrote")

	reopened, err := NewConsumerGroupStateStore(store.dir[:len(store.dir)-len("/groups")])
	require.NoError(t, err)
	recovered, err := reopened.Get("beta", "workers")
	require.NoError(t, err)
	assert.Equal(t, uint64(12), recovered.GetCursor().Cursor)
}

// A group deleted while dirty must not be resurrected by the next Sync, and
// must not leave an entry behind for a group that no longer exists.
func TestSyncSkipsDeletedGroups(t *testing.T) {
	store := newGroupStateStore(t)
	require.NoError(t, store.Save(types.NewConsumerGroupState("orders", "workers", "#")))
	require.NoError(t, store.UpdateCursor("orders", "workers", 3, 3))
	require.NoError(t, store.Delete("orders", "workers"))

	require.NoError(t, store.Sync())

	_, err := store.Get("orders", "workers")
	assert.ErrorIs(t, err, ErrGroupNotFound)
}
