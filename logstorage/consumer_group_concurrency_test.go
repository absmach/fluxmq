// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package logstorage

import (
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/absmach/fluxmq/queue/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const testWorkersGroup = "workers"

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
	group := types.NewConsumerGroupState("orders", testWorkersGroup, "orders/#")
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

// A queue name containing a slash must not collide with a group name. Both the
// dirty set and the file path used to join the two, so ("$dlq/tasks",
// testWorkersGroup) and ("$dlq", "tasks/workers") were one entry and one file, and
// whichever wrote last destroyed the other. "$dlq/" prefixed queues are what
// the dead-letter path creates, so this is reachable, not theoretical.
func TestGroupsWithSlashesInQueueNamesStayDistinct(t *testing.T) {
	base := t.TempDir()
	store, err := NewConsumerGroupStateStore(base)
	require.NoError(t, err)

	require.NoError(t, store.Save(types.NewConsumerGroupState("$dlq/tasks", testWorkersGroup, "#")))
	require.NoError(t, store.Save(types.NewConsumerGroupState("$dlq", "tasks/workers", "#")))

	require.NoError(t, store.UpdateCursor("$dlq/tasks", testWorkersGroup, 4, 4))
	require.NoError(t, store.UpdateCursor("$dlq", "tasks/workers", 9, 9))

	store.mu.RLock()
	dirty := len(store.dirty)
	store.mu.RUnlock()
	assert.Equal(t, 2, dirty, "two distinct groups must occupy two dirty entries")

	require.NoError(t, store.Sync())

	reopened, err := NewConsumerGroupStateStore(base)
	require.NoError(t, err)

	first, err := reopened.Get("$dlq/tasks", testWorkersGroup)
	require.NoError(t, err)
	assert.Equal(t, uint64(4), first.CursorView().Committed)

	second, err := reopened.Get("$dlq", "tasks/workers")
	require.NoError(t, err)
	assert.Equal(t, uint64(9), second.CursorView().Committed,
		"the second group must survive the first rather than share its file")
}

// A name that resolves to a parent directory must stay inside the store.
func TestGroupPathCannotEscapeTheStoreDirectory(t *testing.T) {
	store := newGroupStateStore(t)

	for _, tc := range []struct{ queueName, groupID string }{
		{"..", testWorkersGroup},
		{".", testWorkersGroup},
		{"../../etc", testWorkersGroup},
		{"orders", ".."},
	} {
		path := store.groupPath(tc.queueName, tc.groupID)
		assert.Equal(t, store.dir, filepath.Dir(filepath.Dir(path)),
			"%q/%q escaped to %q", tc.queueName, tc.groupID, path)
	}
}

// Deleting a group must remove the file it was loaded from, not only the one
// the current naming would write. A file left behind is loaded on the next
// start and brings the group back.
func TestDeleteRemovesLegacyFile(t *testing.T) {
	base := t.TempDir()
	store, err := NewConsumerGroupStateStore(base)
	require.NoError(t, err)

	legacy := store.legacyGroupPath("$dlq/tasks", testWorkersGroup)
	require.NoError(t, os.MkdirAll(filepath.Dir(legacy), 0o755))
	group := types.NewConsumerGroupState("$dlq/tasks", testWorkersGroup, "#")
	require.NoError(t, store.writeGroup(groupRef{queueName: "$dlq/tasks", groupID: testWorkersGroup}, group))
	require.NoError(t, os.Rename(store.groupPath("$dlq/tasks", testWorkersGroup), legacy))

	loaded, err := NewConsumerGroupStateStore(base)
	require.NoError(t, err)
	_, err = loaded.Get("$dlq/tasks", testWorkersGroup)
	require.NoError(t, err, "a file written under the old naming must still load")

	require.NoError(t, loaded.Delete("$dlq/tasks", testWorkersGroup))

	reopened, err := NewConsumerGroupStateStore(base)
	require.NoError(t, err)
	_, err = reopened.Get("$dlq/tasks", testWorkersGroup)
	assert.ErrorIs(t, err, ErrGroupNotFound, "a deleted group must not come back on restart")
}

// Sync must write the groups the dirty set names and clear them, rather than
// scanning every group in the process to find them.
func TestSyncWritesDirtyGroupsAndClearsThem(t *testing.T) {
	store := newGroupStateStore(t)

	for _, name := range []string{"alpha", "beta", "gamma"} {
		require.NoError(t, store.Save(types.NewConsumerGroupState(name, testWorkersGroup, "#")))
	}

	require.NoError(t, store.UpdateCursor("beta", testWorkersGroup, 12, 12))

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
	recovered, err := reopened.Get("beta", testWorkersGroup)
	require.NoError(t, err)
	assert.Equal(t, uint64(12), recovered.CursorView().Cursor)
}

// A group deleted while dirty must not be resurrected by the next Sync, and
// must not leave an entry behind for a group that no longer exists.
func TestSyncSkipsDeletedGroups(t *testing.T) {
	store := newGroupStateStore(t)
	require.NoError(t, store.Save(types.NewConsumerGroupState("orders", testWorkersGroup, "#")))
	require.NoError(t, store.UpdateCursor("orders", testWorkersGroup, 3, 3))
	require.NoError(t, store.Delete("orders", testWorkersGroup))

	require.NoError(t, store.Sync())

	_, err := store.Get("orders", testWorkersGroup)
	assert.ErrorIs(t, err, ErrGroupNotFound)
}

// Deleting a group must never remove a file outside the store directory.
//
// Group IDs come from clients, so ".." is something a client can simply choose.
// The canonical path encodes it, but the legacy path joins raw names, and
// Delete removes both.
func TestDeleteCannotRemoveFilesOutsideTheStore(t *testing.T) {
	base := t.TempDir()
	store, err := NewConsumerGroupStateStore(base)
	require.NoError(t, err)

	// A file that has nothing to do with this store, one level above it.
	bystander := filepath.Join(base, "bystander.json")
	require.NoError(t, os.WriteFile(bystander, []byte(`{"keep":true}`), 0o600))

	for _, tc := range []struct{ queueName, groupID string }{
		{"..", "bystander"},
		{"../..", "bystander"},
		{"orders", "../bystander"},
	} {
		require.NoError(t, store.Delete(tc.queueName, tc.groupID),
			"delete of %q/%q must not error", tc.queueName, tc.groupID)

		_, err := os.Stat(bystander)
		require.NoError(t, err,
			"delete of %q/%q removed a file outside the store", tc.queueName, tc.groupID)
	}
}

// Writing an escaping name lands inside the store, not outside it. The
// canonical path encodes the name, so containment here is a backstop rather
// than the mechanism; this pins the property either way.
func TestSaveStaysInsideTheStoreForEscapingNames(t *testing.T) {
	base := t.TempDir()
	store, err := NewConsumerGroupStateStore(base)
	require.NoError(t, err)

	require.NoError(t, store.writeGroup(groupRef{queueName: "..", groupID: "escaped"},
		types.NewConsumerGroupState("..", "escaped", "#")))

	_, statErr := os.Stat(filepath.Join(base, "escaped.json"))
	assert.True(t, os.IsNotExist(statErr), "a file was created outside the group directory")

	written, err := os.ReadFile(filepath.Join(store.dir, "%2E%2E", "escaped.json"))
	require.NoError(t, err, "the group must be written inside the store under an encoded name")
	assert.Contains(t, string(written), "escaped")
}
