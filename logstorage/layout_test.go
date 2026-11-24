// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package logstorage

import (
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	percentName = "50%"
	nestedName  = "a/b"
)

func openTestStore(t *testing.T, dir string) *Store {
	t.Helper()
	store, err := NewStore(dir, DefaultStoreConfig())
	require.NoError(t, err)
	return store
}

// plantVictim creates a file outside the store that a name escaping the store
// directory would reach, and returns its path.
func plantVictim(t *testing.T, root string) string {
	t.Helper()
	victim := filepath.Join(root, "victim", "keep")
	require.NoError(t, os.MkdirAll(filepath.Dir(victim), 0o755))
	require.NoError(t, os.WriteFile(victim, []byte("x"), 0o600))
	return victim
}

func TestStoreQueueNamesStayInsideStore(t *testing.T) {
	names := []string{
		"../../victim",
		"../../../victim",
		"..",
		".",
		"/abs/olute",
		`back\slash`,
		"a/../b",
		percentName,
		"a%2Fb",
	}

	for _, name := range names {
		t.Run(name, func(t *testing.T) {
			root := t.TempDir()
			victim := plantVictim(t, root)
			dir := filepath.Join(root, "store")
			store := openTestStore(t, dir)

			require.NoError(t, store.CreateQueue(name))
			_, err := store.Append(name, []byte("v"), nil, nil)
			require.NoError(t, err)

			entries, err := os.ReadDir(filepath.Join(dir, queueLogsDir))
			require.NoError(t, err)
			assert.Len(t, entries, 1, "a queue occupies exactly one directory")

			require.NoError(t, store.DeleteQueue(name))
			require.NoError(t, store.Close())

			assert.FileExists(t, victim)
			_, err = os.Stat(filepath.Join(dir, queueLogsDir))
			assert.NoError(t, err, "deleting a queue must not remove the queue root")
		})
	}
}

func TestStoreOverlappingQueueNames(t *testing.T) {
	t.Run("delete/parent keeps child", func(t *testing.T) {
		dir := t.TempDir()
		store := openTestStore(t, dir)
		_, err := store.Append("a", []byte("parent"), nil, nil)
		require.NoError(t, err)
		_, err = store.Append(nestedName, []byte("child"), nil, nil)
		require.NoError(t, err)

		require.NoError(t, store.DeleteQueue("a"))
		require.NoError(t, store.Close())

		reopened := openTestStore(t, dir)
		t.Cleanup(func() { assert.NoError(t, reopened.Close()) })
		assert.Equal(t, []string{nestedName}, reopened.ListQueues())
		msg, err := reopened.Read(nestedName, 0)
		require.NoError(t, err)
		assert.Equal(t, []byte("child"), msg.Value)
	})

	t.Run("alias/dot segments are a distinct queue", func(t *testing.T) {
		dir := t.TempDir()
		store := openTestStore(t, dir)
		_, err := store.Append("b", []byte("plain"), nil, nil)
		require.NoError(t, err)
		_, err = store.Append("a/../b", []byte("dotted"), nil, nil)
		require.NoError(t, err)
		require.NoError(t, store.Close())

		reopened := openTestStore(t, dir)
		t.Cleanup(func() { assert.NoError(t, reopened.Close()) })
		for name, want := range map[string]string{"b": "plain", "a/../b": "dotted"} {
			msg, err := reopened.Read(name, 0)
			require.NoError(t, err)
			assert.Equal(t, []byte(want), msg.Value, name)
			count, err := reopened.Count(name)
			require.NoError(t, err)
			assert.Equal(t, uint64(1), count, name)
		}
	})
}

func TestStoreConsumerGroupIDsStayInsideQueue(t *testing.T) {
	groups := []string{
		"../../../../victim",
		"../../../../../victim",
		"..",
		"client/with/slashes",
		percentName,
	}

	root := t.TempDir()
	victim := plantVictim(t, root)
	dir := filepath.Join(root, "store")
	store := openTestStore(t, dir)
	_, err := store.Append("q", []byte("v"), nil, nil)
	require.NoError(t, err)

	for _, group := range groups {
		require.NoError(t, store.Deliver("q", group, 0, "consumer"), group)
	}
	require.NoError(t, store.Close())

	reopened := openTestStore(t, dir)
	t.Cleanup(func() { assert.NoError(t, reopened.Close()) })
	assert.ElementsMatch(t, groups, reopened.ListConsumerGroups("q"), "every group reloads under its own name")

	for _, group := range groups {
		pending, err := reopened.PendingCount("q", group)
		require.NoError(t, err, group)
		assert.Equal(t, 1, pending, group)
		require.NoError(t, reopened.DeleteConsumerGroup("q", group), group)
	}
	assert.FileExists(t, victim)
	assert.True(t, reopened.QueueExists("q"))
	msg, err := reopened.Read("q", 0)
	require.NoError(t, err)
	assert.Equal(t, []byte("v"), msg.Value)
}

func TestStoreRejectsEmptyNames(t *testing.T) {
	store := openTestStore(t, t.TempDir())
	t.Cleanup(func() { assert.NoError(t, store.Close()) })

	assert.ErrorIs(t, store.CreateQueue(""), ErrInvalidName)
	_, err := store.Append("", []byte("v"), nil, nil)
	assert.ErrorIs(t, err, ErrInvalidName)

	_, err = store.Append("q", []byte("v"), nil, nil)
	require.NoError(t, err)
	assert.ErrorIs(t, store.Deliver("q", "", 0, "consumer"), ErrInvalidName)
}

func TestStoreIgnoresNonCanonicalDirectories(t *testing.T) {
	dir := t.TempDir()
	store := openTestStore(t, dir)
	_, err := store.Append(nestedName, []byte("v"), nil, nil)
	require.NoError(t, err)
	require.NoError(t, store.Close())

	// Directories that decode to an existing name, or not at all, were not
	// written by the store and must not appear as queues.
	for _, junk := range []string{"a%2fb", "a%2", "a%zz", "a%2E"} {
		segments := filepath.Join(dir, queueLogsDir, junk, segmentsDirName)
		require.NoError(t, os.MkdirAll(segments, 0o755))
	}

	reopened := openTestStore(t, dir)
	t.Cleanup(func() { assert.NoError(t, reopened.Close()) })
	assert.Equal(t, []string{nestedName}, reopened.ListQueues())
}

type legacyQueue struct {
	name   string
	value  string
	groups []string
}

// toLegacyLayout rewrites a store written in the encoded layout into the
// raw-named layout earlier builds produced.
func toLegacyLayout(t *testing.T, dir string, queues []legacyQueue) {
	t.Helper()
	ordered := slices.Clone(queues)
	// Parents first so a nested queue lands inside its parent's directory.
	slices.SortFunc(ordered, func(a, b legacyQueue) int {
		return strings.Count(a.name, "/") - strings.Count(b.name, "/")
	})
	for _, q := range ordered {
		legacyDir := filepath.Join(dir, legacyQueuesDir, filepath.FromSlash(q.name))
		require.NoError(t, os.MkdirAll(filepath.Dir(legacyDir), 0o755))
		require.NoError(t, os.Rename(queueDirIn(dir, q.name), legacyDir))

		require.NoError(t, os.MkdirAll(filepath.Join(legacyDir, legacyConsumersDir), 0o755))
		// Parents first, as for queues.
		groups := slices.Clone(q.groups)
		slices.SortFunc(groups, func(a, b string) int { return strings.Count(a, "/") - strings.Count(b, "/") })
		for _, group := range groups {
			legacyGroupDir := filepath.Join(legacyDir, legacyConsumersDir, filepath.FromSlash(group))
			require.NoError(t, os.MkdirAll(filepath.Dir(legacyGroupDir), 0o755))
			require.NoError(t, os.Rename(
				filepath.Join(legacyDir, consumerGroupsDir, encodePathComponent(group)),
				legacyGroupDir,
			))
		}
		require.NoError(t, os.Remove(filepath.Join(legacyDir, consumerGroupsDir)))
	}
	// Present only after a partial rewrite, where it still holds queues.
	_ = os.Remove(filepath.Join(dir, queueLogsDir))
}

func writeQueues(t *testing.T, dir string, queues []legacyQueue) {
	t.Helper()
	store := openTestStore(t, dir)
	for _, q := range queues {
		_, err := store.Append(q.name, []byte(q.value), nil, nil)
		require.NoError(t, err)
		for _, group := range q.groups {
			require.NoError(t, store.Deliver(q.name, group, 0, "consumer"))
		}
	}
	require.NoError(t, store.Close())
}

func assertQueues(t *testing.T, store *Store, queues []legacyQueue) {
	t.Helper()
	want := make([]string, 0, len(queues))
	for _, q := range queues {
		want = append(want, q.name)
		msg, err := store.Read(q.name, 0)
		require.NoError(t, err, q.name)
		assert.Equal(t, []byte(q.value), msg.Value, q.name)

		assert.ElementsMatch(t, q.groups, store.ListConsumerGroups(q.name), q.name)
		for _, group := range q.groups {
			pending, err := store.PendingCount(q.name, group)
			require.NoError(t, err)
			assert.Equal(t, 1, pending, "%s/%s", q.name, group)
		}
	}
	assert.ElementsMatch(t, want, store.ListQueues())
}

func TestStoreMigratesLegacyLayout(t *testing.T) {
	queues := []legacyQueue{
		{name: "invoices", value: "o", groups: []string{"workers", percentName, "team", "team/a"}},
		{name: "$dlq", value: "d"},
		{name: "$dlq/invoices", value: "dlq", groups: []string{"inspect"}},
		// Literally named like the encoding of nestedName, next to a real nestedName.
		{name: "a%2Fb", value: "literal"},
		{name: nestedName, value: "nested"},
	}

	t.Run("full", func(t *testing.T) {
		dir := t.TempDir()
		writeQueues(t, dir, queues)
		toLegacyLayout(t, dir, queues)

		store := openTestStore(t, dir)
		t.Cleanup(func() { assert.NoError(t, store.Close()) })
		assertQueues(t, store, queues)

		_, err := os.Stat(filepath.Join(dir, legacyQueuesDir))
		assert.True(t, os.IsNotExist(err), "the emptied legacy tree is removed")
	})

	t.Run("resumes interrupted migration", func(t *testing.T) {
		dir := t.TempDir()
		writeQueues(t, dir, queues)
		toLegacyLayout(t, dir, queues[3:])

		store := openTestStore(t, dir)
		t.Cleanup(func() { assert.NoError(t, store.Close()) })
		assertQueues(t, store, queues)
	})

	t.Run("refuses to merge a queue present in both layouts", func(t *testing.T) {
		dir := t.TempDir()
		writeQueues(t, dir, queues[:1])
		toLegacyLayout(t, dir, queues[:1])
		current := t.TempDir()
		writeQueues(t, current, []legacyQueue{{name: "invoices", value: "new"}})
		require.NoError(t, os.Rename(filepath.Join(current, queueLogsDir), filepath.Join(dir, queueLogsDir)))

		_, err := NewStore(dir, DefaultStoreConfig())
		require.ErrorIs(t, err, ErrAlreadyExists)

		legacy := filepath.Join(dir, legacyQueuesDir, "invoices", segmentsDirName)
		assert.DirExists(t, legacy, "the legacy copy is left untouched")
	})
}
