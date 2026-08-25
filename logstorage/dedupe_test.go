// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package logstorage

import (
	"context"
	"strconv"
	"sync"
	"testing"

	"github.com/absmach/fluxmq/message"
	"github.com/absmach/fluxmq/queue/storage"
	"github.com/absmach/fluxmq/queue/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	testDedupeQueue = "dedupe-queue"
	testDedupeKey   = "dlq-abc123"
)

func newDedupeAdapter(t *testing.T, dir string) *Adapter {
	t.Helper()

	adapter, err := NewAdapter(dir, DefaultAdapterConfig())
	require.NoError(t, err)
	t.Cleanup(func() {
		if err := adapter.Close(); err != nil {
			t.Errorf("close adapter: %v", err)
		}
	})
	return adapter
}

func dedupeEnvelope(payload string) *message.Envelope {
	return message.New(testDedupeQueue, []byte(payload))
}

// A repeated key must not produce a second record, and must report the offset
// the first attempt landed on so the caller can settle against it.
func TestAppendOnceDeduplicatesRepeatedKey(t *testing.T) {
	ctx := context.Background()
	adapter := newDedupeAdapter(t, t.TempDir())
	require.NoError(t, adapter.CreateQueue(ctx, types.DefaultQueueConfig(testDedupeQueue, testDedupeQueue+"/#")))

	first, duplicated, err := adapter.AppendOnce(ctx, testDedupeQueue, testDedupeKey, dedupeEnvelope("one"))
	require.NoError(t, err)
	assert.False(t, duplicated)

	second, duplicated, err := adapter.AppendOnce(ctx, testDedupeQueue, testDedupeKey, dedupeEnvelope("two"))
	require.NoError(t, err)
	assert.True(t, duplicated, "a repeated key must be recognised")
	assert.Equal(t, first, second, "the caller must learn where the record actually is")

	count, err := adapter.Count(ctx, testDedupeQueue)
	require.NoError(t, err)
	assert.Equal(t, uint64(1), count, "exactly one record must exist")
}

// Distinct keys are distinct records; deduplication must not swallow them.
func TestAppendOnceKeepsDistinctKeys(t *testing.T) {
	ctx := context.Background()
	adapter := newDedupeAdapter(t, t.TempDir())
	require.NoError(t, adapter.CreateQueue(ctx, types.DefaultQueueConfig(testDedupeQueue, testDedupeQueue+"/#")))

	for i := range 5 {
		_, duplicated, err := adapter.AppendOnce(ctx, testDedupeQueue, "key-"+strconv.Itoa(i), dedupeEnvelope("payload"))
		require.NoError(t, err)
		assert.False(t, duplicated)
	}

	count, err := adapter.Count(ctx, testDedupeQueue)
	require.NoError(t, err)
	assert.Equal(t, uint64(5), count)
}

// The index is derived state. Reopening the store must rebuild it from the
// records, or a retry after a crash appends a second copy — which is the whole
// failure this exists to prevent.
func TestAppendOnceSurvivesReopen(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	adapter := newDedupeAdapter(t, dir)
	require.NoError(t, adapter.CreateQueue(ctx, types.DefaultQueueConfig(testDedupeQueue, testDedupeQueue+"/#")))
	first, _, err := adapter.AppendOnce(ctx, testDedupeQueue, testDedupeKey, dedupeEnvelope("one"))
	require.NoError(t, err)
	require.NoError(t, adapter.Close())

	// Reopen: the in-memory index is gone, the records are not.
	reopened, err := NewAdapter(dir, DefaultAdapterConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = reopened.Close() })

	second, duplicated, err := reopened.AppendOnce(ctx, testDedupeQueue, testDedupeKey, dedupeEnvelope("two"))
	require.NoError(t, err)
	assert.True(t, duplicated, "the key was persisted in the record and must be recovered")
	assert.Equal(t, first, second)

	count, err := reopened.Count(ctx, testDedupeQueue)
	require.NoError(t, err)
	assert.Equal(t, uint64(1), count, "a retry after restart must not duplicate")
}

// The key has to be written into the record, not just remembered, or the
// rebuild above has nothing to read.
func TestAppendOncePersistsKeyInRecord(t *testing.T) {
	ctx := context.Background()
	adapter := newDedupeAdapter(t, t.TempDir())
	require.NoError(t, adapter.CreateQueue(ctx, types.DefaultQueueConfig(testDedupeQueue, testDedupeQueue+"/#")))

	offset, _, err := adapter.AppendOnce(ctx, testDedupeQueue, testDedupeKey, dedupeEnvelope("one"))
	require.NoError(t, err)

	stored, err := adapter.Read(ctx, testDedupeQueue, offset)
	require.NoError(t, err)
	t.Cleanup(func() { message.Release(stored) })

	assert.Equal(t, testDedupeKey, stored.Broker.Transfer.ID,
		"the deduplication key must reach the record so a rebuild can recover it")
}

// Concurrent retries of one transfer must still produce one record: the check
// and the append have to be one operation, not two.
func TestAppendOnceIsAtomicUnderConcurrentRetries(t *testing.T) {
	ctx := context.Background()
	adapter := newDedupeAdapter(t, t.TempDir())
	require.NoError(t, adapter.CreateQueue(ctx, types.DefaultQueueConfig(testDedupeQueue, testDedupeQueue+"/#")))

	const attempts = 16
	var (
		wg       sync.WaitGroup
		mu       sync.Mutex
		appended int
		offsets  = make(map[uint64]struct{})
	)

	for range attempts {
		wg.Add(1)
		go func() {
			defer wg.Done()
			offset, duplicated, err := adapter.AppendOnce(ctx, testDedupeQueue, testDedupeKey, dedupeEnvelope("racing"))
			if err != nil {
				t.Errorf("append once: %v", err)
				return
			}
			mu.Lock()
			defer mu.Unlock()
			offsets[offset] = struct{}{}
			if !duplicated {
				appended++
			}
		}()
	}
	wg.Wait()

	assert.Equal(t, 1, appended, "exactly one attempt may report having appended")
	assert.Len(t, offsets, 1, "every attempt must resolve to the same record")

	count, err := adapter.Count(ctx, testDedupeQueue)
	require.NoError(t, err)
	assert.Equal(t, uint64(1), count)
}

// An append that cannot be deduplicated must be refused rather than silently
// degrading to a plain append, which would leave the caller believing it was
// protected.
func TestAppendOnceRequiresKey(t *testing.T) {
	ctx := context.Background()
	adapter := newDedupeAdapter(t, t.TempDir())
	require.NoError(t, adapter.CreateQueue(ctx, types.DefaultQueueConfig(testDedupeQueue, testDedupeQueue+"/#")))

	_, _, err := adapter.AppendOnce(ctx, testDedupeQueue, "", dedupeEnvelope("one"))
	assert.ErrorIs(t, err, storage.ErrDeduplicationKeyRequired)
}

// A truncated record takes its deduplication key with it. Keeping the key would
// tell a retried transfer that its record is already present, at an offset that
// now holds nothing, and the caller would settle its source against a record
// that does not exist — losing the message this mechanism exists to protect.
func TestTruncateDropsKeysForRemovedRecords(t *testing.T) {
	ctx := context.Background()
	adapter := newDedupeAdapter(t, t.TempDir())
	require.NoError(t, adapter.CreateQueue(ctx, types.DefaultQueueConfig(testDedupeQueue, testDedupeQueue+"/#")))

	first, _, err := adapter.AppendOnce(ctx, testDedupeQueue, testDedupeKey, dedupeEnvelope("one"))
	require.NoError(t, err)
	_, _, err = adapter.AppendOnce(ctx, testDedupeQueue, "dlq-second", dedupeEnvelope("two"))
	require.NoError(t, err)

	require.NoError(t, adapter.Truncate(ctx, testDedupeQueue, first+1))

	// The first key's record is gone, so the transfer must be appended again
	// rather than reported as already present.
	retried, duplicated, err := adapter.AppendOnce(ctx, testDedupeQueue, testDedupeKey, dedupeEnvelope("retry"))
	require.NoError(t, err)
	assert.False(t, duplicated, "a key whose record was truncated must not report a duplicate")
	assert.NotEqual(t, first, retried)

	stored, err := adapter.Read(ctx, testDedupeQueue, retried)
	require.NoError(t, err)
	t.Cleanup(func() { message.Release(stored) })
	assert.Equal(t, "retry", string(stored.PayloadBytes()), "the retried transfer must be readable")

	// A key whose record survived truncation is still recognised.
	_, duplicated, err = adapter.AppendOnce(ctx, testDedupeQueue, "dlq-second", dedupeEnvelope("again"))
	require.NoError(t, err)
	assert.True(t, duplicated, "a retained record's key must still deduplicate")
}

// The live index must not recognise a key further back than a rebuild can, or
// the same retry is deduplicated before a restart and duplicated after one.
func TestLiveIndexIsBoundedToTheRecoveryWindow(t *testing.T) {
	ctx := context.Background()
	adapter := newDedupeAdapter(t, t.TempDir())
	require.NoError(t, adapter.CreateQueue(ctx, types.DefaultQueueConfig(testDedupeQueue, testDedupeQueue+"/#")))

	_, _, err := adapter.AppendOnce(ctx, testDedupeQueue, testDedupeKey, dedupeEnvelope("first"))
	require.NoError(t, err)

	for i := range DefaultDeduplicationWindow + 1 {
		_, _, err := adapter.AppendOnce(ctx, testDedupeQueue, "filler-"+strconv.Itoa(i), dedupeEnvelope("filler"))
		require.NoError(t, err)
	}

	_, duplicated, err := adapter.AppendOnce(ctx, testDedupeQueue, testDedupeKey, dedupeEnvelope("retry"))
	require.NoError(t, err)
	assert.False(t, duplicated,
		"a key beyond the window must be forgotten while the process runs, as it would be after a restart")
}
