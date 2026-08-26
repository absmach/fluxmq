// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package logstorage

import (
	"context"
	"fmt"
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

// The index is durable derived state. Reopening the store must validate it
// against the record without decoding the retained queue.
func TestAppendOnceSurvivesReopen(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	adapter := newDedupeAdapter(t, dir)
	require.NoError(t, adapter.CreateQueue(ctx, types.DefaultQueueConfig(testDedupeQueue, testDedupeQueue+"/#")))
	first, _, err := adapter.AppendOnce(ctx, testDedupeQueue, testDedupeKey, dedupeEnvelope("one"))
	require.NoError(t, err)
	require.NoError(t, adapter.Close())

	// Reopen: both the record and its durable identity entry remain.
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

func TestDedupeRecoveryDoesNotDecodeRetainedQueue(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	adapter := newDedupeAdapter(t, dir)
	require.NoError(t, adapter.CreateQueue(ctx, types.DefaultQueueConfig(testDedupeQueue, testDedupeQueue+"/#")))
	first, _, err := adapter.AppendOnce(ctx, testDedupeQueue, testDedupeKey, dedupeEnvelope("target"))
	require.NoError(t, err)

	// A raw record whose envelope metadata cannot be decoded is still a valid
	// log record. A retained-queue rebuild would trip over it; an indexed lookup
	// of the target never needs to read it.
	_, err = adapter.store.Append(testDedupeQueue, []byte("payload"), nil,
		map[string][]byte{headerEnvelope: []byte("not-envelope-metadata")})
	require.NoError(t, err)
	require.NoError(t, adapter.Close())

	reopened, err := NewAdapter(dir, DefaultAdapterConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = reopened.Close() })
	offset, duplicated, err := reopened.AppendOnce(ctx, testDedupeQueue, testDedupeKey, dedupeEnvelope("retry"))
	require.NoError(t, err)
	assert.True(t, duplicated)
	assert.Equal(t, first, offset)
}

// A crash can happen after the reservation is durable and after the record is
// written, but before the accepted offset reaches the index. The reservation's
// old tail must make that gap recoverable without scanning retained history.
func TestPendingDedupeReservationRecoversAcceptedRecord(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	adapter := newDedupeAdapter(t, dir)
	require.NoError(t, adapter.CreateQueue(ctx, types.DefaultQueueConfig(testDedupeQueue, testDedupeQueue+"/#")))

	for range 100 {
		_, err := adapter.Append(ctx, testDedupeQueue, dedupeEnvelope("retained"))
		require.NoError(t, err)
	}
	tail, err := adapter.Tail(ctx, testDedupeQueue)
	require.NoError(t, err)
	_, found, err := adapter.dedupe.state.reserve(testDedupeQueue, testDedupeKey, tail)
	require.NoError(t, err)
	require.False(t, found)

	landed := dedupeEnvelope("accepted-before-crash")
	landed.Broker.Transfer.ID = testDedupeKey
	value, key, headers, err := encodeMessage(landed)
	require.NoError(t, err)
	first, err := adapter.store.Append(testDedupeQueue, value, key, headers)
	require.NoError(t, err)
	message.Release(landed)
	require.NoError(t, adapter.Close())

	reopened, err := NewAdapter(dir, DefaultAdapterConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = reopened.Close() })
	recovered, duplicated, err := reopened.AppendOnce(ctx, testDedupeQueue, testDedupeKey, dedupeEnvelope("retry"))
	require.NoError(t, err)
	assert.True(t, duplicated)
	assert.Equal(t, first, recovered)
}

// A reservation can also survive while its append does not. Recovery must
// prove there is no matching record in the uncertain suffix before appending.
func TestPendingDedupeReservationWithoutRecordCanRetry(t *testing.T) {
	ctx := context.Background()
	adapter := newDedupeAdapter(t, t.TempDir())
	require.NoError(t, adapter.CreateQueue(ctx, types.DefaultQueueConfig(testDedupeQueue, testDedupeQueue+"/#")))

	tail, err := adapter.Tail(ctx, testDedupeQueue)
	require.NoError(t, err)
	_, _, err = adapter.dedupe.state.reserve(testDedupeQueue, testDedupeKey, tail)
	require.NoError(t, err)

	offset, duplicated, err := adapter.AppendOnce(ctx, testDedupeQueue, testDedupeKey, dedupeEnvelope("retry"))
	require.NoError(t, err)
	assert.False(t, duplicated)
	assert.Equal(t, tail, offset)
}

// The persistent index is never authoritative by itself. A stale entry that
// points at another record must be discarded instead of settling the source.
func TestDedupeIndexValidatesReferencedRecord(t *testing.T) {
	ctx := context.Background()
	adapter := newDedupeAdapter(t, t.TempDir())
	require.NoError(t, adapter.CreateQueue(ctx, types.DefaultQueueConfig(testDedupeQueue, testDedupeQueue+"/#")))

	wrongOffset, err := adapter.Append(ctx, testDedupeQueue, dedupeEnvelope("unrelated"))
	require.NoError(t, err)
	require.NoError(t, adapter.dedupe.state.put(testDedupeQueue, testDedupeKey,
		dedupeEntry{offset: wrongOffset, state: dedupeConfirmed}))

	offset, duplicated, err := adapter.AppendOnce(ctx, testDedupeQueue, testDedupeKey, dedupeEnvelope("transfer"))
	require.NoError(t, err)
	assert.False(t, duplicated)
	assert.NotEqual(t, wrongOffset, offset)
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

// A key is recognised for as long as its record is retained, however many
// records arrive after it. The live index and a rebuilt one must agree: an
// earlier version bounded the live index to a fixed window, so the same retry
// was deduplicated before a restart and duplicated after one.
func TestKeyIsRecognisedWhileItsRecordIsRetained(t *testing.T) {
	ctx := context.Background()
	base := t.TempDir()
	adapter := newDedupeAdapter(t, base)
	require.NoError(t, adapter.CreateQueue(ctx, types.DefaultQueueConfig(testDedupeQueue, testDedupeQueue+"/#")))

	first, _, err := adapter.AppendOnce(ctx, testDedupeQueue, testDedupeKey, dedupeEnvelope("first"))
	require.NoError(t, err)

	// Ordinary appends move the tail far past the key's record.
	for range 5000 {
		_, err := adapter.Append(ctx, testDedupeQueue, dedupeEnvelope("filler"))
		require.NoError(t, err)
	}

	_, duplicated, err := adapter.AppendOnce(ctx, testDedupeQueue, testDedupeKey, dedupeEnvelope("retry"))
	require.NoError(t, err)
	assert.True(t, duplicated, "a retained record's key must still be recognised")

	// And recovery must reach the same answer without scanning the fillers.
	require.NoError(t, adapter.Close())
	reopened, err := NewAdapter(base, DefaultAdapterConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = reopened.Close() })

	recovered, duplicated, err := reopened.AppendOnce(ctx, testDedupeQueue, testDedupeKey, dedupeEnvelope("after restart"))
	require.NoError(t, err)
	assert.True(t, duplicated, "the live index and a rebuilt one must agree")
	assert.Equal(t, first, recovered)
}

// Truncation and a deduplicated append must not interleave.
//
// Truncation removes the record; the index prune removes its key. Between those
// two steps a retry can be told its transfer is already present at an offset
// that no longer holds one, and the caller settles its source against nothing —
// losing the message this mechanism exists to protect. Every earlier test here
// was single-threaded and passed while that window was open.
func TestTruncateDoesNotRaceDeduplicatedAppend(t *testing.T) {
	ctx := context.Background()
	adapter := newDedupeAdapter(t, t.TempDir())
	require.NoError(t, adapter.CreateQueue(ctx, types.DefaultQueueConfig(testDedupeQueue, testDedupeQueue+"/#")))

	const rounds = 200

	var wg sync.WaitGroup
	failures := make(chan string, rounds)

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := range rounds {
			// Each round is a distinct transfer, retried once.
			key := "dlq-" + strconv.Itoa(i)
			offset, _, err := adapter.AppendOnce(ctx, testDedupeQueue, key, dedupeEnvelope("transfer"))
			if err != nil {
				continue
			}

			retried, duplicated, err := adapter.AppendOnce(ctx, testDedupeQueue, key, dedupeEnvelope("retry"))
			if err != nil {
				continue
			}
			if !duplicated {
				continue
			}

			// The transfer was reported already present. The caller would now
			// settle its source, so the record it names has to exist.
			if _, err := adapter.Read(ctx, testDedupeQueue, retried); err != nil {
				failures <- "deduplicated against offset " + strconv.FormatUint(retried, 10) +
					" (first landed at " + strconv.FormatUint(offset, 10) + "): " + err.Error()
			}
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for range rounds {
			tail, err := adapter.Tail(ctx, testDedupeQueue)
			if err != nil || tail == 0 {
				continue
			}
			_ = adapter.Truncate(ctx, testDedupeQueue, tail)
		}
	}()

	wg.Wait()
	close(failures)

	for failure := range failures {
		t.Errorf("settled against a record that does not exist: %s", failure)
	}
}

// A durability barrier that fails after the record is written must not let the
// retry append a second copy.
//
// appendDurable writes the record and then runs the barrier, returning the real
// offset alongside a barrier error. Discarding both leaves the key unrecorded,
// so the retry sees nothing and appends again. The failure that triggers it is
// the ordinary one — a sync that fails and then works — because the manager
// retries the barrier on the next append and proceeds once it succeeds.
func TestFailedDurabilityBarrierDoesNotDuplicate(t *testing.T) {
	ctx := context.Background()
	adapter := newDedupeAdapter(t, t.TempDir())
	require.NoError(t, adapter.CreateQueue(ctx, types.DefaultQueueConfig(testDedupeQueue, testDedupeQueue+"/#")))

	// The record lands; the barrier over it does not.
	barrierFailed := func(queueName string, value, key []byte, headers map[string][]byte) (uint64, error) {
		offset, err := adapter.store.Append(queueName, value, key, headers)
		if err != nil {
			return offset, err
		}
		return offset, fmt.Errorf("%w: injected", storage.ErrDurabilityUnconfirmed)
	}

	first, _, err := adapter.appendOnce(ctx, testDedupeQueue, testDedupeKey, dedupeEnvelope("transfer"), barrierFailed)
	require.ErrorIs(t, err, storage.ErrDurabilityUnconfirmed, "the caller must learn the record is not durable")

	// The retry runs against a working barrier, as it would once the transient
	// failure clears.
	retried, duplicated, err := adapter.AppendOnceAndSync(ctx, testDedupeQueue, testDedupeKey, dedupeEnvelope("retry"))
	require.NoError(t, err)
	assert.True(t, duplicated, "the record written before the barrier failed must be recognised")
	assert.Equal(t, first, retried, "the retry must resolve to the record that already exists")

	count, err := adapter.Count(ctx, testDedupeQueue)
	require.NoError(t, err)
	assert.Equal(t, uint64(1), count, "a failed barrier must not produce a second record")
}
