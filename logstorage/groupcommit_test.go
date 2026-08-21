// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package logstorage

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// holdNextSync blocks the next fsync until the returned release func is called,
// so a test can park callers on a barrier that is provably in flight.
func holdNextSync(t *testing.T) (entered <-chan struct{}, release func()) {
	t.Helper()

	in := make(chan struct{})
	gate := make(chan struct{})
	var once sync.Once

	beforeSegmentSync = func() error {
		once.Do(func() {
			close(in)
			<-gate
		})
		return nil
	}
	t.Cleanup(func() { beforeSegmentSync = nil })

	return in, func() { close(gate) }
}

// TestSyncThroughCoalescesConcurrentBarriers is the point of group commit: a
// publisher that arrives while a barrier is in flight rides it instead of
// queueing for an fsync of its own. Without this, durable throughput is one
// message per fsync no matter how many publishers there are.
func TestSyncThroughCoalescesConcurrentBarriers(t *testing.T) {
	mgr := newTestManager(t, durableManagerConfig())
	defer mgr.Close()

	// Two records, both written before either barrier starts, so one fsync can
	// cover both.
	first := NewBatch(0)
	first.Append([]byte("one"), nil, nil)
	_, target, throughFirst, err := mgr.appendLocked(first)
	require.NoError(t, err)

	second := NewBatch(0)
	second.Append([]byte("two"), nil, nil)
	_, _, throughSecond, err := mgr.appendLocked(second)
	require.NoError(t, err)

	entered, release := holdNextSync(t)

	errs := make(chan error, 2)
	go func() { errs <- target.SyncThrough(throughFirst, nil) }()
	<-entered // the first caller is inside the fsync

	go func() { errs <- target.SyncThrough(throughSecond, nil) }()
	require.Eventually(t, func() bool { return target.commit.waiting.Load() == 1 },
		time.Second, time.Millisecond, "second caller never parked on the in-flight barrier")

	release()
	require.NoError(t, <-errs)
	require.NoError(t, <-errs)

	assert.Equal(t, uint64(1), target.commit.syncs.Load(),
		"two publishers should have shared one fsync")
	assert.GreaterOrEqual(t, target.commit.through, throughSecond)
}

// TestSyncThroughSkipsWorkAlreadyDurable covers the cheap path: a caller whose
// record was covered by someone else's barrier returns without touching the
// disk at all.
func TestSyncThroughSkipsWorkAlreadyDurable(t *testing.T) {
	mgr := newTestManager(t, durableManagerConfig())
	defer mgr.Close()

	batch := NewBatch(0)
	batch.Append([]byte("one"), nil, nil)
	_, target, through, err := mgr.appendLocked(batch)
	require.NoError(t, err)

	require.NoError(t, target.SyncThrough(through, nil))
	require.Equal(t, uint64(1), target.commit.syncs.Load())

	require.NoError(t, target.SyncThrough(through, nil))
	assert.Equal(t, uint64(1), target.commit.syncs.Load(), "a covered offset must not re-sync")
}

// TestSyncThroughRetriesWhenTheBarrierMissedTheRecord is the correctness edge:
// a sync only promises the bytes written before it began, so a publisher whose
// append landed while it was in flight must take the next barrier rather than
// treat someone else's as its own.
func TestSyncThroughRetriesWhenTheBarrierMissedTheRecord(t *testing.T) {
	mgr := newTestManager(t, durableManagerConfig())
	defer mgr.Close()

	early := NewBatch(0)
	early.Append([]byte("early"), nil, nil)
	_, target, _, err := mgr.appendLocked(early)
	require.NoError(t, err)

	entered, release := holdNextSync(t)

	syncing := make(chan error, 1)
	go func() { syncing <- target.SyncThrough(1, nil) }()
	<-entered // the barrier has captured its coverage and is inside the fsync

	// This record is written after that capture, so the in-flight fsync does
	// not cover it.
	late := NewBatch(0)
	late.Append([]byte("late"), nil, nil)
	_, _, throughLate, err := mgr.appendLocked(late)
	require.NoError(t, err)

	lateErr := make(chan error, 1)
	go func() { lateErr <- target.SyncThrough(throughLate, nil) }()
	require.Eventually(t, func() bool { return target.commit.waiting.Load() == 1 },
		time.Second, time.Millisecond, "late publisher never parked")

	release()
	require.NoError(t, <-syncing)
	require.NoError(t, <-lateErr)

	assert.Equal(t, uint64(2), target.commit.syncs.Load(),
		"the late record needed a second barrier, not the one already in flight")
	assert.GreaterOrEqual(t, target.commit.through, throughLate)
}

// TestSyncThroughOnClosedSegmentFails refuses to acknowledge what it cannot
// make durable: a closed segment reports an error rather than success.
func TestSyncThroughOnClosedSegmentFails(t *testing.T) {
	mgr := newTestManager(t, durableManagerConfig())

	batch := NewBatch(0)
	batch.Append([]byte("one"), nil, nil)
	_, target, through, err := mgr.appendLocked(batch)
	require.NoError(t, err)

	require.NoError(t, mgr.Close())
	require.ErrorIs(t, target.SyncThrough(through+1, nil), ErrSegmentClosed)
}

func durableManagerConfig() ManagerConfig {
	cfg := DefaultManagerConfig()
	cfg.MaxSegmentSize = 1 << 20
	cfg.MaxSegmentAge = 0
	cfg.SyncInterval = 0
	cfg.Compression = CompressionNone
	return cfg
}

// TestAppendNeverReportsSuccessOnAFailingDevice is the line that makes sharing
// a barrier safe. Publishers are accepted while a barrier is in flight — that
// is the point — so on a device that cannot sync, some appends land after the
// fsync covering them has already failed. None of them may report success: an
// acknowledged durable publish that is not on disk is the failure the fsync
// policy exists to prevent.
//
// The writes themselves succeed here. Only the barrier fails, which is the case
// that could otherwise be acknowledged.
func TestAppendNeverReportsSuccessOnAFailingDevice(t *testing.T) {
	mgr := newTestManager(t, durableManagerConfig())
	t.Cleanup(func() { _ = mgr.Close() })

	errSyncFailed := errors.New("device cannot sync")
	beforeSegmentSync = func() error { return errSyncFailed }
	t.Cleanup(func() { beforeSegmentSync = nil })

	const publishers = 16
	var wg sync.WaitGroup
	results := make([]error, publishers)
	for i := range publishers {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			batch := NewBatch(0)
			batch.Append([]byte("durable"), nil, nil)
			_, results[i] = mgr.AppendAndSync(batch)
		}(i)
	}
	wg.Wait()

	for i, err := range results {
		require.Error(t, err, "publisher %d was told its message was durable on a device that cannot sync", i)
	}

	// The failure was recorded inside the barrier rather than after it, so it
	// is already visible to whatever append comes next instead of racing it.
	mgr.mu.Lock()
	recorded := mgr.syncErr
	mgr.mu.Unlock()
	require.ErrorIs(t, recorded, errSyncFailed,
		"a failed barrier must be recorded before its waiters wake")

	// That append retries the barrier under the lock. Here the retry reaches a
	// working fsync, so it clears and the append proceeds to a barrier of its
	// own — which fails again, and is reported rather than acknowledged.
	batch := NewBatch(0)
	batch.Append([]byte("after"), nil, nil)
	_, err := mgr.AppendAndSync(batch)
	require.Error(t, err)
}
