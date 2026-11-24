// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package cluster

import (
	"context"
	"io"
	"log/slog"
	"maps"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNodeBatcherFlushBySize(t *testing.T) {
	stopCh := make(chan struct{})
	defer close(stopCh)

	var (
		mu    sync.Mutex
		calls [][]int
	)

	b := newNodeBatcher[int](
		3,
		100*time.Millisecond,
		1,
		stopCh,
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		"test",
		func(_ context.Context, _ string, items []int) error {
			mu.Lock()
			defer mu.Unlock()
			cp := append([]int(nil), items...)
			calls = append(calls, cp)
			return nil
		},
	)

	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := 1; i <= 3; i++ {
		wg.Add(1)
		go func(v int) {
			defer wg.Done()
			<-start
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			if err := b.Enqueue(ctx, "node-a", []int{v}); err != nil {
				t.Errorf("enqueue failed: %v", err)
			}
		}(i)
	}
	close(start)
	wg.Wait()

	mu.Lock()
	defer mu.Unlock()
	if len(calls) != 1 {
		t.Fatalf("expected 1 flush call, got %d", len(calls))
	}
	if len(calls[0]) != 3 {
		t.Fatalf("expected 3 items in batch, got %d", len(calls[0]))
	}
}

func TestNodeBatcherFlushByDelay(t *testing.T) {
	stopCh := make(chan struct{})
	defer close(stopCh)

	var (
		mu    sync.Mutex
		calls [][]int
	)

	delay := 25 * time.Millisecond
	b := newNodeBatcher[int](
		10,
		delay,
		1,
		stopCh,
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		"test",
		func(_ context.Context, _ string, items []int) error {
			mu.Lock()
			defer mu.Unlock()
			cp := append([]int(nil), items...)
			calls = append(calls, cp)
			return nil
		},
	)

	start := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := b.Enqueue(ctx, "node-a", []int{1}); err != nil {
		t.Fatalf("enqueue failed: %v", err)
	}
	elapsed := time.Since(start)
	if elapsed < delay {
		t.Fatalf("expected delay-based flush to wait at least %v, got %v", delay, elapsed)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(calls) != 1 {
		t.Fatalf("expected 1 flush call, got %d", len(calls))
	}
	if len(calls[0]) != 1 {
		t.Fatalf("expected 1 item in batch, got %d", len(calls[0]))
	}
}

func TestNodeBatcherChunksOversizedFlush(t *testing.T) {
	stopCh := make(chan struct{})
	defer close(stopCh)

	var (
		mu    sync.Mutex
		calls [][]int
	)

	b := newNodeBatcher[int](
		3,
		100*time.Millisecond,
		1,
		stopCh,
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		"test",
		func(_ context.Context, _ string, items []int) error {
			mu.Lock()
			defer mu.Unlock()
			cp := append([]int(nil), items...)
			calls = append(calls, cp)
			return nil
		},
	)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := b.Enqueue(ctx, "node-a", []int{1, 2, 3, 4, 5, 6, 7}); err != nil {
		t.Fatalf("enqueue failed: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(calls) != 3 {
		t.Fatalf("expected 3 chunked flush calls, got %d", len(calls))
	}
	if len(calls[0]) != 3 || len(calls[1]) != 3 || len(calls[2]) != 1 {
		t.Fatalf("unexpected chunk sizes: %d, %d, %d", len(calls[0]), len(calls[1]), len(calls[2]))
	}
}

func TestNodeBatcherConcurrentFlush(t *testing.T) {
	stopCh := make(chan struct{})
	defer close(stopCh)

	const workers = 4
	var maxConcurrent atomic.Int32
	var current atomic.Int32

	b := newNodeBatcher[int](
		1,
		time.Millisecond,
		workers,
		stopCh,
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		"test",
		func(_ context.Context, _ string, items []int) error {
			c := current.Add(1)
			for {
				old := maxConcurrent.Load()
				if c <= old || maxConcurrent.CompareAndSwap(old, c) {
					break
				}
			}
			time.Sleep(50 * time.Millisecond)
			current.Add(-1)
			return nil
		},
	)

	var wg sync.WaitGroup
	for i := 0; i < workers*2; i++ {
		wg.Add(1)
		go func(v int) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_ = b.Enqueue(ctx, "node-a", []int{v})
		}(i)
	}
	wg.Wait()

	mc := maxConcurrent.Load()
	if mc < 2 {
		t.Fatalf("expected at least 2 concurrent flushes, got %d", mc)
	}
	if mc > int32(workers) {
		t.Fatalf("expected at most %d concurrent flushes, got %d", workers, mc)
	}
}

func TestNodeBatcherDetachedResultWait(t *testing.T) {
	stopCh := make(chan struct{})
	defer close(stopCh)

	flushDelay := 100 * time.Millisecond
	b := newNodeBatcher[int](
		1,
		time.Millisecond,
		1,
		stopCh,
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		"test",
		func(_ context.Context, _ string, items []int) error {
			time.Sleep(flushDelay)
			return nil
		},
	)

	// Caller context expires quickly, but flush completes within the detached timeout.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	err := b.Enqueue(ctx, "node-a", []int{1})
	if err != nil {
		t.Fatalf("expected nil error with detached result-wait, got %v", err)
	}
}

// releaseCounter records how often the batcher ended its ownership of each item.
type releaseCounter struct {
	mu     sync.Mutex
	counts map[int]int
}

func (r *releaseCounter) release(item int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.counts == nil {
		r.counts = make(map[int]int)
	}
	r.counts[item]++
}

func (r *releaseCounter) snapshot() map[int]int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return maps.Clone(r.counts)
}

func onceEach(items ...int) map[int]int {
	want := make(map[int]int, len(items))
	for _, item := range items {
		want[item] = 1
	}
	return want
}

func newOwningBatcher(t *testing.T, maxSize int, stopCh <-chan struct{}, released *releaseCounter,
	send func(context.Context, string, []int) error,
) *nodeBatcher[int] {
	t.Helper()
	return newNodeBatcher(maxSize, time.Millisecond, 1, stopCh,
		slog.New(slog.NewTextHandler(io.Discard, nil)), "test", send,
		withItemRelease(released.release))
}

func TestNodeBatcherOwnership(t *testing.T) {
	t.Run("flush/releases each item once", func(t *testing.T) {
		stopCh := make(chan struct{})
		t.Cleanup(func() { close(stopCh) })
		var released releaseCounter
		b := newOwningBatcher(t, 10, stopCh, &released, func(context.Context, string, []int) error { return nil })

		require.NoError(t, b.Enqueue(context.Background(), "node-a", []int{1, 2, 3}))
		assert.Equal(t, onceEach(1, 2, 3), released.snapshot())
	})

	t.Run("timeout/items stay owned until the flush ends", func(t *testing.T) {
		stopCh := make(chan struct{})
		t.Cleanup(func() { close(stopCh) })
		unblock := make(chan struct{})
		var released releaseCounter
		b := newOwningBatcher(t, 10, stopCh, &released, func(context.Context, string, []int) error {
			<-unblock
			return nil
		})
		b.resultTimeout = 20 * time.Millisecond

		err := b.Enqueue(context.Background(), "node-a", []int{1, 2})
		require.ErrorIs(t, err, ErrBatchOutcomeUnknown)
		assert.Empty(t, released.snapshot(), "a flush still reading the items must keep them")

		close(unblock)
		require.Eventually(t, func() bool { return maps.Equal(onceEach(1, 2), released.snapshot()) },
			time.Second, time.Millisecond)
	})

	t.Run("stop/queued requests are released", func(t *testing.T) {
		stopCh := make(chan struct{})
		unblock := make(chan struct{})
		started := make(chan struct{}, 1)
		var released releaseCounter
		b := newOwningBatcher(t, 1, stopCh, &released, func(context.Context, string, []int) error {
			select {
			case started <- struct{}{}:
			default:
			}
			<-unblock
			return nil
		})

		for item := range 5 {
			require.NoError(t, b.EnqueueAsync(context.Background(), "node-a", []int{item}))
		}
		<-started
		close(stopCh)
		close(unblock)
		require.Eventually(t, func() bool { return maps.Equal(onceEach(0, 1, 2, 3, 4), released.snapshot()) },
			time.Second, time.Millisecond)
	})

	t.Run("stopped/unadmitted items are released", func(t *testing.T) {
		stopCh := make(chan struct{})
		close(stopCh)
		var released releaseCounter
		b := newOwningBatcher(t, 10, stopCh, &released, func(context.Context, string, []int) error { return nil })

		require.Error(t, b.Enqueue(context.Background(), "node-a", []int{7}))
		require.Eventually(t, func() bool { return maps.Equal(onceEach(7), released.snapshot()) },
			time.Second, time.Millisecond)
	})
}

// Stopping while senders are between acquiring a worker and sending must not
// strand a request in a channel no worker will read again.
func TestNodeBatcherStopReleasesEveryItem(t *testing.T) {
	stopCh := make(chan struct{})
	var released releaseCounter
	b := newOwningBatcher(t, 4, stopCh, &released, func(context.Context, string, []int) error { return nil })

	const senders, perSender = 8, 50
	var wg sync.WaitGroup
	start := make(chan struct{})
	for s := range senders {
		wg.Go(func() {
			<-start
			for i := range perSender {
				_ = b.EnqueueAsync(context.Background(), "node-"+strconv.Itoa(i%3), []int{s*perSender + i})
			}
		})
	}
	close(start)
	close(stopCh)
	wg.Wait()

	want := make([]int, 0, senders*perSender)
	for i := range senders * perSender {
		want = append(want, i)
	}
	require.Eventually(t, func() bool { return maps.Equal(onceEach(want...), released.snapshot()) },
		5*time.Second, time.Millisecond, "every item is released exactly once")
}

func TestNodeBatcherRetiresIdleWorkers(t *testing.T) {
	stopCh := make(chan struct{})
	t.Cleanup(func() { close(stopCh) })
	var sent atomic.Int64
	var released releaseCounter
	b := newOwningBatcher(t, 4, stopCh, &released, func(_ context.Context, _ string, items []int) error {
		sent.Add(int64(len(items)))
		return nil
	})
	b.idleTimeout = time.Millisecond
	b.resultTimeout = 5 * time.Second

	workerCount := func() int {
		b.mu.Lock()
		defer b.mu.Unlock()
		return len(b.workers)
	}

	require.NoError(t, b.Enqueue(context.Background(), "departed", []int{0}))
	require.Eventually(t, func() bool { return workerCount() == 0 }, time.Second, time.Millisecond)

	// Workers retire and respawn underneath concurrent senders; a request
	// stranded in a retired worker's channel would surface as a timeout.
	const senders, perSender = 8, 50
	var wg sync.WaitGroup
	for s := range senders {
		wg.Go(func() {
			for i := range perSender {
				assert.NoError(t, b.Enqueue(context.Background(), "node-a", []int{s*perSender + i + 1}))
			}
		})
	}
	wg.Wait()
	assert.Equal(t, int64(senders*perSender+1), sent.Load())
}
