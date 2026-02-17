// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package cluster

import (
	"context"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"
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
