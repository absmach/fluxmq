// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package cluster

import (
	"context"
	"io"
	"log/slog"
	"sync"
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
