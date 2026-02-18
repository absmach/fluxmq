// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package cluster

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"
)

var errBatcherStopped = errors.New("batcher stopped")

const flushResultTimeout = 35 * time.Second

type batchRequest[T any] struct {
	items  []T
	result chan error
}

// nodeBatcher batches items per remote node and flushes on max size or max delay.
type nodeBatcher[T any] struct {
	name         string
	maxSize      int
	maxDelay     time.Duration
	flushWorkers int
	stopCh       <-chan struct{}
	logger       *slog.Logger
	sendFn       func(ctx context.Context, nodeID string, items []T) error

	mu      sync.Mutex
	workers map[string]chan batchRequest[T]
}

func newNodeBatcher[T any](
	maxSize int,
	maxDelay time.Duration,
	flushWorkers int,
	stopCh <-chan struct{},
	logger *slog.Logger,
	name string,
	sendFn func(ctx context.Context, nodeID string, items []T) error,
) *nodeBatcher[T] {
	if logger == nil {
		logger = slog.Default()
	}
	if flushWorkers < 1 {
		flushWorkers = 1
	}
	return &nodeBatcher[T]{
		name:         name,
		maxSize:      maxSize,
		maxDelay:     maxDelay,
		flushWorkers: flushWorkers,
		stopCh:       stopCh,
		logger:       logger,
		sendFn:       sendFn,
		workers:      make(map[string]chan batchRequest[T]),
	}
}

func (b *nodeBatcher[T]) Enqueue(ctx context.Context, nodeID string, items []T) error {
	if len(items) == 0 {
		return nil
	}

	req := batchRequest[T]{
		items:  items,
		result: make(chan error, 1),
	}
	worker := b.worker(nodeID)

	select {
	case worker <- req:
	case <-ctx.Done():
		return ctx.Err()
	case <-b.stopCh:
		return errBatcherStopped
	}

	timer := time.NewTimer(flushResultTimeout)
	defer timer.Stop()
	select {
	case err := <-req.result:
		return err
	case <-timer.C:
		return context.DeadlineExceeded
	case <-b.stopCh:
		return errBatcherStopped
	}
}

// EnqueueAsync enqueues a batch request and returns once the worker accepts it.
// It does not wait for flush completion.
func (b *nodeBatcher[T]) EnqueueAsync(ctx context.Context, nodeID string, items []T) error {
	if len(items) == 0 {
		return nil
	}

	req := batchRequest[T]{
		items:  items,
		result: make(chan error, 1),
	}
	worker := b.worker(nodeID)

	select {
	case worker <- req:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-b.stopCh:
		return errBatcherStopped
	}
}

func (b *nodeBatcher[T]) worker(nodeID string) chan batchRequest[T] {
	b.mu.Lock()
	defer b.mu.Unlock()

	if ch, ok := b.workers[nodeID]; ok {
		return ch
	}

	queueCap := b.maxSize * 64
	if queueCap < 1024 {
		queueCap = 1024
	}
	ch := make(chan batchRequest[T], queueCap)
	b.workers[nodeID] = ch

	go b.runWorker(nodeID, ch)
	return ch
}

func (b *nodeBatcher[T]) runWorker(nodeID string, ch <-chan batchRequest[T]) {
	sem := make(chan struct{}, b.flushWorkers)

	for {
		select {
		case <-b.stopCh:
			return
		case first := <-ch:
			pending := []batchRequest[T]{first}
			items := append([]T(nil), first.items...)

			timer := time.NewTimer(b.maxDelay)
			collecting := true
			for collecting && len(items) < b.maxSize {
				select {
				case <-b.stopCh:
					b.completeAll(pending, errBatcherStopped)
					if !timer.Stop() {
						select {
						case <-timer.C:
						default:
						}
					}
					return
				case <-timer.C:
					collecting = false
				case req := <-ch:
					pending = append(pending, req)
					items = append(items, req.items...)
					if len(items) >= b.maxSize {
						collecting = false
					}
				}
			}

			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}

			// Acquire a flush slot (blocks if all flushers are busy).
			select {
			case sem <- struct{}{}:
			case <-b.stopCh:
				b.completeAll(pending, errBatcherStopped)
				return
			}

			go func(items []T, pending []batchRequest[T]) {
				defer func() { <-sem }()
				err := b.flush(nodeID, items)
				b.completeAll(pending, err)
			}(items, pending)
		}
	}
}

func (b *nodeBatcher[T]) flush(nodeID string, items []T) error {
	if len(items) == 0 {
		return nil
	}

	for start := 0; start < len(items); start += b.maxSize {
		end := start + b.maxSize
		if end > len(items) {
			end = len(items)
		}

		sendCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		err := b.sendFn(sendCtx, nodeID, items[start:end])
		cancel()
		if err != nil {
			b.logger.Warn("batch flush failed",
				slog.String("node_id", nodeID),
				slog.String("batch", b.name),
				slog.Int("size", end-start),
				slog.String("error", err.Error()))
			return err
		}
	}

	return nil
}

func (b *nodeBatcher[T]) completeAll(reqs []batchRequest[T], err error) {
	for _, req := range reqs {
		select {
		case req.result <- err:
		default:
		}
	}
}
