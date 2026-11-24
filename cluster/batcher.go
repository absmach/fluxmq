// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package cluster

import (
	"context"
	"errors"
	"log/slog"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

var errBatcherStopped = errors.New("batcher stopped")

// ErrBatchOutcomeUnknown reports that a batch was handed to its node worker but
// its result was not observed: the wait timed out or the batcher stopped. The
// items may still be delivered, so a caller must not resend them as if they
// had definitely failed.
var ErrBatchOutcomeUnknown = errors.New("batch outcome unknown")

const (
	flushResultTimeout = 35 * time.Second
	workerIdleTimeout  = 2 * time.Minute
)

type batchRequest[T any] struct {
	items  []T
	result chan error
}

type nodeWorker[T any] struct {
	ch chan batchRequest[T]
	// senders counts Enqueue calls holding ch that have not finished sending.
	// It is incremented under nodeBatcher.mu and decremented only after the
	// send, so a retire holding mu that sees zero and an empty ch knows no
	// request can still land in a channel nobody reads.
	senders atomic.Int32
}

// nodeBatcher batches items per remote node and flushes on max size or max delay.
//
// Enqueue and EnqueueAsync take ownership of the items they are given. When a
// release function is configured, every item is passed to it exactly once,
// after nothing in the batcher reads it any more, whether the batch was sent,
// failed, or was dropped. The caller may therefore return, time out, or free
// its own copies at any point after the call.
type nodeBatcher[T any] struct {
	name          string
	maxSize       int
	maxDelay      time.Duration
	flushWorkers  int
	stopCh        <-chan struct{}
	logger        *slog.Logger
	sendFn        func(ctx context.Context, nodeID string, items []T) error
	releaseFn     func(T)
	idleTimeout   time.Duration
	resultTimeout time.Duration

	mu      sync.Mutex
	workers map[string]*nodeWorker[T]
}

type batcherOption[T any] func(*nodeBatcher[T])

// withItemRelease sets the function that ends the batcher's ownership of an item.
func withItemRelease[T any](release func(T)) batcherOption[T] {
	return func(b *nodeBatcher[T]) { b.releaseFn = release }
}

func newNodeBatcher[T any](
	maxSize int,
	maxDelay time.Duration,
	flushWorkers int,
	stopCh <-chan struct{},
	logger *slog.Logger,
	name string,
	sendFn func(ctx context.Context, nodeID string, items []T) error,
	opts ...batcherOption[T],
) *nodeBatcher[T] {
	if logger == nil {
		logger = slog.Default()
	}
	if flushWorkers < 1 {
		flushWorkers = 1
	}
	b := &nodeBatcher[T]{
		name:          name,
		maxSize:       maxSize,
		maxDelay:      maxDelay,
		flushWorkers:  flushWorkers,
		stopCh:        stopCh,
		logger:        logger,
		sendFn:        sendFn,
		idleTimeout:   workerIdleTimeout,
		resultTimeout: flushResultTimeout,
		workers:       make(map[string]*nodeWorker[T]),
	}
	for _, opt := range opts {
		opt(b)
	}
	return b
}

// Enqueue hands items to the node's worker and waits for the flush result.
// The caller's context bounds only admission; once admitted, the wait is
// detached from it so the caller learns the real outcome.
func (b *nodeBatcher[T]) Enqueue(ctx context.Context, nodeID string, items []T) error {
	if len(items) == 0 {
		return nil
	}

	req := batchRequest[T]{
		items:  items,
		result: make(chan error, 1),
	}
	if err := b.admit(ctx, nodeID, req); err != nil {
		return err
	}

	timer := time.NewTimer(b.resultTimeout)
	defer timer.Stop()
	select {
	case err := <-req.result:
		return err
	case <-timer.C:
		return errors.Join(ErrBatchOutcomeUnknown, context.DeadlineExceeded)
	case <-b.stopCh:
		return errors.Join(ErrBatchOutcomeUnknown, errBatcherStopped)
	}
}

// EnqueueAsync enqueues a batch request and returns once the worker accepts it.
// It does not wait for flush completion.
func (b *nodeBatcher[T]) EnqueueAsync(ctx context.Context, nodeID string, items []T) error {
	if len(items) == 0 {
		return nil
	}

	return b.admit(ctx, nodeID, batchRequest[T]{
		items:  items,
		result: make(chan error, 1),
	})
}

// admit sends req to the node's worker. Items of a request that is not
// admitted are released here, since no worker will ever see them.
func (b *nodeBatcher[T]) admit(ctx context.Context, nodeID string, req batchRequest[T]) error {
	worker := b.acquireWorker(nodeID)
	if worker == nil {
		b.releaseItems(req.items)
		return errBatcherStopped
	}
	defer b.releaseWorker(worker)

	select {
	case worker.ch <- req:
		return nil
	case <-ctx.Done():
		b.releaseItems(req.items)
		return ctx.Err()
	case <-b.stopCh:
		b.releaseItems(req.items)
		return errBatcherStopped
	}
}

// acquireWorker returns the node's worker, registered as held by one more
// sender, or nil once the batcher has stopped. Refusing under the lock is what
// lets a stopping worker know when no request can still arrive.
func (b *nodeBatcher[T]) acquireWorker(nodeID string) *nodeWorker[T] {
	b.mu.Lock()
	defer b.mu.Unlock()

	select {
	case <-b.stopCh:
		return nil
	default:
	}

	worker, ok := b.workers[nodeID]
	if !ok {
		queueCap := max(b.maxSize*64, 1024)
		worker = &nodeWorker[T]{ch: make(chan batchRequest[T], queueCap)}
		b.workers[nodeID] = worker
		go b.runWorker(nodeID, worker)
	}
	worker.senders.Add(1)
	return worker
}

func (b *nodeBatcher[T]) releaseWorker(worker *nodeWorker[T]) {
	worker.senders.Add(-1)
}

// retire removes an idle worker so that departed nodes do not keep one forever.
// It refuses while a sender holds the worker or a request is queued.
func (b *nodeBatcher[T]) retire(nodeID string, worker *nodeWorker[T]) bool {
	b.mu.Lock()
	defer b.mu.Unlock()

	if worker.senders.Load() > 0 || len(worker.ch) > 0 || b.workers[nodeID] != worker {
		return false
	}
	delete(b.workers, nodeID)
	return true
}

func (b *nodeBatcher[T]) runWorker(nodeID string, worker *nodeWorker[T]) {
	sem := make(chan struct{}, b.flushWorkers)
	idle := time.NewTimer(b.idleTimeout)
	defer idle.Stop()

	for {
		select {
		case <-b.stopCh:
			b.drainUntilIdle(nodeID, worker)
			return
		case <-idle.C:
			if b.retire(nodeID, worker) {
				return
			}
			idle.Reset(b.idleTimeout)
		case first := <-worker.ch:
			idle.Stop()
			pending, items, stopped := b.collect(worker.ch, first)
			if stopped {
				b.finish(pending, errBatcherStopped)
				b.drainUntilIdle(nodeID, worker)
				return
			}

			// Acquire a flush slot (blocks if all flushers are busy).
			select {
			case sem <- struct{}{}:
			case <-b.stopCh:
				b.finish(pending, errBatcherStopped)
				b.drainUntilIdle(nodeID, worker)
				return
			}

			go func(items []T, pending []batchRequest[T]) {
				defer func() { <-sem }()
				b.finish(pending, b.flush(nodeID, items))
			}(items, pending)
			idle.Reset(b.idleTimeout)
		}
	}
}

// collect gathers requests behind first until the batch is full or maxDelay
// passes. It reports whether the batcher stopped while collecting.
func (b *nodeBatcher[T]) collect(ch <-chan batchRequest[T], first batchRequest[T]) ([]batchRequest[T], []T, bool) {
	pending := []batchRequest[T]{first}
	items := append([]T(nil), first.items...)

	timer := time.NewTimer(b.maxDelay)
	defer timer.Stop()
	for len(items) < b.maxSize {
		select {
		case <-b.stopCh:
			return pending, items, true
		case <-timer.C:
			return pending, items, false
		case req := <-ch:
			pending = append(pending, req)
			items = append(items, req.items...)
		}
	}
	return pending, items, false
}

// drainUntilIdle fails every request queued after the batcher stopped,
// including those from senders still between acquiring the worker and sending.
// No sender can acquire it once stopCh is closed, so the wait is bounded by
// the senders already in flight.
func (b *nodeBatcher[T]) drainUntilIdle(nodeID string, worker *nodeWorker[T]) {
	for {
		b.drain(worker.ch)

		b.mu.Lock()
		idle := worker.senders.Load() == 0 && len(worker.ch) == 0
		if idle && b.workers[nodeID] == worker {
			delete(b.workers, nodeID)
		}
		b.mu.Unlock()
		if idle {
			return
		}
		runtime.Gosched()
	}
}

// drain fails every request currently queued.
func (b *nodeBatcher[T]) drain(ch <-chan batchRequest[T]) {
	for {
		select {
		case req := <-ch:
			b.finish([]batchRequest[T]{req}, errBatcherStopped)
		default:
			return
		}
	}
}

func (b *nodeBatcher[T]) flush(nodeID string, items []T) error {
	if len(items) == 0 {
		return nil
	}

	for start := 0; start < len(items); start += b.maxSize {
		end := min(start+b.maxSize, len(items))

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

// finish ends the batcher's ownership of reqs and reports err to their callers.
func (b *nodeBatcher[T]) finish(reqs []batchRequest[T], err error) {
	for _, req := range reqs {
		b.releaseItems(req.items)
		select {
		case req.result <- err:
		default:
		}
	}
}

func (b *nodeBatcher[T]) releaseItems(items []T) {
	if b.releaseFn == nil {
		return
	}
	for _, item := range items {
		b.releaseFn(item)
	}
}
