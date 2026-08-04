// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package queue

import (
	"context"
	"hash/fnv"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/absmach/fluxmq/queue/types"
)

const (
	// defaultCaptureWorkers is the number of independent capture lanes. A queue
	// is always handled by the same lane, so per-queue append order matches
	// publish order, and a queue whose store stalls blocks only the lane it
	// hashes to rather than every capture on the node.
	//
	// Lanes are shared, not per queue: queues are hashed onto a fixed number of
	// them so the goroutine count does not follow the queue count. The cost is
	// that a stalled queue also stalls the unrelated queues sharing its lane,
	// until its backlog fills and their jobs are dropped. More lanes make that
	// collision less likely without eliminating it; only a lane per queue would,
	// at a goroutine per queue.
	defaultCaptureWorkers = 4
	// defaultCaptureQueueDepth is the per-lane backlog, counted in jobs rather
	// than bytes. It bounds how long a stalled store can absorb publishes before
	// jobs are dropped, so memory stays flat instead of growing with the stall —
	// but the ceiling is workers x depth payloads, so a deployment capturing
	// large messages should lower queue_manager.capture_queue_depth rather than
	// assume the default is small.
	defaultCaptureQueueDepth = 1024
	// defaultCaptureDrainTimeout bounds how long Stop waits for queued capture
	// to be written. Anything still queued after it is counted as dropped
	// rather than delaying shutdown indefinitely.
	defaultCaptureDrainTimeout = 5 * time.Second
)

// captureJob is one captured publish bound for one queue, or — when target is
// nil — the per-publish cluster forward for queues this node does not know.
//
// Jobs are per target rather than per publish so that a lane can be chosen by
// queue name. The publish they share is already detached from the protocol
// broker's buffers by the time it is enqueued, and the workers only read it.
type captureJob struct {
	publish types.PublishRequest
	target  *queuePublishTarget
}

// captureDispatcher moves the storage half of topic capture off the publish
// path.
//
// Capture is broker policy applied to whichever queues match a topic; the
// publisher neither asked for it nor learns of it. Running it inline made a
// queue whose store stalls delay every subscriber of a matching topic, because
// the append honours no cancellation. The dispatcher keeps the publish path
// free of that: enqueueing never blocks, and a full lane drops the job and
// counts it rather than waiting for room.
//
// Lanes are never closed. Producers may race Stop, and sending on a closed
// channel panics, so shutdown is signalled with stopCh and the buffered jobs are
// drained by the workers instead.
type captureDispatcher struct {
	lanes        []chan captureJob
	apply        func(context.Context, captureJob)
	metrics      captureMetrics
	logger       *slog.Logger
	drainTimeout time.Duration

	// closedMu makes accepting a job and shutting down mutually exclusive. A
	// send that merely happened to run after stopCh closed would sit in a lane
	// no worker reads again: accepted by the caller, never written, and never
	// counted. Stop takes the write lock, so once it has it no enqueue is in
	// flight and none can start.
	closedMu sync.RWMutex
	closed   bool

	// lastWarn throttles the drop log. Dropping happens on the publishing
	// goroutine and a saturated backlog reaches it on every publish, so logging
	// each one would put the I/O this type exists to remove back on the publish
	// path.
	lastWarn atomic.Int64

	stopCh   chan struct{}
	stopOnce sync.Once
	wg       sync.WaitGroup
}

// dropWarnInterval bounds how often a drop is logged. The counter is exact; the
// log is only a sample of it.
const dropWarnInterval = 10 * time.Second

// captureMetrics is the subset of counters the dispatcher owns.
type captureMetrics interface {
	RecordCaptureDropped()
}

func newCaptureDispatcher(
	workers, depth int,
	drainTimeout time.Duration,
	metrics captureMetrics,
	logger *slog.Logger,
	apply func(context.Context, captureJob),
) *captureDispatcher {
	if workers <= 0 {
		workers = defaultCaptureWorkers
	}
	if depth <= 0 {
		depth = defaultCaptureQueueDepth
	}
	if drainTimeout <= 0 {
		drainTimeout = defaultCaptureDrainTimeout
	}

	d := &captureDispatcher{
		lanes:        make([]chan captureJob, workers),
		apply:        apply,
		metrics:      metrics,
		logger:       logger,
		drainTimeout: drainTimeout,
		stopCh:       make(chan struct{}),
	}
	for i := range d.lanes {
		d.lanes[i] = make(chan captureJob, depth)
	}
	return d
}

// Start launches one worker per lane.
func (d *captureDispatcher) Start(ctx context.Context) {
	for i := range d.lanes {
		d.wg.Add(1)
		go d.run(ctx, d.lanes[i])
	}
}

// laneFor picks the lane a queue is always handled by. Jobs with no target are
// the per-publish cluster forward, which has no queue to order against and so
// rides the first lane.
func (d *captureDispatcher) laneFor(job captureJob) chan captureJob {
	if job.target == nil {
		return d.lanes[0]
	}
	hash := fnv.New32a()
	// Hash writes never fail.
	_, _ = hash.Write([]byte(job.target.name))
	return d.lanes[int(hash.Sum32())%len(d.lanes)]
}

// enqueue submits a job without ever blocking the caller. It reports whether the
// job was accepted; a rejected job has already been counted as dropped.
//
// The newest job is dropped rather than the oldest so that a saturated queue
// holds a contiguous prefix of the stream: a consumer knows it has everything up
// to the moment capture saturated, instead of a log with a hole in it.
func (d *captureDispatcher) enqueue(job captureJob) bool {
	// Held across the send, so a job cannot land in a lane that shutdown has
	// already stopped reading. It is a read lock: publishes never contend with
	// each other, only with Stop.
	d.closedMu.RLock()
	defer d.closedMu.RUnlock()

	if d.closed {
		d.drop(job, "dispatcher stopped")
		return false
	}

	select {
	case d.laneFor(job) <- job:
		return true
	default:
		d.drop(job, "capture backlog full")
		return false
	}
}

// drop counts a lost job and, at most once per dropWarnInterval, logs one.
//
// Counting is unconditional because queues.capture_dropped is the contract.
// Logging is throttled because drop runs on the publishing goroutine, and a
// saturated backlog would otherwise write a log line per publish — the very
// thing dispatching capture exists to keep off that path.
func (d *captureDispatcher) drop(job captureJob, reason string) {
	if d.metrics != nil {
		d.metrics.RecordCaptureDropped()
	}
	if d.logger == nil || !d.shouldWarn() {
		return
	}

	queueName := ""
	if job.target != nil {
		queueName = job.target.name
	}
	d.logger.Warn("capture job dropped",
		slog.String("reason", reason),
		slog.String("queue", queueName),
		slog.String("topic", job.publish.Topic))
}

func (d *captureDispatcher) shouldWarn() bool {
	now := time.Now().UnixNano()
	last := d.lastWarn.Load()
	return now-last >= int64(dropWarnInterval) && d.lastWarn.CompareAndSwap(last, now)
}

func (d *captureDispatcher) run(ctx context.Context, lane chan captureJob) {
	defer d.wg.Done()

	for {
		select {
		case job := <-lane:
			d.apply(ctx, job)
		case <-d.stopCh:
			d.drain(ctx, lane)
			return
		}
	}
}

// drain writes what is already queued, bounded by drainTimeout so a stalled
// store cannot hold shutdown open. Whatever is left is counted as dropped, so
// the loss appears in the same counter as any other.
func (d *captureDispatcher) drain(ctx context.Context, lane chan captureJob) {
	deadline := time.Now().Add(d.drainTimeout)
	for {
		select {
		case job := <-lane:
			if time.Now().After(deadline) {
				d.drop(job, "shutdown drain timed out")
				continue
			}
			d.apply(ctx, job)
		default:
			return
		}
	}
}

// Stop refuses further jobs, drains what is queued, and returns within
// drainTimeout whether or not the workers finished.
//
// A worker already inside an append cannot be interrupted: the store takes no
// context and the write is not cancellable. Waiting on it would let one stalled
// queue hold shutdown open indefinitely — the same failure dispatching capture
// exists to prevent, moved to a different moment — so the wait is bounded and a
// worker still wedged when the budget expires is left to finish on its own.
func (d *captureDispatcher) Stop() {
	// Close acceptance first, and under the write lock, so no enqueue is in
	// flight when the lanes stop being read.
	d.closedMu.Lock()
	d.closed = true
	d.closedMu.Unlock()

	d.stopOnce.Do(func() { close(d.stopCh) })

	drained := make(chan struct{})
	go func() {
		d.wg.Wait()
		close(drained)
	}()

	timer := time.NewTimer(d.drainTimeout)
	defer timer.Stop()
	select {
	case <-drained:
	case <-timer.C:
		if d.logger != nil {
			d.logger.Warn("capture drain timed out; a queue store is still blocking a worker",
				slog.Duration("timeout", d.drainTimeout))
		}
	}

	// Count whatever the workers did not reach, so a job lost to shutdown is
	// visible in the same counter as any other. Acceptance is already closed, so
	// no lane can grow while this runs; a worker racing for the same job only
	// means one of the two accounts for it.
	for _, lane := range d.lanes {
		d.sweep(lane)
	}
}

// sweep counts every job left in a lane.
func (d *captureDispatcher) sweep(lane chan captureJob) {
	for {
		select {
		case job := <-lane:
			d.drop(job, "shutdown left the job unwritten")
		default:
			return
		}
	}
}
