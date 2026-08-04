// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package queue

import (
	"context"
	"hash/fnv"
	"log/slog"
	"sync"
	"time"

	"github.com/absmach/fluxmq/queue/types"
)

const (
	// defaultCaptureWorkers is the number of independent capture lanes. A queue
	// is always handled by the same lane, so per-queue append order matches
	// publish order, and a queue whose store stalls blocks only the lane it
	// hashes to rather than every capture on the node.
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

	stopCh   chan struct{}
	stopOnce sync.Once
	wg       sync.WaitGroup
}

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
	select {
	case <-d.stopCh:
		d.drop(job, "dispatcher stopped")
		return false
	default:
	}

	select {
	case d.laneFor(job) <- job:
		return true
	default:
		d.drop(job, "capture backlog full")
		return false
	}
}

func (d *captureDispatcher) drop(job captureJob, reason string) {
	if d.metrics != nil {
		d.metrics.RecordCaptureDropped()
	}
	if d.logger != nil {
		queueName := ""
		if job.target != nil {
			queueName = job.target.name
		}
		d.logger.Warn("capture job dropped",
			slog.String("reason", reason),
			slog.String("queue", queueName),
			slog.String("topic", job.publish.Topic))
	}
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

// Stop signals the workers, waits for them to finish draining, and returns.
func (d *captureDispatcher) Stop() {
	d.stopOnce.Do(func() { close(d.stopCh) })
	d.wg.Wait()
}
