// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package logstorage

import (
	"sync"
	"sync/atomic"
)

// beforeSegmentSync runs inside a segment's barrier, after it has captured the
// offset it will cover and immediately before the fsync itself. It exists so a
// test can hold a barrier open and observe what concurrent callers do; it is
// nil in production and costs one nil check per fsync.
var beforeSegmentSync func()

// segmentCommit is a segment's group-commit state.
//
// Without it, every publisher waiting on a durable acknowledgement pays for its
// own fsync while holding the segment lock, so publishers to one queue
// serialize and the queue is capped at the reciprocal of the device's fsync
// latency — measured at roughly 200 messages a second on consumer NVMe, no
// matter how many publishers there are.
//
// With it, one caller performs the fsync and everyone who arrived before it
// started rides the same barrier. Each caller learns whether its own record was
// covered rather than trusting that a sync happened: a sync only promises the
// bytes written before it began, so a caller whose append landed later loops
// and takes the next barrier.
type segmentCommit struct {
	mu sync.Mutex

	// syncing reports whether an fsync is in flight; waiters is closed when it
	// finishes, which is what wakes everyone riding that barrier.
	syncing bool
	waiters chan struct{}

	// through is the highest offset known to be on disk.
	through uint64

	// syncs counts completed fsyncs and waiting counts callers parked on
	// someone else's barrier. Both are observation points for tests and, later,
	// for metrics; neither is load-bearing.
	syncs   atomic.Uint64
	waiting atomic.Int64
}

// syncThrough makes every batch appended before offset durable, coalescing
// concurrent callers into a single fsync. It returns when offset is on disk, or
// with the error from the fsync that failed to put it there.
//
// fsync runs with no segment lock held, which is the entire point: appends
// continue while a barrier is in flight and ride the next one.
func (c *segmentCommit) syncThrough(offset uint64, sync func() (covered uint64, err error)) error {
	for {
		c.mu.Lock()
		if c.through >= offset {
			c.mu.Unlock()
			return nil
		}

		if c.syncing {
			waiters := c.waiters
			c.mu.Unlock()

			c.waiting.Add(1)
			<-waiters
			c.waiting.Add(-1)
			continue
		}

		c.syncing = true
		waiters := make(chan struct{})
		c.waiters = waiters
		c.mu.Unlock()

		covered, err := sync()

		c.mu.Lock()
		if err == nil {
			c.syncs.Add(1)
			if covered > c.through {
				c.through = covered
			}
		}
		c.syncing = false
		close(waiters)
		c.mu.Unlock()

		if err != nil {
			return err
		}
		// Loop rather than return: this barrier covered whatever had been
		// written when it started, which may predate this caller's append.
	}
}
