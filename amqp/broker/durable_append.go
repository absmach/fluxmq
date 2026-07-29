// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package broker

import "sync"

// maxOutstandingDurableAppends bounds how many durable appends may be waiting
// on one stream's storage barrier at the same time.
//
// A publisher that times out is answered with a NACK, but the append it started
// keeps running: an fsync cannot be cancelled. Without a bound, a publisher
// retrying against permanently stalled storage would start a new barrier per
// attempt, each holding its message payload and queueing behind the same
// per-queue storage mutex, so a stall would turn into unbounded goroutine and
// memory growth. The limit leaves room for genuinely concurrent publishers
// while capping what one stalled stream can retain.
//
// It is a variable only so tests can lower it; production code must not
// reassign it.
var maxOutstandingDurableAppends = 16

// durableAppendLimiter counts the durable appends currently running per queue.
// Its zero value is ready for use.
type durableAppendLimiter struct {
	mu          sync.Mutex
	outstanding map[string]int
}

// acquire reserves a slot for one durable append, reporting false when the
// queue already has the maximum number of appends waiting on storage.
func (l *durableAppendLimiter) acquire(queueName string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.outstanding == nil {
		l.outstanding = make(map[string]int)
	}
	if l.outstanding[queueName] >= maxOutstandingDurableAppends {
		return false
	}
	l.outstanding[queueName]++
	return true
}

// release returns a slot once the append and its barrier have finished, which
// may be long after the publisher was answered.
func (l *durableAppendLimiter) release(queueName string) {
	l.mu.Lock()
	defer l.mu.Unlock()

	count, ok := l.outstanding[queueName]
	if !ok {
		return
	}
	if count <= 1 {
		delete(l.outstanding, queueName)
		return
	}
	l.outstanding[queueName] = count - 1
}

// outstandingFor reports how many durable appends are in flight for a queue.
func (l *durableAppendLimiter) outstandingFor(queueName string) int {
	l.mu.Lock()
	defer l.mu.Unlock()

	return l.outstanding[queueName]
}
