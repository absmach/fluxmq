// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package queue

import (
	"log/slog"
	"sync"
)

// deliveryScheduleDepth bounds the pending wake-ups. A full channel drops the
// trigger and the next sweep picks the queue up, so this is a coalescing buffer
// rather than a work queue that must not lose entries.
const deliveryScheduleDepth = 4096

// deliveryQueue is the wake-up channel between whatever writes a record and
// whatever delivers it, with duplicate triggers for one queue coalesced.
//
// It is a type of its own, owned by neither side, because the two sides need
// each other: appending wakes delivery, and delivery consumes appends. Holding
// this instead of holding the engine is what lets the record core be built
// complete — before the engine exists — rather than assigned into afterwards.
type deliveryQueue struct {
	logger *slog.Logger

	mu       sync.Mutex
	enqueued map[string]struct{}
	queue    chan string
}

func newDeliveryQueue(depth int, logger *slog.Logger) *deliveryQueue {
	if logger == nil {
		logger = slog.Default()
	}
	return &deliveryQueue{
		logger:   logger,
		enqueued: make(map[string]struct{}),
		queue:    make(chan string, depth),
	}
}

// Schedule enqueues a queue name for delivery. Duplicate schedules for the same
// queue are coalesced until it is delivered.
func (d *deliveryQueue) Schedule(queueName string) {
	if queueName == "" {
		return
	}

	d.mu.Lock()
	if _, exists := d.enqueued[queueName]; exists {
		d.mu.Unlock()
		return
	}
	d.enqueued[queueName] = struct{}{}
	d.mu.Unlock()

	select {
	case d.queue <- queueName:
	default:
		d.logger.Warn("delivery channel full, dropping trigger (will retry on next sweep)",
			slog.String("queue", queueName))
		d.markDelivered(queueName)
	}
}

// markDelivered lets the queue be scheduled again.
func (d *deliveryQueue) markDelivered(queueName string) {
	d.mu.Lock()
	delete(d.enqueued, queueName)
	d.mu.Unlock()
}

// pending is the channel a consumer reads scheduled queue names from.
func (d *deliveryQueue) pending() <-chan string { return d.queue }
