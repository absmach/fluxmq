// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package consumer

import (
	"sync/atomic"
	"time"
)

// Metrics tracks consumer group manager statistics.
type Metrics struct {
	// Claim metrics
	ClaimAttempts  uint64 // Total claim attempts
	ClaimSuccesses uint64 // Successful claims from cursor
	ClaimFailures  uint64 // Failed claims (no messages)

	// Work stealing metrics
	StealAttempts  uint64 // Total steal attempts
	StealSuccesses uint64 // Successful steals
	StealFailures  uint64 // Failed steals (nothing to steal)

	// Ack metrics
	AckCount    uint64 // Total acks
	NackCount   uint64 // Total nacks
	RejectCount uint64 // Total rejects (sent to DLQ)

	// DLQ metrics
	DLQCount uint64 // Messages moved to DLQ

	// DLQTransferFailures counts poison messages whose dead-letter transfer was
	// attempted and failed. The entry stays pending and the transfer is retried
	// under backoff, so a rising count means records are stuck, not lost.
	DLQTransferFailures uint64

	// PoisonWithoutDLQ counts poison messages returned to ordinary redelivery
	// because the queue has no dead-letter destination at all. They are not
	// stuck: they keep being delivered, which is the only remaining option.
	PoisonWithoutDLQ uint64

	// Capture metrics
	CaptureFailures uint64 // Matching queues a captured publish failed to reach
	CaptureDropped  uint64 // Capture jobs discarded without being attempted

	// Latency tracking (in nanoseconds)
	TotalClaimLatency uint64 // Sum of claim latencies
	TotalStealLatency uint64 // Sum of steal latencies
	TotalAckLatency   uint64 // Sum of ack latencies

	// PEL metrics
	PELSize      uint64 // Current PEL size (approximate)
	PELHighWater uint64 // Maximum PEL size seen
}

// NewMetrics creates a new metrics instance.
func NewMetrics() *Metrics {
	return &Metrics{}
}

// RecordClaim records a claim operation.
func (m *Metrics) RecordClaim(success bool, latency time.Duration) {
	atomic.AddUint64(&m.ClaimAttempts, 1)
	if success {
		atomic.AddUint64(&m.ClaimSuccesses, 1)
	} else {
		atomic.AddUint64(&m.ClaimFailures, 1)
	}
	atomic.AddUint64(&m.TotalClaimLatency, uint64(latency.Nanoseconds()))
}

// RecordSteal records a steal operation.
func (m *Metrics) RecordSteal(success bool, latency time.Duration) {
	atomic.AddUint64(&m.StealAttempts, 1)
	if success {
		atomic.AddUint64(&m.StealSuccesses, 1)
	} else {
		atomic.AddUint64(&m.StealFailures, 1)
	}
	atomic.AddUint64(&m.TotalStealLatency, uint64(latency.Nanoseconds()))
}

// RecordAck records an ack operation.
func (m *Metrics) RecordAck(latency time.Duration) {
	atomic.AddUint64(&m.AckCount, 1)
	atomic.AddUint64(&m.TotalAckLatency, uint64(latency.Nanoseconds()))
}

// RecordNack records a nack operation.
func (m *Metrics) RecordNack() {
	atomic.AddUint64(&m.NackCount, 1)
}

// RecordReject records a reject operation.
func (m *Metrics) RecordReject() {
	atomic.AddUint64(&m.RejectCount, 1)
}

// RecordDLQ records a message moved to DLQ.
func (m *Metrics) RecordDLQ() {
	atomic.AddUint64(&m.DLQCount, 1)
}

// RecordDLQTransferFailure records one failed dead-letter transfer of a poison
// message. The source entry remains pending, so this counts records that are
// stuck rather than records that were lost.
func (m *Metrics) RecordDLQTransferFailure() {
	atomic.AddUint64(&m.DLQTransferFailures, 1)
}

// RecordPoisonWithoutDLQ records one poison message returned to ordinary
// redelivery because its queue has no dead-letter destination.
func (m *Metrics) RecordPoisonWithoutDLQ() {
	atomic.AddUint64(&m.PoisonWithoutDLQ, 1)
}

// RecordCaptureFailure records one matching queue a captured publish failed to
// reach, whether its append failed or its configuration could not be read.
//
// The unit is the queue, not the publish: a publish matching three queues and
// failing two counts twice, so the counter measures records lost rather than how
// often capture was lossy. Capture runs off the publish path and never fails the
// publish, so this and CaptureDropped are the only signals it has.
//
// One case is coarser: when the matching queues cannot be resolved at all, the
// set of queues that would have matched is unknown, so it counts once.
func (m *Metrics) RecordCaptureFailure() {
	atomic.AddUint64(&m.CaptureFailures, 1)
}

// RecordCaptureDropped records one capture job discarded before it was
// attempted, because the backlog was full or shutdown drained past its deadline.
//
// It is deliberately separate from CaptureFailures: a failure means the append
// was tried and did not succeed, while a drop means it was never tried. Both
// lose a message, but only a rising drop count says capture cannot keep up with
// the publish rate.
func (m *Metrics) RecordCaptureDropped() {
	atomic.AddUint64(&m.CaptureDropped, 1)
}

// UpdatePELSize updates the current PEL size.
func (m *Metrics) UpdatePELSize(size uint64) {
	atomic.StoreUint64(&m.PELSize, size)

	// Update high water mark
	for {
		current := atomic.LoadUint64(&m.PELHighWater)
		if size <= current {
			break
		}
		if atomic.CompareAndSwapUint64(&m.PELHighWater, current, size) {
			break
		}
	}
}

// GetClaimRate returns claims per second (requires external timing).
func (m *Metrics) GetClaimSuccessRate() float64 {
	attempts := atomic.LoadUint64(&m.ClaimAttempts)
	if attempts == 0 {
		return 0
	}
	successes := atomic.LoadUint64(&m.ClaimSuccesses)
	return float64(successes) / float64(attempts)
}

// GetStealSuccessRate returns the steal success rate.
func (m *Metrics) GetStealSuccessRate() float64 {
	attempts := atomic.LoadUint64(&m.StealAttempts)
	if attempts == 0 {
		return 0
	}
	successes := atomic.LoadUint64(&m.StealSuccesses)
	return float64(successes) / float64(attempts)
}

// GetAverageClaimLatency returns the average claim latency.
func (m *Metrics) GetAverageClaimLatency() time.Duration {
	attempts := atomic.LoadUint64(&m.ClaimAttempts)
	if attempts == 0 {
		return 0
	}
	total := atomic.LoadUint64(&m.TotalClaimLatency)
	return time.Duration(total / attempts)
}

// GetAverageStealLatency returns the average steal latency.
func (m *Metrics) GetAverageStealLatency() time.Duration {
	attempts := atomic.LoadUint64(&m.StealAttempts)
	if attempts == 0 {
		return 0
	}
	total := atomic.LoadUint64(&m.TotalStealLatency)
	return time.Duration(total / attempts)
}

// Snapshot returns a copy of current metrics.
func (m *Metrics) Snapshot() Metrics {
	return Metrics{
		ClaimAttempts:       atomic.LoadUint64(&m.ClaimAttempts),
		ClaimSuccesses:      atomic.LoadUint64(&m.ClaimSuccesses),
		ClaimFailures:       atomic.LoadUint64(&m.ClaimFailures),
		StealAttempts:       atomic.LoadUint64(&m.StealAttempts),
		StealSuccesses:      atomic.LoadUint64(&m.StealSuccesses),
		StealFailures:       atomic.LoadUint64(&m.StealFailures),
		AckCount:            atomic.LoadUint64(&m.AckCount),
		NackCount:           atomic.LoadUint64(&m.NackCount),
		RejectCount:         atomic.LoadUint64(&m.RejectCount),
		DLQCount:            atomic.LoadUint64(&m.DLQCount),
		DLQTransferFailures: atomic.LoadUint64(&m.DLQTransferFailures),
		PoisonWithoutDLQ:    atomic.LoadUint64(&m.PoisonWithoutDLQ),
		CaptureFailures:     atomic.LoadUint64(&m.CaptureFailures),
		CaptureDropped:      atomic.LoadUint64(&m.CaptureDropped),
		TotalClaimLatency:   atomic.LoadUint64(&m.TotalClaimLatency),
		TotalStealLatency:   atomic.LoadUint64(&m.TotalStealLatency),
		TotalAckLatency:     atomic.LoadUint64(&m.TotalAckLatency),
		PELSize:             atomic.LoadUint64(&m.PELSize),
		PELHighWater:        atomic.LoadUint64(&m.PELHighWater),
	}
}

// Reset resets all metrics to zero.
func (m *Metrics) Reset() {
	atomic.StoreUint64(&m.ClaimAttempts, 0)
	atomic.StoreUint64(&m.ClaimSuccesses, 0)
	atomic.StoreUint64(&m.ClaimFailures, 0)
	atomic.StoreUint64(&m.StealAttempts, 0)
	atomic.StoreUint64(&m.StealSuccesses, 0)
	atomic.StoreUint64(&m.StealFailures, 0)
	atomic.StoreUint64(&m.AckCount, 0)
	atomic.StoreUint64(&m.NackCount, 0)
	atomic.StoreUint64(&m.RejectCount, 0)
	atomic.StoreUint64(&m.DLQCount, 0)
	atomic.StoreUint64(&m.DLQTransferFailures, 0)
	atomic.StoreUint64(&m.PoisonWithoutDLQ, 0)
	atomic.StoreUint64(&m.CaptureFailures, 0)
	atomic.StoreUint64(&m.CaptureDropped, 0)
	atomic.StoreUint64(&m.TotalClaimLatency, 0)
	atomic.StoreUint64(&m.TotalStealLatency, 0)
	atomic.StoreUint64(&m.TotalAckLatency, 0)
	// Don't reset PELSize or PELHighWater
}
