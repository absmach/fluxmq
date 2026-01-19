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
		ClaimAttempts:     atomic.LoadUint64(&m.ClaimAttempts),
		ClaimSuccesses:    atomic.LoadUint64(&m.ClaimSuccesses),
		ClaimFailures:     atomic.LoadUint64(&m.ClaimFailures),
		StealAttempts:     atomic.LoadUint64(&m.StealAttempts),
		StealSuccesses:    atomic.LoadUint64(&m.StealSuccesses),
		StealFailures:     atomic.LoadUint64(&m.StealFailures),
		AckCount:          atomic.LoadUint64(&m.AckCount),
		NackCount:         atomic.LoadUint64(&m.NackCount),
		RejectCount:       atomic.LoadUint64(&m.RejectCount),
		DLQCount:          atomic.LoadUint64(&m.DLQCount),
		TotalClaimLatency: atomic.LoadUint64(&m.TotalClaimLatency),
		TotalStealLatency: atomic.LoadUint64(&m.TotalStealLatency),
		TotalAckLatency:   atomic.LoadUint64(&m.TotalAckLatency),
		PELSize:           atomic.LoadUint64(&m.PELSize),
		PELHighWater:      atomic.LoadUint64(&m.PELHighWater),
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
	atomic.StoreUint64(&m.TotalClaimLatency, 0)
	atomic.StoreUint64(&m.TotalStealLatency, 0)
	atomic.StoreUint64(&m.TotalAckLatency, 0)
	// Don't reset PELSize or PELHighWater
}
