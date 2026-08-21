// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package queue

import "strings"

// AckDurability decides what an acknowledged publish to a durable queue
// guarantees once the broker has said yes.
type AckDurability string

const (
	// AckDurabilityFsync syncs the append before the publisher is
	// acknowledged, so an acknowledged message survives a process or host
	// crash. It is not the default, and the reason is measured rather than
	// assumed: appendWithBarrier holds the segment manager's lock across the
	// sync, so concurrent publishers to one queue serialize into one fsync
	// each. That caps a durable queue at the reciprocal of the device's fsync
	// latency — roughly 200 messages a second on consumer NVMe, against
	// ~130,000 buffered, and concurrency does not improve it. Set it on the
	// queues whose records must not be lost; see BenchmarkAckDurability*.
	AckDurabilityFsync AckDurability = "fsync"

	// AckDurabilityBuffered acknowledges from the page cache and lets the
	// store's background sync catch up. An acknowledged message can be lost up
	// to storage.queue_sync_interval before a crash. This is the default,
	// because it is what the broker has always done; making fsync the default
	// before the log has group commit would cost every deployment ~640x
	// throughput on upgrade.
	AckDurabilityBuffered AckDurability = "buffered"
)

// NormalizeAckDurability canonicalizes a configured policy. Unset means
// buffered, which is what the broker did before the policy existed. Validation
// rejects unknown values at load, so an unrecognized one here means a caller
// built a Config by hand.
func NormalizeAckDurability(policy AckDurability) AckDurability {
	switch AckDurability(strings.ToLower(strings.TrimSpace(string(policy)))) {
	case AckDurabilityFsync:
		return AckDurabilityFsync
	default:
		return AckDurabilityBuffered
	}
}
