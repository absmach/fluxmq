// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package logstorage

import (
	"context"
	"strconv"
	"testing"

	"github.com/absmach/fluxmq/queue/types"
)

// BenchmarkDedupeRebuild measures what the first dead-letter transfer after a
// restart pays: the deduplication index is derived state, so it is rebuilt by
// reading the queue from its head.
//
// Measured here at ~1.85us and ~13 allocations per retained record — about
// 19ms and 12MB for a queue holding 10,000 records. It is paid once per queue
// per restart, on the sweeper rather than on the claim path, so it delays
// dead-lettering rather than delivery.
//
// A persistent per-segment transfer index would remove the scan. It is not
// worth it at these numbers: it is a durable on-disk artifact, so it brings its
// own write path, crash consistency, truncation interaction and corruption
// handling — a lot of new failure surface to save something that happens once
// per restart and off the delivery path. Decoding only the transfer identity
// instead of whole envelopes was tried and measured: 1.4% fewer bytes, so the
// payload copy is not what this costs, and the scan itself is.
//
// Revisit if a deployment retains enough dead-lettered records for the scan to
// matter; the numbers are linear, so they extrapolate.
func benchmarkDedupeRebuild(b *testing.B, records int) {
	ctx := context.Background()

	for b.Loop() {
		b.StopTimer()
		dir := b.TempDir()
		adapter, err := NewAdapter(dir, DefaultAdapterConfig())
		if err != nil {
			b.Fatalf("open: %v", err)
		}
		if err := adapter.CreateQueue(ctx, types.DefaultQueueConfig(testDedupeQueue, testDedupeQueue+"/#")); err != nil {
			b.Fatalf("create queue: %v", err)
		}
		for i := range records {
			if _, _, err := adapter.AppendOnce(ctx, testDedupeQueue, "key-"+strconv.Itoa(i), dedupeEnvelope("payload")); err != nil {
				b.Fatalf("append: %v", err)
			}
		}
		if err := adapter.Close(); err != nil {
			b.Fatalf("close: %v", err)
		}
		reopened, err := NewAdapter(dir, DefaultAdapterConfig())
		if err != nil {
			b.Fatalf("reopen: %v", err)
		}
		b.StartTimer()

		// The first transfer after a restart is what pays the rebuild.
		if _, _, err := reopened.AppendOnce(ctx, testDedupeQueue, "after-restart", dedupeEnvelope("payload")); err != nil {
			b.Fatalf("append after restart: %v", err)
		}

		b.StopTimer()
		if err := reopened.Close(); err != nil {
			b.Fatalf("close reopened: %v", err)
		}
		b.StartTimer()
	}
}

func BenchmarkDedupeRebuild1k(b *testing.B)  { benchmarkDedupeRebuild(b, 1000) }
func BenchmarkDedupeRebuild10k(b *testing.B) { benchmarkDedupeRebuild(b, 10000) }
