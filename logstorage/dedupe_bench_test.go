// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package logstorage

import (
	"context"
	"strconv"
	"testing"

	"github.com/absmach/fluxmq/queue/types"
)

func setupDedupeRecoveryBenchmark(b *testing.B, records int) (context.Context, *Adapter) {
	b.Helper()
	ctx := context.Background()
	dir := b.TempDir()
	adapter, err := NewAdapter(dir, DefaultAdapterConfig())
	if err != nil {
		b.Fatalf("open: %v", err)
	}
	if err := adapter.CreateQueue(ctx, types.DefaultQueueConfig(testDedupeQueue, testDedupeQueue+"/#")); err != nil {
		b.Fatalf("create queue: %v", err)
	}
	if _, _, err := adapter.AppendOnce(ctx, testDedupeQueue, "retained", dedupeEnvelope("payload")); err != nil {
		b.Fatalf("append retained identity: %v", err)
	}
	for i := 1; i < records; i++ {
		if _, err := adapter.Append(ctx, testDedupeQueue, dedupeEnvelope(strconv.Itoa(i))); err != nil {
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
	b.Cleanup(func() { _ = reopened.Close() })
	return ctx, reopened
}

// BenchmarkDedupeLookupAfterRestart measures recovery of a retained transfer
// identity. The durable index should make this independent of queue depth: the
// timed path opens one index transaction and validates one raw log record.
func benchmarkDedupeLookupAfterRestart(b *testing.B, records int) {
	ctx, reopened := setupDedupeRecoveryBenchmark(b, records)

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if _, duplicated, err := reopened.AppendOnce(ctx, testDedupeQueue, "retained", dedupeEnvelope("retry")); err != nil {
			b.Fatalf("lookup after restart: %v", err)
		} else if !duplicated {
			b.Fatal("retained identity was not deduplicated")
		}
	}
}

// BenchmarkDedupeAppendAfterRestart measures the first-transfer shape from the
// former rebuild benchmark. Each operation reserves a new identity, appends it,
// and confirms the durable index. Queue depth must no longer make that cost
// linear, though the two metadata transactions remain visible here.
func benchmarkDedupeAppendAfterRestart(b *testing.B, records int) {
	ctx, reopened := setupDedupeRecoveryBenchmark(b, records)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; b.Loop(); i++ {
		if _, duplicated, err := reopened.AppendOnce(ctx, testDedupeQueue, "new-"+strconv.Itoa(i), dedupeEnvelope("new")); err != nil {
			b.Fatalf("append after restart: %v", err)
		} else if duplicated {
			b.Fatal("new identity was unexpectedly deduplicated")
		}
	}
}

func BenchmarkDedupeLookupAfterRestart1k(b *testing.B) {
	benchmarkDedupeLookupAfterRestart(b, 1000)
}

func BenchmarkDedupeLookupAfterRestart10k(b *testing.B) {
	benchmarkDedupeLookupAfterRestart(b, 10000)
}

func BenchmarkDedupeAppendAfterRestart1k(b *testing.B) {
	benchmarkDedupeAppendAfterRestart(b, 1000)
}

func BenchmarkDedupeAppendAfterRestart10k(b *testing.B) {
	benchmarkDedupeAppendAfterRestart(b, 10000)
}
