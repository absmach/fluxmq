// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package queue

import (
	"context"
	"io"
	"log/slog"
	"testing"

	logstorage "github.com/absmach/fluxmq/logstorage"
	"github.com/absmach/fluxmq/queue/types"
)

// benchmarkAckDurability measures the durable publish path against the real
// append-only log, because the cost being measured is an fsync and an in-memory
// store has none to pay. Defaulting to fsync trades throughput for a guarantee;
// the size of that trade belongs in the release notes rather than in a bug
// report from whoever upgrades first.
func benchmarkAckDurability(b *testing.B, policy AckDurability) {
	b.Helper()

	adapterCfg := logstorage.DefaultAdapterConfig()
	store, err := logstorage.NewAdapter(b.TempDir(), adapterCfg)
	if err != nil {
		b.Fatalf("NewAdapter failed: %v", err)
	}
	b.Cleanup(func() { store.Close() })

	cfg := DefaultConfig()
	cfg.AckDurability = policy
	mgr := NewManager(store, newMockGroupStore(), nil, cfg, slog.New(slog.NewTextHandler(io.Discard, nil)), nil)

	ctx := context.Background()
	const queueName = "bench-durable"
	if err := mgr.CreateQueue(ctx, types.DefaultQueueConfig(queueName, "$queue/"+queueName)); err != nil {
		b.Fatalf("CreateQueue failed: %v", err)
	}

	publish := types.PublishRequest{
		Topic:   "$queue/" + queueName,
		Payload: make([]byte, 256),
	}

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if err := mgr.Publish(ctx, publish); err != nil {
			b.Fatalf("Publish failed: %v", err)
		}
	}
}

func BenchmarkAckDurabilityFsync(b *testing.B) {
	benchmarkAckDurability(b, AckDurabilityFsync)
}

// BenchmarkAckDurabilityFsyncParallel measures whether concurrency amortizes
// the barrier. It does not: appendWithBarrier holds the segment manager's
// exclusive lock across the fsync, so concurrent publishers to one queue
// serialize, one fsync each. That makes the per-queue ceiling the reciprocal of
// the device's fsync latency no matter how many publishers there are, which is
// the number that decides whether fsync can be the default.
func BenchmarkAckDurabilityFsyncParallel(b *testing.B) {
	adapterCfg := logstorage.DefaultAdapterConfig()
	store, err := logstorage.NewAdapter(b.TempDir(), adapterCfg)
	if err != nil {
		b.Fatalf("NewAdapter failed: %v", err)
	}
	b.Cleanup(func() { store.Close() })

	cfg := DefaultConfig()
	cfg.AckDurability = AckDurabilityFsync
	mgr := NewManager(store, newMockGroupStore(), nil, cfg, slog.New(slog.NewTextHandler(io.Discard, nil)), nil)

	ctx := context.Background()
	const queueName = "bench-durable-parallel"
	if err := mgr.CreateQueue(ctx, types.DefaultQueueConfig(queueName, "$queue/"+queueName)); err != nil {
		b.Fatalf("CreateQueue failed: %v", err)
	}
	publish := types.PublishRequest{Topic: "$queue/" + queueName, Payload: make([]byte, 256)}

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if err := mgr.Publish(ctx, publish); err != nil {
				b.Fatalf("Publish failed: %v", err)
			}
		}
	})
}

func BenchmarkAckDurabilityBuffered(b *testing.B) {
	benchmarkAckDurability(b, AckDurabilityBuffered)
}
