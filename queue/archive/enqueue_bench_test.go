//go:build ignore

// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package archive

import (
	"context"
	"fmt"
	"testing"

	"github.com/absmach/fluxmq/queue/storage/memory"
	"github.com/absmach/fluxmq/queue/types"
)

// BenchmarkEnqueue_SinglePartition measures enqueue performance with one partition.
func BenchmarkEnqueue_SinglePartition(b *testing.B) {
	store := memory.New()
	broker := NewMockBroker()
	cfg := Config{
		QueueStore:    store,
		MessageStore:  store,
		ConsumerStore: store,
		DeliverFn:     broker.DeliverToSession,
	}

	mgr, err := NewManager(cfg)
	if err != nil {
		b.Fatal(err)
	}

	ctx := context.Background()
	queueName := "$queue/bench-single"
	config := types.DefaultQueueConfig(queueName)
	config.MaxQueueDepth = 10000000 // Large limit for benchmarks
	config.Partitions = 1
	config.MaxQueueDepth = 10000000 // Large limit for benchmarks

	if err := mgr.CreateQueue(ctx, config); err != nil {
		b.Fatal(err)
	}

	payload := []byte("benchmark message payload")
	props := map[string]string{"key": "value"}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		if err := mgr.Enqueue(ctx, queueName, payload, props); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkEnqueue_MultiplePartitions measures enqueue with multiple partitions.
func BenchmarkEnqueue_MultiplePartitions(b *testing.B) {
	store := memory.New()
	broker := NewMockBroker()
	cfg := Config{
		QueueStore:    store,
		MessageStore:  store,
		ConsumerStore: store,
		DeliverFn:     broker.DeliverToSession,
	}

	mgr, err := NewManager(cfg)
	if err != nil {
		b.Fatal(err)
	}

	ctx := context.Background()
	queueName := "$queue/bench-multi"
	config := types.DefaultQueueConfig(queueName)
	config.MaxQueueDepth = 10000000 // Large limit for benchmarks
	config.Partitions = 10

	if err := mgr.CreateQueue(ctx, config); err != nil {
		b.Fatal(err)
	}

	payload := []byte("benchmark message payload")
	props := map[string]string{"key": "value"}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		if err := mgr.Enqueue(ctx, queueName, payload, props); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkEnqueue_SmallPayload measures enqueue with small messages.
func BenchmarkEnqueue_SmallPayload(b *testing.B) {
	store := memory.New()
	broker := NewMockBroker()
	cfg := Config{
		QueueStore:    store,
		MessageStore:  store,
		ConsumerStore: store,
		DeliverFn:     broker.DeliverToSession,
	}

	mgr, err := NewManager(cfg)
	if err != nil {
		b.Fatal(err)
	}

	ctx := context.Background()
	queueName := "$queue/bench-small"
	config := types.DefaultQueueConfig(queueName)
	config.MaxQueueDepth = 10000000 // Large limit for benchmarks

	if err := mgr.CreateQueue(ctx, config); err != nil {
		b.Fatal(err)
	}

	payload := []byte("tiny")
	props := map[string]string{}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		if err := mgr.Enqueue(ctx, queueName, payload, props); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkEnqueue_LargePayload measures enqueue with large messages.
func BenchmarkEnqueue_LargePayload(b *testing.B) {
	store := memory.New()
	broker := NewMockBroker()
	cfg := Config{
		QueueStore:    store,
		MessageStore:  store,
		ConsumerStore: store,
		DeliverFn:     broker.DeliverToSession,
	}

	mgr, err := NewManager(cfg)
	if err != nil {
		b.Fatal(err)
	}

	ctx := context.Background()
	queueName := "$queue/bench-large"
	config := types.DefaultQueueConfig(queueName)
	config.MaxQueueDepth = 10000000 // Large limit for benchmarks

	if err := mgr.CreateQueue(ctx, config); err != nil {
		b.Fatal(err)
	}

	// 64KB payload
	payload := make([]byte, 64*1024)
	for i := range payload {
		payload[i] = byte(i % 256)
	}
	props := map[string]string{"size": "64KB"}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		if err := mgr.Enqueue(ctx, queueName, payload, props); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkEnqueue_WithPartitionKey measures enqueue with partition key routing.
func BenchmarkEnqueue_WithPartitionKey(b *testing.B) {
	store := memory.New()
	broker := NewMockBroker()
	cfg := Config{
		QueueStore:    store,
		MessageStore:  store,
		ConsumerStore: store,
		DeliverFn:     broker.DeliverToSession,
	}

	mgr, err := NewManager(cfg)
	if err != nil {
		b.Fatal(err)
	}

	ctx := context.Background()
	queueName := "$queue/bench-partkey"
	config := types.DefaultQueueConfig(queueName)
	config.MaxQueueDepth = 10000000 // Large limit for benchmarks
	config.Partitions = 10

	if err := mgr.CreateQueue(ctx, config); err != nil {
		b.Fatal(err)
	}

	payload := []byte("partitioned message")
	props := map[string]string{"partition-key": "user-123"}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		if err := mgr.Enqueue(ctx, queueName, payload, props); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkEnqueue_Parallel measures concurrent enqueue performance.
func BenchmarkEnqueue_Parallel(b *testing.B) {
	store := memory.New()
	broker := NewMockBroker()
	cfg := Config{
		QueueStore:    store,
		MessageStore:  store,
		ConsumerStore: store,
		DeliverFn:     broker.DeliverToSession,
	}

	mgr, err := NewManager(cfg)
	if err != nil {
		b.Fatal(err)
	}

	ctx := context.Background()
	queueName := "$queue/bench-parallel"
	config := types.DefaultQueueConfig(queueName)
	config.MaxQueueDepth = 10000000 // Large limit for benchmarks
	config.Partitions = 10

	if err := mgr.CreateQueue(ctx, config); err != nil {
		b.Fatal(err)
	}

	payload := []byte("concurrent message")
	props := map[string]string{}

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if err := mgr.Enqueue(ctx, queueName, payload, props); err != nil {
				b.Fatal(err)
			}
		}
	})
}

// BenchmarkEnqueue_BatchSize measures different batch sizes.
func BenchmarkEnqueue_BatchSize(b *testing.B) {
	batchSizes := []int{1, 10, 100, 1000}

	for _, size := range batchSizes {
		b.Run(fmt.Sprintf("batch_%d", size), func(b *testing.B) {
			store := memory.New()
			broker := NewMockBroker()
			cfg := Config{
				QueueStore:    store,
				MessageStore:  store,
				ConsumerStore: store,
				DeliverFn:     broker.DeliverToSession,
			}

			mgr, err := NewManager(cfg)
			if err != nil {
				b.Fatal(err)
			}

			ctx := context.Background()
			queueName := fmt.Sprintf("$queue/bench-batch-%d", size)
			config := types.DefaultQueueConfig(queueName)
			config.MaxQueueDepth = 10000000 // Large limit for benchmarks

			if err := mgr.CreateQueue(ctx, config); err != nil {
				b.Fatal(err)
			}

			payload := []byte("batch message")
			props := map[string]string{}

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				for j := 0; j < size; j++ {
					if err := mgr.Enqueue(ctx, queueName, payload, props); err != nil {
						b.Fatal(err)
					}
				}
			}
		})
	}
}
