// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package queue

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/absmach/fluxmq/queue/delivery"
	"github.com/absmach/fluxmq/queue/storage/memory"
	"github.com/absmach/fluxmq/queue/types"
)

// BenchmarkE2E_PublishToAck measures full message lifecycle.
func BenchmarkE2E_PublishToAck(b *testing.B) {
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
	queueName := "$queue/bench-e2e"
	config := types.DefaultQueueConfig(queueName)

	if err := mgr.CreateQueue(ctx, config); err != nil {
		b.Fatal(err)
	}

	// Subscribe consumer
	if err := mgr.Subscribe(ctx, queueName, "client-1", "group-1", ""); err != nil {
		b.Fatal(err)
	}

	q, err := mgr.GetQueue(queueName)
	if err != nil {
		b.Fatal(err)
	}

	worker := delivery.NewWorker(q, store, broker.DeliverToSession, nil, "local", delivery.ProxyMode, nil, nil)

	payload := []byte("e2e benchmark message")
	props := map[string]string{}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		// 1. Enqueue
		if err := mgr.Enqueue(ctx, queueName, payload, props); err != nil {
			b.Fatal(err)
		}

		// 2. Deliver
		worker.DeliverMessages(ctx)

		// 3. Ack
		inflight, err := store.GetInflight(ctx, queueName)
		if err != nil {
			b.Fatal(err)
		}

		for _, msg := range inflight {
			if err := mgr.Ack(ctx, queueName, msg.MessageID); err != nil {
				b.Fatal(err)
			}
		}
	}
}

// BenchmarkE2E_ConcurrentProducerConsumer measures concurrent pub/sub.
func BenchmarkE2E_ConcurrentProducerConsumer(b *testing.B) {
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
	queueName := "$queue/bench-concurrent"
	config := types.DefaultQueueConfig(queueName)
	config.Partitions = 10

	if err := mgr.CreateQueue(ctx, config); err != nil {
		b.Fatal(err)
	}

	// Subscribe multiple consumers
	for i := 0; i < 5; i++ {
		clientID := fmt.Sprintf("client-%d", i)
		if err := mgr.Subscribe(ctx, queueName, clientID, "group-1", ""); err != nil {
			b.Fatal(err)
		}
	}

	q, err := mgr.GetQueue(queueName)
	if err != nil {
		b.Fatal(err)
	}

	worker := delivery.NewWorker(q, store, broker.DeliverToSession, nil, "local", delivery.ProxyMode, nil, nil)

	payload := []byte("concurrent message")
	props := map[string]string{}

	var wg sync.WaitGroup
	var produced, consumed atomic.Int64

	b.ResetTimer()
	b.ReportAllocs()

	// Producer goroutines
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < b.N; i++ {
			if err := mgr.Enqueue(ctx, queueName, payload, props); err != nil {
				b.Error(err)
				return
			}
			produced.Add(1)
		}
	}()

	// Consumer goroutine
	wg.Add(1)
	go func() {
		defer wg.Done()
		for consumed.Load() < int64(b.N) {
			worker.DeliverMessages(ctx)

			inflight, err := store.GetInflight(ctx, queueName)
			if err != nil {
				b.Error(err)
				return
			}

			for _, msg := range inflight {
				if err := mgr.Ack(ctx, queueName, msg.MessageID); err != nil {
					b.Error(err)
					return
				}
				consumed.Add(1)
			}

			time.Sleep(1 * time.Millisecond)
		}
	}()

	wg.Wait()
}

// BenchmarkE2E_Latency measures message latency distribution.
func BenchmarkE2E_Latency(b *testing.B) {
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
	queueName := "$queue/bench-latency"
	config := types.DefaultQueueConfig(queueName)

	if err := mgr.CreateQueue(ctx, config); err != nil {
		b.Fatal(err)
	}

	if err := mgr.Subscribe(ctx, queueName, "client-1", "group-1", ""); err != nil {
		b.Fatal(err)
	}

	q, err := mgr.GetQueue(queueName)
	if err != nil {
		b.Fatal(err)
	}

	worker := delivery.NewWorker(q, store, broker.DeliverToSession, nil, "local", delivery.ProxyMode, nil, nil)

	payload := []byte("latency test message")
	props := map[string]string{}

	latencies := make([]time.Duration, b.N)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		start := time.Now()

		// Enqueue
		if err := mgr.Enqueue(ctx, queueName, payload, props); err != nil {
			b.Fatal(err)
		}

		// Deliver
		worker.DeliverMessages(ctx)

		// Ack
		inflight, err := store.GetInflight(ctx, queueName)
		if err != nil {
			b.Fatal(err)
		}

		for _, msg := range inflight {
			if err := mgr.Ack(ctx, queueName, msg.MessageID); err != nil {
				b.Fatal(err)
			}
		}

		latencies[i] = time.Since(start)
	}

	b.StopTimer()

	// Calculate percentiles
	if len(latencies) > 0 {
		p50 := calculatePercentile(latencies, 0.50)
		p95 := calculatePercentile(latencies, 0.95)
		p99 := calculatePercentile(latencies, 0.99)

		b.ReportMetric(float64(p50.Microseconds()), "p50_µs")
		b.ReportMetric(float64(p95.Microseconds()), "p95_µs")
		b.ReportMetric(float64(p99.Microseconds()), "p99_µs")
	}
}

// BenchmarkE2E_Sustained measures sustained throughput over time.
func BenchmarkE2E_Sustained(b *testing.B) {
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
	queueName := "$queue/bench-sustained"
	config := types.DefaultQueueConfig(queueName)
	config.Partitions = 10

	if err := mgr.CreateQueue(ctx, config); err != nil {
		b.Fatal(err)
	}

	// Subscribe 5 consumers
	for i := 0; i < 5; i++ {
		clientID := fmt.Sprintf("client-%d", i)
		if err := mgr.Subscribe(ctx, queueName, clientID, "group-1", ""); err != nil {
			b.Fatal(err)
		}
	}

	q, err := mgr.GetQueue(queueName)
	if err != nil {
		b.Fatal(err)
	}

	worker := delivery.NewWorker(q, store, broker.DeliverToSession, nil, "local", delivery.ProxyMode, nil, nil)

	payload := []byte("sustained throughput message")
	props := map[string]string{}

	var wg sync.WaitGroup
	var produced, consumed atomic.Int64

	b.ResetTimer()

	// Producer
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < b.N; i++ {
			if err := mgr.Enqueue(ctx, queueName, payload, props); err != nil {
				b.Error(err)
				return
			}
			produced.Add(1)
		}
	}()

	// Consumer
	wg.Add(1)
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(10 * time.Millisecond)
		defer ticker.Stop()

		for consumed.Load() < int64(b.N) {
			select {
			case <-ticker.C:
				worker.DeliverMessages(ctx)

				inflight, err := store.GetInflight(ctx, queueName)
				if err != nil {
					b.Error(err)
					return
				}

				for _, msg := range inflight {
					if err := mgr.Ack(ctx, queueName, msg.MessageID); err != nil {
						b.Error(err)
						return
					}
					consumed.Add(1)
				}
			}
		}
	}()

	wg.Wait()

	elapsed := b.Elapsed()
	throughput := float64(consumed.Load()) / elapsed.Seconds()

	b.ReportMetric(throughput, "msgs/sec")
}

// BenchmarkAllocs_Enqueue measures allocations during enqueue.
func BenchmarkAllocs_Enqueue(b *testing.B) {
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
	queueName := "$queue/bench-allocs-enq"
	config := types.DefaultQueueConfig(queueName)

	if err := mgr.CreateQueue(ctx, config); err != nil {
		b.Fatal(err)
	}

	payload := []byte("allocation test")
	props := map[string]string{}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		if err := mgr.Enqueue(ctx, queueName, payload, props); err != nil {
			b.Fatal(err)
		}
	}
}

// Helper function to calculate percentiles.
func calculatePercentile(latencies []time.Duration, percentile float64) time.Duration {
	if len(latencies) == 0 {
		return 0
	}

	// Simple percentile calculation (not sorting for performance)
	index := int(float64(len(latencies)) * percentile)
	if index >= len(latencies) {
		index = len(latencies) - 1
	}

	return latencies[index]
}
