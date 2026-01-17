// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package queue

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/absmach/fluxmq/queue/delivery"
	"github.com/absmach/fluxmq/queue/storage/memory"
	"github.com/absmach/fluxmq/queue/types"
)

// setupQueueWithMessages creates a queue and enqueues N messages.
func setupQueueWithMessages(b *testing.B, queueName string, partitions, messageCount int) (*Manager, *memory.Store, *MockBroker) {
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
	config := types.DefaultQueueConfig(queueName)
	config.Partitions = partitions

	if err := mgr.CreateQueue(ctx, config); err != nil {
		b.Fatal(err)
	}

	// Pre-populate with messages
	payload := []byte("benchmark message")
	props := map[string]string{}

	for i := 0; i < messageCount; i++ {
		if err := mgr.Enqueue(ctx, queueName, payload, props); err != nil {
			b.Fatal(err)
		}
	}

	return mgr, store, broker
}

// BenchmarkDequeue_SingleConsumer measures dequeue with one consumer.
func BenchmarkDequeue_SingleConsumer(b *testing.B) {
	queueName := "$queue/bench-dequeue-single"
	mgr, store, broker := setupQueueWithMessages(b, queueName, 1, b.N)

	ctx := context.Background()

	// Subscribe consumer
	if err := mgr.Subscribe(ctx, queueName, "client-1", "group-1", ""); err != nil {
		b.Fatal(err)
	}

	// Get queue and start delivery worker
	q, err := mgr.GetQueue(queueName)
	if err != nil {
		b.Fatal(err)
	}

	worker := delivery.NewWorker(q, store, broker.DeliverToSession, nil, "local", delivery.ProxyMode, nil, nil)

	b.ResetTimer()
	b.ReportAllocs()

	// Deliver all messages
	for i := 0; i < b.N; i++ {
		worker.DeliverMessages(ctx)
	}
}

// BenchmarkDequeue_MultipleConsumers measures dequeue with multiple consumers.
func BenchmarkDequeue_MultipleConsumers(b *testing.B) {
	consumerCounts := []int{1, 2, 5, 10}

	for _, count := range consumerCounts {
		b.Run(fmt.Sprintf("consumers_%d", count), func(b *testing.B) {
			queueName := fmt.Sprintf("$queue/bench-dequeue-multi-%d", count)
			mgr, store, broker := setupQueueWithMessages(b, queueName, count, b.N)

			ctx := context.Background()

			// Subscribe multiple consumers
			for i := 0; i < count; i++ {
				clientID := fmt.Sprintf("client-%d", i)
				if err := mgr.Subscribe(ctx, queueName, clientID, "group-1", ""); err != nil {
					b.Fatal(err)
				}
			}

			// Get queue and start delivery worker
			q, err := mgr.GetQueue(queueName)
			if err != nil {
				b.Fatal(err)
			}

			worker := delivery.NewWorker(q, store, broker.DeliverToSession, nil, "local", delivery.ProxyMode, nil, nil)

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				worker.DeliverMessages(ctx)
			}
		})
	}
}

// BenchmarkDequeue_WithAck measures dequeue + ack cycle.
func BenchmarkDequeue_WithAck(b *testing.B) {
	queueName := "$queue/bench-dequeue-ack"
	mgr, store, broker := setupQueueWithMessages(b, queueName, 1, b.N)

	ctx := context.Background()

	// Subscribe consumer
	if err := mgr.Subscribe(ctx, queueName, "client-1", "group-1", ""); err != nil {
		b.Fatal(err)
	}

	// Get queue
	q, err := mgr.GetQueue(queueName)
	if err != nil {
		b.Fatal(err)
	}

	worker := delivery.NewWorker(q, store, broker.DeliverToSession, nil, "local", delivery.ProxyMode, nil, nil)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		// Deliver message
		worker.DeliverMessages(ctx)

		// Get inflight messages
		inflight, err := store.GetInflight(ctx, queueName)
		if err != nil {
			b.Fatal(err)
		}

		// Ack all inflight
		for _, msg := range inflight {
			if err := mgr.Ack(ctx, queueName, msg.MessageID); err != nil {
				b.Fatal(err)
			}
		}
	}
}

// BenchmarkDequeue_PartitionScanning measures partition scan performance.
func BenchmarkDequeue_PartitionScanning(b *testing.B) {
	partitionCounts := []int{1, 5, 10, 20}

	for _, count := range partitionCounts {
		b.Run(fmt.Sprintf("partitions_%d", count), func(b *testing.B) {
			queueName := fmt.Sprintf("$queue/bench-scan-%d", count)
			mgr, store, broker := setupQueueWithMessages(b, queueName, count, 1000)

			ctx := context.Background()

			if err := mgr.Subscribe(ctx, queueName, "client-1", "group-1", ""); err != nil {
				b.Fatal(err)
			}

			q, err := mgr.GetQueue(queueName)
			if err != nil {
				b.Fatal(err)
			}

			worker := delivery.NewWorker(q, store, broker.DeliverToSession, nil, "local", delivery.ProxyMode, nil, nil)

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				worker.DeliverMessages(ctx)
			}
		})
	}
}

// BenchmarkDequeue_Throughput measures sustained dequeue throughput.
func BenchmarkDequeue_Throughput(b *testing.B) {
	queueName := "$queue/bench-throughput"
	messageCount := 100000
	mgr, store, broker := setupQueueWithMessages(b, queueName, 10, messageCount)

	ctx := context.Background()

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

	b.ResetTimer()
	b.ReportAllocs()

	start := time.Now()
	delivered := 0

	for i := 0; i < b.N; i++ {
		worker.DeliverMessages(ctx)
		delivered++

		if delivered >= messageCount {
			break
		}
	}

	elapsed := time.Since(start)
	throughput := float64(delivered) / elapsed.Seconds()

	b.ReportMetric(throughput, "msgs/sec")
}
