// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

// Stress tests for MQTT broker message routing and zero-copy implementation.
// These tests run longer and with higher load than benchmarks to identify
// memory leaks, race conditions, and performance degradation under sustained load.
//
// Run with: go test -v -run=Stress -timeout=30m ./broker

package broker

import (
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/absmach/fluxmq/core"
	"github.com/absmach/fluxmq/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestStress_HighThroughputPublish tests sustained high-throughput publishing.
func TestStress_HighThroughputPublish(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping stress test in short mode")
	}

	broker := createBenchBroker(t)
	defer broker.Close()

	// Create 100 subscribers
	const numSubscribers = 100
	for i := 0; i < numSubscribers; i++ {
		sub := createBenchSession(t, broker, fmt.Sprintf("sub-%d", i))
		broker.subscribe(sub, "stress/test", 0, storage.SubscribeOptions{})
	}

	payload := make([]byte, 1024)
	for i := range payload {
		payload[i] = byte(i % 256)
	}

	const duration = 10 * time.Second
	const targetRate = 10000 // messages per second
	ticker := time.NewTicker(time.Second / time.Duration(targetRate))
	defer ticker.Stop()

	startTime := time.Now()
	endTime := startTime.Add(duration)
	var messageCount atomic.Uint64

	// Track initial memory
	var m1 runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&m1)

	t.Logf("Starting high-throughput test: %d msg/s for %v to %d subscribers",
		targetRate, duration, numSubscribers)

	for time.Now().Before(endTime) {
		<-ticker.C
		msg := &storage.Message{
			Topic: "stress/test",
			QoS:   0,
		}
		msg.SetPayloadFromBytes(payload)
		broker.Publish(msg)
		messageCount.Add(1)
	}

	elapsed := time.Since(startTime)
	totalMessages := messageCount.Load()
	actualRate := float64(totalMessages) / elapsed.Seconds()

	// Check final memory
	var m2 runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&m2)

	memIncrease := m2.Alloc - m1.Alloc
	memIncreasePerMsg := float64(memIncrease) / float64(totalMessages)

	t.Logf("Completed: %d messages in %v (%.0f msg/s)", totalMessages, elapsed, actualRate)
	t.Logf("Memory: %d KB increase, %.2f bytes/msg", memIncrease/1024, memIncreasePerMsg)
	t.Logf("HeapAlloc: %d MB -> %d MB", m1.Alloc/1024/1024, m2.Alloc/1024/1024)

	// Verify reasonable memory usage (should not leak)
	assert.Less(t, memIncreasePerMsg, 1000.0, "Memory usage per message should be minimal")
}

// TestStress_ConcurrentPublishers tests multiple concurrent publishers.
func TestStress_ConcurrentPublishers(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping stress test in short mode")
	}

	broker := createBenchBroker(t)
	defer broker.Close()

	// Create 50 subscribers
	const numSubscribers = 50
	for i := 0; i < numSubscribers; i++ {
		sub := createBenchSession(t, broker, fmt.Sprintf("sub-%d", i))
		broker.subscribe(sub, "stress/concurrent", 0, storage.SubscribeOptions{})
	}

	const numPublishers = 10
	const messagesPerPublisher = 10000
	const messageSize = 512

	var wg sync.WaitGroup
	var totalMessages atomic.Uint64
	var errors atomic.Uint64

	startTime := time.Now()

	t.Logf("Starting concurrent publishers: %d publishers, %d messages each",
		numPublishers, messagesPerPublisher)

	for i := 0; i < numPublishers; i++ {
		wg.Add(1)
		go func(publisherID int) {
			defer wg.Done()

			payload := make([]byte, messageSize)
			for j := range payload {
				payload[j] = byte(publisherID + j%256)
			}

			for j := 0; j < messagesPerPublisher; j++ {
				msg := &storage.Message{
					Topic: "stress/concurrent",
					QoS:   0,
				}
				msg.SetPayloadFromBytes(payload)

				if err := broker.Publish(msg); err != nil {
					errors.Add(1)
				} else {
					totalMessages.Add(1)
				}
			}
		}(i)
	}

	wg.Wait()
	elapsed := time.Since(startTime)

	rate := float64(totalMessages.Load()) / elapsed.Seconds()

	t.Logf("Completed: %d messages in %v (%.0f msg/s)",
		totalMessages.Load(), elapsed, rate)
	t.Logf("Errors: %d", errors.Load())

	assert.Equal(t, uint64(numPublishers*messagesPerPublisher), totalMessages.Load())
	assert.Equal(t, uint64(0), errors.Load())
}

// TestStress_MemoryPressure tests behavior under memory pressure with large messages.
func TestStress_MemoryPressure(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping stress test in short mode")
	}

	broker := createBenchBroker(t)
	defer broker.Close()

	// Create 20 subscribers
	const numSubscribers = 20
	for i := 0; i < numSubscribers; i++ {
		sub := createBenchSession(t, broker, fmt.Sprintf("sub-%d", i))
		broker.subscribe(sub, "stress/memory", 0, storage.SubscribeOptions{})
	}

	// Test with various large message sizes
	sizes := []int{
		64 * 1024,   // 64 KB
		256 * 1024,  // 256 KB
		512 * 1024,  // 512 KB
		1024 * 1024, // 1 MB
	}

	var m1, m2 runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&m1)

	t.Logf("Starting memory pressure test with %d subscribers", numSubscribers)

	for _, size := range sizes {
		payload := make([]byte, size)
		for i := range payload {
			payload[i] = byte(i % 256)
		}

		// Publish 100 messages of this size
		for i := 0; i < 100; i++ {
			msg := &storage.Message{
				Topic: "stress/memory",
				QoS:   0,
			}
			msg.SetPayloadFromBytes(payload)
			require.NoError(t, broker.Publish(msg))
		}

		runtime.GC()
		t.Logf("Published 100 messages of %d KB", size/1024)
	}

	runtime.GC()
	runtime.ReadMemStats(&m2)

	memIncrease := int64(m2.Alloc) - int64(m1.Alloc)
	t.Logf("Memory after test: %d MB (increase: %d KB)",
		m2.Alloc/1024/1024, memIncrease/1024)

	// Memory should not increase excessively (allowing for some GC lag)
	// With zero-copy, memory increase should be minimal
	assert.Less(t, memIncrease, int64(100*1024*1024), "Memory increase should be reasonable")
}

// TestStress_SustainedLoad tests broker under sustained mixed load.
func TestStress_SustainedLoad(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping stress test in short mode")
	}

	broker := createBenchBroker(t)
	defer broker.Close()

	// Create subscribers on different topics
	topics := []string{"sensor/temp", "sensor/humidity", "alerts", "metrics"}
	for _, topic := range topics {
		for i := 0; i < 25; i++ {
			sub := createBenchSession(t, broker, fmt.Sprintf("sub-%s-%d", topic, i))
			broker.subscribe(sub, topic, 0, storage.SubscribeOptions{})
		}
	}

	const duration = 30 * time.Second
	var totalMessages atomic.Uint64
	var errors atomic.Uint64

	startTime := time.Now()
	endTime := startTime.Add(duration)

	t.Logf("Starting sustained load test for %v", duration)

	var wg sync.WaitGroup

	// Publisher goroutine for each topic
	for _, topic := range topics {
		wg.Add(1)
		go func(topic string) {
			defer wg.Done()

			sizes := []int{100, 256, 512, 1024, 2048}
			sizeIdx := 0

			ticker := time.NewTicker(time.Millisecond)
			defer ticker.Stop()

			for time.Now().Before(endTime) {
				<-ticker.C

				payload := make([]byte, sizes[sizeIdx%len(sizes)])
				for i := range payload {
					payload[i] = byte(i % 256)
				}

				msg := &storage.Message{
					Topic: topic,
					QoS:   0,
				}
				msg.SetPayloadFromBytes(payload)

				if err := broker.Publish(msg); err != nil {
					errors.Add(1)
				} else {
					totalMessages.Add(1)
				}

				sizeIdx++
			}
		}(topic)
	}

	// Monitor goroutine
	wg.Add(1)
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()

		for time.Now().Before(endTime) {
			<-ticker.C
			var m runtime.MemStats
			runtime.ReadMemStats(&m)
			rate := float64(totalMessages.Load()) / time.Since(startTime).Seconds()
			t.Logf("Progress: %d messages (%.0f msg/s), Mem: %d MB, Goroutines: %d",
				totalMessages.Load(), rate, m.Alloc/1024/1024, runtime.NumGoroutine())
		}
	}()

	wg.Wait()

	elapsed := time.Since(startTime)
	rate := float64(totalMessages.Load()) / elapsed.Seconds()

	t.Logf("Sustained load completed: %d messages in %v (%.0f msg/s)",
		totalMessages.Load(), elapsed, rate)
	t.Logf("Errors: %d", errors.Load())

	assert.Greater(t, totalMessages.Load(), uint64(0))
	assert.Less(t, errors.Load(), totalMessages.Load()/1000) // Less than 0.1% errors
}

// TestStress_BufferPoolExhaustion tests buffer pool behavior under extreme load.
func TestStress_BufferPoolExhaustion(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping stress test in short mode")
	}

	// Create a pool with limited capacity
	pool := core.NewBufferPoolWithCapacity(10, 10, 10)

	const numGoroutines = 100
	const operationsPerGoroutine = 10000

	var wg sync.WaitGroup
	startTime := time.Now()

	t.Logf("Testing buffer pool with %d concurrent goroutines", numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			for j := 0; j < operationsPerGoroutine; j++ {
				// Mix of sizes
				size := 512 + (id*j)%1024
				buf := pool.Get(size)

				// Simulate some work
				data := buf.Bytes()
				data[0] = byte(id)
				data[len(data)-1] = byte(j)

				// Sometimes retain and release multiple times
				if j%10 == 0 {
					buf.Retain()
					buf.Release()
				}

				buf.Release()
			}
		}(i)
	}

	wg.Wait()
	elapsed := time.Since(startTime)

	stats := pool.Stats()
	totalOps := numGoroutines * operationsPerGoroutine
	opsPerSec := float64(totalOps) / elapsed.Seconds()

	t.Logf("Completed %d operations in %v (%.0f ops/s)", totalOps, elapsed, opsPerSec)
	t.Logf("Pool stats - Hits: %d, Misses: %d",
		stats.SmallHits+stats.MediumHits+stats.LargeHits,
		stats.SmallMisses+stats.MediumMisses+stats.LargeMisses)

	// Verify we got some hits (pool is working)
	totalHits := stats.SmallHits + stats.MediumHits + stats.LargeHits
	assert.Greater(t, totalHits, uint64(0), "Pool should have some hits")
}

// TestStress_FanOutExtreme tests extreme fanout (1 publisher to many subscribers).
func TestStress_FanOutExtreme(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping stress test in short mode")
	}

	broker := createBenchBroker(t)
	defer broker.Close()

	// Create 5000 subscribers (extreme fanout)
	const numSubscribers = 5000
	t.Logf("Creating %d subscribers...", numSubscribers)

	for i := 0; i < numSubscribers; i++ {
		sub := createBenchSession(t, broker, fmt.Sprintf("sub-%d", i))
		broker.subscribe(sub, "fanout/extreme", 0, storage.SubscribeOptions{})

		if (i+1)%1000 == 0 {
			t.Logf("Created %d subscribers", i+1)
		}
	}

	payload := make([]byte, 256)
	for i := range payload {
		payload[i] = byte(i % 256)
	}

	const numMessages = 1000
	startTime := time.Now()

	var m1, m2 runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&m1)

	t.Logf("Publishing %d messages to %d subscribers...", numMessages, numSubscribers)

	for i := 0; i < numMessages; i++ {
		msg := &storage.Message{
			Topic: "fanout/extreme",
			QoS:   0,
		}
		msg.SetPayloadFromBytes(payload)
		require.NoError(t, broker.Publish(msg))

		if (i+1)%100 == 0 {
			t.Logf("Published %d messages", i+1)
		}
	}

	elapsed := time.Since(startTime)
	runtime.GC()
	runtime.ReadMemStats(&m2)

	totalDeliveries := uint64(numMessages * numSubscribers)
	deliveriesPerSec := float64(totalDeliveries) / elapsed.Seconds()
	memIncrease := m2.Alloc - m1.Alloc

	t.Logf("Completed: %d messages × %d subscribers = %d deliveries",
		numMessages, numSubscribers, totalDeliveries)
	t.Logf("Time: %v (%.0f deliveries/s)", elapsed, deliveriesPerSec)
	t.Logf("Memory increase: %d MB", memIncrease/1024/1024)

	// With zero-copy, memory should not increase proportionally to number of subscribers
	bytesPerDelivery := float64(memIncrease) / float64(totalDeliveries)
	t.Logf("Memory per delivery: %.2f bytes", bytesPerDelivery)

	// Should be well under the message size since we're sharing buffers
	assert.Less(t, bytesPerDelivery, 100.0, "Zero-copy should keep per-delivery memory low")
}

// TestStress_RapidSubscribeUnsubscribe tests stability with subscription churn.
func TestStress_RapidSubscribeUnsubscribe(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping stress test in short mode")
	}

	broker := createBenchBroker(t)
	defer broker.Close()

	const duration = 20 * time.Second
	var subCount atomic.Uint64
	var unsubCount atomic.Uint64
	var pubCount atomic.Uint64

	startTime := time.Now()
	endTime := startTime.Add(duration)

	var wg sync.WaitGroup

	t.Logf("Starting subscription churn test for %v", duration)

	// Subscriber churn goroutines
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			for time.Now().Before(endTime) {
				clientID := fmt.Sprintf("churn-%d-%d", workerID, time.Now().UnixNano())
				sub := createBenchSession(t, broker, clientID)

				topic := fmt.Sprintf("churn/topic/%d", workerID%5)
				broker.subscribe(sub, topic, 0, storage.SubscribeOptions{})
				subCount.Add(1)

				// Keep subscription alive briefly
				time.Sleep(time.Millisecond * 100)

				broker.Unsubscribe(clientID, topic)
				unsubCount.Add(1)

				broker.DestroySession(clientID)

				time.Sleep(time.Millisecond * 10)
			}
		}(i)
	}

	// Publisher goroutine
	wg.Add(1)
	go func() {
		defer wg.Done()

		payload := make([]byte, 128)
		ticker := time.NewTicker(time.Millisecond * 10)
		defer ticker.Stop()

		for time.Now().Before(endTime) {
			<-ticker.C

			topic := fmt.Sprintf("churn/topic/%d", int(pubCount.Load())%5)
			msg := &storage.Message{
				Topic: topic,
				QoS:   0,
			}
			msg.SetPayloadFromBytes(payload)
			broker.Publish(msg)
			pubCount.Add(1)
		}
	}()

	wg.Wait()

	t.Logf("Churn test completed:")
	t.Logf("  Subscriptions: %d", subCount.Load())
	t.Logf("  Unsubscriptions: %d", unsubCount.Load())
	t.Logf("  Messages published: %d", pubCount.Load())

	assert.Greater(t, subCount.Load(), uint64(0))
	assert.Equal(t, subCount.Load(), unsubCount.Load())
}
