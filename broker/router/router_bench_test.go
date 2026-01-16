// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package router

import (
	"fmt"
	"testing"

	"github.com/absmach/fluxmq/storage"
)

// Benchmark: Match operation (read-heavy, the common case)

func BenchmarkRouter_Match_100Subs(b *testing.B) {
	benchmarkRouterMatch(b, NewRouter(), 100)
}

func BenchmarkLockFreeRouter_Match_100Subs(b *testing.B) {
	benchmarkLockFreeRouterMatch(b, NewLockFreeRouter(), 100)
}

func BenchmarkRouter_Match_1000Subs(b *testing.B) {
	benchmarkRouterMatch(b, NewRouter(), 1000)
}

func BenchmarkLockFreeRouter_Match_1000Subs(b *testing.B) {
	benchmarkLockFreeRouterMatch(b, NewLockFreeRouter(), 1000)
}

func BenchmarkRouter_Match_10000Subs(b *testing.B) {
	benchmarkRouterMatch(b, NewRouter(), 10000)
}

func BenchmarkLockFreeRouter_Match_10000Subs(b *testing.B) {
	benchmarkLockFreeRouterMatch(b, NewLockFreeRouter(), 10000)
}

func benchmarkRouterMatch(b *testing.B, r *TrieRouter, numSubs int) {
	// Setup: add subscriptions
	for i := 0; i < numSubs; i++ {
		topic := fmt.Sprintf("sensor/room%d/temperature", i)
		_ = r.Subscribe(fmt.Sprintf("client%d", i), topic, 1, storage.SubscribeOptions{})
	}

	// Add some wildcard subscriptions
	_ = r.Subscribe("wildcard1", "sensor/+/temperature", 1, storage.SubscribeOptions{})
	_ = r.Subscribe("wildcard2", "sensor/#", 1, storage.SubscribeOptions{})

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			topic := fmt.Sprintf("sensor/room%d/temperature", i%numSubs)
			_, _ = r.Match(topic)
			i++
		}
	})
}

func benchmarkLockFreeRouterMatch(b *testing.B, r *LockFreeRouter, numSubs int) {
	// Setup: add subscriptions
	for i := 0; i < numSubs; i++ {
		topic := fmt.Sprintf("sensor/room%d/temperature", i)
		_ = r.Subscribe(fmt.Sprintf("client%d", i), topic, 1, storage.SubscribeOptions{})
	}

	// Add some wildcard subscriptions
	_ = r.Subscribe("wildcard1", "sensor/+/temperature", 1, storage.SubscribeOptions{})
	_ = r.Subscribe("wildcard2", "sensor/#", 1, storage.SubscribeOptions{})

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			topic := fmt.Sprintf("sensor/room%d/temperature", i%numSubs)
			_, _ = r.Match(topic)
			i++
		}
	})
}

// Benchmark: Subscribe operation (write-heavy)

func BenchmarkRouter_Subscribe(b *testing.B) {
	r := NewRouter()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			clientID := fmt.Sprintf("client%d", i)
			topic := fmt.Sprintf("sensor/room%d/temperature", i%1000)
			_ = r.Subscribe(clientID, topic, 1, storage.SubscribeOptions{})
			i++
		}
	})
}

func BenchmarkLockFreeRouter_Subscribe(b *testing.B) {
	r := NewLockFreeRouter()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			clientID := fmt.Sprintf("client%d", i)
			topic := fmt.Sprintf("sensor/room%d/temperature", i%1000)
			_ = r.Subscribe(clientID, topic, 1, storage.SubscribeOptions{})
			i++
		}
	})
}

// Benchmark: Mixed read/write workload (90% reads, 10% writes)

func BenchmarkRouter_Mixed_90Read_10Write(b *testing.B) {
	r := NewRouter()

	// Pre-populate
	for i := 0; i < 1000; i++ {
		topic := fmt.Sprintf("sensor/room%d/temperature", i)
		_ = r.Subscribe(fmt.Sprintf("client%d", i), topic, 1, storage.SubscribeOptions{})
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			if i%10 == 0 {
				// 10% writes
				clientID := fmt.Sprintf("client%d", i)
				topic := fmt.Sprintf("sensor/room%d/temperature", i%1000)
				_ = r.Subscribe(clientID, topic, 1, storage.SubscribeOptions{})
			} else {
				// 90% reads
				topic := fmt.Sprintf("sensor/room%d/temperature", i%1000)
				_, _ = r.Match(topic)
			}
			i++
		}
	})
}

func BenchmarkLockFreeRouter_Mixed_90Read_10Write(b *testing.B) {
	r := NewLockFreeRouter()

	// Pre-populate
	for i := 0; i < 1000; i++ {
		topic := fmt.Sprintf("sensor/room%d/temperature", i)
		_ = r.Subscribe(fmt.Sprintf("client%d", i), topic, 1, storage.SubscribeOptions{})
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			if i%10 == 0 {
				// 10% writes
				clientID := fmt.Sprintf("client%d", i)
				topic := fmt.Sprintf("sensor/room%d/temperature", i%1000)
				_ = r.Subscribe(clientID, topic, 1, storage.SubscribeOptions{})
			} else {
				// 90% reads
				topic := fmt.Sprintf("sensor/room%d/temperature", i%1000)
				_, _ = r.Match(topic)
			}
			i++
		}
	})
}

// Benchmark: Wildcard matching performance

func BenchmarkRouter_Match_Wildcards(b *testing.B) {
	r := NewRouter()

	// Add various wildcard subscriptions
	_ = r.Subscribe("client1", "sensor/+/temperature", 1, storage.SubscribeOptions{})
	_ = r.Subscribe("client2", "sensor/+/+", 1, storage.SubscribeOptions{})
	_ = r.Subscribe("client3", "sensor/#", 1, storage.SubscribeOptions{})
	_ = r.Subscribe("client4", "+/+/temperature", 1, storage.SubscribeOptions{})
	_ = r.Subscribe("client5", "#", 1, storage.SubscribeOptions{})

	// Add some exact matches too
	for i := 0; i < 100; i++ {
		topic := fmt.Sprintf("sensor/room%d/temperature", i)
		_ = r.Subscribe(fmt.Sprintf("client%d", i+10), topic, 1, storage.SubscribeOptions{})
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			topic := fmt.Sprintf("sensor/room%d/temperature", i%100)
			_, _ = r.Match(topic)
			i++
		}
	})
}

func BenchmarkLockFreeRouter_Match_Wildcards(b *testing.B) {
	r := NewLockFreeRouter()

	// Add various wildcard subscriptions
	_ = r.Subscribe("client1", "sensor/+/temperature", 1, storage.SubscribeOptions{})
	_ = r.Subscribe("client2", "sensor/+/+", 1, storage.SubscribeOptions{})
	_ = r.Subscribe("client3", "sensor/#", 1, storage.SubscribeOptions{})
	_ = r.Subscribe("client4", "+/+/temperature", 1, storage.SubscribeOptions{})
	_ = r.Subscribe("client5", "#", 1, storage.SubscribeOptions{})

	// Add some exact matches too
	for i := 0; i < 100; i++ {
		topic := fmt.Sprintf("sensor/room%d/temperature", i)
		_ = r.Subscribe(fmt.Sprintf("client%d", i+10), topic, 1, storage.SubscribeOptions{})
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			topic := fmt.Sprintf("sensor/room%d/temperature", i%100)
			_, _ = r.Match(topic)
			i++
		}
	})
}

// Benchmark: Deep topic hierarchy

func BenchmarkRouter_Match_DeepHierarchy(b *testing.B) {
	r := NewRouter()

	// Subscribe to deep topics
	for i := 0; i < 100; i++ {
		topic := fmt.Sprintf("a/b/c/d/e/f/g/h/i/j/%d", i)
		_ = r.Subscribe(fmt.Sprintf("client%d", i), topic, 1, storage.SubscribeOptions{})
	}

	// Add wildcard
	_ = r.Subscribe("wildcard", "a/b/c/d/#", 1, storage.SubscribeOptions{})

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			topic := fmt.Sprintf("a/b/c/d/e/f/g/h/i/j/%d", i%100)
			_, _ = r.Match(topic)
			i++
		}
	})
}

func BenchmarkLockFreeRouter_Match_DeepHierarchy(b *testing.B) {
	r := NewLockFreeRouter()

	// Subscribe to deep topics
	for i := 0; i < 100; i++ {
		topic := fmt.Sprintf("a/b/c/d/e/f/g/h/i/j/%d", i)
		_ = r.Subscribe(fmt.Sprintf("client%d", i), topic, 1, storage.SubscribeOptions{})
	}

	// Add wildcard
	_ = r.Subscribe("wildcard", "a/b/c/d/#", 1, storage.SubscribeOptions{})

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			topic := fmt.Sprintf("a/b/c/d/e/f/g/h/i/j/%d", i%100)
			_, _ = r.Match(topic)
			i++
		}
	})
}

// Benchmark: Unsubscribe operation

func BenchmarkRouter_Unsubscribe(b *testing.B) {
	r := NewRouter()

	// Pre-populate with lots of subscriptions
	for i := 0; i < b.N; i++ {
		clientID := fmt.Sprintf("client%d", i)
		topic := fmt.Sprintf("sensor/room%d/temperature", i%1000)
		_ = r.Subscribe(clientID, topic, 1, storage.SubscribeOptions{})
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		clientID := fmt.Sprintf("client%d", i)
		topic := fmt.Sprintf("sensor/room%d/temperature", i%1000)
		_ = r.Unsubscribe(clientID, topic)
	}
}

func BenchmarkLockFreeRouter_Unsubscribe(b *testing.B) {
	r := NewLockFreeRouter()

	// Pre-populate with lots of subscriptions
	for i := 0; i < b.N; i++ {
		clientID := fmt.Sprintf("client%d", i)
		topic := fmt.Sprintf("sensor/room%d/temperature", i%1000)
		_ = r.Subscribe(clientID, topic, 1, storage.SubscribeOptions{})
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		clientID := fmt.Sprintf("client%d", i)
		topic := fmt.Sprintf("sensor/room%d/temperature", i%1000)
		_ = r.Unsubscribe(clientID, topic)
	}
}

// Benchmark: Realistic MQTT workload simulation
// - Many concurrent readers (pub/sub matching)
// - Few concurrent writers (subscribe/unsubscribe)
// - 95% reads, 5% writes

func BenchmarkRouter_Realistic_95Read_5Write(b *testing.B) {
	r := NewRouter()

	// Pre-populate with typical subscription count
	for i := 0; i < 10000; i++ {
		topic := fmt.Sprintf("sensor/room%d/temperature", i%1000)
		_ = r.Subscribe(fmt.Sprintf("client%d", i), topic, 1, storage.SubscribeOptions{})
	}

	// Add wildcards
	_ = r.Subscribe("monitor1", "sensor/+/temperature", 1, storage.SubscribeOptions{})
	_ = r.Subscribe("monitor2", "sensor/#", 1, storage.SubscribeOptions{})

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			if i%20 == 0 {
				// 5% writes
				clientID := fmt.Sprintf("client%d", i)
				topic := fmt.Sprintf("sensor/room%d/temperature", i%1000)
				_ = r.Subscribe(clientID, topic, 1, storage.SubscribeOptions{})
			} else {
				// 95% reads
				topic := fmt.Sprintf("sensor/room%d/temperature", i%1000)
				_, _ = r.Match(topic)
			}
			i++
		}
	})
}

func BenchmarkLockFreeRouter_Realistic_95Read_5Write(b *testing.B) {
	r := NewLockFreeRouter()

	// Pre-populate with typical subscription count
	for i := 0; i < 10000; i++ {
		topic := fmt.Sprintf("sensor/room%d/temperature", i%1000)
		_ = r.Subscribe(fmt.Sprintf("client%d", i), topic, 1, storage.SubscribeOptions{})
	}

	// Add wildcards
	_ = r.Subscribe("monitor1", "sensor/+/temperature", 1, storage.SubscribeOptions{})
	_ = r.Subscribe("monitor2", "sensor/#", 1, storage.SubscribeOptions{})

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			if i%20 == 0 {
				// 5% writes
				clientID := fmt.Sprintf("client%d", i)
				topic := fmt.Sprintf("sensor/room%d/temperature", i%1000)
				_ = r.Subscribe(clientID, topic, 1, storage.SubscribeOptions{})
			} else {
				// 95% reads
				topic := fmt.Sprintf("sensor/room%d/temperature", i%1000)
				_, _ = r.Match(topic)
			}
			i++
		}
	})
}

// Benchmarks for OptimizedRouter

func BenchmarkOptimizedRouter_Match_100Subs(b *testing.B) {
	r := NewOptimizedRouter()
	for i := 0; i < 100; i++ {
		topic := fmt.Sprintf("sensor/room%d/temperature", i)
		_ = r.Subscribe(fmt.Sprintf("client%d", i), topic, 1, storage.SubscribeOptions{})
	}
	_ = r.Subscribe("wildcard1", "sensor/+/temperature", 1, storage.SubscribeOptions{})
	_ = r.Subscribe("wildcard2", "sensor/#", 1, storage.SubscribeOptions{})

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			topic := fmt.Sprintf("sensor/room%d/temperature", i%100)
			_, _ = r.Match(topic)
			i++
		}
	})
}

func BenchmarkOptimizedRouter_Realistic_95Read_5Write(b *testing.B) {
	r := NewOptimizedRouter()
	for i := 0; i < 10000; i++ {
		topic := fmt.Sprintf("sensor/room%d/temperature", i%1000)
		_ = r.Subscribe(fmt.Sprintf("client%d", i), topic, 1, storage.SubscribeOptions{})
	}
	_ = r.Subscribe("monitor1", "sensor/+/temperature", 1, storage.SubscribeOptions{})
	_ = r.Subscribe("monitor2", "sensor/#", 1, storage.SubscribeOptions{})

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			if i%20 == 0 {
				clientID := fmt.Sprintf("client%d", i)
				topic := fmt.Sprintf("sensor/room%d/temperature", i%1000)
				_ = r.Subscribe(clientID, topic, 1, storage.SubscribeOptions{})
			} else {
				topic := fmt.Sprintf("sensor/room%d/temperature", i%1000)
				_, _ = r.Match(topic)
			}
			i++
		}
	})
}

func BenchmarkOptimizedRouter_Mixed_90Read_10Write(b *testing.B) {
	r := NewOptimizedRouter()
	for i := 0; i < 1000; i++ {
		topic := fmt.Sprintf("sensor/room%d/temperature", i)
		_ = r.Subscribe(fmt.Sprintf("client%d", i), topic, 1, storage.SubscribeOptions{})
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			if i%10 == 0 {
				clientID := fmt.Sprintf("client%d", i)
				topic := fmt.Sprintf("sensor/room%d/temperature", i%1000)
				_ = r.Subscribe(clientID, topic, 1, storage.SubscribeOptions{})
			} else {
				topic := fmt.Sprintf("sensor/room%d/temperature", i%1000)
				_, _ = r.Match(topic)
			}
			i++
		}
	})
}
