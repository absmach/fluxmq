//go:build ignore

// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package archive

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	badgerstore "github.com/absmach/fluxmq/queue/storage/badger"
	"github.com/absmach/fluxmq/queue/types"
	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

// BenchmarkReplication_SyncMode benchmarks sync replication throughput.
// Target: >5K enqueues/sec per partition.
func BenchmarkReplication_SyncMode(b *testing.B) {
	queueName := "$queue/bench-sync"
	managers, _, cleanup := setupBenchCluster(b, 3, queueName, 3, types.ReplicationSync)
	defer cleanup()

	ctx := context.Background()
	payload := make([]byte, 1024) // 1KB messages
	props := map[string]string{"bench": "sync"}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		// Try each manager until we find the leader for this partition
		for _, mgr := range managers {
			err := mgr.Enqueue(ctx, queueName, payload, props)
			if err == nil {
				break
			}
		}
	}

	b.StopTimer()
	opsPerSec := float64(b.N) / b.Elapsed().Seconds()
	b.ReportMetric(opsPerSec, "ops/s")
}

// BenchmarkReplication_AsyncMode benchmarks async replication throughput.
// Target: >50K enqueues/sec per partition.
func BenchmarkReplication_AsyncMode(b *testing.B) {
	queueName := "$queue/bench-async"
	managers, _, cleanup := setupBenchCluster(b, 3, queueName, 3, types.ReplicationAsync)
	defer cleanup()

	ctx := context.Background()
	payload := make([]byte, 1024) // 1KB messages
	props := map[string]string{"bench": "async"}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		// Try each manager until we find the leader for this partition
		for _, mgr := range managers {
			err := mgr.Enqueue(ctx, queueName, payload, props)
			if err == nil {
				break
			}
		}
	}

	b.StopTimer()
	opsPerSec := float64(b.N) / b.Elapsed().Seconds()
	b.ReportMetric(opsPerSec, "ops/s")
}

// BenchmarkReplication_Latency measures enqueue latency with sync replication.
// Target P99: <50ms (sync).
func BenchmarkReplication_Latency(b *testing.B) {
	queueName := "$queue/bench-latency"
	managers, _, cleanup := setupBenchCluster(b, 3, queueName, 1, types.ReplicationSync)
	defer cleanup()

	ctx := context.Background()
	payload := make([]byte, 1024)
	props := map[string]string{"bench": "latency"}

	// Find the leader node for partition 0
	var leaderMgr *Manager
	for _, mgr := range managers {
		raftMgr := mgr.raftManagers[queueName]
		if raftMgr != nil && raftMgr.IsLeader(0) {
			leaderMgr = mgr
			break
		}
	}
	require.NotNil(b, leaderMgr, "no leader found")

	latencies := make([]time.Duration, b.N)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		start := time.Now()
		err := leaderMgr.Enqueue(ctx, queueName, payload, props)
		latencies[i] = time.Since(start)
		if err != nil {
			b.Fatalf("enqueue failed: %v", err)
		}
	}

	b.StopTimer()

	// Calculate percentiles
	p50, p95, p99 := calculatePercentiles(latencies)
	b.ReportMetric(float64(p50.Microseconds()), "p50_us")
	b.ReportMetric(float64(p95.Microseconds()), "p95_us")
	b.ReportMetric(float64(p99.Microseconds()), "p99_us")
}

// BenchmarkReplication_Concurrent benchmarks concurrent enqueues from multiple goroutines.
func BenchmarkReplication_Concurrent(b *testing.B) {
	queueName := "$queue/bench-concurrent"
	managers, _, cleanup := setupBenchCluster(b, 3, queueName, 3, types.ReplicationSync)
	defer cleanup()

	ctx := context.Background()
	payload := make([]byte, 1024)

	concurrency := []int{1, 10, 50, 100}

	for _, c := range concurrency {
		b.Run(fmt.Sprintf("goroutines-%d", c), func(b *testing.B) {
			b.ResetTimer()

			var wg sync.WaitGroup
			opsPerWorker := b.N / c

			for w := 0; w < c; w++ {
				wg.Add(1)
				go func(workerID int) {
					defer wg.Done()
					props := map[string]string{
						"bench":  "concurrent",
						"worker": fmt.Sprintf("%d", workerID),
					}

					for i := 0; i < opsPerWorker; i++ {
						for _, mgr := range managers {
							err := mgr.Enqueue(ctx, queueName, payload, props)
							if err == nil {
								break
							}
						}
					}
				}(w)
			}

			wg.Wait()
			b.StopTimer()

			opsPerSec := float64(b.N) / b.Elapsed().Seconds()
			b.ReportMetric(opsPerSec, "ops/s")
		})
	}
}

// BenchmarkReplication_MessageSizes benchmarks different message sizes.
func BenchmarkReplication_MessageSizes(b *testing.B) {
	queueName := "$queue/bench-sizes"
	managers, _, cleanup := setupBenchCluster(b, 3, queueName, 1, types.ReplicationSync)
	defer cleanup()

	ctx := context.Background()
	sizes := []int{64, 256, 1024, 4096, 16384} // 64B to 16KB

	for _, size := range sizes {
		b.Run(fmt.Sprintf("size-%dB", size), func(b *testing.B) {
			payload := make([]byte, size)
			props := map[string]string{"bench": "sizes"}

			// Find leader
			var leaderMgr *Manager
			for _, mgr := range managers {
				raftMgr := mgr.raftManagers[queueName]
				if raftMgr != nil && raftMgr.IsLeader(0) {
					leaderMgr = mgr
					break
				}
			}
			require.NotNil(b, leaderMgr)

			b.ResetTimer()
			b.SetBytes(int64(size))

			for i := 0; i < b.N; i++ {
				err := leaderMgr.Enqueue(ctx, queueName, payload, props)
				if err != nil {
					b.Fatalf("enqueue failed: %v", err)
				}
			}

			b.StopTimer()
			throughput := float64(b.N*size) / b.Elapsed().Seconds() / 1024 / 1024 // MB/s
			b.ReportMetric(throughput, "MB/s")
		})
	}
}

// setupBenchCluster creates a 3-node cluster for benchmarking.
func setupBenchCluster(b *testing.B, nodeCount int, queueName string, partitions int, mode types.ReplicationMode) ([]*Manager, []*badgerstore.Store, func()) {
	b.Helper()

	tempDir, err := os.MkdirTemp("", "bench-*")
	require.NoError(b, err)

	managers := make([]*Manager, nodeCount)
	stores := make([]*badgerstore.Store, nodeCount)
	dbs := make([]*badger.DB, nodeCount)
	nodeAddresses := make(map[string]string)

	// Create addresses
	for i := 0; i < nodeCount; i++ {
		nodeID := fmt.Sprintf("node%d", i+1)
		nodeAddresses[nodeID] = fmt.Sprintf("127.0.0.1:%d", 8000+i)
	}

	// Create managers
	for i := 0; i < nodeCount; i++ {
		nodeID := fmt.Sprintf("node%d", i+1)
		nodeDir := filepath.Join(tempDir, nodeID)
		err := os.MkdirAll(nodeDir, 0o755)
		require.NoError(b, err)

		dbPath := filepath.Join(nodeDir, "data")
		opts := badger.DefaultOptions(dbPath)
		opts.Logger = nil
		db, err := badger.Open(opts)
		require.NoError(b, err)
		dbs[i] = db
		stores[i] = badgerstore.New(db)

		broker := NewMockBroker()
		cfg := Config{
			QueueStore:    stores[i],
			MessageStore:  stores[i],
			ConsumerStore: stores[i],
			DeliverFn:     broker.DeliverToSession,
			LocalNodeID:   nodeID,
			DataDir:       nodeDir,
			NodeAddresses: nodeAddresses,
		}

		mgr, err := NewManager(cfg)
		require.NoError(b, err)
		managers[i] = mgr
	}

	// Create replicated queue
	queueConfig := types.QueueConfig{
		Name:       queueName,
		Partitions: partitions,
		Ordering:   types.OrderingPartition,
		Replication: types.ReplicationConfig{
			Enabled:           true,
			ReplicationFactor: nodeCount,
			Mode:              mode,
			Placement:         types.PlacementRoundRobin,
			MinInSyncReplicas: (nodeCount / 2) + 1,
			AckTimeout:        5 * time.Second,
			HeartbeatTimeout:  1 * time.Second,
			ElectionTimeout:   3 * time.Second,
		},
		RetryPolicy: types.RetryPolicy{
			MaxRetries:        3,
			InitialBackoff:    100 * time.Millisecond,
			MaxBackoff:        1 * time.Second,
			BackoffMultiplier: 2.0,
		},
		MaxMessageSize:   1024 * 1024,
		MaxQueueDepth:    1000000,
		DeliveryTimeout:  30 * time.Second,
		BatchSize:        100,
		HeartbeatTimeout: 10 * time.Second,
		MessageTTL:       24 * time.Hour,
	}

	ctx := context.Background()
	for _, mgr := range managers {
		err := mgr.CreateQueue(ctx, queueConfig)
		require.NoError(b, err)
	}

	// Start Raft partitions
	for _, mgr := range managers {
		raftMgr := mgr.raftManagers[queueName]
		require.NotNil(b, raftMgr)

		for partID := 0; partID < partitions; partID++ {
			err := raftMgr.StartPartition(ctx, partID, partitions)
			require.NoError(b, err)
		}
	}

	// Wait for leaders
	for partID := 0; partID < partitions; partID++ {
		waitForRaftStable(b, managers, queueName, partID, 10*time.Second)
	}

	cleanup := func() {
		for _, mgr := range managers {
			mgr.Stop()
		}
		for _, db := range dbs {
			db.Close()
		}
		os.RemoveAll(tempDir)
	}

	return managers, stores, cleanup
}

// calculatePercentiles calculates p50, p95, p99 from a slice of durations.
func calculatePercentiles(durations []time.Duration) (p50, p95, p99 time.Duration) {
	if len(durations) == 0 {
		return 0, 0, 0
	}

	// Simple percentile calculation (not perfectly accurate but good enough for benchmarks)
	sorted := make([]time.Duration, len(durations))
	copy(sorted, durations)

	// Bubble sort (simple but works for benchmarks)
	for i := 0; i < len(sorted); i++ {
		for j := i + 1; j < len(sorted); j++ {
			if sorted[i] > sorted[j] {
				sorted[i], sorted[j] = sorted[j], sorted[i]
			}
		}
	}

	p50 = sorted[len(sorted)*50/100]
	p95 = sorted[len(sorted)*95/100]
	p99 = sorted[len(sorted)*99/100]

	return p50, p95, p99
}

// Note: waitForRaftStable helper is defined in replication_test.go and shared across test files
