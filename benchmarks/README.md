# MQTT Broker Benchmarks

This directory contains benchmark suites and performance analysis results for the MQTT broker.

## Quick Start

### Run All Benchmarks

```bash
# Comprehensive benchmark suite (takes ~15-20 minutes)
./scripts/run_benchmarks.sh

# Quick benchmark for development (takes ~1 minute)
./scripts/quick_benchmark.sh
```

### Run Specific Benchmarks

```bash
# Message publishing benchmarks
go test -bench=BenchmarkMessagePublish -benchtime=5s ./broker

# Router benchmarks
go test -bench=BenchmarkRouter -benchtime=3s ./broker/router

# Queue benchmarks
go test -bench=BenchmarkEnqueue -benchtime=3s ./queue
go test -bench=BenchmarkDequeue -benchtime=3s ./queue
```

## Performance Results

**Current Performance (2026-01-04):**
- **Latency:** 67 μs/op @ 1000 subscribers
- **Throughput:** 14.9K messages/sec to 1000 subscribers
- **Memory:** 177 B/op
- **Allocations:** 4 allocs/op

**See:** [`docs/PERFORMANCE_RESULTS.md`](../docs/PERFORMANCE_RESULTS.md) for comprehensive results.

## Benchmark Suite

### Broker Benchmarks (`broker/message_bench_test.go`)

**Message Publishing:**
- `BenchmarkMessagePublish_SingleSubscriber` - Single subscriber performance
- `BenchmarkMessagePublish_MultipleSubscribers` - Fanout to N subscribers (1, 10, 100, 1000)
- `BenchmarkMessagePublish_QoS0/1/2` - QoS level performance
- `BenchmarkMessagePublish_SharedSubscription` - Consumer group performance
- `BenchmarkMessagePublish_FanOut` - 1:N distribution (10, 100, 500, 1000)

**Buffer Pooling:**
- `BenchmarkBufferPooling` - Buffer pool hit rate
- `BenchmarkMessageCopy_ZeroCopy` - Zero-copy vs legacy comparison

### Router Benchmarks (`broker/router/router_bench_test.go`)

**Topic Matching:**
- `BenchmarkRouter_Match_*` - Match performance with 100, 1K, 10K subscribers
- `BenchmarkRouter_Match_Wildcards` - Wildcard (+, #) matching
- `BenchmarkRouter_Match_DeepHierarchy` - Multi-level topic matching

**Operations:**
- `BenchmarkRouter_Subscribe` - Subscription addition
- `BenchmarkRouter_Unsubscribe` - Subscription removal
- `BenchmarkRouter_Mixed_*` - Mixed read/write workloads

### Queue Benchmarks (`queue/*_bench_test.go`)

**Enqueue:**
- `BenchmarkEnqueue_SinglePartition` - Single partition enqueue
- `BenchmarkEnqueue_MultiplePartitions` - Multi-partition enqueue
- `BenchmarkEnqueue_SmallPayload` - Small message performance
- `BenchmarkEnqueue_LargePayload` - Large message performance
- `BenchmarkEnqueue_Parallel` - Concurrent enqueue

**Dequeue:**
- `BenchmarkDequeue_SingleConsumer` - Single consumer dequeue
- `BenchmarkDequeue_MultipleConsumers` - Multi-consumer scaling
- `BenchmarkDequeue_PartitionScanning` - Partition scanning overhead

## Profiling

### Generate Profiles

```bash
# CPU profile
go test -bench=BenchmarkMessagePublish_MultipleSubscribers/1000 \
    -cpuprofile=cpu.prof -benchtime=30s ./broker

# Memory profile
go test -bench=BenchmarkMessagePublish_MultipleSubscribers/1000 \
    -memprofile=mem.prof -benchtime=30s ./broker

# Mutex contention profile
go test -bench=BenchmarkMessagePublish_MultipleSubscribers/1000 \
    -mutexprofile=mutex.prof -benchtime=30s ./broker
```

### Analyze Profiles

```bash
# CPU hotspots
go tool pprof -top -cum cpu.prof
go tool pprof -http=:8080 cpu.prof  # Interactive web UI

# Memory allocations
go tool pprof -top mem.prof
go tool pprof -list=functionName mem.prof  # Specific function

# Lock contention
go tool pprof -top mutex.prof
```

## Results Archive

Historical benchmark results and analysis are stored in `results/archive/`:

- `baseline_2026-01-04.md` - Initial baseline measurements
- `profiling_analysis_2026-01-04.md` - Bottleneck identification
- `pooling_results_2026-01-04.md` - Object pooling optimization results

## Regression Testing

### CI Integration

Add to CI pipeline to prevent performance regressions:

```bash
# Run benchmark and fail if performance degrades
go test -bench=BenchmarkMessagePublish_MultipleSubscribers/1000 \
    -benchtime=10s ./broker | tee current.txt

# Compare with baseline (requires benchstat tool)
benchstat baseline.txt current.txt

# Fail if:
# - Latency > 100 μs/op (current: 67 μs)
# - Allocations > 10/op (current: 4)
# - Memory > 1 KB/op (current: 177 B)
```

### Baseline Comparison

```bash
# Install benchstat
go install golang.org/x/perf/cmd/benchstat@latest

# Run before and after benchmarks
go test -bench=... -benchtime=10s -count=5 ./broker > old.txt
# ... make changes ...
go test -bench=... -benchtime=10s -count=5 ./broker > new.txt

# Compare
benchstat old.txt new.txt
```

## Performance Targets

### Current Targets (Single Instance)

| Metric | Target | Current | Status |
|--------|--------|---------|--------|
| Latency (1000 subs) | < 100 μs | 67 μs | ✅ |
| Memory/op | < 1 KB | 177 B | ✅ |
| Allocations/op | < 10 | 4 | ✅ |
| GC Time | < 50% CPU | ~40% | ✅ |
| Concurrent clients | 500K+ | TBD | ⏳ |

### Future Targets

- **Throughput:** 100K+ msg/sec to 1000 subscribers (current: 14.9K)
- **Concurrent clients:** 1M+ on single instance
- **Cluster:** 3-5 nodes handling 10M+ clients

## Optimization History

### 2026-01-04: Object Pooling & Router Optimization

**Optimizations:**
1. Object pooling (Message, Publish packets) - 2.98x gain
2. Router match pooling - 1.10x additional gain
3. Mutex profiling (no action needed - low contention)

**Total Gain:** 3.27x faster

**Files Modified:**
- `storage/pool.go` (new)
- `core/packets/v5/pool.go` (new)
- `core/packets/v3/pool.go` (new)
- `broker/router/pool.go` (new)
- `broker/publish.go`
- `broker/delivery.go`
- `broker/router/router.go`

## Tips for Benchmark Development

### Best Practices

1. **Use realistic workloads**
   - Mix of message sizes (small, medium, large)
   - Realistic subscriber counts (1, 10, 100, 1000)
   - Include wildcards and shared subscriptions

2. **Measure what matters**
   - End-to-end latency (not just internal functions)
   - Memory allocations (use `-benchmem`)
   - CPU time distribution (use `-cpuprofile`)

3. **Stable baselines**
   - Run with `-benchtime=10s` or longer
   - Use `-count=5` for statistical significance
   - Disable CPU frequency scaling if possible

4. **Avoid common pitfalls**
   - Don't optimize in the benchmark loop (compiler may optimize away)
   - Use `b.ResetTimer()` after setup
   - Use `b.StopTimer()` for cleanup that shouldn't count

### Example Benchmark

```go
func BenchmarkMyFeature(b *testing.B) {
    // Setup (not timed)
    broker := createBroker()
    defer broker.Close()

    payload := make([]byte, 1024)

    b.ResetTimer()      // Start timing here
    b.ReportAllocs()    // Report allocations

    for i := 0; i < b.N; i++ {
        // Code to benchmark
        broker.Publish(payload)
    }
}
```

## Troubleshooting

### Benchmark Too Slow

```bash
# Reduce benchtime for development
go test -bench=... -benchtime=1s ./broker

# Run specific benchmark only
go test -bench=BenchmarkSpecificTest -benchtime=3s ./broker
```

### Inconsistent Results

```bash
# Run multiple times and average
go test -bench=... -benchtime=10s -count=10 ./broker

# Use benchstat for statistical analysis
go test -bench=... -count=10 ./broker > results.txt
benchstat results.txt
```

### High Memory Usage

```bash
# Check for memory leaks
go test -bench=... -memprofile=mem.prof ./broker
go tool pprof -alloc_space mem.prof  # Total allocations
go tool pprof -inuse_space mem.prof  # Live heap
```

## Contributing

When adding new benchmarks:

1. Follow naming convention: `Benchmark<Feature>_<Variant>`
2. Include `-benchmem` results
3. Add to appropriate benchmark file
4. Document expected performance in comments
5. Update this README with new benchmarks

## License

Copyright (c) Abstract Machines
SPDX-License-Identifier: Apache-2.0
