# Benchmarking and Stress Testing Guide

This guide covers how to run benchmarks and stress tests for the MQTT broker.

## Quick Start

```bash
# Show all available targets
make help

# Run all benchmarks
make bench

# Run all stress tests (takes ~30 minutes)
make stress

# Run a specific stress test
make stress-pool
```

---

## Benchmark Targets

### `make bench`
Runs all benchmarks across the entire codebase.

```bash
make bench
```

### `make bench-broker`
Runs only the broker package benchmarks (message routing, zero-copy, etc.).

```bash
make bench-broker
```

### `make bench-zerocopy`
Runs the zero-copy vs legacy comparison benchmarks to show performance improvements.

```bash
make bench-zerocopy
```

**Expected output:**
```
BenchmarkMessageCopy_Legacy/100_bytes     120.4 ns/op   336 B/op   3 allocs/op
BenchmarkMessageCopy_ZeroCopy/100_bytes    42.9 ns/op     0 B/op   0 allocs/op
```

### `make bench-report`
Generates a comprehensive benchmark report and saves it to `build/benchmark-results.txt`.

```bash
make bench-report
```

---

## Stress Test Targets

Stress tests run longer and with higher load than benchmarks to identify memory leaks, race conditions, and performance degradation under sustained load.

**Note**: Stress tests are skipped when running regular `make test` (they require non-short mode).

### `make stress`
Runs all stress tests. Takes approximately 30 minutes.

```bash
make stress
```

Includes:
- High throughput test
- Concurrent publishers test
- Memory pressure test
- Sustained load test
- Buffer pool exhaustion test
- Extreme fan-out test
- Subscription churn test

### Individual Stress Tests

#### `make stress-throughput`
Tests sustained high-throughput publishing (10,000 msg/s for 10 seconds to 100 subscribers).

```bash
make stress-throughput
```

**What it tests:**
- Sustained message rate
- Memory stability under constant load
- Throughput consistency

**Duration:** ~15 seconds

---

#### `make stress-concurrent`
Tests multiple concurrent publishers (10 publishers × 10,000 messages each).

```bash
make stress-concurrent
```

**What it tests:**
- Concurrent publish safety
- Message ordering
- No data races

**Duration:** ~10 seconds

---

#### `make stress-memory`
Tests behavior under memory pressure with large messages (64KB to 1MB).

```bash
make stress-memory
```

**What it tests:**
- Zero-copy memory efficiency
- No memory leaks
- GC behavior with large buffers

**Duration:** ~20 seconds

---

#### `make stress-sustained`
Tests broker under sustained mixed load (30 seconds, multiple topics).

```bash
make stress-sustained
```

**What it tests:**
- Long-running stability
- Mixed message sizes
- Multiple topics
- Continuous throughput

**Duration:** ~30 seconds

---

#### `make stress-pool`
Tests buffer pool behavior under extreme concurrent load (100 goroutines, 1M operations).

```bash
make stress-pool
```

**What it tests:**
- Buffer pool thread safety
- Pool hit rate under contention
- Reference counting correctness

**Duration:** ~1 second

**Expected results:**
- 8M+ operations per second
- >99% pool hit rate

---

#### `make stress-fanout`
Tests extreme fan-out scenario (1,000 messages to 5,000 subscribers).

```bash
make stress-fanout
```

**What it tests:**
- Zero-copy effectiveness at scale
- Memory usage with high fan-out
- Per-delivery overhead

**Duration:** ~60 seconds

**Expected results:**
- <100 bytes per delivery (vs message size)
- Demonstrates zero-copy benefits

---

#### `make stress-churn`
Tests stability with rapid subscription churn (subscribe/unsubscribe while publishing).

```bash
make stress-churn
```

**What it tests:**
- Subscription management stability
- Session lifecycle
- Concurrent subscription changes
- Message routing during churn

**Duration:** ~20 seconds

---

## Running Tests Directly

You can also run tests directly with `go test`:

```bash
# Run all benchmarks
go test -bench=. -benchmem ./broker

# Run specific benchmark
go test -bench=BenchmarkMessagePublish_FanOut -benchmem ./broker

# Run all stress tests
go test -v -run=TestStress -timeout=30m ./broker

# Run specific stress test
go test -v -run=TestStress_BufferPoolExhaustion ./broker

# Run with race detector
go test -race -run=TestStress_ConcurrentPublishers ./broker
```

---

## Interpreting Benchmark Results

### Benchmark Output Format

```
BenchmarkName-16    iterations    ns/op    B/op    allocs/op
```

- **iterations**: Number of times the benchmark loop ran
- **ns/op**: Nanoseconds per operation (lower is better)
- **B/op**: Bytes allocated per operation (lower is better)
- **allocs/op**: Number of allocations per operation (lower is better)

### Zero-Copy Indicators

Look for these signs of effective zero-copy:

1. **Zero allocations** in payload sharing:
   ```
   BenchmarkMessageCopy_ZeroCopy/1024_bytes-16    0 B/op    0 allocs/op
   ```

2. **Memory independent of subscriber count**:
   ```
   1 subscriber:     600 B/op
   1000 subscribers: 424,369 B/op  (~424 B per subscriber overhead)
   ```

3. **Constant time for different message sizes**:
   ```
   100 bytes:   42.9 ns/op
   1 KB:        49.2 ns/op
   64 KB:     1059 ns/op   (mostly copy time, not allocation)
   ```

### Stress Test Success Criteria

Stress tests use assertions to verify:

- ✅ No errors during message publishing
- ✅ Memory usage stays reasonable (no leaks)
- ✅ Throughput remains consistent
- ✅ Pool hit rate >95%
- ✅ All messages delivered successfully

---

## Continuous Integration

To run benchmarks and tests in CI:

```bash
# Run tests (excludes stress tests by default)
make test

# Run quick benchmarks
make bench-broker

# Optional: Run stress tests in nightly builds
make stress
```

---

## Comparing Benchmark Results

To compare before/after performance:

```bash
# Run baseline
make bench-broker | tee baseline.txt

# Make changes...

# Run comparison
make bench-broker | tee optimized.txt

# Use benchstat to compare (if installed)
benchstat baseline.txt optimized.txt
```

---

## Troubleshooting

### Stress Tests Skipped

If stress tests don't run:

```bash
# Ensure you're not in short mode
go test -run=TestStress ./broker  # Will run

# This will skip stress tests:
go test -short -run=TestStress ./broker  # Will skip
```

### Out of Memory

If stress tests fail with OOM:

1. Check available memory: `free -h`
2. Reduce subscriber counts in test
3. Run individual stress tests instead of all at once

### Slow Benchmarks

Benchmarks might be slow on first run due to:
- Go test cache warming
- OS page cache
- First-time JIT compilation

Run benchmarks twice for consistent results:

```bash
make bench-broker  # Warmup
make bench-broker  # Actual measurement
```

---

## Related Documentation

- [Zero-Copy Benchmark Results](./zero-copy-benchmark-results.md) - Detailed performance analysis
- [Performance Optimization Progress](./performance-optimization-progress.md) - Optimization history
- `broker/message_bench_test.go` - Benchmark source code
- `broker/message_stress_test.go` - Stress test source code

---

## Contributing Benchmarks

When adding new benchmarks:

1. Place in appropriate `_test.go` file
2. Follow naming: `Benchmark<Feature>_<Scenario>`
3. Use `b.ReportAllocs()` for memory tracking
4. Add sub-benchmarks for different sizes/configurations
5. Document what the benchmark measures
6. Update this guide with new make targets if needed

Example:

```go
func BenchmarkMyFeature_SmallPayload(b *testing.B) {
    // Setup
    payload := make([]byte, 100)

    b.ResetTimer()
    b.ReportAllocs()

    for i := 0; i < b.N; i++ {
        // Code to benchmark
    }
}
```
