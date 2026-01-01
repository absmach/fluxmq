# Queue Performance Baseline Analysis

**Date**: 2026-01-01
**Hardware**: AMD Ryzen 7 PRO 5850U (16 threads)
**Go Version**: 1.24
**Test Configuration**: In-memory storage, single-node

## Executive Summary

Baseline performance measurements establish current queue performance characteristics before optimization. Key findings:
- **Enqueue latency**: 1.6-1.8 µs/op with 6 allocs/op
- **E2E latency**: p50=4µs, p95=6µs, p99=34µs
- **Sustained throughput**: ~870-890 msgs/sec
- **GC overhead**: 36.6% of CPU time
- **Memory allocation**: 3.3 KB/op in E2E tests

## Detailed Benchmark Results

### Enqueue Performance

```
BenchmarkEnqueue_SinglePartition-16      738585    1641 ns/op    1028 B/op    6 allocs/op
BenchmarkEnqueue_MultiplePartitions-16   642056    1701 ns/op    1050 B/op    6 allocs/op
BenchmarkEnqueue_SmallPayload-16         706914    1672 ns/op    1044 B/op    6 allocs/op
BenchmarkEnqueue_LargePayload-16         683486    1708 ns/op    1046 B/op    6 allocs/op
BenchmarkEnqueue_WithPartitionKey-16     680894    1645 ns/op    1031 B/op    6 allocs/op
BenchmarkEnqueue_Parallel-16             650775    1832 ns/op    1048 B/op    6 allocs/op
```

**Key Observations**:
- Consistent ~1.6-1.8 µs latency regardless of payload size (small vs 64KB)
- 6 allocations per enqueue operation
- ~1 KB allocated per operation
- Good scalability: parallel performance only 11% slower than single-threaded

### Dequeue Performance

```
BenchmarkDequeue_SingleConsumer-16       10000    235807 ns/op    2265 B/op    8 allocs/op
BenchmarkDequeue_MultipleConsumers:
  consumers_1-16                         10000    247445 ns/op    2263 B/op    8 allocs/op
  consumers_2-16                         10000    208712 ns/op    2252 B/op    9 allocs/op
  consumers_5-16                         10000    130696 ns/op    2275 B/op   12 allocs/op
  consumers_10-16                        10000    126167 ns/op    2304 B/op   17 allocs/op
BenchmarkDequeue_WithAck-16              10000    111172 ns/op    2293 B/op   10 allocs/op
```

**Key Observations**:
- Dequeue is 140x slower than enqueue (235µs vs 1.6µs)
- Scales well with multiple consumers: 10 consumers = 2x faster per operation
- Additional allocations scale linearly with consumer count
- Ack overhead is minimal (~111µs total for dequeue+ack)

### Partition Scanning

```
BenchmarkDequeue_PartitionScanning:
  partitions_1-16        119942     9172 ns/op      27 B/op     1 allocs/op
  partitions_5-16        133119     8834 ns/op      59 B/op     5 allocs/op
  partitions_10-16       106942    11362 ns/op     107 B/op    10 allocs/op
  partitions_20-16        88161    13265 ns/op     197 B/op    20 allocs/op
```

**Key Observations**:
- Partition scanning overhead is low (~9-13 µs)
- Scales linearly with partition count
- Memory allocation increases with partition count (expected)

### End-to-End Performance

```
BenchmarkE2E_PublishToAck-16             156291     6481 ns/op    3372 B/op   25 allocs/op
BenchmarkE2E_ConcurrentProducerConsumer   5324    192340 ns/op    3597 B/op   16 allocs/op
BenchmarkE2E_Latency-16                  167499     6426 ns/op    3364 B/op   25 allocs/op
  p50: 4µs, p95: 6µs, p99: 34µs
BenchmarkE2E_Sustained-16                  1180   1119225 ns/op    4374 B/op   22 allocs/op
  Throughput: 893.5 msgs/sec
BenchmarkDequeue_Throughput-16             1374   1146460 ns/op   24211 B/op   88 allocs/op
  Throughput: 872.3 msgs/sec
```

**Key Observations**:
- Full message lifecycle: 6.4 µs on average
- Latency distribution shows good p50/p95, but p99 has 8.5x spike
- Sustained throughput: ~870-890 msgs/sec (bottleneck in delivery)
- 25 allocations per E2E operation is high

### Batch Performance

```
BenchmarkEnqueue_BatchSize:
  batch_1-16        707878     1588 ns/op    1042 B/op     6 allocs/op
  batch_10-16        59200    19027 ns/op   10490 B/op    60 allocs/op
  batch_100-16        7345   197836 ns/op  104039 B/op   600 allocs/op
  batch_1000-16        583  1800991 ns/op 1044644 B/op  6007 allocs/op
```

**Key Observations**:
- Batch operations scale linearly (no batching optimization yet)
- Per-message overhead stays constant regardless of batch size
- Opportunity: implement batch enqueue for ~10x improvement

## CPU Profile Analysis

**Total CPU Time**: 27.07s
**GC Time**: 9.89s (36.6% of total)

### Top CPU Hotspots (cumulative)

| Function | Cum% | Description |
|----------|------|-------------|
| `deliverMessages` | 41.67% | Main delivery loop |
| `deliverNext` | 27.26% | Single message delivery |
| `Manager.Enqueue` | 13.22% | Message enqueueing |
| `toStorageMessage` | 10.90% | Message conversion |
| `BufferPool.GetWithData` | 9.20% | Buffer allocation |
| `ListGroups` | 8.05% | Consumer group listing |
| **GC functions** | **36.57%** | **Garbage collection** |

### Critical Observations

1. **GC Pressure**: 36.6% of CPU time spent in garbage collection
   - `scanobject`: 20.13%
   - `gcDrain`: 19% combined
   - Root cause: excessive allocations

2. **Memory Allocation**: 13.93% in `mallocgc`
   - Creating significant overhead
   - Points to pooling opportunities

3. **Map Iteration**: 9.20% in `mapIterStart`
   - Property map copying
   - Consumer group listing

## Memory Profile Analysis

**Total Allocated**: 9.74 GB during benchmark run

### Top Memory Allocators

| Function | Allocation | % | Issue |
|----------|-----------|---|-------|
| `BufferPool.Get` | 3.04 GB | 31.20% | Pool not working efficiently |
| `Manager.Enqueue` | 1.80 GB | 18.48% | Map copying, UUID generation |
| `Dequeue` | 832 MB | 8.55% | New message structs |
| `Enqueue` (storage) | 831 MB | 8.54% | Message copies |
| `UpdateMessage` | 816 MB | 8.38% | State updates |
| `toStorageMessage` | 638 MB | 6.56% | Message conversion |
| `deliverNext` | 305 MB | 3.13% | Inflight tracking |
| `GetInflight` | 340 MB | 3.49% | Query results |
| `MarkInflight` | 299 MB | 3.08% | State tracking |
| `ListGroups` | 222 MB | 2.28% | Group enumeration |
| `UUID.String` | 148 MB | 1.52% | String conversions |

### Critical Observations

1. **BufferPool Inefficiency**: Should be reusing buffers, but allocating 3 GB
   - Investigate: are buffers being returned to pool?
   - Check: buffer lifecycle and reference counting

2. **Message Struct Allocations**: ~2.4 GB across Enqueue/Dequeue/Update
   - **Solution**: Implement `sync.Pool` for `QueueMessage` structs

3. **Map Allocations**: Properties map copying in Enqueue
   - **Solution**: Pre-allocate or pool property maps

4. **UUID String Conversions**: 148 MB
   - **Solution**: Cache UUID strings or use byte representation

5. **Storage Operations**: Memory store creating copies on every operation
   - **Solution**: Investigate if we can reduce copying

## Identified Optimization Opportunities

### High Impact (Estimated 40-60% improvement)

1. **sync.Pool for QueueMessage structs** (Est: -2.4 GB, -15 allocs/op)
   - Pool message objects instead of allocating new ones
   - Reduces GC pressure by ~25%

2. **Buffer Pool Investigation** (Est: -3 GB if fixed)
   - Current buffer pool is allocating, not reusing
   - Either fix pooling or switch to zero-copy RefCountedBuffer

3. **Property Map Pooling** (Est: -1.8 GB from Enqueue)
   - Pool small maps (most messages have <5 properties)
   - Reuse instead of copying

### Medium Impact (Estimated 20-30% improvement)

4. **UUID String Caching** (Est: -148 MB)
   - Cache UUID.String() result in QueueMessage
   - Avoid repeated conversions

5. **Batch Operations** (Est: 10x for batched workloads)
   - Implement batch Enqueue/Dequeue
   - Reduce per-message overhead

6. **Consumer Group List Caching** (Est: -222 MB)
   - Cache consumer group lists
   - Invalidate on group changes only

### Low Impact (Estimated 5-10% improvement)

7. **Message Conversion Optimization** (Est: -638 MB)
   - Optimize `toStorageMessage` allocations
   - Reuse conversion buffers

8. **Storage Layer Optimizations**
   - Reduce defensive copying in memory store
   - Use pointers where safe

## Performance Goals

Based on baseline analysis, optimization targets:

| Metric | Baseline | Target | Improvement |
|--------|----------|--------|-------------|
| Enqueue latency | 1.6 µs | 0.8 µs | 2x |
| Enqueue allocs | 6 allocs/op | 2 allocs/op | 3x |
| Enqueue memory | 1 KB/op | 300 B/op | 3.3x |
| E2E latency | 6.4 µs | 1.5 µs | 4x |
| E2E allocs | 25 allocs/op | 5 allocs/op | 5x |
| E2E memory | 3.3 KB/op | 800 B/op | 4x |
| Sustained throughput | 870 msg/s | 50,000 msg/s | 57x |
| GC overhead | 36.6% | <5% | 7x |

## Next Steps

**Priority 1: Allocation Reduction**
1. Implement sync.Pool for QueueMessage structs
2. Fix/optimize BufferPool or switch to RefCountedBuffer
3. Pool property maps

**Priority 2: Hot Path Optimization**
4. Optimize deliverMessages loop
5. Cache UUID strings
6. Reduce toStorageMessage allocations

**Priority 3: Batch Operations**
7. Implement batch Enqueue
8. Implement batch Dequeue
9. Optimize for sustained throughput

**Priority 4: Storage Layer**
10. Reduce copying in memory store
11. Investigate BadgerDB batch operations
12. Optimize MarkInflight/GetInflight

## Test Environment

- **OS**: Linux 6.18.2-arch2-1
- **CPU**: AMD Ryzen 7 PRO 5850U (8 cores, 16 threads)
- **Go**: go1.24
- **Storage**: In-memory (baseline for pure queue performance)
- **Benchmark Duration**: 1s per benchmark (default)
- **Profile Duration**: 10s for CPU/memory profiling
