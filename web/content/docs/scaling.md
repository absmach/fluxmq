---
title: Scaling & Performance
description: Comprehensive scaling guide covering capacity analysis, performance optimizations, benchmarks, topic sharding, and architecture for 100M+ clients
---

# Scaling & Performance

**Last Updated:** 2026-02-05

Performance and scaling are workload-dependent. Use the benchmark suites in `benchmarks/` and validate on your target hardware and network before making production commitments.

## Benchmarking

- See `benchmarks/README.md` for available benchmarks and how to run them.
- Run benchmarks on the same class of hardware you plan to deploy.
- Capture results with `-benchmem` and keep baselines in version control if needed.

## Practical Tuning Levers

- `server.tcp.plain.max_connections`: protect the broker from excess concurrent connections
- `session.max_sessions`: cap active MQTT sessions
- `broker.max_message_size`: limit payload size
- `session.max_offline_queue_size` and `session.max_inflight_messages`: control per-client memory usage
- `queues.*.limits`: bound queue depth, message size, and TTL

## OS and Runtime Considerations

- Ensure file descriptor limits are high enough for your target connection count.
- Tune TCP keepalive and timeouts based on your workload (IoT vs low-latency systems).
- Measure with production-like TLS settings if TLS is enabled in production.

## Cluster Scaling

- Clustering spreads sessions and subscriptions across nodes but requires etcd coordination.
- Use multiple nodes to improve availability and distribute connections.
- Queue replication is optional and configured via `cluster.raft`.

For configuration details, see `docs/configuration.md`.
