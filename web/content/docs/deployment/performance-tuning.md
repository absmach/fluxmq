---
title: Performance Tuning
description: Go runtime tuning and GC configuration for low-latency workloads
---

# Performance Tuning

**Last Updated:** 2026-02-26

FluxMQ is written in Go and benefits from runtime tuning under high-throughput or latency-sensitive workloads. This page covers Go garbage collector configuration and memory limits.

## Go Garbage Collector Tuning

Go's garbage collector runs concurrently but still introduces stop-the-world (STW) pauses and GC-assist work that can affect tail latency. Two environment variables control GC behavior:

### GOGC

Controls how much the heap must grow (as a percentage of the live heap) before triggering a GC cycle. Default is `100` (GC triggers when heap doubles).

| Value           | Effect                                                             |
| --------------- | ------------------------------------------------------------------ |
| `100` (default) | GC runs frequently, lower peak memory, more CPU spent on GC        |
| `200`–`400`     | Fewer GC cycles, higher peak memory, less CPU overhead             |
| `off`           | Disables growth-triggered GC entirely (use only with `GOMEMLIMIT`) |

Raising `GOGC` reduces GC frequency at the cost of higher memory usage. For a broker handling sustained message throughput, fewer GC cycles typically improve p99 latency.

### GOMEMLIMIT

Sets a soft memory ceiling for the Go runtime (available since Go 1.19). When heap usage approaches this limit, the runtime triggers GC more aggressively regardless of `GOGC`. This prevents OOM while allowing relaxed GC under normal load.

```bash
GOGC=off GOMEMLIMIT=3200MiB ./fluxmq -config config.yaml
```

Set `GOMEMLIMIT` to roughly 80% of available memory to leave headroom for goroutine stacks, OS caches, and non-heap allocations.

### Recommended Configurations

**Dedicated host or container (known memory budget):**

```bash
# 4 GB container
GOGC=off GOMEMLIMIT=3200MiB

# 8 GB container
GOGC=off GOMEMLIMIT=6400MiB
```

This eliminates GC cycles driven by heap growth ratio and only triggers collection under real memory pressure. It is the simplest and most effective configuration for servers with a known memory budget.

**Shared host (no hard memory budget):**

```bash
GOGC=200
```

Doubles the growth threshold before GC triggers. A conservative middle ground that reduces GC frequency without requiring a memory limit.

**Default (no tuning):**

Leave both unset. Suitable for development, testing, and low-throughput deployments where GC overhead is negligible.

## GC Diagnostics

To verify whether GC is impacting your workload, enable per-cycle GC logging:

```bash
GODEBUG=gctrace=1 ./fluxmq -config config.yaml
```

Each GC cycle prints a line like:

```
gc 42 @6.123s 1%: 0.044+2.1+0.031 ms clock, 0.35+1.2/3.8/0.92+0.25 ms cpu, 48->50->26 MB, 52 MB goal, 8 P
```

Key fields:
- `0.044+2.1+0.031 ms clock` — STW mark-start, concurrent mark, STW mark-end
- `48->50->26 MB` — heap before GC, heap at mark completion, live heap after
- `1%` — fraction of total CPU spent on GC

If STW pauses exceed your latency budget or GC CPU% is consistently above 5–10%, tuning is warranted.

## Tuning Workflow

1. **Start with defaults.** For most deployments, the default GC settings are sufficient.
2. **Enable `gctrace`.** Monitor GC behavior under production load to determine if tuning is needed.
3. **Apply tuning.** If GC is a factor, set `GOMEMLIMIT` to 80% of available memory and `GOGC=off`.
4. **Monitor.** Use OpenTelemetry metrics (`server.metrics_enabled`) alongside `gctrace` during initial rollout to confirm behavior under real traffic.

## Learn More

- [Running in Production](/docs/deployment/running-in-production) — production checklist and application-level tuning levers
- [Configuration Reference](/docs/reference/configuration-reference) — full configuration schema
