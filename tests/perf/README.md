# Performance Test Scripts

All new performance scripts live under `tests/perf/scripts`.

## Available scripts

- `run_smoke.sh`: quick signal (cross-node QoS tests + short benchmark suite)
- `run_load.sh`: medium load run (stress tests + broader benchmark suite)
- `run_soak.sh`: long-running soak loop
- `compare_bench.sh`: `benchstat` comparison of benchmark outputs

## Make targets

- `make perf-smoke`
- `make perf-load`
- `make perf-soak`
- `make perf-compare BASELINE=<baseline-bench-file> [CANDIDATE=<candidate-bench-file>]`
- `make perf-cluster-up`
- `make perf-cluster-down`
- `make perf-cluster-reset`
- `make perf-cluster-ps`
- `make perf-cluster-logs [SERVICE=node1|node2|node3]`

## Docker Compose cluster for local clients

Compose file: `tests/perf/compose/compose.yaml`

Host port mapping:

- `node1`: MQTT `11883`, AMQP1 `15672`, AMQP091 `15682`, Health `18081`
- `node2`: MQTT `11884`, AMQP1 `15673`, AMQP091 `15683`, Health `18082`
- `node3`: MQTT `11885`, AMQP1 `15674`, AMQP091 `15684`, Health `18083`

Example local client run:

```bash
go run ./examples/messaging/main.go -mqtt localhost:11883 -amqp091 localhost:15682
```

## Configuration via environment variables

- `PERF_RESULTS_DIR`: where logs/results are stored (default: `tests/perf/results`)
- `PERF_SKIP_NETWORK_TESTS=1`: skip socket-based cluster/e2e workloads (useful in restricted sandboxes)
- `PERF_COMPOSE_FILE`: override compose path (default: `tests/perf/compose/compose.yaml`)
- `PERF_CLUSTER_BUILD`: `1` (default) to build image on `perf-cluster-up`, `0` to reuse image
- `PERF_CLUSTER_WAIT_READY`: `1` (default) to wait for `/ready` checks
- `PERF_CLUSTER_READY_TIMEOUT`: readiness timeout in seconds (default: `180`)
- `PERF_SMOKE_BENCHTIME`, `PERF_SMOKE_BENCH_COUNT`, `PERF_SMOKE_TEST_TIMEOUT`
- `PERF_LOAD_BENCHTIME`, `PERF_LOAD_BENCH_COUNT`, `PERF_LOAD_TEST_TIMEOUT`
- `PERF_SOAK_MINUTES`, `PERF_SOAK_TEST_TIMEOUT`
- `PERF_COMPARE_BENCHTIME`, `PERF_COMPARE_BENCH_COUNT`
