# Performance Test Suites

Performance tests are organized as suite files in `tests/perf/suites` and run with one entrypoint: `make perf-suite`.

This setup runs real MQTT and AMQP 0.9.1 clients against the 3-node Docker Compose cluster and writes machine-readable JSONL results plus human-readable logs.

AMQP 1.0 is intentionally out of scope for this test collection.

## Quick start

```bash
# 1) Start 3-node cluster
make run-cluster

# 2) Run default suite (tests/perf/suites/all.txt)
make perf-suite

# 3) Cleanup cluster state and generated result files
make perf-cleanup
```

## Prerequisites

- Go toolchain available in PATH
- Docker + Docker Compose plugin available
- `make`, `bash`, `curl`
- Shared Docker bridge network `fluxmq-local-net` (auto-created by `make run-cluster`)
- Local ports available:
  - MQTT: `11883`, `11884`, `11885`
  - AMQP 0.9.1: `15682`, `15683`, `15684`
  - readiness endpoints: `18081`, `18082`, `18083`

## Directory layout

- runner script: `tests/perf/scripts/run_suite.sh`
- cleanup script: `tests/perf/scripts/cleanup.sh`
- scenario implementation: `tests/perf/loadgen/main.go`
- result table formatter: `tests/perf/report/main.go`
- suite definitions: `tests/perf/suites/*.txt`
- output artifacts: `tests/perf/results/`

## Cluster setup and validation

1. Start cluster:
   - `make run-cluster`
2. Optional status checks:
   - `make run-cluster-ps`
   - `make run-cluster-logs SERVICE=node1`
   - direct container endpoints on `fluxmq-local-net`: `node1`/`10.247.0.11`, `node2`/`10.247.0.12`, `node3`/`10.247.0.13`
3. Readiness checks used by perf runner:
   - `http://127.0.0.1:18081/ready`
   - `http://127.0.0.1:18082/ready`
   - `http://127.0.0.1:18083/ready`

Set `PERF_SKIP_READY_CHECK=1` only when you intentionally want to bypass readiness probing.

## Running test suites

Default suite:

```bash
make perf-suite
```

Specific suite file by name:

```bash
PERF_SUITE=mqtt make perf-suite
PERF_SUITE=amqp make perf-suite
PERF_SUITE=queue make perf-suite
PERF_SUITE=bridge make perf-suite
```

Custom suite file path:

```bash
PERF_SUITE_FILE=tests/perf/suites/mqtt.txt make perf-suite
```

Direct scenario list (overrides suite file):

```bash
PERF_SCENARIOS=mqtt-fanin,mqtt-shared-subscriptions,mqtt-last-will make perf-suite
```

## Scenario matrix

| Scenario                     | Protocol           | Pattern             | Focus                                                          |
| ---------------------------- | ------------------ | ------------------- | -------------------------------------------------------------- |
| `mqtt-fanin`                 | MQTT               | topic pub/sub       | Many publishers -> one topic with many subscribers             |
| `mqtt-fanout`                | MQTT               | topic pub/sub       | Broad fanout delivery under subscriber-heavy load              |
| `mqtt-substorm`              | MQTT               | topic pub/sub       | Subscribe/unsubscribe churn with wildcard matching             |
| `mqtt-consumer-groups`       | MQTT               | queue               | Queue fanout across multiple consumer groups                   |
| `mqtt-shared-subscriptions`  | MQTT               | shared subscription | `$share/<group>/<topic>` group load balancing                  |
| `mqtt-last-will`             | MQTT               | last will           | LWT emission after abrupt disconnect                           |
| `amqp-fanin`                 | AMQP 0.9.1         | topic pub/sub       | Many publishers -> one topic with many subscribers             |
| `amqp-fanout`                | AMQP 0.9.1         | topic pub/sub       | Broad fanout delivery on AMQP topics                           |
| `amqp091-consumer-groups`    | AMQP 0.9.1         | queue               | Queue fanout across multiple AMQP consumer groups              |
| `amqp091-stream-cursor`      | AMQP 0.9.1         | stream              | Full read from `first` + replay from `offset=<cursor>`         |
| `amqp091-retention-policies` | AMQP 0.9.1         | stream retention    | Retention bound check using `x-max-length` policy              |
| `queue-fanin`                | MQTT queue API     | queue               | Many publishers feeding one consumer group                     |
| `queue-fanout`               | MQTT queue API     | queue               | One queue delivered to multiple consumer groups                |
| `bridge-queue-mqtt-amqp`     | MQTT -> AMQP 0.9.1 | queue bridge        | Cross-protocol queue delivery MQTT producers to AMQP consumers |
| `bridge-queue-amqp-mqtt`     | AMQP 0.9.1 -> MQTT | queue bridge        | Cross-protocol queue delivery AMQP producers to MQTT consumers |

## Output and metrics

For each scenario run, output includes:

- scenario name and description
- payload size
- messages sent, expected, received
- concurrent publishers
- concurrent consumers/subscribers
- consumer group count (where relevant)
- sent/received throughput in messages per second
- delivery ratio, error count, pass/fail

Artifacts in `tests/perf/results`:

- `clients_suite_<suite>_<timestamp>.log`
- `clients_suite_<suite>_<timestamp>.jsonl`

At the end of suite execution, a summary table is printed from JSONL via `tests/perf/report/main.go`.

## Configuration reference

Set variables inline before `make perf-suite`.

| Variable                      | Default                                           | Description                                              |
| ----------------------------- | ------------------------------------------------- | -------------------------------------------------------- |
| `PERF_SUITE`                  | `all`                                             | Suite name from `tests/perf/suites/<name>.txt`           |
| `PERF_SUITE_FILE`             | derived from `PERF_SUITE`                         | Explicit suite file path                                 |
| `PERF_SCENARIOS`              | empty                                             | Comma-separated scenario list; overrides suite file      |
| `PERF_MESSAGE_SIZES`          | `small,medium,large`                              | Preset size matrix                                       |
| `PERF_MESSAGE_SIZE_BYTES`     | empty                                             | Exact payload bytes (single size)                        |
| `PERF_PUBLISHERS`             | scenario default                                  | Override concurrent publishers                           |
| `PERF_SUBSCRIBERS`            | scenario default                                  | Override concurrent subscribers/consumers                |
| `PERF_CONSUMER_GROUPS`        | scenario default                                  | Override consumer group count                            |
| `PERF_CONSUMERS_PER_GROUP`    | scenario default                                  | Override consumers per group                             |
| `PERF_MESSAGES_PER_PUBLISHER` | scenario default                                  | Override per-publisher message count                     |
| `PERF_PUBLISH_INTERVAL`       | `0`                                               | Delay between publishes per publisher (for rate shaping) |
| `PERF_MQTT_ADDRS`             | `127.0.0.1:11883,127.0.0.1:11884,127.0.0.1:11885` | MQTT endpoints                                           |
| `PERF_AMQP_ADDRS`             | `127.0.0.1:15682,127.0.0.1:15683,127.0.0.1:15684` | AMQP 0.9.1 endpoints                                     |
| `PERF_MIN_RATIO`              | `0.95`                                            | Pass threshold for non-queue topic scenarios             |
| `PERF_QUEUE_MIN_RATIO`        | `0.99`                                            | Pass threshold for queue/bridge/stream scenarios         |
| `PERF_DRAIN_TIMEOUT`          | `45s`                                             | Max wait for subscribers/consumers to drain              |
| `PERF_SKIP_READY_CHECK`       | `0`                                               | Skip readiness probes when set to `1`                    |
| `PERF_CLUSTER_NETWORK_NAME`   | `fluxmq-local-net`                                | Local Docker network name used by cluster compose        |
| `PERF_CLUSTER_NETWORK_SUBNET` | `10.247.0.0/24`                                   | Subnet used when auto-creating the cluster network       |

## Example env configs and runs

Use one of these profiles, then run `make perf-suite`.

### Example 1: MQTT throughput profile

```bash
export PERF_SUITE=mqtt
export PERF_MESSAGE_SIZES=small,medium
export PERF_PUBLISHERS=180
export PERF_SUBSCRIBERS=120
export PERF_MESSAGES_PER_PUBLISHER=500
export PERF_PUBLISH_INTERVAL=1ms
export PERF_DRAIN_TIMEOUT=60s

make perf-suite
```

### Example 2: Queue and consumer-group profile

```bash
export PERF_SCENARIOS=mqtt-consumer-groups,amqp091-consumer-groups,queue-fanout
export PERF_MESSAGE_SIZE_BYTES=4096
export PERF_PUBLISHERS=220
export PERF_CONSUMER_GROUPS=6
export PERF_CONSUMERS_PER_GROUP=32
export PERF_MESSAGES_PER_PUBLISHER=450
export PERF_PUBLISH_INTERVAL=2ms
export PERF_QUEUE_MIN_RATIO=0.99

make perf-suite
```

### Example 3: AMQP 0.9.1 stream cursor and retention profile

```bash
export PERF_SCENARIOS=amqp091-stream-cursor,amqp091-retention-policies
export PERF_MESSAGE_SIZE_BYTES=2048
export PERF_PUBLISHERS=80
export PERF_MESSAGES_PER_PUBLISHER=700
export PERF_PUBLISH_INTERVAL=1ms
export PERF_DRAIN_TIMEOUT=75s
export PERF_MIN_RATIO=0.95
export PERF_QUEUE_MIN_RATIO=0.99

make perf-suite
```

### One-shot run without exporting

```bash
PERF_SCENARIOS=mqtt-shared-subscriptions,mqtt-last-will \
PERF_MESSAGE_SIZE_BYTES=1024 \
PERF_PUBLISHERS=140 \
PERF_CONSUMER_GROUPS=4 \
PERF_CONSUMERS_PER_GROUP=36 \
PERF_MESSAGES_PER_PUBLISHER=350 \
PERF_PUBLISH_INTERVAL=2ms \
make perf-suite
```

## Setup and run examples

```bash
make run-cluster

# MQTT shared subscriptions with explicit concurrency and rate shaping.
PERF_SCENARIOS=mqtt-shared-subscriptions \
PERF_MESSAGE_SIZE_BYTES=1024 \
PERF_PUBLISHERS=150 \
PERF_CONSUMER_GROUPS=4 \
PERF_CONSUMERS_PER_GROUP=40 \
PERF_MESSAGES_PER_PUBLISHER=400 \
PERF_PUBLISH_INTERVAL=2ms \
make perf-suite

# MQTT last will under client disconnect pressure.
PERF_SCENARIOS=mqtt-last-will \
PERF_MESSAGE_SIZES=small \
PERF_PUBLISHERS=120 \
PERF_SUBSCRIBERS=60 \
make perf-suite

# AMQP 0.9.1 cursor replay from a stream offset.
PERF_SCENARIOS=amqp091-stream-cursor \
PERF_MESSAGE_SIZE_BYTES=2048 \
PERF_PUBLISHERS=60 \
PERF_MESSAGES_PER_PUBLISHER=300 \
PERF_PUBLISH_INTERVAL=1ms \
make perf-suite

# AMQP 0.9.1 retention policy validation.
PERF_SCENARIOS=amqp091-retention-policies \
PERF_MESSAGE_SIZES=medium \
PERF_PUBLISHERS=80 \
PERF_MESSAGES_PER_PUBLISHER=600 \
make perf-suite

# Queue and consumer-group focused run.
PERF_SCENARIOS=mqtt-consumer-groups,amqp091-consumer-groups,queue-fanout \
PERF_MESSAGE_SIZE_BYTES=4096 \
PERF_PUBLISHERS=200 \
PERF_CONSUMER_GROUPS=6 \
PERF_CONSUMERS_PER_GROUP=30 \
PERF_MESSAGES_PER_PUBLISHER=500 \
PERF_PUBLISH_INTERVAL=2ms \
make perf-suite

make perf-cleanup
```

## Cleanup

`make perf-cleanup` supports:

- `PERF_CLEAN_RESET_CLUSTER=1` (default): reset containers and mapped volumes
- `PERF_CLEAN_RESULTS=1` (default): remove suite logs and JSONL files

Examples:

```bash
# remove only result files, keep cluster running
PERF_CLEAN_RESET_CLUSTER=0 PERF_CLEAN_RESULTS=1 make perf-cleanup

# reset cluster state, keep result files
PERF_CLEAN_RESET_CLUSTER=1 PERF_CLEAN_RESULTS=0 make perf-cleanup
```
