# Performance Test Suites

Performance tests are config-driven (`tests/perf/configs/topics/*.json`) and run with `make run-perf` (single config) or `make perf-suite` (list of configs).

This setup runs real MQTT and AMQP 0.9.1 clients against the 3-node Docker Compose cluster and writes machine-readable JSONL results plus human-readable logs.

AMQP 1.0 is intentionally out of scope for this test collection.

## Quick start

```bash
# 1) Start 3-node cluster
make run-cluster

# 2) Run one config-driven scenario
make run-perf CONFIG=tests/perf/configs/topics/fanout_mqtt_amqp.json

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
- config presets (topic fan-in/fan-out): `tests/perf/configs/topics/*.json`
- result table formatter: `tests/perf/report/main.go`
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

Config suite:

```bash
PERF_SCENARIO_CONFIGS=tests/perf/configs/topics/fanin_mqtt_mqtt.json,tests/perf/configs/topics/fanout_mqtt_amqp.json \
make perf-suite
```

## Config-driven topic fan-in/fan-out

For simpler scenario tuning, `loadgen` supports JSON config files for topic fan-in/fan-out runs:

```bash
go run ./tests/perf/loadgen \
  -scenario-config tests/perf/configs/topics/fanout_mqtt_amqp.json \
  -payload medium
```

Equivalent Make target:

```bash
make run-perf PERF_SCENARIO_CONFIG=tests/perf/configs/topics/fanout_mqtt_amqp.json
```

More `run-perf` examples:

```bash
# Same as above, using CONFIG alias
make run-perf CONFIG=tests/perf/configs/topics/fanout_mqtt_amqp.json

# Override publishers/subscribers/rate knobs from CLI
make run-perf \
  CONFIG=tests/perf/configs/topics/fanin_mqtt_mqtt.json \
  PERF_PUBLISHERS=5000 \
  PERF_SUBSCRIBERS=100 \
  PERF_MESSAGES_PER_PUBLISHER=600 \
  PERF_PUBLISH_INTERVAL=100ms

# Use explicit payload bytes and write JSON result line
make run-perf \
  CONFIG=tests/perf/configs/topics/fanout_amqp_mqtt.json \
  PERF_PAYLOAD_BYTES=2048 \
  PERF_JSON_OUT=tests/perf/results/manual_run.jsonl
```

You can also run config scenarios through the suite runner:

```bash
PERF_SCENARIO_CONFIGS=tests/perf/configs/topics/fanin_mqtt_mqtt.json,tests/perf/configs/topics/fanout_mqtt_amqp.json \
PERF_MESSAGE_SIZES=small \
make perf-suite
```

Config files included:

- `tests/perf/configs/topics/fanin_mqtt_mqtt.json`
- `tests/perf/configs/topics/fanin_amqp_amqp.json`
- `tests/perf/configs/topics/fanout_mqtt_amqp.json`
- `tests/perf/configs/topics/fanout_amqp_mqtt.json`

Supported JSON fields:

- `name`
- `description`
- `pattern` (`fanin` or `fanout`)
- `flow` (`mqtt-mqtt`, `mqtt-amqp`, `amqp-mqtt`, `amqp-amqp`)
- `topic` (supports `{run_id}` placeholder)
- `qos` (`0|1|2`, MQTT side only)
- `publishers`
- `messages_per_publisher`
- `publish_interval` (Go duration, e.g. `100ms`)
- `subscribers`
- `topic_count` (fan-out)
- `wildcard_subscribers` (randomly picked subscriber count)
- `wildcard_patterns` (supports `{base}` and `{topic}`)

CLI overrides (`-publishers`, `-subscribers`, `-messages-per-publisher`, `-publish-interval`) still apply on top of config values.

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

Client placement uses configured cluster endpoints with role-aware distribution:
publishers start from one node set and subscribers/consumers are offset to other nodes when available, so routing paths are exercised.

Artifacts in `tests/perf/results`:

- `clients_suite_configs_<timestamp>.log`
- `clients_suite_configs_<timestamp>.jsonl`

At the end of suite execution, a summary table is printed from JSONL via `tests/perf/report/main.go`.

## Configuration reference

Set variables inline before `make perf-suite`.

| Variable                      | Default                                           | Description                                              |
| ----------------------------- | ------------------------------------------------- | -------------------------------------------------------- |
| `PERF_SCENARIO_CONFIGS`       | empty                                             | Comma-separated config JSON paths for `make perf-suite` |
| `PERF_MESSAGE_SIZES`          | `small,medium,large`                              | Preset payload size matrix                               |
| `PERF_MESSAGE_SIZE_BYTES`     | empty                                             | Exact payload bytes (single or comma list)              |
| `PERF_PUBLISHERS`             | config value                                      | Override concurrent publishers                           |
| `PERF_SUBSCRIBERS`            | config value                                      | Override concurrent subscribers/consumers                |
| `PERF_MESSAGES_PER_PUBLISHER` | config value                                      | Override per-publisher message count                     |
| `PERF_PUBLISH_INTERVAL`       | config value                                      | Delay between publishes per publisher                    |
| `PERF_MQTT_ADDRS`             | `127.0.0.1:11883,127.0.0.1:11884,127.0.0.1:11885` | MQTT endpoints                                                               |
| `PERF_AMQP_ADDRS`             | `127.0.0.1:15682,127.0.0.1:15683,127.0.0.1:15684` | AMQP 0.9.1 endpoints                                                         |
| `PERF_MIN_RATIO`              | `0.95`                                            | Pass threshold for non-queue topic scenarios                                 |
| `PERF_QUEUE_MIN_RATIO`        | `0.99`                                            | Pass threshold for queue/bridge/stream scenarios                             |
| `PERF_DRAIN_TIMEOUT`          | `45s`                                             | Max wait for subscribers/consumers to drain                                  |
| `PERF_SKIP_READY_CHECK`       | `0`                                               | Skip readiness probes when set to `1`                                        |
| `PERF_CLUSTER_NETWORK_NAME`   | `fluxmq-local-net`                                | Local Docker network name used by cluster compose                            |
| `PERF_CLUSTER_NETWORK_SUBNET` | `10.247.0.0/24`                                   | Subnet used when auto-creating the cluster network                           |

## Example env configs and runs

### Example 1: single config run

```bash
make run-cluster
make run-perf CONFIG=tests/perf/configs/topics/fanout_mqtt_amqp.json
```

### Example 2: single config with high-load overrides

```bash
make run-perf \
  CONFIG=tests/perf/configs/topics/fanin_mqtt_mqtt.json \
  PERF_PUBLISHERS=5000 \
  PERF_SUBSCRIBERS=100 \
  PERF_MESSAGES_PER_PUBLISHER=600 \
  PERF_PUBLISH_INTERVAL=100ms \
  PERF_DRAIN_TIMEOUT=120s
```

### Example 3: config suite run (multiple config files)

```bash
PERF_SCENARIO_CONFIGS=tests/perf/configs/topics/fanin_mqtt_mqtt.json,tests/perf/configs/topics/fanout_mqtt_amqp.json \
PERF_MESSAGE_SIZES=small \
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
