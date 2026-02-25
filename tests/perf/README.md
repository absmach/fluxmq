# Performance Test Suites

Performance tests are config-driven (`tests/perf/configs/*.json`) and run with `make run-perf` (single config) or `make perf-suite` (list of configs).

This setup runs real MQTT and AMQP 0.9.1 clients against a 3-node cluster (local or Docker) and writes machine-readable JSONL results plus human-readable logs.

AMQP 1.0 is intentionally out of scope for this test collection.

## Quick start

```bash
# 1) Start 3-node cluster (pick one)
make cluster-up      # local processes
make docker-cluster-up       # docker with host networking

# 2) Run one config-driven scenario
make run-perf CONFIG=tests/perf/configs/fanout_mqtt_amqp.json

# 3) Cleanup result files
make perf-cleanup

# 4) Stop cluster and optionally remove /tmp/fluxmq data
make cluster-down            # local mode
make docker-cluster-down     # docker mode
make clean-data              # optional
```

## Prerequisites

- Go toolchain available in PATH
- `make`, `bash`, `curl`
- For Docker mode: Docker + Docker Compose plugin
- Local ports available (see full port map below)

### Cluster port map

| Service      | Node 1 | Node 2 | Node 3 |
|--------------|--------|--------|--------|
| MQTT v3      | 1883   | 1885   | 1887   |
| MQTT v5      | 1884   | 1886   | 1888   |
| WebSocket    | 8883   | 8884   | 8885   |
| HTTP         | 8090   | 8091   | 8092   |
| AMQP 1.0     | 5672   | 5673   | 5674   |
| AMQP 0.9.1   | 5682   | 5683   | 5684   |
| Health       | 8081   | 8082   | 8083   |
| etcd peer    | 2380   | 2381   | 2382   |
| etcd client  | 2379   | 2389   | 2399   |
| gRPC transport | 7948 | 7949   | 7950   |

## Directory layout

- runner script: `tests/perf/scripts/run_suite.sh`
- cleanup script: `tests/perf/scripts/cleanup.sh`
- scenario implementation: `tests/perf/loadgen/main.go`
- config presets (topic fan-in/fan-out): `tests/perf/configs/*.json`

- result table formatter: `tests/perf/report/main.go`
- output artifacts: `tests/perf/results/`

## Cluster setup and validation

1. Start cluster:
   - `make cluster-up` (local) or `make docker-cluster-up` (Docker)
2. Readiness checks used by perf runner:
   - `http://127.0.0.1:8081/ready`
   - `http://127.0.0.1:8082/ready`
   - `http://127.0.0.1:8083/ready`

Set `PERF_SKIP_READY_CHECK=1` only when you intentionally want to bypass readiness probing.

## Running test suites

Config suite:

```bash
PERF_SCENARIO_CONFIGS=tests/perf/configs/fanin_mqtt_mqtt.json,tests/perf/configs/fanout_mqtt_amqp.json \
make perf-suite
```

## Config-driven topic fan-in/fan-out

For simpler scenario tuning, `loadgen` supports JSON config files for topic fan-in/fan-out runs:

```bash
go run ./tests/perf/loadgen \
  -scenario-config tests/perf/configs/fanout_mqtt_amqp.json \
  -payload medium
```

Equivalent Make target:

```bash
make run-perf PERF_SCENARIO_CONFIG=tests/perf/configs/fanout_mqtt_amqp.json
```

More `run-perf` examples:

```bash
# Same as above, using CONFIG alias
make run-perf CONFIG=tests/perf/configs/fanout_mqtt_amqp.json

# Override publishers/subscribers/rate knobs from CLI
make run-perf \
  CONFIG=tests/perf/configs/fanin_mqtt_mqtt.json \
  PERF_PUBLISHERS=5000 \
  PERF_SUBSCRIBERS=100 \
  PERF_MESSAGES_PER_PUBLISHER=600 \
  PERF_PUBLISH_INTERVAL=100ms \
  PERF_PUBLISH_JITTER=25ms

# Use explicit payload bytes and write JSON result line
make run-perf \
  CONFIG=tests/perf/configs/fanout_amqp_mqtt.json \
  PERF_PAYLOAD_BYTES=2048 \
  PERF_JSON_OUT=tests/perf/results/manual_run.jsonl
```

You can also run config scenarios through the suite runner:

```bash
PERF_SCENARIO_CONFIGS=tests/perf/configs/fanin_mqtt_mqtt.json,tests/perf/configs/fanout_mqtt_amqp.json \
PERF_MESSAGE_SIZES=small \
make perf-suite
```

Config files included:

- `tests/perf/configs/fanin_mqtt_mqtt.json`
- `tests/perf/configs/fanin_mqtt_amqp.json`
- `tests/perf/configs/fanin_amqp_mqtt.json`
- `tests/perf/configs/fanin_amqp_amqp.json`
- `tests/perf/configs/fanout_mqtt_mqtt.json`
- `tests/perf/configs/fanout_mqtt_amqp.json`
- `tests/perf/configs/fanout_amqp_mqtt.json`
- `tests/perf/configs/fanout_amqp_amqp.json`

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
- `publish_jitter` (Go duration; per-publisher random jitter for cadence and initial start stagger)
- `subscribers`
- `topic_count` (fan-out)
- `wildcard_subscribers` (randomly picked subscriber count)
- `wildcard_patterns` (supports `{base}` and `{topic}`)
- `drain_timeout` (Go duration, e.g. `45s`; overrides `PERF_DRAIN_TIMEOUT`)
- `payload_bytes` (integer; overrides `-payload-bytes` / `-payload` CLI flags)

CLI overrides (`-publishers`, `-subscribers`, `-messages-per-publisher`, `-publish-interval`, `-publish-jitter`, `-payload-bytes`, `-drain-timeout`) still apply on top of config values.

## Scenario matrix

| Config                                | Flow        | Pattern  | Focus                                           |
| ------------------------------------- | ----------- | -------- | ----------------------------------------------- |
| `tests/perf/configs/fanin_mqtt_mqtt.json`   | mqtt-mqtt   | fanin    | MQTT topic fan-in baseline                      |
| `tests/perf/configs/fanin_mqtt_amqp.json`   | mqtt-amqp   | fanin    | MQTT publish -> AMQP subscribe compatibility    |
| `tests/perf/configs/fanin_amqp_mqtt.json`   | amqp-mqtt   | fanin    | AMQP publish -> MQTT subscribe compatibility    |
| `tests/perf/configs/fanin_amqp_amqp.json`   | amqp-amqp   | fanin    | AMQP topic fan-in baseline                      |
| `tests/perf/configs/fanout_mqtt_mqtt.json`  | mqtt-mqtt   | fanout   | MQTT topic fan-out baseline                     |
| `tests/perf/configs/fanout_mqtt_amqp.json`  | mqtt-amqp   | fanout   | MQTT publish -> AMQP subscribe compatibility    |
| `tests/perf/configs/fanout_amqp_mqtt.json`  | amqp-mqtt   | fanout   | AMQP publish -> MQTT subscribe compatibility    |
| `tests/perf/configs/fanout_amqp_amqp.json`  | amqp-amqp   | fanout   | AMQP topic fan-out baseline                     |

## Output and metrics

For each scenario run, output includes:

- scenario name and description
- payload size
- messages sent, expected, received
- concurrent publishers
- concurrent subscribers
- sent/received throughput in messages per second
- delivery ratio, error count, pass/fail

Client placement uses configured cluster endpoints with role-aware distribution.
For MQTT clients, loadgen mixes MQTT v3 and v5 connections randomly (using `PERF_MQTT_V3_ADDRS` and `PERF_MQTT_V5_ADDRS`) so both listener families are exercised.

Artifacts in `tests/perf/results`:

- `clients_suite_configs_<timestamp>.log`
- `clients_suite_configs_<timestamp>.jsonl`

At the end of suite execution, a summary table is printed from JSONL via `tests/perf/report/main.go`.

## Configuration reference

Set variables inline before `make perf-suite`.

| Variable                      | Default                                           | Description                                             |
| ----------------------------- | ------------------------------------------------- | ------------------------------------------------------- |
| `PERF_SCENARIO_CONFIGS`       | empty                                             | Comma-separated config JSON paths for `make perf-suite` |
| `PERF_MESSAGE_SIZES`          | `small,medium,large`                              | Preset payload size matrix                              |
| `PERF_MESSAGE_SIZE_BYTES`     | empty                                             | Exact payload bytes (single or comma list)              |
| `PERF_PUBLISHERS`             | config value                                      | Override concurrent publishers                          |
| `PERF_SUBSCRIBERS`            | config value                                      | Override concurrent subscribers/consumers               |
| `PERF_MESSAGES_PER_PUBLISHER` | config value                                      | Override per-publisher message count                    |
| `PERF_PUBLISH_INTERVAL`       | config value                                      | Delay between publishes per publisher                   |
| `PERF_PUBLISH_JITTER`         | `0`                                               | Random per-publisher cadence jitter (`+/- duration`)    |
| `PERF_MQTT_V3_ADDRS`          | `127.0.0.1:1883,127.0.0.1:1885,127.0.0.1:1887`   | MQTT v3 endpoints                                       |
| `PERF_MQTT_V5_ADDRS`          | `127.0.0.1:1884,127.0.0.1:1886,127.0.0.1:1888`   | MQTT v5 endpoints                                       |
| `PERF_AMQP_ADDRS`             | `127.0.0.1:5682,127.0.0.1:5683,127.0.0.1:5684`   | AMQP 0.9.1 endpoints                                    |
| `PERF_MIN_RATIO`              | `0.95`                                            | Pass threshold for topic scenario delivery ratio        |
| `PERF_DRAIN_TIMEOUT`          | `45s`                                             | Max wait for subscribers/consumers to drain             |
| `PERF_SKIP_READY_CHECK`       | `0`                                               | Skip readiness probes when set to `1`                   |

## Example env configs and runs

### Example 1: single config run

```bash
make cluster-up
make run-perf CONFIG=tests/perf/configs/fanout_mqtt_amqp.json
```

### Example 2: single config with high-load overrides

```bash
make run-perf \
  CONFIG=tests/perf/configs/fanin_mqtt_mqtt.json \
  PERF_PUBLISHERS=5000 \
  PERF_SUBSCRIBERS=100 \
  PERF_MESSAGES_PER_PUBLISHER=600 \
  PERF_PUBLISH_INTERVAL=100ms \
  PERF_PUBLISH_JITTER=25ms \
  PERF_DRAIN_TIMEOUT=120s
```

### Example 3: config suite run (multiple config files)

```bash
PERF_SCENARIO_CONFIGS=tests/perf/configs/fanin_mqtt_mqtt.json,tests/perf/configs/fanout_mqtt_amqp.json \
PERF_MESSAGE_SIZES=small \
make perf-suite

make perf-cleanup
```

## Cleanup

`make perf-cleanup` removes suite log and JSONL files from the results directory.

Cluster teardown:

```bash
# local
make cluster-down

# docker
make docker-cluster-down

# optional: remove /tmp/fluxmq data
make clean-data
```
