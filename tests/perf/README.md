# Performance Test Suites

Performance testing is organized as **suite files** under `tests/perf/suites` and executed with one runner script.

## What this provides

- Real protocol clients against the running 3-node cluster
- Configurable load knobs (clients, message size, message frequency)
- Per-scenario results with throughput and delivery metrics
- Cleanup script for cluster state + result artifacts

## Main commands

- Run suite: `make perf-suite`
- Cleanup: `make perf-cleanup`
- Cluster lifecycle:
  - `make run-cluster`
  - `make run-cluster-down`
  - `make run-cluster-clean`

## Suite files

Suite files are in `tests/perf/suites/*.txt`.

Examples:

- `tests/perf/suites/all.txt`
- `tests/perf/suites/mqtt.txt`
- `tests/perf/suites/amqp.txt`
- `tests/perf/suites/queue.txt`
- `tests/perf/suites/bridge.txt`

Each file lists scenario names, one per line.

## Scenarios

- `mqtt-fanin`
- `mqtt-fanout`
- `mqtt-substorm`
- `amqp-fanin`
- `amqp-fanout`
- `queue-fanin`
- `queue-fanout`
- `bridge-queue-mqtt-amqp`
- `bridge-queue-amqp-mqtt`

## Result output

Each scenario run emits:

- scenario name + description
- message size
- messages sent / expected / received
- concurrent publishers
- concurrent consumers
- consumer groups
- sent and received messages/sec
- delivery ratio
- errors
- pass/fail

Artifacts are written to `tests/perf/results`:

- suite log: `clients_suite_<suite>_<timestamp>.log`
- JSONL result file: `clients_suite_<suite>_<timestamp>.jsonl`

The suite runner prints a summary table from JSONL at the end.

## Load configuration

Set environment variables before `make perf-suite`.

### Suite selection

- `PERF_SUITE`: suite name from `tests/perf/suites` (default `all`)
- `PERF_SUITE_FILE`: explicit suite file path (overrides `PERF_SUITE`)
- `PERF_SCENARIOS`: comma list of scenarios (overrides suite file)

### Message size

- `PERF_MESSAGE_SIZES`: comma list, e.g. `small,medium,large` (default)
- `PERF_MESSAGE_SIZE_BYTES`: single exact size in bytes, e.g. `1024`

### Concurrency and frequency

- `PERF_PUBLISHERS`: concurrent publishers override
- `PERF_SUBSCRIBERS`: concurrent subscribers/consumers override
- `PERF_CONSUMER_GROUPS`: queue fanout group override
- `PERF_CONSUMERS_PER_GROUP`: queue fanout group size override
- `PERF_MESSAGES_PER_PUBLISHER`: messages sent by each publisher
- `PERF_PUBLISH_INTERVAL`: per-publisher pause between messages, e.g. `5ms`

### Endpoints and thresholds

- `PERF_MQTT_ADDRS` (default `127.0.0.1:11883,127.0.0.1:11884,127.0.0.1:11885`)
- `PERF_AMQP_ADDRS` (default `127.0.0.1:15682,127.0.0.1:15683,127.0.0.1:15684`)
- `PERF_MIN_RATIO` (default `0.95`)
- `PERF_QUEUE_MIN_RATIO` (default `0.99`)
- `PERF_DRAIN_TIMEOUT` (default `45s`)
- `PERF_SKIP_READY_CHECK=1` to skip `/ready` checks

## Cleanup options

`make perf-cleanup` supports:

- `PERF_CLEAN_RESET_CLUSTER=1` (default): reset containers + volumes
- `PERF_CLEAN_RESULTS=1` (default): remove suite logs/jsonl

## Example

```bash
make run-cluster
PERF_SUITE=queue PERF_MESSAGE_SIZE_BYTES=2048 PERF_PUBLISHERS=200 PERF_SUBSCRIBERS=120 PERF_MESSAGES_PER_PUBLISHER=500 PERF_PUBLISH_INTERVAL=2ms make perf-suite
make perf-cleanup
```
