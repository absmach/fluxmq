#!/usr/bin/env bash
# Copyright (c) Abstract Machines
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=tests/perf/scripts/common.sh
source "$SCRIPT_DIR/common.sh"

require_cmd go

LOAD_BENCH_TIME="${PERF_LOAD_BENCHTIME:-10s}"
LOAD_BENCH_COUNT="${PERF_LOAD_BENCH_COUNT:-1}"
LOAD_TEST_TIMEOUT="${PERF_LOAD_TEST_TIMEOUT:-30m}"
LOG_FILE="$RESULTS_DIR/load_${TIMESTAMP}.log"

log_info "Starting load suite"
log_info "Log file: $LOG_FILE"
write_header "$LOG_FILE"

if [[ "${PERF_SKIP_NETWORK_TESTS:-0}" == "1" ]]; then
	log_warn "Skipping cross-node load tests (PERF_SKIP_NETWORK_TESTS=1)"
else
	run_and_log "$LOG_FILE" go test ./cluster \
		-run 'TestCrossNode_HighThroughput|TestCrossNode_MultipleSubscribers|TestCrossNode_WildcardSubscriptions' \
		-count=1 \
		-timeout "$LOAD_TEST_TIMEOUT"
fi

run_and_log "$LOG_FILE" go test ./mqtt/broker \
	-run 'TestStress_HighThroughputPublish|TestStress_ConcurrentPublishers|TestStress_FanOutExtreme' \
	-count=1 \
	-timeout "$LOAD_TEST_TIMEOUT"

run_bench_suite "$LOAD_BENCH_TIME" "$LOAD_BENCH_COUNT" "$LOG_FILE"

if [[ "${PERF_SKIP_NETWORK_TESTS:-0}" == "1" ]]; then
	log_warn "Skipping end-to-end benchmark package (PERF_SKIP_NETWORK_TESTS=1)"
else
	run_and_log "$LOG_FILE" go test ./benchmarks \
		-run '^$' \
		-bench 'Benchmark(MessageThroughput_EndToEnd|MessageThroughput_QoS|FanOut|WildcardSubscriptions|ConcurrentClients)' \
		-benchmem \
		-benchtime "$LOAD_BENCH_TIME" \
		-count "$LOAD_BENCH_COUNT"
fi

log_success "Load suite complete. Results: $LOG_FILE"
