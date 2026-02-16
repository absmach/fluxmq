#!/usr/bin/env bash
# Copyright (c) Abstract Machines
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=tests/perf/scripts/common.sh
source "$SCRIPT_DIR/common.sh"

require_cmd go

SMOKE_BENCH_TIME="${PERF_SMOKE_BENCHTIME:-2s}"
SMOKE_BENCH_COUNT="${PERF_SMOKE_BENCH_COUNT:-1}"
SMOKE_TEST_TIMEOUT="${PERF_SMOKE_TEST_TIMEOUT:-15m}"
LOG_FILE="$RESULTS_DIR/smoke_${TIMESTAMP}.log"

log_info "Starting performance smoke suite"
log_info "Log file: $LOG_FILE"
write_header "$LOG_FILE"

if [[ "${PERF_SKIP_NETWORK_TESTS:-0}" == "1" ]]; then
	log_warn "Skipping cross-node smoke tests (PERF_SKIP_NETWORK_TESTS=1)"
else
	run_and_log "$LOG_FILE" go test ./cluster \
		-run 'TestCrossNode_QoS0_PublishSubscribe|TestCrossNode_QoS1_PublishSubscribe|TestCrossNode_QoS2_PublishSubscribe' \
		-count=1 \
		-timeout "$SMOKE_TEST_TIMEOUT"
fi

run_bench_suite "$SMOKE_BENCH_TIME" "$SMOKE_BENCH_COUNT" "$LOG_FILE"

log_success "Smoke suite complete. Results: $LOG_FILE"
