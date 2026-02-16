#!/usr/bin/env bash
# Copyright (c) Abstract Machines
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=tests/perf/scripts/common.sh
source "$SCRIPT_DIR/common.sh"

require_cmd go

SOAK_MINUTES="${PERF_SOAK_MINUTES:-120}"
SOAK_TIMEOUT="${PERF_SOAK_TEST_TIMEOUT:-20m}"
LOG_FILE="$RESULTS_DIR/soak_${TIMESTAMP}.log"

if ! [[ "$SOAK_MINUTES" =~ ^[0-9]+$ ]]; then
	log_error "PERF_SOAK_MINUTES must be an integer, got: $SOAK_MINUTES"
	exit 1
fi

END_EPOCH=$(( "$(date +%s)" + SOAK_MINUTES * 60 ))
ITERATION=0

log_info "Starting soak suite for ${SOAK_MINUTES} minute(s)"
log_info "Log file: $LOG_FILE"
write_header "$LOG_FILE"

while [[ "$(date +%s)" -lt "$END_EPOCH" ]]; do
	ITERATION=$((ITERATION + 1))
	log_info "Soak iteration $ITERATION"

	run_and_log "$LOG_FILE" go test ./mqtt/broker \
		-run 'TestStress_SustainedLoad|TestStress_RapidSubscribeUnsubscribe' \
		-count=1 \
		-timeout "$SOAK_TIMEOUT"

	if [[ "${PERF_SKIP_NETWORK_TESTS:-0}" == "1" ]]; then
		log_warn "Skipping cross-node soak test in iteration $ITERATION (PERF_SKIP_NETWORK_TESTS=1)"
	else
		run_and_log "$LOG_FILE" go test ./cluster \
			-run 'TestCrossNode_HighThroughput' \
			-count=1 \
			-timeout "$SOAK_TIMEOUT"
	fi
done

log_success "Soak suite complete after $ITERATION iteration(s). Results: $LOG_FILE"
