#!/usr/bin/env bash
# Copyright (c) Abstract Machines
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=tests/perf/scripts/common.sh
source "$SCRIPT_DIR/common.sh"

require_cmd go
require_cmd benchstat

BASELINE_FILE="${1:-}"
CANDIDATE_FILE="${2:-}"

if [[ -z "$BASELINE_FILE" ]]; then
	log_error "Usage: $0 <baseline-bench-file> [candidate-bench-file]"
	exit 1
fi

if [[ ! -f "$BASELINE_FILE" ]]; then
	log_error "Baseline file not found: $BASELINE_FILE"
	exit 1
fi

if [[ -z "$CANDIDATE_FILE" ]]; then
	CANDIDATE_FILE="$RESULTS_DIR/candidate_${TIMESTAMP}.bench"
	log_info "No candidate benchmark provided; running benchmark suite"
	run_bench_suite "${PERF_COMPARE_BENCHTIME:-5s}" "${PERF_COMPARE_BENCH_COUNT:-1}" "$CANDIDATE_FILE"
fi

if [[ ! -f "$CANDIDATE_FILE" ]]; then
	log_error "Candidate file not found: $CANDIDATE_FILE"
	exit 1
fi

REPORT_FILE="$RESULTS_DIR/compare_${TIMESTAMP}.txt"
log_info "Comparing baseline vs candidate"
log_info "Baseline: $BASELINE_FILE"
log_info "Candidate: $CANDIDATE_FILE"
log_info "Report: $REPORT_FILE"

benchstat "$BASELINE_FILE" "$CANDIDATE_FILE" | tee "$REPORT_FILE"

log_success "Benchmark comparison complete. Report: $REPORT_FILE"

