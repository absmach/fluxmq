#!/usr/bin/env bash
# Copyright (c) Abstract Machines
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=tests/perf/scripts/common.sh
source "$SCRIPT_DIR/common.sh"

require_cmd rm

RESET_CLUSTER="${PERF_CLEAN_RESET_CLUSTER:-1}"
REMOVE_RESULTS="${PERF_CLEAN_RESULTS:-1}"

if [[ "$RESET_CLUSTER" == "1" ]]; then
	log_info "Resetting cluster state (containers + volumes)"
	bash "$SCRIPT_DIR/cluster_reset.sh"
fi

if [[ "$REMOVE_RESULTS" == "1" ]]; then
	log_info "Removing suite result files"
	rm -f "$RESULTS_DIR"/clients_suite_*.log
	rm -f "$RESULTS_DIR"/clients_suite_*.jsonl
	log_success "Removed suite result files from $RESULTS_DIR"
fi

log_success "Perf cleanup complete"
