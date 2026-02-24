#!/usr/bin/env bash
# Copyright (c) Abstract Machines
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=tests/perf/scripts/common.sh
source "$SCRIPT_DIR/common.sh"

log_info "Removing suite result files"
rm -f "$RESULTS_DIR"/clients_suite_*.log
rm -f "$RESULTS_DIR"/clients_suite_*.jsonl
log_success "Removed suite result files from $RESULTS_DIR"

log_success "Perf cleanup complete"
