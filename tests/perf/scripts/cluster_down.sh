#!/usr/bin/env bash
# Copyright (c) Abstract Machines
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=tests/perf/scripts/common.sh
source "$SCRIPT_DIR/common.sh"

require_cmd docker

LOG_FILE="$RESULTS_DIR/cluster_down_${TIMESTAMP}.log"
log_info "Stopping perf cluster"
log_info "Compose: $COMPOSE_FILE"
log_info "Log file: $LOG_FILE"
write_header "$LOG_FILE"

run_and_log "$LOG_FILE" compose down --remove-orphans
log_success "Perf cluster stopped"

