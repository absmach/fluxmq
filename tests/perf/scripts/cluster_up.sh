#!/usr/bin/env bash
# Copyright (c) Abstract Machines
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=tests/perf/scripts/common.sh
source "$SCRIPT_DIR/common.sh"

require_cmd docker

PERF_CLUSTER_WAIT_READY="${PERF_CLUSTER_WAIT_READY:-1}"
PERF_CLUSTER_READY_TIMEOUT="${PERF_CLUSTER_READY_TIMEOUT:-180}"
LOG_FILE="$RESULTS_DIR/cluster_up_${TIMESTAMP}.log"

log_info "Bringing up perf cluster"
log_info "Compose: $COMPOSE_FILE"
log_info "Log file: $LOG_FILE"
write_header "$LOG_FILE"

run_and_log "$LOG_FILE" compose up -d

if [[ "$PERF_CLUSTER_WAIT_READY" != "1" ]]; then
	log_warn "Skipping readiness wait (PERF_CLUSTER_WAIT_READY=$PERF_CLUSTER_WAIT_READY)"
	log_success "Cluster is up"
	exit 0
fi

if ! command -v curl >/dev/null 2>&1; then
	log_warn "curl not found; skipping readiness checks"
	log_success "Cluster is up"
	exit 0
fi

deadline=$(( "$(date +%s)" + PERF_CLUSTER_READY_TIMEOUT ))
ports=(18081 18082 18083)

while [[ "$(date +%s)" -lt "$deadline" ]]; do
	ps_out="$(compose ps)"
	if echo "$ps_out" | grep -Eq 'Restarting|Exited|Dead'; then
		log_error "Perf cluster contains non-running services"
		echo "$ps_out" | tee -a "$LOG_FILE"
		run_and_log "$LOG_FILE" compose logs --tail=200
		exit 1
	fi

	all_ready=1
	for port in "${ports[@]}"; do
		if ! curl -fsS "http://127.0.0.1:${port}/ready" 2>/dev/null | grep -q '"status":"ready"'; then
			all_ready=0
			break
		fi
	done

	if [[ "$all_ready" == "1" ]]; then
		log_success "Perf cluster is ready"
		exit 0
	fi

	sleep 2
done

log_error "Timed out waiting for perf cluster readiness after ${PERF_CLUSTER_READY_TIMEOUT}s"
run_and_log "$LOG_FILE" compose ps
run_and_log "$LOG_FILE" compose logs --tail=200
exit 1
