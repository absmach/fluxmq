#!/usr/bin/env bash
# Copyright (c) Abstract Machines
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=tests/perf/scripts/common.sh
source "$SCRIPT_DIR/common.sh"

require_cmd go
require_cmd curl

SUITE_NAME="${PERF_SUITE:-all}"
SUITE_FILE="${PERF_SUITE_FILE:-$PROJECT_ROOT/tests/perf/suites/${SUITE_NAME}.txt}"
SCENARIOS_RAW="${PERF_SCENARIOS:-}"
SIZES_RAW="${PERF_MESSAGE_SIZES:-small,medium,large}"
MESSAGE_SIZE_BYTES="${PERF_MESSAGE_SIZE_BYTES:-}"

MQTT_ADDRS="${PERF_MQTT_ADDRS:-127.0.0.1:11883,127.0.0.1:11884,127.0.0.1:11885}"
AMQP_ADDRS="${PERF_AMQP_ADDRS:-127.0.0.1:15682,127.0.0.1:15683,127.0.0.1:15684}"
MIN_RATIO="${PERF_MIN_RATIO:-0.95}"
QUEUE_MIN_RATIO="${PERF_QUEUE_MIN_RATIO:-0.99}"
DRAIN_TIMEOUT="${PERF_DRAIN_TIMEOUT:-45s}"
SKIP_READY_CHECK="${PERF_SKIP_READY_CHECK:-0}"

PUBLISHERS="${PERF_PUBLISHERS:-}"
SUBSCRIBERS="${PERF_SUBSCRIBERS:-}"
CONSUMER_GROUPS="${PERF_CONSUMER_GROUPS:-}"
CONSUMERS_PER_GROUP="${PERF_CONSUMERS_PER_GROUP:-}"
MESSAGES_PER_PUBLISHER="${PERF_MESSAGES_PER_PUBLISHER:-}"
PUBLISH_INTERVAL="${PERF_PUBLISH_INTERVAL:-}"

LOG_FILE="$RESULTS_DIR/clients_suite_${SUITE_NAME}_${TIMESTAMP}.log"
JSON_FILE="$RESULTS_DIR/clients_suite_${SUITE_NAME}_${TIMESTAMP}.jsonl"

log_info "Starting load test suite"
log_info "Suite: $SUITE_NAME"
log_info "Suite file: $SUITE_FILE"
log_info "Message sizes: ${MESSAGE_SIZE_BYTES:-$SIZES_RAW}"
log_info "MQTT addrs: $MQTT_ADDRS"
log_info "AMQP addrs: $AMQP_ADDRS"
log_info "Log file: $LOG_FILE"
log_info "Result JSONL: $JSON_FILE"
write_header "$LOG_FILE"

if [[ "$SKIP_READY_CHECK" != "1" ]]; then
	for port in 18081 18082 18083; do
		if ! curl -fsS "http://127.0.0.1:${port}/ready" 2>/dev/null | grep -q '"status":"ready"'; then
			log_error "Cluster readiness check failed on port $port. Start cluster with: make run-cluster"
			exit 1
		fi
	done
fi

load_scenarios_from_file() {
	if [[ ! -f "$SUITE_FILE" ]]; then
		log_error "Suite file not found: $SUITE_FILE"
		exit 1
	fi
	mapfile -t SCENARIOS < <(sed -e 's/#.*$//' -e '/^\s*$/d' "$SUITE_FILE" | xargs -n1)
}

if [[ -n "$SCENARIOS_RAW" ]]; then
	IFS=',' read -r -a SCENARIOS <<< "$SCENARIOS_RAW"
else
	load_scenarios_from_file
fi

if [[ -n "$MESSAGE_SIZE_BYTES" ]]; then
	SIZES=("$MESSAGE_SIZE_BYTES")
else
	IFS=',' read -r -a SIZES <<< "$SIZES_RAW"
fi

FAILURES=0
TOTAL=0

for scenario in "${SCENARIOS[@]}"; do
	scenario="$(echo "$scenario" | xargs)"
	if [[ -z "$scenario" ]]; then
		continue
	fi

	for size in "${SIZES[@]}"; do
		size="$(echo "$size" | xargs)"
		if [[ -z "$size" ]]; then
			continue
		fi

		TOTAL=$((TOTAL + 1))
		log_info "Running scenario=$scenario msg_size=${size}"

		set +e
		(
			cd "$PROJECT_ROOT"
			cmd=(go run ./tests/perf/loadgen
				-scenario "$scenario"
				-mqtt-addrs "$MQTT_ADDRS"
				-amqp-addrs "$AMQP_ADDRS"
				-min-ratio "$MIN_RATIO"
				-queue-min-ratio "$QUEUE_MIN_RATIO"
				-drain-timeout "$DRAIN_TIMEOUT"
				-json-out "$JSON_FILE")

			if [[ "$size" =~ ^[0-9]+$ ]]; then
				cmd+=( -payload-bytes "$size" )
			else
				cmd+=( -payload "$size" )
			fi

			if [[ -n "$PUBLISHERS" ]]; then cmd+=( -publishers "$PUBLISHERS" ); fi
			if [[ -n "$SUBSCRIBERS" ]]; then cmd+=( -subscribers "$SUBSCRIBERS" ); fi
			if [[ -n "$CONSUMER_GROUPS" ]]; then cmd+=( -consumer-groups "$CONSUMER_GROUPS" ); fi
			if [[ -n "$CONSUMERS_PER_GROUP" ]]; then cmd+=( -consumers-per-group "$CONSUMERS_PER_GROUP" ); fi
			if [[ -n "$MESSAGES_PER_PUBLISHER" ]]; then cmd+=( -messages-per-publisher "$MESSAGES_PER_PUBLISHER" ); fi
			if [[ -n "$PUBLISH_INTERVAL" ]]; then cmd+=( -publish-interval "$PUBLISH_INTERVAL" ); fi

			echo ">> ${cmd[*]}" | tee -a "$LOG_FILE"
			"${cmd[@]}" 2>&1 | tee -a "$LOG_FILE"
		)
		RC=$?
		set -e

		if [[ $RC -ne 0 ]]; then
			FAILURES=$((FAILURES + 1))
			log_warn "Scenario failed: scenario=$scenario msg_size=$size (rc=$RC)"
		else
			log_success "Scenario passed: scenario=$scenario msg_size=$size"
		fi
	done
done

if [[ $TOTAL -eq 0 ]]; then
	log_error "No scenarios were executed (check PERF_SCENARIOS / suite file)"
	exit 1
fi

log_info "Suite summary table"
run_and_log "$LOG_FILE" go run ./tests/perf/report -input "$JSON_FILE"

if [[ $FAILURES -gt 0 ]]; then
	log_error "Suite finished with failures: $FAILURES/$TOTAL"
	log_error "Inspect logs: $LOG_FILE"
	exit 1
fi

log_success "Suite finished successfully: $TOTAL/$TOTAL"
log_success "Results JSONL: $JSON_FILE"
