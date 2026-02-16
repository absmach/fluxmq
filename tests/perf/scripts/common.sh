#!/usr/bin/env bash
# Copyright (c) Abstract Machines
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PERF_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
PROJECT_ROOT="$(cd "$PERF_ROOT/../.." && pwd)"
RESULTS_DIR="${PERF_RESULTS_DIR:-$PERF_ROOT/results}"
COMPOSE_FILE="${PERF_COMPOSE_FILE:-$PERF_ROOT/compose/compose.yaml}"
TIMESTAMP="$(date +%Y%m%d_%H%M%S)"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

mkdir -p "$RESULTS_DIR"

log_info() {
	echo -e "${BLUE}[INFO]${NC} $*"
}

log_warn() {
	echo -e "${YELLOW}[WARN]${NC} $*"
}

log_error() {
	echo -e "${RED}[ERROR]${NC} $*" >&2
}

log_success() {
	echo -e "${GREEN}[OK]${NC} $*"
}

require_cmd() {
	if ! command -v "$1" >/dev/null 2>&1; then
		log_error "Missing required command: $1"
		exit 1
	fi
}

ensure_compose_file() {
	if [[ ! -f "$COMPOSE_FILE" ]]; then
		log_error "Compose file not found: $COMPOSE_FILE"
		exit 1
	fi
}

compose() {
	ensure_compose_file
	if docker compose version >/dev/null 2>&1; then
		docker compose -f "$COMPOSE_FILE" "$@"
		return
	fi
	if command -v docker-compose >/dev/null 2>&1; then
		docker-compose -f "$COMPOSE_FILE" "$@"
		return
	fi
	log_error "Docker Compose is not available (docker compose / docker-compose)"
	exit 1
}

write_header() {
	local log_file="$1"
	{
		echo "timestamp: $TIMESTAMP"
		echo "project_root: $PROJECT_ROOT"
		echo "go_version: $(go version)"
		echo "uname: $(uname -a)"
		echo "----------------------------------------"
	} | tee -a "$log_file"
}

run_and_log() {
	local log_file="$1"
	shift
	(
		cd "$PROJECT_ROOT"
		echo ">> $*" | tee -a "$log_file"
		"$@" 2>&1 | tee -a "$log_file"
	)
}

run_bench_suite() {
	local bench_time="$1"
	local count="$2"
	local out_file="$3"

	run_and_log "$out_file" go test ./mqtt/broker \
		-run '^$' \
		-bench 'BenchmarkMessagePublish_(SingleSubscriber|MultipleSubscribers|QoS1|QoS2|FanOut)|BenchmarkMessageDistribute' \
		-benchmem \
		-benchtime "$bench_time" \
		-count "$count"

	run_and_log "$out_file" go test ./broker/router \
		-run '^$' \
		-bench 'BenchmarkRouter_(Match_1000Subs|Match_10000Subs|Mixed_90Read_10Write|Realistic_95Read_5Write)' \
		-benchmem \
		-benchtime "$bench_time" \
		-count "$count"

	if [[ "${PERF_SKIP_NETWORK_TESTS:-0}" == "1" ]]; then
		log_warn "Skipping network-sensitive cluster benchmarks (PERF_SKIP_NETWORK_TESTS=1)"
		run_and_log "$out_file" go test ./cluster \
			-run '^$' \
			-bench 'BenchmarkSubscribersLookup_Trie_10k|BenchmarkQueueConsumersLookup_Indexed_50k' \
			-benchmem \
			-benchtime "$bench_time" \
			-count "$count"
	else
		run_and_log "$out_file" go test ./cluster \
			-run '^$' \
			-bench 'BenchmarkSubscribersLookup_Trie_10k|BenchmarkQueueConsumersLookup_Indexed_50k|BenchmarkCrossNode_(MessageLatency|Throughput)' \
			-benchmem \
			-benchtime "$bench_time" \
			-count "$count"
	fi
}
