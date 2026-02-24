#!/usr/bin/env bash
# Copyright (c) Abstract Machines
# SPDX-License-Identifier: Apache-2.0
#
# Start a 3-node FluxMQ cluster as local processes.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
BINARY="$PROJECT_ROOT/build/fluxmq"
PID_FILE="/tmp/fluxmq/pids"
HEALTH_PORTS=(8081 8082 8083)
HEALTH_TIMEOUT="${HEALTH_TIMEOUT:-30}"

RED='\033[0;31m'
GREEN='\033[0;32m'
BLUE='\033[0;34m'
NC='\033[0m'

log_info()  { echo -e "${BLUE}[INFO]${NC} $*"; }
log_ok()    { echo -e "${GREEN}[OK]${NC} $*"; }
log_error() { echo -e "${RED}[ERROR]${NC} $*" >&2; }

cleanup() {
	log_info "Caught signal, stopping nodes..."
	bash "$SCRIPT_DIR/local-stop.sh"
	exit 0
}
trap cleanup SIGINT SIGTERM

if [[ ! -x "$BINARY" ]]; then
	log_info "Building fluxmq..."
	(cd "$PROJECT_ROOT" && go build -trimpath -ldflags "-s -w" -o build/fluxmq ./cmd)
fi

for n in 1 2 3; do
	mkdir -p "/tmp/fluxmq/node${n}/data" "/tmp/fluxmq/node${n}/etcd"
done

log_info "Starting 3-node cluster..."

PIDS=()
for n in 1 2 3; do
	"$BINARY" -config "$SCRIPT_DIR/config/node${n}.yaml" &
	PIDS+=($!)
	log_info "Started node${n} (PID ${PIDS[-1]})"
done

printf '%s\n' "${PIDS[@]}" > "$PID_FILE"

log_info "Waiting for health endpoints (timeout ${HEALTH_TIMEOUT}s)..."
for port in "${HEALTH_PORTS[@]}"; do
	elapsed=0
	while (( elapsed < HEALTH_TIMEOUT )); do
		if curl -fsS "http://127.0.0.1:${port}/ready" 2>/dev/null | grep -q '"status":"ready"'; then
			log_ok "127.0.0.1:${port} ready"
			break
		fi
		sleep 1
		elapsed=$((elapsed + 1))
	done
	if (( elapsed >= HEALTH_TIMEOUT )); then
		log_error "Health check timed out on port ${port}"
		bash "$SCRIPT_DIR/local-stop.sh"
		exit 1
	fi
done

log_ok "Cluster is up (PIDs: ${PIDS[*]})"
log_info "Stop with: make cluster-down  (or deployments/cluster/local-stop.sh)"

wait
