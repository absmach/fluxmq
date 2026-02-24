#!/usr/bin/env bash
# Copyright (c) Abstract Machines
# SPDX-License-Identifier: Apache-2.0
#
# Stop a local 3-node FluxMQ cluster started by local.sh.

set -euo pipefail

PID_FILE="/tmp/fluxmq/pids"

RED='\033[0;31m'
GREEN='\033[0;32m'
BLUE='\033[0;34m'
NC='\033[0m'

log_info()  { echo -e "${BLUE}[INFO]${NC} $*"; }
log_ok()    { echo -e "${GREEN}[OK]${NC} $*"; }
log_error() { echo -e "${RED}[ERROR]${NC} $*" >&2; }

if [[ ! -f "$PID_FILE" ]]; then
	log_info "No PID file found at $PID_FILE — nothing to stop."
	exit 0
fi

while IFS= read -r pid; do
	pid="$(echo "$pid" | xargs)"
	[[ -z "$pid" ]] && continue
	if kill -0 "$pid" 2>/dev/null; then
		log_info "Sending SIGTERM to PID $pid"
		kill "$pid"
	else
		log_info "PID $pid already exited"
	fi
done < "$PID_FILE"

log_info "Waiting for processes to exit..."
while IFS= read -r pid; do
	pid="$(echo "$pid" | xargs)"
	[[ -z "$pid" ]] && continue
	# Wait up to 10s for each process
	for _ in $(seq 1 10); do
		kill -0 "$pid" 2>/dev/null || break
		sleep 1
	done
	if kill -0 "$pid" 2>/dev/null; then
		log_info "Force killing PID $pid"
		kill -9 "$pid" 2>/dev/null || true
	fi
done < "$PID_FILE"

rm -f "$PID_FILE"
log_ok "All nodes stopped."
