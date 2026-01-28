#!/bin/bash
# Copyright (c) Abstract Machines
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
BENCH_DIR="$PROJECT_ROOT/benchmarks"
RESULTS_DIR="$BENCH_DIR/results"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
RESULTS_FILE="$RESULTS_DIR/benchmark_${TIMESTAMP}.txt"

# Create results directory
mkdir -p "$RESULTS_DIR"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}======================================"
echo -e "MQTT Broker Benchmark Suite"
echo -e "======================================${NC}"
echo ""
echo -e "${GREEN}Timestamp:${NC} $TIMESTAMP"
echo -e "${GREEN}Results:${NC} $RESULTS_FILE"
echo ""

# System info
echo -e "${YELLOW}System Information:${NC}" | tee -a "$RESULTS_FILE"
echo "=====================================" | tee -a "$RESULTS_FILE"
uname -a | tee -a "$RESULTS_FILE"
echo "" | tee -a "$RESULTS_FILE"
echo "CPU Information:" | tee -a "$RESULTS_FILE"
lscpu | grep -E "Model name|CPU\(s\)|Thread|Core" | tee -a "$RESULTS_FILE"
echo "" | tee -a "$RESULTS_FILE"
echo "Memory Information:" | tee -a "$RESULTS_FILE"
free -h | tee -a "$RESULTS_FILE"
echo "" | tee -a "$RESULTS_FILE"
echo "Go Version:" | tee -a "$RESULTS_FILE"
go version | tee -a "$RESULTS_FILE"
echo "" | tee -a "$RESULTS_FILE"
echo "=====================================" | tee -a "$RESULTS_FILE"
echo "" | tee -a "$RESULTS_FILE"

cd "$PROJECT_ROOT"

# Function to run benchmarks for a package
run_package_benchmarks() {
    local pkg=$1
    local name=$2

    echo -e "${BLUE}Running benchmarks: ${name}${NC}"
    echo "=====================================" | tee -a "$RESULTS_FILE"
    echo "Package: $pkg" | tee -a "$RESULTS_FILE"
    echo "=====================================" | tee -a "$RESULTS_FILE"

    # Run benchmarks with memory stats
    go test -bench=. -benchmem -benchtime=10s -timeout=30m "$pkg" 2>&1 | tee -a "$RESULTS_FILE"

    echo "" | tee -a "$RESULTS_FILE"
    echo "" | tee -a "$RESULTS_FILE"
}

# Function to run benchmarks with profiling
run_with_profiling() {
    local pkg=$1
    local bench_pattern=$2
    local name=$3

    echo -e "${BLUE}Running with profiling: ${name}${NC}"

    local profile_dir="$RESULTS_DIR/profiles_${TIMESTAMP}"
    mkdir -p "$profile_dir"

    # CPU profile
    echo -e "${YELLOW}  - CPU profiling...${NC}"
    go test -bench="$bench_pattern" \
        -benchtime=30s \
        -cpuprofile="$profile_dir/${name}_cpu.prof" \
        -memprofile="$profile_dir/${name}_mem.prof" \
        -mutexprofile="$profile_dir/${name}_mutex.prof" \
        "$pkg" > /dev/null 2>&1

    echo -e "${GREEN}  ✓ Profiles saved to: $profile_dir${NC}"
    echo ""
}

# 1. Core packet encoding/decoding benchmarks
run_package_benchmarks "./mqtt/packets/v5" "MQTT v5 Packet Encoding/Decoding"

# 2. Router benchmarks
run_package_benchmarks "./broker/router" "Topic Router Performance"

# 3. Message distribution benchmarks
run_package_benchmarks "./broker" "Message Distribution & Broker Core"

# 4. Queue benchmarks
echo -e "${BLUE}Running Queue Benchmarks${NC}"
run_package_benchmarks "./queue" "Queue Operations"

# 5. Storage benchmarks (if they exist)
if [ -d "$PROJECT_ROOT/storage/badger" ]; then
    run_package_benchmarks "./storage/badger" "BadgerDB Storage Backend"
fi

if [ -d "$PROJECT_ROOT/storage/memory" ]; then
    run_package_benchmarks "./storage/memory" "In-Memory Storage Backend"
fi

# Run profiling on critical benchmarks
echo ""
echo -e "${YELLOW}======================================"
echo -e "Running Profiled Benchmarks"
echo -e "======================================${NC}"
echo ""

run_with_profiling "./broker" "BenchmarkMessagePublish_MultipleSubscribers/1000" "message_fanout_1000"
run_with_profiling "./broker" "BenchmarkMessagePublish_FanOut" "message_fanout"
run_with_profiling "./broker/router" "BenchmarkRouter" "router"
run_with_profiling "./queue" "BenchmarkEnqueue" "queue_enqueue"
run_with_profiling "./queue" "BenchmarkDequeue" "queue_dequeue"

# Generate summary
echo ""
echo -e "${YELLOW}======================================"
echo -e "Benchmark Summary"
echo -e "======================================${NC}"
echo ""

# Extract key metrics
echo "Extracting key performance metrics..." | tee -a "$RESULTS_FILE"
echo "" | tee -a "$RESULTS_FILE"

# Parse results and create summary
parse_results() {
    local results_file=$1

    echo "TOP BENCHMARKS BY THROUGHPUT:" | tee -a "${results_file}.summary"
    echo "=====================================" | tee -a "${results_file}.summary"
    grep "ns/op" "$results_file" | \
        awk '{print $1, $3, $4}' | \
        sort -k2 -n | \
        head -20 | tee -a "${results_file}.summary"

    echo "" | tee -a "${results_file}.summary"
    echo "HIGH ALLOCATION BENCHMARKS:" | tee -a "${results_file}.summary"
    echo "=====================================" | tee -a "${results_file}.summary"
    grep "B/op" "$results_file" | \
        awk '{print $1, $5, $6}' | \
        sort -k2 -n -r | \
        head -20 | tee -a "${results_file}.summary"

    echo "" | tee -a "${results_file}.summary"
    echo "HIGH ALLOCS/OP BENCHMARKS:" | tee -a "${results_file}.summary"
    echo "=====================================" | tee -a "${results_file}.summary"
    grep "allocs/op" "$results_file" | \
        awk '{print $1, $7, $8}' | \
        sort -k2 -n -r | \
        head -20 | tee -a "${results_file}.summary"
}

parse_results "$RESULTS_FILE"

echo ""
echo -e "${GREEN}======================================"
echo -e "Benchmarks Complete!"
echo -e "======================================${NC}"
echo ""
echo -e "${GREEN}Results:${NC} $RESULTS_FILE"
echo -e "${GREEN}Summary:${NC} ${RESULTS_FILE}.summary"
echo -e "${GREEN}Profiles:${NC} $RESULTS_DIR/profiles_${TIMESTAMP}/"
echo ""
echo -e "${YELLOW}To analyze CPU profile:${NC}"
echo "  go tool pprof $RESULTS_DIR/profiles_${TIMESTAMP}/message_fanout_1000_cpu.prof"
echo ""
echo -e "${YELLOW}To analyze memory profile:${NC}"
echo "  go tool pprof $RESULTS_DIR/profiles_${TIMESTAMP}/message_fanout_1000_mem.prof"
echo ""
echo -e "${YELLOW}To analyze mutex contention:${NC}"
echo "  go tool pprof $RESULTS_DIR/profiles_${TIMESTAMP}/message_fanout_1000_mutex.prof"
echo ""
echo -e "${YELLOW}To compare with previous run:${NC}"
echo "  benchstat <old_results> $RESULTS_FILE"
echo ""
