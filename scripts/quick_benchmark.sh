#!/bin/bash
# Quick benchmark runner for iterative development
# Runs shorter benchmarks for faster feedback

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# Colors
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m'

cd "$PROJECT_ROOT"

echo -e "${BLUE}Quick Benchmark Run${NC}"
echo "======================================"
echo ""

# Run key benchmarks with shorter time
echo -e "${YELLOW}1. Message Distribution (1000 subscribers)${NC}"
go test -bench="BenchmarkMessagePublish_MultipleSubscribers/1000" \
    -benchtime=3s -benchmem ./broker 2>/dev/null | grep -E "Benchmark|ns/op"

echo ""
echo -e "${YELLOW}2. Router Performance${NC}"
go test -bench="BenchmarkRouter" \
    -benchtime=3s -benchmem ./broker/router 2>/dev/null | grep -E "Benchmark|ns/op"

echo ""
echo -e "${YELLOW}3. Queue Operations${NC}"
go test -bench="BenchmarkEnqueue" \
    -benchtime=3s -benchmem ./queue 2>/dev/null | grep -E "Benchmark|ns/op"
go test -bench="BenchmarkDequeue" \
    -benchtime=3s -benchmem ./queue 2>/dev/null | grep -E "Benchmark|ns/op"

echo ""
echo -e "${YELLOW}4. Zero-Copy vs Legacy${NC}"
go test -bench="BenchmarkMessageCopy" \
    -benchtime=3s -benchmem ./broker 2>/dev/null | grep -E "Benchmark|ns/op"

echo ""
echo -e "${GREEN}✓ Quick benchmark complete${NC}"
