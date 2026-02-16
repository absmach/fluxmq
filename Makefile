# MQTT Broker Makefile

# Build configuration
BUILD_DIR := build
BINARY := fluxmq
GO := go

# Build flags
LDFLAGS := -s -w
GOFLAGS := -trimpath
DOCKER_IMAGE_LATEST := ghcr.io/absmach/fluxmq:latest
DOCKER_IMAGE_GIT := ghcr.io/absmach/fluxmq:$(shell git describe --tags --always --dirty)
PERF_SCRIPT_DIR := tests/perf/scripts


# Default target
.PHONY: all
all: build

# Build the broker binary
.PHONY: build
build: $(BUILD_DIR)/$(BINARY)

$(BUILD_DIR)/$(BINARY): cmd/main.go $(shell find . -name '*.go' -not -path './build/*')
	@mkdir -p $(BUILD_DIR)
	$(GO) build $(GOFLAGS) -ldflags "$(LDFLAGS)" -o $(BUILD_DIR)/$(BINARY) ./cmd

# Build Docker image (latest tag)
.PHONY: docker
docker:
	docker build -f docker/Dockerfile -t $(DOCKER_IMAGE_LATEST) .

# Build Docker image tagged with the current git tag/sha
.PHONY: docker-latest
docker-latest:
	docker build -f docker/Dockerfile -t $(DOCKER_IMAGE_GIT) .

# Run the broker (uses default configuration)
.PHONY: run
run: build
	$(BUILD_DIR)/$(BINARY)

# Run with custom config file
.PHONY: run-config
run-config: build
	$(BUILD_DIR)/$(BINARY) -config config.yaml

# Run with debug logging (uses default config with debug level override via env)
.PHONY: run-debug
run-debug: build
	MQTT_LOG_LEVEL=debug $(BUILD_DIR)/$(BINARY)

# Run tests
.PHONY: test
test:
	$(GO) test -short -race -failfast -timeout 3m -v ./...

# Run client package unit tests (MQTT + AMQP 0.9.1 client packages).
.PHONY: test-client
test-client:
	# No containers required.
	$(GO) test ./client/... -count=1

# Run client integration tests (currently AMQP integration tests with RabbitMQ).
.PHONY: test-client-integration
test-client-integration:
	# Requires Docker daemon access.
	# Uses tests tagged with `integration` in client/amqp/rabbitmq_integration_test.go.
	# Optional override: FLUXMQ_AMQP_TEST_IMAGE=rabbitmq:3.13-alpine
	# Optional override: FLUXMQ_AMQP_TEST_URL=amqp://guest:guest@127.0.0.1:5672/
	# Optional override: FLUXMQ_AMQP_TEST_HOST=<docker-host-ip-or-name>
	$(GO) test ./client/... -tags=integration -run RabbitMQ -count=1 -v -timeout 10m

# Run full tests (including stress)
.PHONY: test-full
test-full:
	$(GO) test -race -count=1 -v -timeout 30m ./...

# Run tests with coverage
.PHONY: test-cover
test-cover:
	$(GO) test -coverprofile=$(BUILD_DIR)/coverage.out ./...
	$(GO) tool cover -html=$(BUILD_DIR)/coverage.out -o $(BUILD_DIR)/coverage.html

# Run benchmarks
.PHONY: bench
bench:
	$(GO) test -bench=. -benchmem -run=^$$ ./...

# Run benchmarks for broker only
.PHONY: bench-broker
bench-broker:
	$(GO) test -bench=. -benchmem -run=^$$ ./mqtt/broker

# Run zero-copy comparison benchmarks
.PHONY: bench-zerocopy
bench-zerocopy:
	$(GO) test -bench=BenchmarkMessageCopy -benchmem -run=^$$ ./mqtt/broker

# Run stress tests (all)
.PHONY: stress
stress:
	$(GO) test -v -run=TestStress -timeout=30m ./mqtt/broker

# Run specific stress tests
.PHONY: stress-throughput
stress-throughput:
	$(GO) test -v -run=TestStress_HighThroughputPublish -timeout=5m ./mqtt/broker

.PHONY: stress-concurrent
stress-concurrent:
	$(GO) test -v -run=TestStress_ConcurrentPublishers -timeout=5m ./mqtt/broker

.PHONY: stress-memory
stress-memory:
	$(GO) test -v -run=TestStress_MemoryPressure -timeout=5m ./mqtt/broker

.PHONY: stress-sustained
stress-sustained:
	$(GO) test -v -run=TestStress_SustainedLoad -timeout=5m ./mqtt/broker

.PHONY: stress-pool
stress-pool:
	$(GO) test -v -run=TestStress_BufferPoolExhaustion -timeout=5m ./mqtt/broker

.PHONY: stress-fanout
stress-fanout:
	$(GO) test -v -run=TestStress_FanOutExtreme -timeout=5m ./mqtt/broker

.PHONY: stress-churn
stress-churn:
	$(GO) test -v -run=TestStress_RapidSubscribeUnsubscribe -timeout=5m ./mqtt/broker

# Generate benchmark report
.PHONY: bench-report
bench-report:
	@mkdir -p $(BUILD_DIR)
	$(GO) test -bench=. -benchmem -run=^$$ ./mqtt/broker | tee $(BUILD_DIR)/benchmark-results.txt
	@echo ""
	@echo "Benchmark results saved to $(BUILD_DIR)/benchmark-results.txt"

# Performance suites (scripts under tests/perf/scripts)
.PHONY: perf-smoke
perf-smoke:
	bash $(PERF_SCRIPT_DIR)/run_smoke.sh

.PHONY: perf-load
perf-load:
	bash $(PERF_SCRIPT_DIR)/run_load.sh

.PHONY: perf-soak
perf-soak:
	bash $(PERF_SCRIPT_DIR)/run_soak.sh

.PHONY: perf-compare
perf-compare:
	@if [ -z "$(BASELINE)" ]; then \
		echo "Usage: make perf-compare BASELINE=<baseline-bench-file> [CANDIDATE=<candidate-bench-file>]"; \
		exit 1; \
	fi
	bash $(PERF_SCRIPT_DIR)/compare_bench.sh "$(BASELINE)" "$(CANDIDATE)"

.PHONY: perf-cluster-up
perf-cluster-up:
	bash $(PERF_SCRIPT_DIR)/cluster_up.sh

.PHONY: perf-cluster-down
perf-cluster-down:
	bash $(PERF_SCRIPT_DIR)/cluster_down.sh

.PHONY: perf-cluster-reset
perf-cluster-reset:
	bash $(PERF_SCRIPT_DIR)/cluster_reset.sh

.PHONY: perf-cluster-ps
perf-cluster-ps:
	bash $(PERF_SCRIPT_DIR)/cluster_ps.sh

.PHONY: perf-cluster-logs
perf-cluster-logs:
	bash $(PERF_SCRIPT_DIR)/cluster_logs.sh $(SERVICE)

# 3-node Docker Compose cluster (shared with perf scripts)
.PHONY: run-cluster
run-cluster: perf-cluster-up

.PHONY: run-cluster-down
run-cluster-down: perf-cluster-down

.PHONY: run-cluster-clean
run-cluster-clean: perf-cluster-reset

.PHONY: run-cluster-ps
run-cluster-ps: perf-cluster-ps

.PHONY: run-cluster-logs
run-cluster-logs: perf-cluster-logs

# Run linter
.PHONY: lint
lint:
	golangci-lint run ./...

# Format code
.PHONY: fmt
fmt:
	$(GO) fmt ./...
	goimports -w .

# Clean build artifacts
.PHONY: clean
clean:
	rm -rf $(BUILD_DIR)

# Install dependencies
.PHONY: deps
deps:
	$(GO) mod tidy
	$(GO) mod download

# Run examples
.PHONY: run-no-cluster
run-no-cluster: build
	$(BUILD_DIR)/$(BINARY) -config examples/no-cluster.yaml

.PHONY: run-single-cluster
run-single-cluster: build
	$(BUILD_DIR)/$(BINARY) -config examples/single-node-cluster.yaml

.PHONY: run-node1
run-node1: build
	$(BUILD_DIR)/$(BINARY) -config examples/node1.yaml

.PHONY: run-node2
run-node2: build
	$(BUILD_DIR)/$(BINARY) -config examples/node2.yaml

.PHONY: run-node3
run-node3: build
	$(BUILD_DIR)/$(BINARY) -config examples/node3.yaml

# Clean all temporary data directories
.PHONY: clean-data
clean-data:
	rm -rf /tmp/fluxmq

.PHONY: proto
proto:
	buf generate

.PHONY: proto-lint
proto-lint:
	buf lint

.PHONY: proto-breaking
proto-breaking:
	buf breaking --against '.git#branch=main'

# Show help
.PHONY: help
help:
	@echo "MQTT Broker Makefile"
	@echo ""
	@echo "Build Targets:"
	@echo "  build              Build the broker binary to $(BUILD_DIR)/$(BINARY)"
	@echo "  run                Build and run the broker with default config"
	@echo "  run-config         Build and run with config.yaml"
	@echo "  run-debug          Build and run with debug logging"
	@echo ""
	@echo "Example Configurations:"
	@echo "  run-no-cluster     Run single node without clustering"
	@echo "  run-single-cluster Run single node with clustering enabled"
	@echo "  run-node1          Run first node of 3-node cluster (bootstrap)"
	@echo "  run-node2          Run second node of 3-node cluster"
	@echo "  run-node3          Run third node of 3-node cluster"
	@echo "  run-cluster        Start Docker Compose 3-node cluster (docker/docker-compose-cluster.yaml)"
	@echo "  run-cluster-down   Stop Docker Compose cluster containers"
	@echo "  run-cluster-clean  Stop cluster and remove mapped volumes"
	@echo "  run-cluster-ps     Show cluster container status"
	@echo "  run-cluster-logs   Show cluster logs (optional SERVICE=node1|node2|node3)"
	@echo ""
	@echo "  NOTE: For 3-node cluster, run node1, node2, and node3 in separate terminals"
	@echo "  NOTE: examples/node{1,2,3}.yaml include per-queue Raft replication groups (including an auto-provisioned group)"
	@echo ""
	@echo "Testing Targets:"
	@echo "  test               Run all tests"
	@echo "  test-client        Run MQTT/AMQP client package unit tests"
	@echo "  test-client-integration Run client integration tests (Docker-backed RabbitMQ)"
	@echo "  test-cover         Run tests with coverage report"
	@echo "  lint               Run golangci-lint"
	@echo ""
	@echo "Benchmark Targets:"
	@echo "  bench              Run all benchmarks"
	@echo "  bench-broker       Run broker benchmarks only"
	@echo "  bench-zerocopy     Run zero-copy vs legacy comparison"
	@echo "  bench-report       Generate benchmark report to $(BUILD_DIR)/"
	@echo "  perf-smoke         Run quick perf smoke suite (tests + short benchmarks)"
	@echo "  perf-load          Run medium perf load suite"
	@echo "  perf-soak          Run long-running soak suite"
	@echo "  perf-compare       Compare benchmark files with benchstat"
	@echo "                     Usage: make perf-compare BASELINE=<file> [CANDIDATE=<file>]"
	@echo "  perf-cluster-up    Start 3-node perf cluster in Docker Compose"
	@echo "  perf-cluster-down  Stop perf cluster containers"
	@echo "  perf-cluster-reset Stop cluster and remove volumes"
	@echo "  perf-cluster-ps    Show perf cluster container status"
	@echo "  perf-cluster-logs  Show cluster logs (optional SERVICE=node1|node2|node3)"
	@echo ""
	@echo "Stress Test Targets:"
	@echo "  stress             Run all stress tests (~30 min)"
	@echo "  stress-throughput  High throughput test (10K msg/s)"
	@echo "  stress-concurrent  Concurrent publishers test"
	@echo "  stress-memory      Memory pressure test (large messages)"
	@echo "  stress-sustained   Sustained mixed load test (30s)"
	@echo "  stress-pool        Buffer pool exhaustion test"
	@echo "  stress-fanout      Extreme fan-out test (5K subscribers)"
	@echo "  stress-churn       Subscription churn test"
	@echo ""
	@echo "Utility Targets:"
	@echo "  fmt                Format code"
	@echo "  clean              Remove build artifacts"
	@echo "  clean-data         Remove all /tmp/fluxmq data directories"
	@echo "  deps               Download and tidy dependencies"
	@echo "  docker             Build Docker image ($(DOCKER_IMAGE_LATEST))"
	@echo "  docker-latest      Build Docker image ($(DOCKER_IMAGE_GIT))"
	@echo "  help               Show this help message"
