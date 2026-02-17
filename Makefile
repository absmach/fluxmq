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
PERF_SCENARIO_CONFIG ?= $(CONFIG)


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
.PHONY: perf-suite
perf-suite:
	@bash -lc 'set -euo pipefail; \
		if [[ -z "$${PERF_SCENARIO_CONFIGS:-}" ]]; then \
			echo "Usage: make perf-suite PERF_SCENARIO_CONFIGS=<cfg1.json,cfg2.json,...> [PERF_MESSAGE_SIZES=small,medium,large|PERF_MESSAGE_SIZE_BYTES=<n>] [PERF_* overrides]"; \
			exit 1; \
		fi; \
		timestamp="$$(date +%Y%m%d_%H%M%S)"; \
		results_dir="$${PERF_RESULTS_DIR:-tests/perf/results}"; \
		mkdir -p "$$results_dir"; \
		json_file="$$results_dir/clients_suite_configs_$${timestamp}.jsonl"; \
		log_file="$$results_dir/clients_suite_configs_$${timestamp}.log"; \
		mqtt_addrs="$${PERF_MQTT_ADDRS:-127.0.0.1:11883,127.0.0.1:11884,127.0.0.1:11885}"; \
		amqp_addrs="$${PERF_AMQP_ADDRS:-127.0.0.1:15682,127.0.0.1:15683,127.0.0.1:15684}"; \
		min_ratio="$${PERF_MIN_RATIO:-0.95}"; \
		queue_min_ratio="$${PERF_QUEUE_MIN_RATIO:-0.99}"; \
		drain_timeout="$${PERF_DRAIN_TIMEOUT:-45s}"; \
		if [[ "$${PERF_SKIP_READY_CHECK:-0}" != "1" ]]; then \
			for port in 18081 18082 18083; do \
				if ! curl -fsS "http://127.0.0.1:$${port}/ready" 2>/dev/null | grep -q "\"status\":\"ready\""; then \
					echo "Cluster readiness check failed on port $$port. Start cluster with: make run-cluster"; \
					exit 1; \
				fi; \
			done; \
		fi; \
		IFS=, read -r -a configs <<< "$${PERF_SCENARIO_CONFIGS}"; \
		if [[ -n "$${PERF_MESSAGE_SIZE_BYTES:-}" ]]; then \
			IFS=, read -r -a sizes <<< "$${PERF_MESSAGE_SIZE_BYTES}"; \
		else \
			IFS=, read -r -a sizes <<< "$${PERF_MESSAGE_SIZES:-small,medium,large}"; \
		fi; \
		total=0; fail=0; \
		for cfg in "$${configs[@]}"; do \
			cfg="$$(echo "$$cfg" | xargs)"; \
			[[ -z "$$cfg" ]] && continue; \
			for size in "$${sizes[@]}"; do \
				size="$$(echo "$$size" | xargs)"; \
				[[ -z "$$size" ]] && continue; \
				total=$$((total+1)); \
				cmd=(go run ./tests/perf/loadgen \
					-scenario-config "$$cfg" \
					-mqtt-addrs "$$mqtt_addrs" \
					-amqp-addrs "$$amqp_addrs" \
					-min-ratio "$$min_ratio" \
					-queue-min-ratio "$$queue_min_ratio" \
					-drain-timeout "$$drain_timeout" \
					-json-out "$$json_file"); \
				if [[ "$$size" =~ ^[0-9]+$$ ]]; then cmd+=( -payload-bytes "$$size" ); else cmd+=( -payload "$$size" ); fi; \
				if [[ -n "$${PERF_PUBLISHERS:-}" ]]; then cmd+=( -publishers "$${PERF_PUBLISHERS}" ); fi; \
				if [[ -n "$${PERF_SUBSCRIBERS:-}" ]]; then cmd+=( -subscribers "$${PERF_SUBSCRIBERS}" ); fi; \
				if [[ -n "$${PERF_MESSAGES_PER_PUBLISHER:-}" ]]; then cmd+=( -messages-per-publisher "$${PERF_MESSAGES_PER_PUBLISHER}" ); fi; \
				if [[ -n "$${PERF_PUBLISH_INTERVAL:-}" ]]; then cmd+=( -publish-interval "$${PERF_PUBLISH_INTERVAL}" ); fi; \
				echo ">> $${cmd[*]}" | tee -a "$$log_file"; \
				if ! "$${cmd[@]}" 2>&1 | tee -a "$$log_file"; then \
					fail=$$((fail+1)); \
				fi; \
			done; \
		done; \
		if [[ $$total -eq 0 ]]; then \
			echo "No config scenarios were executed (check PERF_SCENARIO_CONFIGS)"; \
			exit 1; \
		fi; \
		echo "Results JSONL: $$json_file"; \
		echo "Log file: $$log_file"; \
		if [[ $$fail -gt 0 ]]; then \
			echo "Suite finished with failures: $$fail/$$total"; \
			exit 1; \
		fi; \
		echo "Suite finished successfully: $$total/$$total"'

.PHONY: run-perf
run-perf:
	@if [ -z "$(PERF_SCENARIO_CONFIG)" ]; then \
		echo "Usage: make run-perf PERF_SCENARIO_CONFIG=<path-to-config.json> [or CONFIG=<path>] [PERF_PAYLOAD=small|medium|large|PERF_PAYLOAD_BYTES=<bytes>] [PERF_PUBLISHERS=<n>] [PERF_SUBSCRIBERS=<n>] [PERF_MESSAGES_PER_PUBLISHER=<n>] [PERF_PUBLISH_INTERVAL=<duration>]"; \
		exit 1; \
	fi
	@bash -lc 'set -euo pipefail; \
		cmd=(go run ./tests/perf/loadgen \
			-scenario-config "$(PERF_SCENARIO_CONFIG)" \
			-mqtt-addrs "$${PERF_MQTT_ADDRS:-127.0.0.1:11883,127.0.0.1:11884,127.0.0.1:11885}" \
			-amqp-addrs "$${PERF_AMQP_ADDRS:-127.0.0.1:15682,127.0.0.1:15683,127.0.0.1:15684}" \
			-min-ratio "$${PERF_MIN_RATIO:-0.95}" \
			-queue-min-ratio "$${PERF_QUEUE_MIN_RATIO:-0.99}" \
			-drain-timeout "$${PERF_DRAIN_TIMEOUT:-45s}"); \
		if [[ -n "$${PERF_PAYLOAD_BYTES:-}" ]]; then cmd+=( -payload-bytes "$${PERF_PAYLOAD_BYTES}" ); else cmd+=( -payload "$${PERF_PAYLOAD:-small}" ); fi; \
		if [[ -n "$${PERF_PUBLISHERS:-}" ]]; then cmd+=( -publishers "$${PERF_PUBLISHERS}" ); fi; \
		if [[ -n "$${PERF_SUBSCRIBERS:-}" ]]; then cmd+=( -subscribers "$${PERF_SUBSCRIBERS}" ); fi; \
		if [[ -n "$${PERF_MESSAGES_PER_PUBLISHER:-}" ]]; then cmd+=( -messages-per-publisher "$${PERF_MESSAGES_PER_PUBLISHER}" ); fi; \
		if [[ -n "$${PERF_PUBLISH_INTERVAL:-}" ]]; then cmd+=( -publish-interval "$${PERF_PUBLISH_INTERVAL}" ); fi; \
		if [[ -n "$${PERF_JSON_OUT:-}" ]]; then cmd+=( -json-out "$${PERF_JSON_OUT}" ); fi; \
		echo ">> $${cmd[*]}"; \
		"$${cmd[@]}"'

.PHONY: perf-cleanup
perf-cleanup:
	bash $(PERF_SCRIPT_DIR)/cleanup.sh

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
	@echo "  perf-suite         Run configurable real-client performance suite"
	@echo "                     Config via PERF_SCENARIO_CONFIGS / PERF_* knobs (see tests/perf/README.md)"
	@echo "  run-perf           Run one config-driven perf scenario"
	@echo "                     Usage: make run-perf PERF_SCENARIO_CONFIG=<config.json> (or CONFIG=<config.json>)"
	@echo "  perf-cleanup       Reset perf cluster state and remove suite result files"
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
