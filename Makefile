# MQTT Broker Makefile

# Build configuration
BUILD_DIR := build
BINARY := fluxmq
GO := go

# Build flags
LDFLAGS := -s -w
GOFLAGS := -trimpath
DOCKER_IMAGE_LATEST := ghcr.io/absmach/fluxmq:latest
PERF_SCRIPT_DIR := tests/perf/scripts
PERF_SCENARIO_CONFIG ?= $(CONFIG)
DEPLOY_COMPOSE := deployments/cluster/docker-compose.yaml


# Default target
.PHONY: all
all: build

# Build the broker binary
.PHONY: build
build: $(BUILD_DIR)/$(BINARY)

$(BUILD_DIR)/$(BINARY): cmd/main.go $(shell find . -name '*.go' -not -path './build/*')
	@mkdir -p $(BUILD_DIR)
	$(GO) build $(GOFLAGS) -ldflags "$(LDFLAGS)"  -gcflags="all=-N -l" -o $(BUILD_DIR)/$(BINARY) ./cmd

# Build Docker image (latest tag)
.PHONY: docker
docker:
	docker build -f deployments/docker/Dockerfile -t $(DOCKER_IMAGE_LATEST) .

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

# --- Cluster (local processes) ---

.PHONY: cluster-up
cluster-up:
	bash deployments/cluster/local.sh

.PHONY: cluster-down
cluster-down:
	bash deployments/cluster/local-stop.sh

# --- Cluster (docker) ---

.PHONY: docker-cluster-up
docker-cluster-up:
	@docker compose -f $(DEPLOY_COMPOSE) up -d
	@echo "Waiting for health endpoints..."
	@for port in 8081 8082 8083; do \
		elapsed=0; \
		while [ $$elapsed -lt 30 ]; do \
			if curl -fsS "http://127.0.0.1:$${port}/ready" 2>/dev/null | grep -q '"status":"ready"'; then \
				echo "[OK] 127.0.0.1:$${port} ready"; \
				break; \
			fi; \
			sleep 1; \
			elapsed=$$((elapsed + 1)); \
		done; \
		if [ $$elapsed -ge 30 ]; then \
			echo "[ERROR] Health check timed out on port $${port}"; \
			docker compose -f $(DEPLOY_COMPOSE) down; \
			exit 1; \
		fi; \
	done

.PHONY: docker-cluster-down
docker-cluster-down:
	docker compose -f $(DEPLOY_COMPOSE) down

# --- Tests ---

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

# --- Benchmarks ---

.PHONY: bench
bench:
	$(GO) test -bench=. -benchmem -run=^$$ ./...

.PHONY: bench-broker
bench-broker:
	$(GO) test -bench=. -benchmem -run=^$$ ./mqtt/broker

.PHONY: bench-zerocopy
bench-zerocopy:
	$(GO) test -bench=BenchmarkMessageCopy -benchmem -run=^$$ ./mqtt/broker

# --- Stress tests ---

.PHONY: stress
stress:
	$(GO) test -v -run=TestStress -timeout=30m ./mqtt/broker

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

# --- Performance suites ---

.PHONY: perf-suite
perf-suite:
	bash $(PERF_SCRIPT_DIR)/run_suite.sh

.PHONY: run-perf
run-perf:
	@if [ -z "$(PERF_SCENARIO_CONFIG)" ]; then \
		echo "Usage: make run-perf PERF_SCENARIO_CONFIG=<path-to-config.json> [or CONFIG=<path>] [PERF_PAYLOAD=small|medium|large|PERF_PAYLOAD_BYTES=<bytes>] [PERF_PUBLISHERS=<n>] [PERF_SUBSCRIBERS=<n>] [PERF_MESSAGES_PER_PUBLISHER=<n>] [PERF_PUBLISH_INTERVAL=<duration>] [PERF_PUBLISH_JITTER=<duration>]"; \
		exit 1; \
	fi
	@bash -lc 'set -euo pipefail; \
		cmd=(go run ./tests/perf/loadgen \
			-scenario-config "$(PERF_SCENARIO_CONFIG)" \
			-mqtt-v3-addrs "$${PERF_MQTT_V3_ADDRS:-127.0.0.1:1883,127.0.0.1:1885,127.0.0.1:1887}" \
			-mqtt-v5-addrs "$${PERF_MQTT_V5_ADDRS:-127.0.0.1:1884,127.0.0.1:1886,127.0.0.1:1888}" \
			-amqp-addrs "$${PERF_AMQP_ADDRS:-127.0.0.1:5682,127.0.0.1:5683,127.0.0.1:5684}" \
			-min-ratio "$${PERF_MIN_RATIO:-0.95}" \
			-drain-timeout "$${PERF_DRAIN_TIMEOUT:-45s}"); \
		if [[ -n "$${PERF_PAYLOAD_BYTES:-}" ]]; then cmd+=( -payload-bytes "$${PERF_PAYLOAD_BYTES}" ); else cmd+=( -payload "$${PERF_PAYLOAD:-small}" ); fi; \
		if [[ -n "$${PERF_PUBLISHERS:-}" ]]; then cmd+=( -publishers "$${PERF_PUBLISHERS}" ); fi; \
		if [[ -n "$${PERF_SUBSCRIBERS:-}" ]]; then cmd+=( -subscribers "$${PERF_SUBSCRIBERS}" ); fi; \
		if [[ -n "$${PERF_MESSAGES_PER_PUBLISHER:-}" ]]; then cmd+=( -messages-per-publisher "$${PERF_MESSAGES_PER_PUBLISHER}" ); fi; \
		if [[ -n "$${PERF_PUBLISH_INTERVAL:-}" ]]; then cmd+=( -publish-interval "$${PERF_PUBLISH_INTERVAL}" ); fi; \
		if [[ -n "$${PERF_PUBLISH_JITTER:-}" ]]; then cmd+=( -publish-jitter "$${PERF_PUBLISH_JITTER}" ); fi; \
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

# --- Example configs ---

.PHONY: run-no-cluster
run-no-cluster: build
	$(BUILD_DIR)/$(BINARY) -config examples/no-cluster.yaml

.PHONY: run-single-cluster
run-single-cluster: build
	$(BUILD_DIR)/$(BINARY) -config examples/single-node-cluster.yaml

# --- Cleanup ---

.PHONY: clean
clean:
	rm -rf $(BUILD_DIR)

.PHONY: clean-data
clean-data:
	rm -rf /tmp/fluxmq

# --- Utilities ---

.PHONY: lint
lint:
	golangci-lint run ./...

.PHONY: fmt
fmt:
	$(GO) fmt ./...
	goimports -w .

.PHONY: deps
deps:
	$(GO) mod tidy
	$(GO) mod download

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
	@echo "FluxMQ Makefile"
	@echo ""
	@echo "Build:"
	@echo "  build              Build the broker binary to $(BUILD_DIR)/$(BINARY)"
	@echo "  docker             Build Docker image ($(DOCKER_IMAGE_LATEST))"
	@echo ""
	@echo "Run (single node):"
	@echo "  run                Build and run with default config"
	@echo "  run-config         Build and run with config.yaml"
	@echo "  run-debug          Build and run with debug logging"
	@echo "  run-no-cluster     Run single node without clustering"
	@echo "  run-single-cluster Run single node with clustering enabled"
	@echo ""
	@echo "Cluster (local processes):"
	@echo "  cluster-up         Build and start 3-node local cluster"
	@echo "  cluster-down       Gracefully stop local cluster"
	@echo ""
	@echo "Cluster (docker):"
	@echo "  docker-cluster-up      Start 3-node Docker cluster (host networking)"
	@echo "  docker-cluster-down    Stop Docker cluster"
	@echo ""
	@echo "Tests:"
	@echo "  test               Run all tests (short, with race detector)"
	@echo "  test-client        Run MQTT/AMQP client package unit tests"
	@echo "  test-client-integration  Run client integration tests (Docker-backed RabbitMQ)"
	@echo "  test-full          Run full tests including stress (30m timeout)"
	@echo "  test-cover         Run tests with coverage report"
	@echo "  lint               Run golangci-lint"
	@echo ""
	@echo "Benchmarks:"
	@echo "  bench              Run all benchmarks"
	@echo "  bench-broker       Run broker benchmarks only"
	@echo "  bench-zerocopy     Run zero-copy vs legacy comparison"
	@echo "  bench-report       Generate benchmark report to $(BUILD_DIR)/"
	@echo ""
	@echo "Performance:"
	@echo "  run-perf           Run one config-driven perf scenario"
	@echo "                     Usage: make run-perf CONFIG=<config.json>"
	@echo "  perf-suite         Run configurable performance suite"
	@echo "  perf-cleanup       Remove suite result files"
	@echo "  perf-compare       Compare benchmark files with benchstat"
	@echo "                     Usage: make perf-compare BASELINE=<file> [CANDIDATE=<file>]"
	@echo ""
	@echo "Stress Tests:"
	@echo "  stress             Run all stress tests (~30 min)"
	@echo "  stress-throughput  High throughput test (10K msg/s)"
	@echo "  stress-concurrent  Concurrent publishers test"
	@echo "  stress-memory      Memory pressure test (large messages)"
	@echo "  stress-sustained   Sustained mixed load test (30s)"
	@echo "  stress-pool        Buffer pool exhaustion test"
	@echo "  stress-fanout      Extreme fan-out test (5K subscribers)"
	@echo "  stress-churn       Subscription churn test"
	@echo ""
	@echo "Cleanup:"
	@echo "  clean              Remove build artifacts"
	@echo "  clean-data         Remove all /tmp/fluxmq data directories"
	@echo ""
	@echo "Utilities:"
	@echo "  fmt                Format code"
	@echo "  deps               Download and tidy dependencies"
	@echo "  proto              Generate protobuf code"
	@echo "  proto-lint         Lint protobuf definitions"
	@echo "  help               Show this help message"
