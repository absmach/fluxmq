# MQTT Broker Makefile

# Build configuration
BUILD_DIR := build
BINARY := fluxmq
GO := go

# Build flags
LDFLAGS := -s -w
GOFLAGS := -trimpath



# Default target
.PHONY: all
all: build

# Build the broker binary
.PHONY: build
build: $(BUILD_DIR)/$(BINARY)

$(BUILD_DIR)/$(BINARY): cmd/broker/main.go $(shell find . -name '*.go' -not -path './build/*')
	@mkdir -p $(BUILD_DIR)
	$(GO) build $(GOFLAGS) -ldflags "$(LDFLAGS)" -o $(BUILD_DIR)/$(BINARY) ./cmd/broker

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
	$(GO) test -v ./...

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
	$(GO) test -bench=. -benchmem -run=^$$ ./broker

# Run zero-copy comparison benchmarks
.PHONY: bench-zerocopy
bench-zerocopy:
	$(GO) test -bench=BenchmarkMessageCopy -benchmem -run=^$$ ./broker

# Run stress tests (all)
.PHONY: stress
stress:
	$(GO) test -v -run=TestStress -timeout=30m ./broker

# Run specific stress tests
.PHONY: stress-throughput
stress-throughput:
	$(GO) test -v -run=TestStress_HighThroughputPublish -timeout=5m ./broker

.PHONY: stress-concurrent
stress-concurrent:
	$(GO) test -v -run=TestStress_ConcurrentPublishers -timeout=5m ./broker

.PHONY: stress-memory
stress-memory:
	$(GO) test -v -run=TestStress_MemoryPressure -timeout=5m ./broker

.PHONY: stress-sustained
stress-sustained:
	$(GO) test -v -run=TestStress_SustainedLoad -timeout=5m ./broker

.PHONY: stress-pool
stress-pool:
	$(GO) test -v -run=TestStress_BufferPoolExhaustion -timeout=5m ./broker

.PHONY: stress-fanout
stress-fanout:
	$(GO) test -v -run=TestStress_FanOutExtreme -timeout=5m ./broker

.PHONY: stress-churn
stress-churn:
	$(GO) test -v -run=TestStress_RapidSubscribeUnsubscribe -timeout=5m ./broker

# Generate benchmark report
.PHONY: bench-report
bench-report:
	@mkdir -p $(BUILD_DIR)
	$(GO) test -bench=. -benchmem -run=^$$ ./broker | tee $(BUILD_DIR)/benchmark-results.txt
	@echo ""
	@echo "Benchmark results saved to $(BUILD_DIR)/benchmark-results.txt"

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
	@echo ""
	@echo "  NOTE: For 3-node cluster, run node1, node2, and node3 in separate terminals"
	@echo ""
	@echo "Testing Targets:"
	@echo "  test               Run all tests"
	@echo "  test-cover         Run tests with coverage report"
	@echo "  lint               Run golangci-lint"
	@echo ""
	@echo "Benchmark Targets:"
	@echo "  bench              Run all benchmarks"
	@echo "  bench-broker       Run broker benchmarks only"
	@echo "  bench-zerocopy     Run zero-copy vs legacy comparison"
	@echo "  bench-report       Generate benchmark report to $(BUILD_DIR)/"
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
	@echo "  help               Show this help message"
