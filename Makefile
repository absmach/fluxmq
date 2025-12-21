# MQTT Broker Makefile

# Build configuration
BUILD_DIR := build
BINARY := mqttd
GO := go

# Build flags
LDFLAGS := -s -w
GOFLAGS := -trimpath


PKG_PROTO_GEN_OUT_DIR=cluster/grpc
INTERNAL_PROTO_DIR=proto
INTERNAL_PROTO_FILES := $(shell find $(INTERNAL_PROTO_DIR) -name "*.proto" | sed 's|$(INTERNAL_PROTO_DIR)/||')

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
	rm -rf /tmp/mqtt

.PHONY: proto
proto:
	mkdir -p $(PKG_PROTO_GEN_OUT_DIR)
	protoc -I $(INTERNAL_PROTO_DIR) --go_out=$(PKG_PROTO_GEN_OUT_DIR) --go_opt=paths=source_relative --go-grpc_out=$(PKG_PROTO_GEN_OUT_DIR) --go-grpc_opt=paths=source_relative $(INTERNAL_PROTO_FILES)

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
	@echo "Utility Targets:"
	@echo "  fmt                Format code"
	@echo "  clean              Remove build artifacts"
	@echo "  clean-data         Remove all /tmp/mqtt data directories"
	@echo "  deps               Download and tidy dependencies"
	@echo "  help               Show this help message"
