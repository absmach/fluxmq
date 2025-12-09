# MQTT Broker Makefile

# Build configuration
BUILD_DIR := build
BINARY := mqttd
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

# Run the broker
.PHONY: run
run: build
	$(BUILD_DIR)/$(BINARY) --log=info

# Run with debug logging
.PHONY: run-debug
run-debug: build
	$(BUILD_DIR)/$(BINARY) --log=debug

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

# Show help
.PHONY: help
help:
	@echo "MQTT Broker Makefile"
	@echo ""
	@echo "Targets:"
	@echo "  build       Build the broker binary to $(BUILD_DIR)/$(BINARY)"
	@echo "  run         Build and run the broker"
	@echo "  run-debug   Build and run with debug logging"
	@echo "  test        Run all tests"
	@echo "  test-cover  Run tests with coverage report"
	@echo "  lint        Run golangci-lint"
	@echo "  fmt         Format code"
	@echo "  clean       Remove build artifacts"
	@echo "  deps        Download and tidy dependencies"
	@echo "  help        Show this help message"
