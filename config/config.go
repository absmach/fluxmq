// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package config

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

// Config holds all configuration for the MQTT broker.
type Config struct {
	Server  ServerConfig  `yaml:"server"`
	Broker  BrokerConfig  `yaml:"broker"`
	Session SessionConfig `yaml:"session"`
	Log     LogConfig     `yaml:"log"`
	Storage StorageConfig `yaml:"storage"`
	Cluster ClusterConfig `yaml:"cluster"`
	Webhook WebhookConfig `yaml:"webhook"`
}

// ServerConfig holds server-related configuration.
type ServerConfig struct {
	TCPAddr         string        `yaml:"tcp_addr"`
	TLSCertFile     string        `yaml:"tls_cert_file"`
	TLSKeyFile      string        `yaml:"tls_key_file"`
	TLSCAFile       string        `yaml:"tls_ca_file"`     // CA certificate for client verification
	TLSClientAuth   string        `yaml:"tls_client_auth"` // "none", "request", or "require"
	HTTPAddr        string        `yaml:"http_addr"`
	WSAddr          string        `yaml:"ws_addr"`
	WSPath          string        `yaml:"ws_path"`
	CoAPAddr        string        `yaml:"coap_addr"`
	HealthAddr      string        `yaml:"health_addr"`
	MetricsAddr     string        `yaml:"metrics_addr"` // Now used for OTLP endpoint
	TCPMaxConn      int           `yaml:"tcp_max_connections"`
	TCPReadTimeout  time.Duration `yaml:"tcp_read_timeout"`
	TCPWriteTimeout time.Duration `yaml:"tcp_write_timeout"`
	ShutdownTimeout time.Duration `yaml:"shutdown_timeout"`
	TLSEnabled      bool          `yaml:"tls_enabled"`
	HTTPEnabled     bool          `yaml:"http_enabled"`
	WSEnabled       bool          `yaml:"ws_enabled"`
	CoAPEnabled     bool          `yaml:"coap_enabled"`
	HealthEnabled   bool          `yaml:"health_enabled"`
	MetricsEnabled  bool          `yaml:"metrics_enabled"` // Now enables OTel

	// OpenTelemetry configuration
	OtelServiceName     string  `yaml:"otel_service_name"`
	OtelServiceVersion  string  `yaml:"otel_service_version"`
	OtelTracesEnabled   bool    `yaml:"otel_traces_enabled"`
	OtelMetricsEnabled  bool    `yaml:"otel_metrics_enabled"`
	OtelTraceSampleRate float64 `yaml:"otel_trace_sample_rate"` // 0.0 to 1.0
}

// BrokerConfig holds broker-specific settings.
type BrokerConfig struct {
	// Maximum message size in bytes
	MaxMessageSize int `yaml:"max_message_size"`

	// Retained message limits
	MaxRetainedMessages int `yaml:"max_retained_messages"`

	// QoS retry settings
	RetryInterval time.Duration `yaml:"retry_interval"`
	MaxRetries    int           `yaml:"max_retries"`
}

// SessionConfig holds session management settings.
type SessionConfig struct {
	// Maximum sessions allowed
	MaxSessions int `yaml:"max_sessions"`

	// Default expiry interval (seconds) if client doesn't specify
	DefaultExpiryInterval uint32 `yaml:"default_expiry_interval"`

	// Maximum queued messages per offline client
	MaxOfflineQueueSize int `yaml:"max_offline_queue_size"`

	// Maximum inflight messages per session
	MaxInflightMessages int `yaml:"max_inflight_messages"`
}

// LogConfig holds logging configuration.
type LogConfig struct {
	Level  string `yaml:"level"`  // debug, info, warn, error
	Format string `yaml:"format"` // text, json
}

// StorageConfig holds storage backend configuration.
type StorageConfig struct {
	Type string `yaml:"type"` // memory, badger

	// BadgerDB settings
	BadgerDir string `yaml:"badger_dir"`
}

// ClusterConfig holds clustering configuration.
type ClusterConfig struct {
	Enabled bool   `yaml:"enabled"`
	NodeID  string `yaml:"node_id"`

	// Embedded etcd settings
	Etcd EtcdConfig `yaml:"etcd"`

	// Inter-broker transport
	Transport TransportConfig `yaml:"transport"`
}

// EtcdConfig holds embedded etcd configuration.
type EtcdConfig struct {
	DataDir        string `yaml:"data_dir"`
	BindAddr       string `yaml:"bind_addr"`       // Peer address (e.g., "0.0.0.0:2380")
	ClientAddr     string `yaml:"client_addr"`     // Client address (e.g., "0.0.0.0:2379")
	InitialCluster string `yaml:"initial_cluster"` // "node1=http://host1:2380,node2=http://host2:2380"
	Bootstrap      bool   `yaml:"bootstrap"`       // true only for first node

	// Hybrid retained message storage threshold (in bytes)
	// Messages smaller than this are replicated to all nodes via etcd
	// Messages larger than this are stored on owner node and fetched on-demand via gRPC
	// Default: 1024 (1KB)
	HybridRetainedSizeThreshold int `yaml:"hybrid_retained_size_threshold"`
}

// TransportConfig holds inter-broker transport configuration.
type TransportConfig struct {
	BindAddr string            `yaml:"bind_addr"` // gRPC address (e.g., "0.0.0.0:7948")
	Peers    map[string]string `yaml:"peers"`     // Map of nodeID -> transport address for peers
}

// WebhookConfig holds webhook notification configuration.
type WebhookConfig struct {
	Enabled         bool              `yaml:"enabled"`
	QueueSize       int               `yaml:"queue_size"`
	DropPolicy      string            `yaml:"drop_policy"`      // "oldest" or "newest"
	Workers         int               `yaml:"workers"`          // Number of worker goroutines
	IncludePayload  bool              `yaml:"include_payload"`  // Include message payload in events
	ShutdownTimeout time.Duration     `yaml:"shutdown_timeout"` // Graceful shutdown timeout
	Defaults        WebhookDefaults   `yaml:"defaults"`
	Endpoints       []WebhookEndpoint `yaml:"endpoints"`
}

// WebhookDefaults holds default settings for webhook endpoints.
type WebhookDefaults struct {
	Timeout        time.Duration        `yaml:"timeout"`
	Retry          RetryConfig          `yaml:"retry"`
	CircuitBreaker CircuitBreakerConfig `yaml:"circuit_breaker"`
}

// RetryConfig holds retry configuration for webhook delivery.
type RetryConfig struct {
	MaxAttempts     int           `yaml:"max_attempts"`
	InitialInterval time.Duration `yaml:"initial_interval"`
	MaxInterval     time.Duration `yaml:"max_interval"`
	Multiplier      float64       `yaml:"multiplier"`
}

// CircuitBreakerConfig holds circuit breaker configuration.
type CircuitBreakerConfig struct {
	FailureThreshold int           `yaml:"failure_threshold"`
	ResetTimeout     time.Duration `yaml:"reset_timeout"`
}

// WebhookEndpoint defines a single webhook endpoint configuration.
type WebhookEndpoint struct {
	Name         string            `yaml:"name"`
	Type         string            `yaml:"type"` // "http" (future: "grpc")
	URL          string            `yaml:"url"`
	Events       []string          `yaml:"events"`        // Event type filter (empty = all)
	TopicFilters []string          `yaml:"topic_filters"` // Topic pattern filter (empty = all)
	Headers      map[string]string `yaml:"headers"`
	Timeout      time.Duration     `yaml:"timeout,omitempty"` // Override default
	Retry        *RetryConfig      `yaml:"retry,omitempty"`   // Override default
}

// Default returns a configuration with sensible defaults.
func Default() *Config {
	return &Config{
		Server: ServerConfig{
			TCPAddr:         ":1883",
			TCPMaxConn:      10000,
			TCPReadTimeout:  60 * time.Second,
			TCPWriteTimeout: 60 * time.Second,
			TLSEnabled:      false,
			TLSClientAuth:   "none",
			HTTPAddr:        ":8080",
			HTTPEnabled:     false,
			WSAddr:          ":8083",
			WSPath:          "/mqtt",
			WSEnabled:       true,
			CoAPAddr:        ":5683",
			CoAPEnabled:     false,
			HealthAddr:      ":8081",
			HealthEnabled:   true,
			MetricsAddr:     "localhost:4317",
			MetricsEnabled:  false,
			ShutdownTimeout: 30 * time.Second,

			// OpenTelemetry defaults
			OtelServiceName:     "mqtt-broker",
			OtelServiceVersion:  "1.0.0",
			OtelMetricsEnabled:  true,
			OtelTracesEnabled:   false, // Disabled by default for performance
			OtelTraceSampleRate: 0.1,   // 10% sampling when enabled
		},
		Broker: BrokerConfig{
			MaxMessageSize:      1024 * 1024, // 1MB
			MaxRetainedMessages: 10000,
			RetryInterval:       20 * time.Second,
			MaxRetries:          0, // Infinite retries
		},
		Session: SessionConfig{
			MaxSessions:           10000,
			DefaultExpiryInterval: 300, // 5 minutes
			MaxOfflineQueueSize:   1000,
			MaxInflightMessages:   100,
		},
		Log: LogConfig{
			Level:  "info",
			Format: "text",
		},
		Storage: StorageConfig{
			Type:      "badger",
			BadgerDir: "/tmp/mqtt/data",
		},
		Cluster: ClusterConfig{
			Enabled: true,
			NodeID:  "broker-1",
			Etcd: EtcdConfig{
				DataDir:        "/tmp/mqtt/etcd",
				BindAddr:       "0.0.0.0:2380",
				ClientAddr:     "0.0.0.0:2379",
				InitialCluster: "broker-1=http://0.0.0.0:2380",
				Bootstrap:      true,
			},
			Transport: TransportConfig{
				BindAddr: "0.0.0.0:7948",
			},
		},
		Webhook: WebhookConfig{
			Enabled:         false,
			QueueSize:       10000,
			DropPolicy:      "oldest",
			Workers:         5,
			IncludePayload:  false,
			ShutdownTimeout: 30 * time.Second,
			Defaults: WebhookDefaults{
				Timeout: 5 * time.Second,
				Retry: RetryConfig{
					MaxAttempts:     3,
					InitialInterval: 1 * time.Second,
					MaxInterval:     30 * time.Second,
					Multiplier:      2.0,
				},
				CircuitBreaker: CircuitBreakerConfig{
					FailureThreshold: 5,
					ResetTimeout:     60 * time.Second,
				},
			},
			Endpoints: []WebhookEndpoint{},
		},
	}
}

// Load loads configuration from a YAML file.
// If the file doesn't exist, returns default configuration.
func Load(filename string) (*Config, error) {
	if filename == "" {
		return Default(), nil
	}

	data, err := os.ReadFile(filename)
	if err != nil {
		if os.IsNotExist(err) {
			return Default(), nil
		}
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	cfg := Default()
	if err := yaml.Unmarshal(data, cfg); err != nil {
		return nil, fmt.Errorf("failed to parse config file: %w", err)
	}

	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid configuration: %w", err)
	}

	return cfg, nil
}

// Validate checks if the configuration is valid.
func (c *Config) Validate() error {
	if c.Server.TCPAddr == "" {
		return fmt.Errorf("server.tcp_addr cannot be empty")
	}
	if c.Server.TCPMaxConn < 0 {
		return fmt.Errorf("server.tcp_max_connections cannot be negative")
	}
	if c.Server.TLSEnabled {
		if c.Server.TLSCertFile == "" {
			return fmt.Errorf("server.tls_cert_file required when TLS is enabled")
		}
		if c.Server.TLSKeyFile == "" {
			return fmt.Errorf("server.tls_key_file required when TLS is enabled")
		}

		// Validate client auth mode
		validClientAuth := map[string]bool{"none": true, "request": true, "require": true}
		if !validClientAuth[c.Server.TLSClientAuth] {
			return fmt.Errorf("server.tls_client_auth must be one of: none, request, require")
		}

		// CA file required for client certificate verification
		if (c.Server.TLSClientAuth == "request" || c.Server.TLSClientAuth == "require") && c.Server.TLSCAFile == "" {
			return fmt.Errorf("server.tls_ca_file required when tls_client_auth is '%s'", c.Server.TLSClientAuth)
		}
	}

	if c.Broker.MaxMessageSize < 1024 {
		return fmt.Errorf("broker.max_message_size must be at least 1KB")
	}
	if c.Broker.RetryInterval < time.Second {
		return fmt.Errorf("broker.retry_interval must be at least 1 second")
	}

	if c.Session.MaxSessions < 1 {
		return fmt.Errorf("session.max_sessions must be at least 1")
	}
	if c.Session.MaxOfflineQueueSize < 10 {
		return fmt.Errorf("session.max_offline_queue_size must be at least 10")
	}

	validLevels := map[string]bool{"debug": true, "info": true, "warn": true, "error": true}
	if !validLevels[c.Log.Level] {
		return fmt.Errorf("log.level must be one of: debug, info, warn, error")
	}
	validFormats := map[string]bool{"text": true, "json": true}
	if !validFormats[c.Log.Format] {
		return fmt.Errorf("log.format must be one of: text, json")
	}

	validStorage := map[string]bool{"memory": true, "badger": true}
	if !validStorage[c.Storage.Type] {
		return fmt.Errorf("storage.type must be one of: memory, badger")
	}

	if c.Storage.Type == "badger" && c.Storage.BadgerDir == "" {
		return fmt.Errorf("storage.badger_dir required when type is badger")
	}

	// OpenTelemetry validation (only if metrics enabled)
	if c.Server.MetricsEnabled {
		if c.Server.OtelServiceName == "" {
			return fmt.Errorf("server.otel_service_name cannot be empty when metrics enabled")
		}
		if c.Server.OtelTraceSampleRate < 0.0 || c.Server.OtelTraceSampleRate > 1.0 {
			return fmt.Errorf("server.otel_trace_sample_rate must be between 0.0 and 1.0")
		}
	}

	// Cluster validation (only if enabled)
	if c.Cluster.Enabled {
		if c.Cluster.NodeID == "" {
			return fmt.Errorf("cluster.node_id required when clustering is enabled")
		}
		if c.Cluster.Etcd.DataDir == "" {
			return fmt.Errorf("cluster.etcd.data_dir required when clustering is enabled")
		}
		if c.Cluster.Etcd.BindAddr == "" {
			return fmt.Errorf("cluster.etcd.bind_addr required when clustering is enabled")
		}
		if c.Cluster.Etcd.ClientAddr == "" {
			return fmt.Errorf("cluster.etcd.client_addr required when clustering is enabled")
		}
		if c.Cluster.Transport.BindAddr == "" {
			return fmt.Errorf("cluster.transport.bind_addr required when clustering is enabled")
		}
	}

	// Webhook validation (only if enabled)
	if c.Webhook.Enabled {
		if c.Webhook.QueueSize < 100 {
			return fmt.Errorf("webhook.queue_size must be at least 100")
		}
		if c.Webhook.DropPolicy != "oldest" && c.Webhook.DropPolicy != "newest" {
			return fmt.Errorf("webhook.drop_policy must be 'oldest' or 'newest'")
		}
		if c.Webhook.Workers < 1 {
			return fmt.Errorf("webhook.workers must be at least 1")
		}
		if c.Webhook.ShutdownTimeout < time.Second {
			return fmt.Errorf("webhook.shutdown_timeout must be at least 1 second")
		}
		if c.Webhook.Defaults.Timeout < time.Second {
			return fmt.Errorf("webhook.defaults.timeout must be at least 1 second")
		}
		if c.Webhook.Defaults.Retry.MaxAttempts < 1 {
			return fmt.Errorf("webhook.defaults.retry.max_attempts must be at least 1")
		}
		if c.Webhook.Defaults.Retry.Multiplier < 1.0 {
			return fmt.Errorf("webhook.defaults.retry.multiplier must be at least 1.0")
		}
		if c.Webhook.Defaults.CircuitBreaker.FailureThreshold < 1 {
			return fmt.Errorf("webhook.defaults.circuit_breaker.failure_threshold must be at least 1")
		}

		// Validate each endpoint
		for i, endpoint := range c.Webhook.Endpoints {
			if endpoint.Name == "" {
				return fmt.Errorf("webhook.endpoints[%d].name cannot be empty", i)
			}
			if endpoint.Type != "http" {
				return fmt.Errorf("webhook.endpoints[%d].type must be 'http' (grpc not yet supported)", i)
			}
			if endpoint.URL == "" {
				return fmt.Errorf("webhook.endpoints[%d].url cannot be empty", i)
			}
		}
	}

	return nil
}

// Save writes the configuration to a YAML file.
func (c *Config) Save(filename string) error {
	data, err := yaml.Marshal(c)
	if err != nil {
		return fmt.Errorf("failed to marshal config: %w", err)
	}

	if err := os.WriteFile(filename, data, 0o644); err != nil {
		return fmt.Errorf("failed to write config file: %w", err)
	}

	return nil
}
