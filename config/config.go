// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package config

import (
	"fmt"
	"os"
	"strings"
	"time"

	mqtttls "github.com/absmach/fluxmq/pkg/tls"
	"gopkg.in/yaml.v3"
)

// Config holds all configuration for the MQTT broker.
type Config struct {
	Server  ServerConfig  `yaml:"server"`
	Broker  BrokerConfig  `yaml:"broker"`
	Session SessionConfig `yaml:"session"`
	Log        LogConfig        `yaml:"log"`
	Storage    StorageConfig    `yaml:"storage"`
	Cluster    ClusterConfig    `yaml:"cluster"`
	Webhook    WebhookConfig    `yaml:"webhook"`
	RateLimit  RateLimitConfig  `yaml:"ratelimit"`
	Queues     []QueueConfig    `yaml:"queues"`
}

// QueueConfig defines configuration for a persistent queue.
type QueueConfig struct {
	Name     string        `yaml:"name"`
	Topics   []string      `yaml:"topics"`
	Reserved bool          `yaml:"reserved"`
	Limits   QueueLimits   `yaml:"limits"`
	Retry    QueueRetry    `yaml:"retry"`
	DLQ      QueueDLQ      `yaml:"dlq"`
}

// QueueLimits defines resource limits for a queue.
type QueueLimits struct {
	MaxMessageSize int64         `yaml:"max_message_size"`
	MaxDepth       int64         `yaml:"max_depth"`
	MessageTTL     time.Duration `yaml:"message_ttl"`
}

// QueueRetry defines retry policy for failed message delivery.
type QueueRetry struct {
	MaxRetries     int           `yaml:"max_retries"`
	InitialBackoff time.Duration `yaml:"initial_backoff"`
	MaxBackoff     time.Duration `yaml:"max_backoff"`
	Multiplier     float64       `yaml:"multiplier"`
}

// QueueDLQ defines dead-letter queue configuration.
type QueueDLQ struct {
	Enabled bool   `yaml:"enabled"`
	Topic   string `yaml:"topic"`
}

// RateLimitConfig holds rate limiting configuration.
type RateLimitConfig struct {
	Enabled    bool                      `yaml:"enabled"`
	Connection ConnectionRateLimitConfig `yaml:"connection"`
	Message    MessageRateLimitConfig    `yaml:"message"`
	Subscribe  SubscribeRateLimitConfig  `yaml:"subscribe"`
}

// ConnectionRateLimitConfig holds per-IP connection rate limiting settings.
type ConnectionRateLimitConfig struct {
	Enabled         bool          `yaml:"enabled"`
	Rate            float64       `yaml:"rate"`             // connections per second per IP
	Burst           int           `yaml:"burst"`            // burst allowance
	CleanupInterval time.Duration `yaml:"cleanup_interval"` // cleanup interval for stale entries
}

// MessageRateLimitConfig holds per-client message rate limiting settings.
type MessageRateLimitConfig struct {
	Enabled bool    `yaml:"enabled"`
	Rate    float64 `yaml:"rate"`  // messages per second per client
	Burst   int     `yaml:"burst"` // burst allowance
}

// SubscribeRateLimitConfig holds per-client subscription rate limiting settings.
type SubscribeRateLimitConfig struct {
	Enabled bool    `yaml:"enabled"`
	Rate    float64 `yaml:"rate"`  // subscriptions per second per client
	Burst   int     `yaml:"burst"` // burst allowance
}

// ServerConfig holds server-related configuration.
type ServerConfig struct {
	TCP       TCPConfig       `yaml:"tcp"`
	WebSocket WebSocketConfig `yaml:"websocket"`
	HTTP      HTTPConfig      `yaml:"http"`
	CoAP      CoAPConfig      `yaml:"coap"`
	AMQP      AMQPConfig      `yaml:"amqp"`
	AMQP091   AMQP091Config   `yaml:"amqp091"`

	HealthAddr      string        `yaml:"health_addr"`
	MetricsAddr     string        `yaml:"metrics_addr"` // Now used for OTLP endpoint
	ShutdownTimeout time.Duration `yaml:"shutdown_timeout"`
	HealthEnabled   bool          `yaml:"health_enabled"`
	MetricsEnabled  bool          `yaml:"metrics_enabled"` // Now enables OTel

	// OpenTelemetry configuration
	OtelServiceName     string  `yaml:"otel_service_name"`
	OtelServiceVersion  string  `yaml:"otel_service_version"`
	OtelTracesEnabled   bool    `yaml:"otel_traces_enabled"`
	OtelMetricsEnabled  bool    `yaml:"otel_metrics_enabled"`
	OtelTraceSampleRate float64 `yaml:"otel_trace_sample_rate"` // 0.0 to 1.0

	// Queue API server (Connect/gRPC)
	APIEnabled bool   `yaml:"api_enabled"`
	APIAddr    string `yaml:"api_addr"`
}

// TCPListenerConfig holds TCP listener configuration.
type TCPListenerConfig struct {
	Addr           string         `yaml:"addr"`
	MaxConnections int            `yaml:"max_connections"`
	ReadTimeout    time.Duration  `yaml:"read_timeout"`
	WriteTimeout   time.Duration  `yaml:"write_timeout"`
	TLS            mqtttls.Config `yaml:",inline"`
}

// TCPConfig groups TCP listeners by mode.
type TCPConfig struct {
	Plain TCPListenerConfig `yaml:"plain"`
	TLS   TCPListenerConfig `yaml:"tls"`
	MTLS  TCPListenerConfig `yaml:"mtls"`
}

// WSListenerConfig holds WebSocket listener configuration.
type WSListenerConfig struct {
	Addr           string         `yaml:"addr"`
	Path           string         `yaml:"path"`
	AllowedOrigins []string       `yaml:"allowed_origins"`
	TLS            mqtttls.Config `yaml:",inline"`
}

// WebSocketConfig groups WebSocket listeners by mode.
type WebSocketConfig struct {
	Plain WSListenerConfig `yaml:"plain"`
	TLS   WSListenerConfig `yaml:"tls"`
	MTLS  WSListenerConfig `yaml:"mtls"`
}

// HTTPListenerConfig holds HTTP listener configuration.
type HTTPListenerConfig struct {
	Addr string         `yaml:"addr"`
	TLS  mqtttls.Config `yaml:",inline"`
}

// HTTPConfig groups HTTP listeners by mode.
type HTTPConfig struct {
	Plain HTTPListenerConfig `yaml:"plain"`
	TLS   HTTPListenerConfig `yaml:"tls"`
	MTLS  HTTPListenerConfig `yaml:"mtls"`
}

// CoAPListenerConfig holds CoAP listener configuration.
type CoAPListenerConfig struct {
	Addr string         `yaml:"addr"`
	TLS  mqtttls.Config `yaml:",inline"`
}

// CoAPConfig groups CoAP listeners by mode.
type CoAPConfig struct {
	Plain CoAPListenerConfig `yaml:"plain"`
	DTLS  CoAPListenerConfig `yaml:"dtls"`
	MDTLS CoAPListenerConfig `yaml:"mdtls"`
}

// AMQPListenerConfig holds AMQP listener configuration.
type AMQPListenerConfig struct {
	Addr           string         `yaml:"addr"`
	MaxConnections int            `yaml:"max_connections"`
	TLS            mqtttls.Config `yaml:",inline"`
}

// AMQPConfig groups AMQP listeners by mode.
type AMQPConfig struct {
	Plain AMQPListenerConfig `yaml:"plain"`
	TLS   AMQPListenerConfig `yaml:"tls"`
	MTLS  AMQPListenerConfig `yaml:"mtls"`
}

// AMQP091ListenerConfig holds AMQP 0.9.1 listener configuration.
type AMQP091ListenerConfig struct {
	Addr           string         `yaml:"addr"`
	MaxConnections int            `yaml:"max_connections"`
	TLS            mqtttls.Config `yaml:",inline"`
}

// AMQP091Config groups AMQP 0.9.1 listeners by mode.
type AMQP091Config struct {
	Plain AMQP091ListenerConfig `yaml:"plain"`
	TLS   AMQP091ListenerConfig `yaml:"tls"`
	MTLS  AMQP091ListenerConfig `yaml:"mtls"`
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

	// Maximum QoS level supported (0, 1, or 2). Default: 2
	// Server will downgrade publish QoS to this level per MQTT 5.0 spec
	MaxQoS int `yaml:"max_qos"`
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

	// Offline queue eviction policy: "evict" (drop oldest) or "reject" (reject new)
	OfflineQueuePolicy string `yaml:"offline_queue_policy"`
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
	BadgerDir  string `yaml:"badger_dir"`
	SyncWrites bool   `yaml:"sync_writes"`
}

// ClusterConfig holds clustering configuration.
type ClusterConfig struct {
	Enabled bool   `yaml:"enabled"`
	NodeID  string `yaml:"node_id"`

	// Embedded etcd settings
	Etcd EtcdConfig `yaml:"etcd"`

	// Inter-broker transport
	Transport TransportConfig `yaml:"transport"`

	// Raft replication for queue data
	Raft RaftConfig `yaml:"raft"`
}

// RaftConfig holds Raft replication configuration for queue data.
type RaftConfig struct {
	Enabled           bool              `yaml:"enabled"`
	ReplicationFactor int               `yaml:"replication_factor"` // Number of replicas per partition (default: 3)
	SyncMode          bool              `yaml:"sync_mode"`          // true=wait for quorum, false=async
	MinInSyncReplicas int               `yaml:"min_in_sync_replicas"`
	AckTimeout        time.Duration     `yaml:"ack_timeout"`
	BindAddr          string            `yaml:"bind_addr"` // Base address for Raft (e.g., "127.0.0.1:7100")
	DataDir           string            `yaml:"data_dir"`  // Directory for Raft data
	Peers             map[string]string `yaml:"peers"`     // Map of nodeID -> raft base address

	// Raft tuning
	HeartbeatTimeout  time.Duration `yaml:"heartbeat_timeout"`
	ElectionTimeout   time.Duration `yaml:"election_timeout"`
	SnapshotInterval  time.Duration `yaml:"snapshot_interval"`
	SnapshotThreshold uint64        `yaml:"snapshot_threshold"`
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

	// TLS configuration for inter-broker communication
	TLSEnabled  bool   `yaml:"tls_enabled"`   // Enable TLS for gRPC transport
	TLSCertFile string `yaml:"tls_cert_file"` // Server certificate file
	TLSKeyFile  string `yaml:"tls_key_file"`  // Server private key file
	TLSCAFile   string `yaml:"tls_ca_file"`   // CA certificate for verifying peer certificates
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
			TCP: TCPConfig{
				Plain: TCPListenerConfig{
					Addr:           ":1883",
					MaxConnections: 10000,
					ReadTimeout:    60 * time.Second,
					WriteTimeout:   60 * time.Second,
				},
				TLS: TCPListenerConfig{
					MaxConnections: 10000,
					ReadTimeout:    60 * time.Second,
					WriteTimeout:   60 * time.Second,
				},
				MTLS: TCPListenerConfig{
					MaxConnections: 10000,
					ReadTimeout:    60 * time.Second,
					WriteTimeout:   60 * time.Second,
				},
			},
			WebSocket: WebSocketConfig{
				Plain: WSListenerConfig{
					Addr: ":8083",
					Path: "/mqtt",
				},
				TLS: WSListenerConfig{
					Path: "/mqtt",
				},
				MTLS: WSListenerConfig{
					Path: "/mqtt",
				},
			},
			HTTP: HTTPConfig{
				Plain: HTTPListenerConfig{},
				TLS:   HTTPListenerConfig{},
				MTLS:  HTTPListenerConfig{},
			},
			CoAP: CoAPConfig{
				Plain: CoAPListenerConfig{},
				DTLS:  CoAPListenerConfig{},
				MDTLS: CoAPListenerConfig{},
			},
			AMQP: AMQPConfig{
				Plain: AMQPListenerConfig{
					Addr:           ":5672",
					MaxConnections: 10000,
				},
				TLS: AMQPListenerConfig{
					MaxConnections: 10000,
				},
				MTLS: AMQPListenerConfig{
					MaxConnections: 10000,
				},
			},
			AMQP091: AMQP091Config{
				Plain: AMQP091ListenerConfig{
					Addr:           ":5682",
					MaxConnections: 10000,
				},
				TLS: AMQP091ListenerConfig{
					MaxConnections: 10000,
				},
				MTLS: AMQP091ListenerConfig{
					MaxConnections: 10000,
				},
			},
			HealthAddr:      ":8081",
			HealthEnabled:   true,
			MetricsAddr:     "localhost:4317",
			MetricsEnabled:  false,
			ShutdownTimeout: 30 * time.Second,

			// OpenTelemetry defaults
			OtelServiceName:     "fluxmq",
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
			MaxQoS:              2, // Support all QoS levels
		},
		Session: SessionConfig{
			MaxSessions:           10000,
			DefaultExpiryInterval: 300, // 5 minutes
			MaxOfflineQueueSize:   1000,
			MaxInflightMessages:   100,
			OfflineQueuePolicy:    "evict",
		},
		Log: LogConfig{
			Level:  "info",
			Format: "text",
		},
		Storage: StorageConfig{
			Type:      "badger",
			BadgerDir: "/tmp/fluxmq/data",
		},
		Cluster: ClusterConfig{
			Enabled: true,
			NodeID:  "broker-1",
			Etcd: EtcdConfig{
				DataDir:        "/tmp/fluxmq/etcd",
				BindAddr:       "0.0.0.0:2380",
				ClientAddr:     "0.0.0.0:2379",
				InitialCluster: "broker-1=http://0.0.0.0:2380",
				Bootstrap:      true,
			},
			Transport: TransportConfig{
				BindAddr: "0.0.0.0:7948",
			},
			Raft: RaftConfig{
				Enabled:           false, // Disabled by default
				ReplicationFactor: 3,
				SyncMode:          true,
				MinInSyncReplicas: 2,
				AckTimeout:        5 * time.Second,
				BindAddr:          "127.0.0.1:7100",
				DataDir:           "/tmp/fluxmq/raft",
				Peers:             map[string]string{},
				HeartbeatTimeout:  1 * time.Second,
				ElectionTimeout:   3 * time.Second,
				SnapshotInterval:  5 * time.Minute,
				SnapshotThreshold: 8192,
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
		RateLimit: RateLimitConfig{
			Enabled: false,
			Connection: ConnectionRateLimitConfig{
				Enabled:         true,
				Rate:            100.0 / 60.0, // 100 connections per minute per IP
				Burst:           20,
				CleanupInterval: 5 * time.Minute,
			},
			Message: MessageRateLimitConfig{
				Enabled: true,
				Rate:    1000, // 1000 messages per second per client
				Burst:   100,
			},
			Subscribe: SubscribeRateLimitConfig{
				Enabled: true,
				Rate:    100, // 100 subscriptions per second per client
				Burst:   10,
			},
		},
		Queues: []QueueConfig{
			{
				Name:     "mqtt",
				Topics:   []string{"$queue/#"},
				Reserved: true,
				Limits: QueueLimits{
					MaxMessageSize: 10 * 1024 * 1024, // 10MB
					MaxDepth:       100000,
					MessageTTL:     7 * 24 * time.Hour,
				},
				Retry: QueueRetry{
					MaxRetries:     10,
					InitialBackoff: 5 * time.Second,
					MaxBackoff:     5 * time.Minute,
					Multiplier:     2.0,
				},
				DLQ: QueueDLQ{
					Enabled: true,
				},
			},
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
	tcpSlots := []struct {
		name              string
		cfg               TCPListenerConfig
		requireClientAuth bool
	}{
		{name: "plain", cfg: c.Server.TCP.Plain, requireClientAuth: false},
		{name: "tls", cfg: c.Server.TCP.TLS, requireClientAuth: false},
		{name: "mtls", cfg: c.Server.TCP.MTLS, requireClientAuth: true},
	}

	wsSlots := []struct {
		name              string
		cfg               WSListenerConfig
		requireClientAuth bool
	}{
		{name: "plain", cfg: c.Server.WebSocket.Plain, requireClientAuth: false},
		{name: "tls", cfg: c.Server.WebSocket.TLS, requireClientAuth: false},
		{name: "mtls", cfg: c.Server.WebSocket.MTLS, requireClientAuth: true},
	}

	httpSlots := []struct {
		name              string
		cfg               HTTPListenerConfig
		requireClientAuth bool
	}{
		{name: "plain", cfg: c.Server.HTTP.Plain, requireClientAuth: false},
		{name: "tls", cfg: c.Server.HTTP.TLS, requireClientAuth: false},
		{name: "mtls", cfg: c.Server.HTTP.MTLS, requireClientAuth: true},
	}

	hasMQTTListener := false

	for _, slot := range tcpSlots {
		if !hasAddr(slot.cfg.Addr) {
			if tlsConfigured(slot.cfg.TLS) && slot.name == "plain" {
				return fmt.Errorf("server.tcp.%s TLS fields are not supported for plain listeners", slot.name)
			}
			continue
		}

		hasMQTTListener = true
		if slot.cfg.MaxConnections < 0 {
			return fmt.Errorf("server.tcp.%s.max_connections cannot be negative", slot.name)
		}
		if slot.name == "plain" && tlsConfigured(slot.cfg.TLS) {
			return fmt.Errorf("server.tcp.%s TLS fields are not supported for plain listeners", slot.name)
		}
		if slot.name != "plain" {
			if err := validateListenerTLS("server.tcp."+slot.name, slot.cfg.TLS, slot.requireClientAuth); err != nil {
				return err
			}
		}
	}

	for _, slot := range wsSlots {
		if !hasAddr(slot.cfg.Addr) {
			if tlsConfigured(slot.cfg.TLS) && slot.name == "plain" {
				return fmt.Errorf("server.websocket.%s TLS fields are not supported for plain listeners", slot.name)
			}
			continue
		}

		hasMQTTListener = true
		if slot.name == "plain" && tlsConfigured(slot.cfg.TLS) {
			return fmt.Errorf("server.websocket.%s TLS fields are not supported for plain listeners", slot.name)
		}
		if slot.name != "plain" {
			if err := validateListenerTLS("server.websocket."+slot.name, slot.cfg.TLS, slot.requireClientAuth); err != nil {
				return err
			}
		}
	}

	if !hasMQTTListener {
		return fmt.Errorf("at least one TCP or WebSocket listener must be configured")
	}

	for _, slot := range httpSlots {
		if !hasAddr(slot.cfg.Addr) {
			if tlsConfigured(slot.cfg.TLS) && slot.name == "plain" {
				return fmt.Errorf("server.http.%s TLS fields are not supported for plain listeners", slot.name)
			}
			continue
		}

		if slot.name == "plain" && tlsConfigured(slot.cfg.TLS) {
			return fmt.Errorf("server.http.%s TLS fields are not supported for plain listeners", slot.name)
		}
		if slot.name != "plain" {
			if err := validateListenerTLS("server.http."+slot.name, slot.cfg.TLS, slot.requireClientAuth); err != nil {
				return err
			}
		}
	}

	coapSlots := []struct {
		name              string
		cfg               CoAPListenerConfig
		requireClientAuth bool
	}{
		{name: "plain", cfg: c.Server.CoAP.Plain},
		{name: "dtls", cfg: c.Server.CoAP.DTLS},
		{name: "mdtls", cfg: c.Server.CoAP.MDTLS, requireClientAuth: true},
	}

	for _, slot := range coapSlots {
		if !hasAddr(slot.cfg.Addr) {
			if tlsConfigured(slot.cfg.TLS) && slot.name == "plain" {
				return fmt.Errorf("server.coap.%s TLS fields are not supported for plain listeners", slot.name)
			}
			continue
		}

		if slot.name == "plain" && tlsConfigured(slot.cfg.TLS) {
			return fmt.Errorf("server.coap.%s TLS fields are not supported for plain listeners", slot.name)
		}
		if slot.name != "plain" {
			if err := validateListenerTLS("server.coap."+slot.name, slot.cfg.TLS, slot.requireClientAuth); err != nil {
				return err
			}
		}
	}

	// AMQP validation
	amqpSlots := []struct {
		name              string
		cfg               AMQPListenerConfig
		requireClientAuth bool
	}{
		{name: "plain", cfg: c.Server.AMQP.Plain, requireClientAuth: false},
		{name: "tls", cfg: c.Server.AMQP.TLS, requireClientAuth: false},
		{name: "mtls", cfg: c.Server.AMQP.MTLS, requireClientAuth: true},
	}

	for _, slot := range amqpSlots {
		if !hasAddr(slot.cfg.Addr) {
			if tlsConfigured(slot.cfg.TLS) && slot.name == "plain" {
				return fmt.Errorf("server.amqp.%s TLS fields are not supported for plain listeners", slot.name)
			}
			continue
		}

		if slot.cfg.MaxConnections < 0 {
			return fmt.Errorf("server.amqp.%s.max_connections cannot be negative", slot.name)
		}
		if slot.name == "plain" && tlsConfigured(slot.cfg.TLS) {
			return fmt.Errorf("server.amqp.%s TLS fields are not supported for plain listeners", slot.name)
		}
		if slot.name != "plain" {
			if err := validateListenerTLS("server.amqp."+slot.name, slot.cfg.TLS, slot.requireClientAuth); err != nil {
				return err
			}
		}
	}

	// AMQP 0.9.1 validation
	amqp091Slots := []struct {
		name              string
		cfg               AMQP091ListenerConfig
		requireClientAuth bool
	}{
		{name: "plain", cfg: c.Server.AMQP091.Plain, requireClientAuth: false},
		{name: "tls", cfg: c.Server.AMQP091.TLS, requireClientAuth: false},
		{name: "mtls", cfg: c.Server.AMQP091.MTLS, requireClientAuth: true},
	}

	for _, slot := range amqp091Slots {
		if !hasAddr(slot.cfg.Addr) {
			if tlsConfigured(slot.cfg.TLS) && slot.name == "plain" {
				return fmt.Errorf("server.amqp091.%s TLS fields are not supported for plain listeners", slot.name)
			}
			continue
		}

		if slot.cfg.MaxConnections < 0 {
			return fmt.Errorf("server.amqp091.%s.max_connections cannot be negative", slot.name)
		}
		if slot.name == "plain" && tlsConfigured(slot.cfg.TLS) {
			return fmt.Errorf("server.amqp091.%s TLS fields are not supported for plain listeners", slot.name)
		}
		if slot.name != "plain" {
			if err := validateListenerTLS("server.amqp091."+slot.name, slot.cfg.TLS, slot.requireClientAuth); err != nil {
				return err
			}
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
	if c.Session.OfflineQueuePolicy != "evict" && c.Session.OfflineQueuePolicy != "reject" {
		return fmt.Errorf("session.offline_queue_policy must be 'evict' or 'reject'")
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

		// Transport TLS validation
		if c.Cluster.Transport.TLSEnabled {
			if c.Cluster.Transport.TLSCertFile == "" {
				return fmt.Errorf("cluster.transport.tls_cert_file required when transport TLS is enabled")
			}
			if c.Cluster.Transport.TLSKeyFile == "" {
				return fmt.Errorf("cluster.transport.tls_key_file required when transport TLS is enabled")
			}
			if c.Cluster.Transport.TLSCAFile == "" {
				return fmt.Errorf("cluster.transport.tls_ca_file required when transport TLS is enabled")
			}
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

	// Queue validation
	seenQueues := make(map[string]bool)
	for i, q := range c.Queues {
		if q.Name == "" {
			return fmt.Errorf("queues[%d].name cannot be empty", i)
		}
		if seenQueues[q.Name] {
			return fmt.Errorf("queues[%d].name '%s' is duplicated", i, q.Name)
		}
		seenQueues[q.Name] = true
		if len(q.Topics) == 0 {
			return fmt.Errorf("queues[%d].topics cannot be empty", i)
		}
	}

	return nil
}

func validateListenerTLS(prefix string, cfg mqtttls.Config, requireCA bool) error {
	if cfg.CertFile == "" {
		return fmt.Errorf("%s.cert_file required", prefix)
	}
	if cfg.KeyFile == "" {
		return fmt.Errorf("%s.key_file required", prefix)
	}
	if requireCA && cfg.ClientCAFile == "" {
		return fmt.Errorf("%s.ca_file required", prefix)
	}
	return nil
}

func tlsConfigured(cfg mqtttls.Config) bool {
	return cfg.CertFile != "" ||
		cfg.KeyFile != "" ||
		cfg.ServerCAFile != "" ||
		cfg.ClientCAFile != "" ||
		cfg.ClientAuth != "" ||
		cfg.MinVersion != "" ||
		len(cfg.CipherSuites) > 0 ||
		cfg.PreferServerCipherSuites != nil
}

func hasAddr(addr string) bool {
	return strings.TrimSpace(addr) != ""
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
