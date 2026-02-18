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
	Server       ServerConfig       `yaml:"server"`
	Broker       BrokerConfig       `yaml:"broker"`
	Session      SessionConfig      `yaml:"session"`
	Log          LogConfig          `yaml:"log"`
	Storage      StorageConfig      `yaml:"storage"`
	Cluster      ClusterConfig      `yaml:"cluster"`
	Webhook      WebhookConfig      `yaml:"webhook"`
	RateLimit    RateLimitConfig    `yaml:"ratelimit"`
	QueueManager QueueManagerConfig `yaml:"queue_manager"`
	Queues       []QueueConfig      `yaml:"queues"`
}

// QueueConfig defines configuration for a persistent queue.
type QueueConfig struct {
	Name         string           `yaml:"name"`
	Topics       []string         `yaml:"topics"`
	Reserved     bool             `yaml:"reserved"`
	Type         string           `yaml:"type"`
	PrimaryGroup string           `yaml:"primary_group"`
	Retention    QueueRetention   `yaml:"retention"`
	Limits       QueueLimits      `yaml:"limits"`
	Retry        QueueRetry       `yaml:"retry"`
	DLQ          QueueDLQ         `yaml:"dlq"`
	Replication  QueueReplication `yaml:"replication"`
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

// QueueRetention defines retention policy for a queue.
type QueueRetention struct {
	MaxAge            time.Duration `yaml:"max_age"`
	MaxLengthBytes    int64         `yaml:"max_length_bytes"`
	MaxLengthMessages int64         `yaml:"max_length_messages"`
}

// QueueReplication defines per-queue replication settings.
type QueueReplication struct {
	Enabled           bool          `yaml:"enabled"`
	Group             string        `yaml:"group"`
	ReplicationFactor int           `yaml:"replication_factor"`
	Mode              string        `yaml:"mode"` // sync, async
	MinInSyncReplicas int           `yaml:"min_in_sync_replicas"`
	AckTimeout        time.Duration `yaml:"ack_timeout"`

	// Optional per-queue Raft tuning overrides (zero = use cluster defaults).
	HeartbeatTimeout  time.Duration `yaml:"heartbeat_timeout"`
	ElectionTimeout   time.Duration `yaml:"election_timeout"`
	SnapshotInterval  time.Duration `yaml:"snapshot_interval"`
	SnapshotThreshold uint64        `yaml:"snapshot_threshold"`
}

// QueueManagerConfig defines runtime behavior for the queue manager.
type QueueManagerConfig struct {
	// AutoCommitInterval controls how often stream groups auto-commit offsets.
	// Zero means commit on every delivery batch.
	AutoCommitInterval time.Duration `yaml:"auto_commit_interval"`
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

	// MaxSendQueueSize controls per-connection outbound send queue depth.
	// 0 keeps synchronous writes; values > 0 enable asynchronous send queues.
	MaxSendQueueSize int `yaml:"max_send_queue_size"`

	// DisconnectOnFull controls behavior when send queue is full in async mode.
	// false blocks the producer (backpressure), true disconnects the slow client.
	DisconnectOnFull bool `yaml:"disconnect_on_full"`
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
	Enabled             bool              `yaml:"enabled"`
	AutoProvisionGroups bool              `yaml:"auto_provision_groups"` // Dynamically provision groups not listed in `groups`
	ReplicationFactor   int               `yaml:"replication_factor"`    // Number of replicas per partition (default: 3)
	SyncMode            bool              `yaml:"sync_mode"`             // true=wait for quorum, false=async
	MinInSyncReplicas   int               `yaml:"min_in_sync_replicas"`
	AckTimeout          time.Duration     `yaml:"ack_timeout"`
	WritePolicy         string            `yaml:"write_policy"`      // local, reject, forward
	DistributionMode    string            `yaml:"distribution_mode"` // forward, replicate
	BindAddr            string            `yaml:"bind_addr"`         // Base address for Raft (e.g., "127.0.0.1:7100")
	DataDir             string            `yaml:"data_dir"`          // Directory for Raft data
	Peers               map[string]string `yaml:"peers"`             // Map of nodeID -> raft base address

	// Raft tuning
	HeartbeatTimeout  time.Duration `yaml:"heartbeat_timeout"`
	ElectionTimeout   time.Duration `yaml:"election_timeout"`
	SnapshotInterval  time.Duration `yaml:"snapshot_interval"`
	SnapshotThreshold uint64        `yaml:"snapshot_threshold"`

	// Optional per-group overrides for true multi-group replication.
	// The key "default" overrides the base group above.
	Groups map[string]RaftGroupConfig `yaml:"groups"`
}

// RaftGroupConfig defines overrides for an individual Raft replication group.
type RaftGroupConfig struct {
	Enabled *bool `yaml:"enabled"`

	// Group network/storage endpoints.
	BindAddr string            `yaml:"bind_addr"` // Raft bind address for this group
	DataDir  string            `yaml:"data_dir"`  // Data dir for this group
	Peers    map[string]string `yaml:"peers"`     // nodeID -> raft bind address for this group

	// Optional per-group replication behavior overrides (zero/nil = inherit base RaftConfig).
	ReplicationFactor int           `yaml:"replication_factor"`
	SyncMode          *bool         `yaml:"sync_mode"`
	MinInSyncReplicas int           `yaml:"min_in_sync_replicas"`
	AckTimeout        time.Duration `yaml:"ack_timeout"`

	// Optional per-group Raft tuning overrides.
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

	// Inter-node routing batch policy.
	// route_batch_max_size controls flush size.
	// route_batch_max_delay controls max wait before flushing a partial batch.
	// route_batch_flush_workers controls the number of concurrent flush
	// goroutines per remote node. Higher values increase throughput when
	// gRPC calls are slow but use more goroutines. Default: 4.
	RouteBatchMaxSize      int           `yaml:"route_batch_max_size"`
	RouteBatchMaxDelay     time.Duration `yaml:"route_batch_max_delay"`
	RouteBatchFlushWorkers int           `yaml:"route_batch_flush_workers"`

	// RoutePublishTimeout is the maximum time to wait for a cross-cluster
	// publish to complete (including retries). Zero uses the default (15s).
	RoutePublishTimeout time.Duration `yaml:"route_publish_timeout"`

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
			MaxInflightMessages:   256,
			OfflineQueuePolicy:    "evict",
			MaxSendQueueSize:      0,
			DisconnectOnFull:      false,
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
				// Keep batches modest by default and latency low.
				RouteBatchMaxSize:      256,
				RouteBatchMaxDelay:     5 * time.Millisecond,
				RouteBatchFlushWorkers: 4,
				RoutePublishTimeout:    15 * time.Second,
			},
			Raft: RaftConfig{
				Enabled:             false, // Disabled by default
				AutoProvisionGroups: true,
				ReplicationFactor:   3,
				SyncMode:            true,
				MinInSyncReplicas:   2,
				AckTimeout:          5 * time.Second,
				WritePolicy:         "forward",
				DistributionMode:    "replicate",
				BindAddr:            "127.0.0.1:7100",
				DataDir:             "/tmp/fluxmq/raft",
				Peers:               map[string]string{},
				HeartbeatTimeout:    1 * time.Second,
				ElectionTimeout:     3 * time.Second,
				SnapshotInterval:    5 * time.Minute,
				SnapshotThreshold:   8192,
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
		QueueManager: QueueManagerConfig{
			AutoCommitInterval: 5 * time.Second,
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
				Replication: QueueReplication{
					Enabled:           false,
					ReplicationFactor: 3,
					Mode:              "sync",
					MinInSyncReplicas: 2,
					AckTimeout:        5 * time.Second,
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
	if c.Session.MaxSendQueueSize < 0 {
		return fmt.Errorf("session.max_send_queue_size cannot be negative")
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
		if c.Cluster.Transport.RouteBatchMaxSize < 0 {
			return fmt.Errorf("cluster.transport.route_batch_max_size must be >= 0")
		}
		if c.Cluster.Transport.RouteBatchMaxDelay < 0 {
			return fmt.Errorf("cluster.transport.route_batch_max_delay must be >= 0")
		}
		if c.Cluster.Transport.RouteBatchFlushWorkers < 0 {
			return fmt.Errorf("cluster.transport.route_batch_flush_workers must be >= 0")
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

		if c.Cluster.Raft.WritePolicy != "" {
			switch strings.ToLower(c.Cluster.Raft.WritePolicy) {
			case "local", "reject", "forward":
			default:
				return fmt.Errorf("cluster.raft.write_policy must be one of: local, reject, forward")
			}
		}
		if c.Cluster.Raft.DistributionMode != "" {
			switch strings.ToLower(c.Cluster.Raft.DistributionMode) {
			case "forward", "replicate":
			default:
				return fmt.Errorf("cluster.raft.distribution_mode must be one of: forward, replicate")
			}
		}

		if c.Cluster.Raft.Enabled {
			if strings.TrimSpace(c.Cluster.Raft.BindAddr) == "" {
				return fmt.Errorf("cluster.raft.bind_addr required when raft is enabled")
			}
			if strings.TrimSpace(c.Cluster.Raft.DataDir) == "" {
				return fmt.Errorf("cluster.raft.data_dir required when raft is enabled")
			}
			if c.Cluster.Raft.ReplicationFactor < 1 || c.Cluster.Raft.ReplicationFactor > 10 {
				return fmt.Errorf("cluster.raft.replication_factor must be between 1 and 10")
			}
			if c.Cluster.Raft.MinInSyncReplicas < 1 || c.Cluster.Raft.MinInSyncReplicas > c.Cluster.Raft.ReplicationFactor {
				return fmt.Errorf("cluster.raft.min_in_sync_replicas must be between 1 and replication_factor")
			}
			if c.Cluster.Raft.AckTimeout <= 0 {
				return fmt.Errorf("cluster.raft.ack_timeout must be > 0")
			}

			for groupID, groupCfg := range c.Cluster.Raft.Groups {
				gid := strings.TrimSpace(groupID)
				if gid == "" {
					return fmt.Errorf("cluster.raft.groups key cannot be empty")
				}

				groupEnabled := true
				if groupCfg.Enabled != nil {
					groupEnabled = *groupCfg.Enabled
				}
				if !groupEnabled {
					continue
				}

				// Non-default groups must define dedicated endpoints.
				if gid != "default" && strings.TrimSpace(groupCfg.BindAddr) == "" {
					return fmt.Errorf("cluster.raft.groups.%s.bind_addr required for non-default group", gid)
				}
				if gid != "default" && len(groupCfg.Peers) == 0 {
					return fmt.Errorf("cluster.raft.groups.%s.peers required for non-default group", gid)
				}

				if groupCfg.ReplicationFactor < 0 || groupCfg.ReplicationFactor > 10 {
					return fmt.Errorf("cluster.raft.groups.%s.replication_factor must be between 0 and 10", gid)
				}
				effectiveRF := c.Cluster.Raft.ReplicationFactor
				if groupCfg.ReplicationFactor > 0 {
					effectiveRF = groupCfg.ReplicationFactor
				}

				if groupCfg.MinInSyncReplicas < 0 || groupCfg.MinInSyncReplicas > effectiveRF {
					return fmt.Errorf("cluster.raft.groups.%s.min_in_sync_replicas must be between 0 and effective replication_factor", gid)
				}
				if groupCfg.AckTimeout < 0 {
					return fmt.Errorf("cluster.raft.groups.%s.ack_timeout must be >= 0", gid)
				}
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

	if c.QueueManager.AutoCommitInterval < 0 {
		return fmt.Errorf("queue_manager.auto_commit_interval must be >= 0")
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
		if q.Replication.Enabled {
			if q.Replication.Group != "" && strings.TrimSpace(q.Replication.Group) == "" {
				return fmt.Errorf("queues[%d].replication.group cannot be only whitespace", i)
			}

			if c.Cluster.Enabled && c.Cluster.Raft.Enabled && !c.Cluster.Raft.AutoProvisionGroups {
				groupID := strings.TrimSpace(q.Replication.Group)
				if groupID == "" {
					groupID = "default"
				}
				if groupID != "default" {
					if _, ok := c.Cluster.Raft.Groups[groupID]; !ok {
						return fmt.Errorf("queues[%d].replication.group '%s' is not configured under cluster.raft.groups and auto_provision_groups is disabled", i, groupID)
					}
				}
			}

			if q.Replication.ReplicationFactor < 1 || q.Replication.ReplicationFactor > 10 {
				return fmt.Errorf("queues[%d].replication.replication_factor must be between 1 and 10", i)
			}
			if q.Replication.MinInSyncReplicas < 1 || q.Replication.MinInSyncReplicas > q.Replication.ReplicationFactor {
				return fmt.Errorf("queues[%d].replication.min_in_sync_replicas must be between 1 and replication_factor", i)
			}
			switch strings.ToLower(q.Replication.Mode) {
			case "sync", "async":
			default:
				return fmt.Errorf("queues[%d].replication.mode must be one of: sync, async", i)
			}
			if q.Replication.AckTimeout <= 0 {
				return fmt.Errorf("queues[%d].replication.ack_timeout must be > 0", i)
			}
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
