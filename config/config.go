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
}

// ServerConfig holds server-related configuration.
type ServerConfig struct {
	// TCP server settings
	TCPAddr         string        `yaml:"tcp_addr"`
	TCPMaxConn      int           `yaml:"tcp_max_connections"`
	TCPReadTimeout  time.Duration `yaml:"tcp_read_timeout"`
	TCPWriteTimeout time.Duration `yaml:"tcp_write_timeout"`

	// TLS settings
	TLSEnabled  bool   `yaml:"tls_enabled"`
	TLSCertFile string `yaml:"tls_cert_file"`
	TLSKeyFile  string `yaml:"tls_key_file"`

	// HTTP-MQTT bridge settings
	HTTPAddr    string `yaml:"http_addr"`
	HTTPEnabled bool   `yaml:"http_enabled"`

	// WebSocket settings
	WSAddr    string `yaml:"ws_addr"`
	WSPath    string `yaml:"ws_path"`
	WSEnabled bool   `yaml:"ws_enabled"`

	// CoAP settings
	CoAPAddr    string `yaml:"coap_addr"`
	CoAPEnabled bool   `yaml:"coap_enabled"`

	// Graceful shutdown timeout
	ShutdownTimeout time.Duration `yaml:"shutdown_timeout"`
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
}

// TransportConfig holds inter-broker transport configuration.
type TransportConfig struct {
	BindAddr string            `yaml:"bind_addr"` // gRPC address (e.g., "0.0.0.0:7948")
	Peers    map[string]string `yaml:"peers"`     // Map of nodeID -> transport address for peers
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
			HTTPAddr:        ":8080",
			HTTPEnabled:     false,
			WSAddr:          ":8083",
			WSPath:          "/mqtt",
			WSEnabled:       true,
			CoAPAddr:        ":5683",
			CoAPEnabled:     false,
			ShutdownTimeout: 30 * time.Second,
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
