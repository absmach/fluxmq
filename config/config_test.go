// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package config

import (
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const (
	testLogLevelDebug = "debug"
	testBindAddr      = "127.0.0.1:8100"
	testAuthURL       = "localhost:7016"
	testProfileHot    = "hot"
)

func TestDefault(t *testing.T) {
	cfg := Default()

	// Test server defaults
	if cfg.Server.TCP.V3.Addr != ":1883" {
		t.Errorf("expected default TCP v3 addr :1883, got %s", cfg.Server.TCP.V3.Addr)
	}
	if cfg.Server.TCP.V5.Addr != ":1884" {
		t.Errorf("expected default TCP v5 addr :1884, got %s", cfg.Server.TCP.V5.Addr)
	}
	if cfg.Server.TCP.V3.MaxConnections != 10000 {
		t.Errorf("expected default max connections 10000, got %d", cfg.Server.TCP.V3.MaxConnections)
	}
	if cfg.Server.TCP.V3.Protocol != ProtocolModeV3 {
		t.Errorf("expected default TCP v3 protocol %q, got %q", ProtocolModeV3, cfg.Server.TCP.V3.Protocol)
	}
	if cfg.Server.TCP.V5.Protocol != ProtocolModeV5 {
		t.Errorf("expected default TCP v5 protocol %q, got %q", ProtocolModeV5, cfg.Server.TCP.V5.Protocol)
	}
	if cfg.Server.WebSocket.V3.Protocol != ProtocolModeV3 {
		t.Errorf("expected default WebSocket v3 protocol %q, got %q", ProtocolModeV3, cfg.Server.WebSocket.V3.Protocol)
	}
	if cfg.Server.WebSocket.V5.Protocol != ProtocolModeV5 {
		t.Errorf("expected default WebSocket v5 protocol %q, got %q", ProtocolModeV5, cfg.Server.WebSocket.V5.Protocol)
	}
	if cfg.Server.AdminAPIAddr != ":8082" {
		t.Errorf("expected default admin API addr :8082, got %q", cfg.Server.AdminAPIAddr)
	}

	// Test broker defaults
	if cfg.Broker.RetryInterval != 20*time.Second {
		t.Errorf("expected retry interval 20s, got %v", cfg.Broker.RetryInterval)
	}

	// Test session defaults
	if cfg.Session.MaxSessions != 10000 {
		t.Errorf("expected max sessions 10000, got %d", cfg.Session.MaxSessions)
	}
	if cfg.Session.MaxSendQueueSize != 0 {
		t.Errorf("expected max send queue size 0, got %d", cfg.Session.MaxSendQueueSize)
	}
	if cfg.Session.DisconnectOnFull {
		t.Error("expected disconnect_on_full default false")
	}

	// Test log defaults
	if cfg.Log.Level != "info" {
		t.Errorf("expected log level info, got %s", cfg.Log.Level)
	}
}

func TestValidate(t *testing.T) {
	tests := []struct {
		name    string
		modify  func(*Config)
		wantErr bool
	}{
		{
			name:    "default config is valid",
			modify:  func(c *Config) {},
			wantErr: false,
		},
		{
			name: "no MQTT listeners configured",
			modify: func(c *Config) {
				c.Server.TCP.V3.Addr = ""
				c.Server.TCP.V5.Addr = ""
				c.Server.TCP.TLS.Addr = ""
				c.Server.TCP.MTLS.Addr = ""
				c.Server.WebSocket.V3.Addr = ""
				c.Server.WebSocket.V5.Addr = ""
				c.Server.WebSocket.TLS.Addr = ""
				c.Server.WebSocket.MTLS.Addr = ""
			},
			wantErr: true,
		},
		{
			name: "TCP TLS listener without cert",
			modify: func(c *Config) {
				c.Server.TCP.TLS.Addr = ":8883"
				c.Server.TCP.TLS.TLS.CertFile = ""
				c.Server.TCP.TLS.TLS.KeyFile = ""
			},
			wantErr: true,
		},
		{
			name: "message size too small",
			modify: func(c *Config) {
				c.Broker.MaxMessageSize = 100
			},
			wantErr: true,
		},
		{
			name: "invalid log level",
			modify: func(c *Config) {
				c.Log.Level = "invalid"
			},
			wantErr: true,
		},
		{
			name: "invalid tcp protocol mode",
			modify: func(c *Config) {
				c.Server.TCP.V3.Protocol = "v4"
			},
			wantErr: true,
		},
		{
			name: "invalid websocket protocol mode",
			modify: func(c *Config) {
				c.Server.WebSocket.V3.Protocol = "mqtt5"
			},
			wantErr: true,
		},
		{
			name: "retry interval too short",
			modify: func(c *Config) {
				c.Broker.RetryInterval = 500 * time.Millisecond
			},
			wantErr: true,
		},
		{
			name: "valid raft groups config",
			modify: func(c *Config) {
				c.Cluster.Raft.Enabled = true
				c.Cluster.Raft.Groups = map[string]RaftGroupConfig{
					testProfileHot: {
						BindAddr: testBindAddr,
						DataDir:  "/tmp/fluxmq/raft-hot",
						Peers: map[string]string{
							"broker-1": testBindAddr,
						},
						ReplicationFactor: 3,
						MinInSyncReplicas: 2,
					},
				}
			},
			wantErr: false,
		},
		{
			name: "invalid raft group missing bind addr",
			modify: func(c *Config) {
				c.Cluster.Raft.Enabled = true
				c.Cluster.Raft.Groups = map[string]RaftGroupConfig{
					testProfileHot: {
						Peers: map[string]string{
							"broker-1": testBindAddr,
						},
					},
				}
			},
			wantErr: true,
		},
		{
			name: "queue group must exist when auto provision disabled",
			modify: func(c *Config) {
				c.Cluster.Raft.Enabled = true
				c.Cluster.Raft.AutoProvisionGroups = false
				c.Queues = []QueueConfig{
					{
						Name:   "hot-events",
						Topics: []string{"$queue/hot-events/#"},
						Replication: QueueReplication{
							Enabled:           true,
							Group:             testProfileHot,
							ReplicationFactor: 3,
							Mode:              "sync",
							MinInSyncReplicas: 2,
							AckTimeout:        5 * time.Second,
						},
					},
				}
			},
			wantErr: true,
		},
		{
			name: "negative max send queue size",
			modify: func(c *Config) {
				c.Session.MaxSendQueueSize = -1
			},
			wantErr: true,
		},
		{
			name: "valid auth protocols",
			modify: func(c *Config) {
				c.Auth.URL = testAuthURL
				c.Auth.Protocols = map[string]bool{protocolMQTT: true, protocolAMQP091: false}
			},
			wantErr: false,
		},
		{
			name: "unknown auth protocol",
			modify: func(c *Config) {
				c.Auth.URL = testAuthURL
				c.Auth.Protocols = map[string]bool{protocolMQTT: true, "websocket": true}
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := Default()
			tt.modify(cfg)

			err := cfg.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestLoadNonExistent(t *testing.T) {
	cfg, err := Load("nonexistent.yaml")
	if err != nil {
		t.Fatalf("Load() should return default config and no error when file doesn't exist, got error: %v", err)
	}
	if cfg == nil {
		t.Fatal("Load() should return a default config, got nil")
	}

	if cfg.Server.TCP.V3.Addr != ":1883" {
		t.Errorf("expected default config, got TCP v3 addr %s", cfg.Server.TCP.V3.Addr)
	}
	if cfg.Server.TCP.V5.Addr != ":1884" {
		t.Errorf("expected default config, got TCP v5 addr %s", cfg.Server.TCP.V5.Addr)
	}
}

func TestSaveLoad(t *testing.T) {
	tmpfile := t.TempDir() + "/config.yaml"

	// Create custom config
	cfg := Default()
	cfg.Server.TCP.V3.Addr = ":2883"
	cfg.Server.TCP.V5.Addr = ":2884"
	cfg.Broker.RetryInterval = 30 * time.Second
	cfg.Log.Level = testLogLevelDebug

	// Save
	if err := cfg.Save(tmpfile); err != nil {
		t.Fatalf("Save() error = %v", err)
	}

	// Load
	loaded, err := Load(tmpfile)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	// Verify
	if loaded.Server.TCP.V3.Addr != ":2883" {
		t.Errorf("expected TCP v3 addr :2883, got %s", loaded.Server.TCP.V3.Addr)
	}
	if loaded.Server.TCP.V5.Addr != ":2884" {
		t.Errorf("expected TCP v5 addr :2884, got %s", loaded.Server.TCP.V5.Addr)
	}
	if loaded.Broker.RetryInterval != 30*time.Second {
		t.Errorf("expected retry interval 30s, got %v", loaded.Broker.RetryInterval)
	}
	if loaded.Log.Level != testLogLevelDebug {
		t.Errorf("expected log level debug, got %s", loaded.Log.Level)
	}
}

func TestAuthEnabledFor(t *testing.T) {
	tests := []struct {
		name     string
		cfg      AuthConfig
		protocol string
		want     bool
	}{
		{
			name:     "no URL disables all",
			cfg:      AuthConfig{},
			protocol: protocolMQTT,
			want:     false,
		},
		{
			name:     "URL set, empty protocols enables all",
			cfg:      AuthConfig{URL: testAuthURL},
			protocol: protocolAMQP091,
			want:     true,
		},
		{
			name:     "protocol explicitly enabled",
			cfg:      AuthConfig{URL: testAuthURL, Protocols: map[string]bool{protocolMQTT: true, protocolAMQP091: false}},
			protocol: protocolMQTT,
			want:     true,
		},
		{
			name:     "protocol explicitly disabled",
			cfg:      AuthConfig{URL: testAuthURL, Protocols: map[string]bool{protocolMQTT: true, protocolAMQP091: false}},
			protocol: protocolAMQP091,
			want:     false,
		},
		{
			name:     "protocol not in map defaults to false",
			cfg:      AuthConfig{URL: testAuthURL, Protocols: map[string]bool{protocolMQTT: true}},
			protocol: "amqp",
			want:     false,
		},
		{
			name:     "atom provider enables without callout URL",
			cfg:      AuthConfig{Provider: AuthProviderAtom, Atom: AtomAuthConfig{GRPCAddr: "atom:8081", ServiceTokenEnv: "TOKEN"}},
			protocol: AuthProtocolHTTP,
			want:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.cfg.AuthEnabledFor(tt.protocol); got != tt.want {
				t.Fatalf("AuthEnabledFor(%q) = %v, want %v", tt.protocol, got, tt.want)
			}
		})
	}
}

func TestAuthProviderName(t *testing.T) {
	tests := []struct {
		name string
		cfg  AuthConfig
		want string
	}{
		{name: "disabled", cfg: AuthConfig{}, want: ""},
		{name: "legacy url defaults to callout", cfg: AuthConfig{URL: testAuthURL}, want: AuthProviderCallout},
		{name: "explicit atom", cfg: AuthConfig{Provider: "ATOM"}, want: AuthProviderAtom},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.cfg.ProviderName(); got != tt.want {
				t.Fatalf("ProviderName() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestValidateAuthProvider(t *testing.T) {
	tests := []struct {
		name    string
		auth    AuthConfig
		wantErr string
	}{
		{
			name: "valid atom provider",
			auth: AuthConfig{
				Provider: AuthProviderAtom,
				Atom: AtomAuthConfig{
					GRPCAddr:        "atom:8081",
					ServiceTokenEnv: "FLUXMQ_ATOM_SERVICE_TOKEN",
				},
			},
		},
		{
			name:    "callout requires url when explicit",
			auth:    AuthConfig{Provider: AuthProviderCallout},
			wantErr: "auth.url is required",
		},
		{
			name:    "atom requires grpc address",
			auth:    AuthConfig{Provider: AuthProviderAtom, Atom: AtomAuthConfig{ServiceTokenEnv: "TOKEN"}},
			wantErr: "auth.atom.grpc_addr is required",
		},
		{
			name:    "atom requires service token source",
			auth:    AuthConfig{Provider: AuthProviderAtom, Atom: AtomAuthConfig{GRPCAddr: "atom:8081"}},
			wantErr: "service_token_env or auth.atom.service_token_file is required",
		},
		{
			name:    "unknown provider",
			auth:    AuthConfig{Provider: "custom"},
			wantErr: "auth.provider: unsupported provider",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := Default()
			cfg.Auth = tt.auth
			err := cfg.Validate()
			if tt.wantErr == "" {
				if err != nil {
					t.Fatalf("Validate() error = %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("Validate() error = %v, want containing %q", err, tt.wantErr)
			}
		})
	}
}

func TestExampleConfigsValid(t *testing.T) {
	files, err := filepath.Glob("../examples/*.yaml")
	if err != nil {
		t.Fatalf("glob examples: %v", err)
	}
	if len(files) == 0 {
		t.Fatal("no example configs found in ../examples/")
	}

	for _, f := range files {
		t.Run(filepath.Base(f), func(t *testing.T) {
			if _, err := Load(f); err != nil {
				t.Fatalf("Load(%s): %v", f, err)
			}
		})
	}
}

func TestNormalizeProtocolMode(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{name: "empty defaults to auto", in: "", want: ProtocolModeAuto},
		{name: "mixed case v3", in: "V3", want: ProtocolModeV3},
		{name: "mixed case v5", in: "V5", want: ProtocolModeV5},
		{name: "spaces around auto", in: " auto ", want: ProtocolModeAuto},
		{name: "unknown defaults to auto", in: " MQTT ", want: ProtocolModeAuto},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := NormalizeProtocolMode(tt.in); got != tt.want {
				t.Fatalf("NormalizeProtocolMode(%q)=%q, want %q", tt.in, got, tt.want)
			}
		})
	}
}
