// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package config

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	mqtttls "github.com/absmach/fluxmq/pkg/tls"
	"github.com/stretchr/testify/require"
)

const (
	testLogLevelDebug  = "debug"
	testBindAddr       = "127.0.0.1:8100"
	testAuthURL        = "localhost:7016"
	testAuthHTTPSURL   = "https://auth.internal:7016"
	testClientCert     = "client.crt"
	testProfileHot     = "hot"
	testInitialCluster = "broker-1=http://127.0.0.1:2380"
	testBroker2        = "broker-2"
	testBroker3        = "broker-3"
	testRaftPeer2Addr  = "127.0.0.1:7101"
	testRaftPeer3Addr  = "127.0.0.1:7102"
	testCAFile         = "ca.crt"
)

// ptr builds a pointer to a literal, for the configuration keys that keep
// absent and zero distinct.
func ptr[T any](v T) *T { return &v }

func enableInsecureTestCluster(c *Config) {
	c.Cluster.Enabled = true
	c.Cluster.AllowInsecure = true
	c.Cluster.Etcd.InitialCluster = testInitialCluster
	c.Cluster.Raft.Peers = map[string]string{
		testBroker2: testRaftPeer2Addr,
		testBroker3: testRaftPeer3Addr,
	}
}

func TestDefault(t *testing.T) {
	cfg := Default()

	// Test server defaults
	if cfg.Server.MQTT.TCP.V3.Addr != ":1883" {
		t.Errorf("expected default TCP v3 addr :1883, got %s", cfg.Server.MQTT.TCP.V3.Addr)
	}
	if cfg.Server.MQTT.TCP.V5.Addr != ":1884" {
		t.Errorf("expected default TCP v5 addr :1884, got %s", cfg.Server.MQTT.TCP.V5.Addr)
	}
	if cfg.Server.MQTT.TCP.V3.MaxConnections != 10000 {
		t.Errorf("expected default max connections 10000, got %d", cfg.Server.MQTT.TCP.V3.MaxConnections)
	}
	if cfg.Server.MQTT.TCP.V3.Protocol != ProtocolModeV3 {
		t.Errorf("expected default TCP v3 protocol %q, got %q", ProtocolModeV3, cfg.Server.MQTT.TCP.V3.Protocol)
	}
	if cfg.Server.MQTT.TCP.V5.Protocol != ProtocolModeV5 {
		t.Errorf("expected default TCP v5 protocol %q, got %q", ProtocolModeV5, cfg.Server.MQTT.TCP.V5.Protocol)
	}
	if cfg.Server.MQTT.WebSocket.V3.Protocol != ProtocolModeV3 {
		t.Errorf("expected default WebSocket v3 protocol %q, got %q", ProtocolModeV3, cfg.Server.MQTT.WebSocket.V3.Protocol)
	}
	if cfg.Server.MQTT.WebSocket.V5.Protocol != ProtocolModeV5 {
		t.Errorf("expected default WebSocket v5 protocol %q, got %q", ProtocolModeV5, cfg.Server.MQTT.WebSocket.V5.Protocol)
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

	// Queue distribution must remain usable when the optional Raft engine is
	// disabled. Replicate without Raft is rejected at queue-manager startup.
	if cfg.Cluster.Raft.Enabled {
		t.Error("expected queue Raft default disabled")
	}
	if cfg.Cluster.Raft.DistributionMode != distributionModeForward {
		t.Errorf("expected distribution mode default %q when Raft is disabled, got %q", distributionModeForward, cfg.Cluster.Raft.DistributionMode)
	}
}

func TestClusterTransportSecurityValidation(t *testing.T) {
	tests := []struct {
		name      string
		configure func(*Config)
		wantError string
	}{
		{
			name: "plaintext requires explicit opt-in",
			configure: func(c *Config) {
				c.Cluster.Enabled = true
				c.Cluster.Etcd.InitialCluster = testInitialCluster
			},
			wantError: "cluster transport TLS required",
		},
		{
			name: "explicit insecure loopback development cluster",
			configure: func(c *Config) {
				c.Cluster.Enabled = true
				c.Cluster.AllowInsecure = true
				c.Cluster.Etcd.InitialCluster = testInitialCluster
			},
		},
		{
			name: "etcd client cannot bind wildcard",
			configure: func(c *Config) {
				c.Cluster.Enabled = true
				c.Cluster.AllowInsecure = true
				c.Cluster.Etcd.ClientAddr = "0.0.0.0:2379"
				c.Cluster.Etcd.InitialCluster = testInitialCluster
			},
			wantError: "cluster.etcd.client_addr must be loopback-only",
		},
		{
			name: "secure cluster requires https etcd members",
			configure: func(c *Config) {
				c.Cluster.Enabled = true
				c.Cluster.Transport.TLSEnabled = true
				c.Cluster.Transport.TLSCertFile = "node.crt"
				c.Cluster.Transport.TLSKeyFile = "node.key"
				c.Cluster.Transport.TLSCAFile = testCAFile
				c.Cluster.Etcd.InitialCluster = testInitialCluster
			},
			wantError: "must use https",
		},
		{
			name: "secure cluster",
			configure: func(c *Config) {
				c.Cluster.Enabled = true
				c.Cluster.Transport.TLSEnabled = true
				c.Cluster.Transport.TLSCertFile = "node.crt"
				c.Cluster.Transport.TLSKeyFile = "node.key"
				c.Cluster.Transport.TLSCAFile = testCAFile
				c.Cluster.Etcd.InitialCluster = "broker-1=https://127.0.0.1:2380"
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := Default()
			tt.configure(cfg)
			err := cfg.Validate()
			if tt.wantError == "" {
				if err != nil {
					t.Fatalf("Validate() error = %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tt.wantError) {
				t.Fatalf("Validate() error = %v, want it to contain %q", err, tt.wantError)
			}
		})
	}
}

func TestReplicatedQueueConfigurationFailsClosed(t *testing.T) {
	configureRaft := func(c *Config) {
		enableInsecureTestCluster(c)
		c.Cluster.Raft.Enabled = true
		c.Cluster.Raft.WritePolicy = writePolicyForward
		c.Cluster.Raft.Peers = map[string]string{
			testBroker2: testRaftPeer2Addr,
			testBroker3: testRaftPeer3Addr,
		}
	}
	enableQueueReplication := func(c *Config) {
		c.Queues[0].Replication.Enabled = true
	}
	tests := []struct {
		name      string
		configure func(*Config)
		wantError string
	}{
		{
			name: "raft disabled",
			configure: func(c *Config) {
				enableQueueReplication(c)
			},
			wantError: "requires cluster.raft.enabled",
		},
		{
			name: "local write policy",
			configure: func(c *Config) {
				configureRaft(c)
				c.Cluster.Raft.WritePolicy = writePolicyLocal
				enableQueueReplication(c)
			},
			wantError: "requires cluster.raft.write_policy reject or forward",
		},
		{
			name: "membership mismatch",
			configure: func(c *Config) {
				configureRaft(c)
				delete(c.Cluster.Raft.Peers, testBroker3)
				enableQueueReplication(c)
			},
			wantError: "must match configured membership",
		},
		{
			name: "queue factor mismatch",
			configure: func(c *Config) {
				configureRaft(c)
				enableQueueReplication(c)
				c.Queues[0].Replication.ReplicationFactor = 2
			},
			wantError: "must match raft group factor",
		},
		{
			name: "valid replicated queue",
			configure: func(c *Config) {
				configureRaft(c)
				enableQueueReplication(c)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := Default()
			tt.configure(cfg)
			err := cfg.Validate()
			if tt.wantError == "" {
				if err != nil {
					t.Fatalf("Validate() error = %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tt.wantError) {
				t.Fatalf("Validate() error = %v, want it to contain %q", err, tt.wantError)
			}
		})
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
			name:    "no messaging listeners configured",
			modify:  disableMessagingListeners,
			wantErr: true,
		},
		{
			name: "AMQP 0.9.1-only deployment is valid",
			modify: func(c *Config) {
				disableMessagingListeners(c)
				c.Server.AMQP091.Plain.Addr = ":5682"
				c.Server.AMQP091.Plain.MaxConnections = 100
			},
			wantErr: false,
		},
		{
			name: "TCP TLS listener without cert",
			modify: func(c *Config) {
				c.Server.MQTT.TCP.TLS.Addr = ":8883"
				c.Server.MQTT.TCP.TLS.TLS.CertFile = ""
				c.Server.MQTT.TCP.TLS.TLS.KeyFile = ""
			},
			wantErr: true,
		},
		{
			name: "negative websocket max_connections",
			modify: func(c *Config) {
				c.Server.MQTT.WebSocket.V3.MaxConnections = -1
			},
			wantErr: true,
		},
		{
			name: "negative websocket read_timeout",
			modify: func(c *Config) {
				c.Server.MQTT.WebSocket.V3.ReadTimeout = -time.Second
			},
			wantErr: true,
		},
		{
			name: "negative websocket write_timeout",
			modify: func(c *Config) {
				c.Server.MQTT.WebSocket.V3.WriteTimeout = -time.Second
			},
			wantErr: true,
		},
		{
			name: "negative tcp read_timeout",
			modify: func(c *Config) {
				c.Server.MQTT.TCP.V3.ReadTimeout = -time.Second
			},
			wantErr: true,
		},
		{
			name: "negative tcp write_timeout",
			modify: func(c *Config) {
				c.Server.MQTT.TCP.V3.WriteTimeout = -time.Second
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
				c.Server.MQTT.TCP.V3.Protocol = "v4"
			},
			wantErr: true,
		},
		{
			name: "invalid websocket protocol mode",
			modify: func(c *Config) {
				c.Server.MQTT.WebSocket.V3.Protocol = "mqtt5"
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
				enableInsecureTestCluster(c)
				c.Cluster.Raft.Enabled = true
				c.Cluster.Raft.Groups = map[string]RaftGroupConfig{
					testProfileHot: {
						BindAddr: testBindAddr,
						DataDir:  "/tmp/fluxmq/raft-hot",
						Peers: map[string]string{
							testBroker2: testBindAddr,
							testBroker3: "127.0.0.1:8101",
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
				enableInsecureTestCluster(c)
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
				enableInsecureTestCluster(c)
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
				c.Auth.External.URL = testAuthURL
				c.Auth.External.Protocols = map[string]bool{protocolMQTT: true, protocolAMQP091: false}
			},
			wantErr: false,
		},
		{
			name: "auth callout TLS over https",
			modify: func(c *Config) {
				c.Auth.External.URL = testAuthHTTPSURL
				c.Auth.External.TLS = &mqtttls.ClientConfig{
					CertFile: testClientCert,
					KeyFile:  "client.key",
					CAFile:   testCAFile,
				}
			},
			wantErr: false,
		},
		{
			// The connection would be cleartext while the config advertises a
			// client certificate: it looks mutually authenticated and is not
			// authenticated at all.
			name: "auth callout TLS over a plaintext url",
			modify: func(c *Config) {
				c.Auth.External.URL = "http://auth.internal:7016"
				c.Auth.External.TLS = &mqtttls.ClientConfig{
					CertFile: testClientCert,
					KeyFile:  "client.key",
				}
			},
			wantErr: true,
		},
		{
			name: "auth callout TLS with a certificate but no key",
			modify: func(c *Config) {
				c.Auth.External.URL = testAuthHTTPSURL
				c.Auth.External.TLS = &mqtttls.ClientConfig{CertFile: testClientCert}
			},
			wantErr: true,
		},
		{
			name: "auth callout TLS with an unknown min_version",
			modify: func(c *Config) {
				c.Auth.External.URL = testAuthHTTPSURL
				c.Auth.External.TLS = &mqtttls.ClientConfig{MinVersion: "TLS9.9"}
			},
			wantErr: true,
		},
		{
			name: "unknown auth protocol",
			modify: func(c *Config) {
				c.Auth.External.URL = testAuthURL
				c.Auth.External.Protocols = map[string]bool{protocolMQTT: true, sectionMQTTWebSocket: true}
			},
			wantErr: true,
		},
		{
			name: "valid blocking hook protocols",
			modify: func(c *Config) {
				c.Hooks.URL = testAuthURL
				c.Hooks.Protocols = map[string]bool{protocolMQTT: true, protocolAMQP: true, protocolAMQP091: false}
				c.Hooks.Events = map[string]bool{"auth_on_publish": true}
			},
			wantErr: false,
		},
		{
			name: "unknown blocking hook protocol",
			modify: func(c *Config) {
				c.Hooks.URL = testAuthURL
				c.Hooks.Protocols = map[string]bool{protocolMQTT: true, sectionMQTTWebSocket: true}
			},
			wantErr: true,
		},
		{
			name: "unknown blocking hook event",
			modify: func(c *Config) {
				c.Hooks.URL = testAuthURL
				c.Hooks.Events = map[string]bool{"auth_publish": true}
			},
			wantErr: true,
		},
		{
			name: "invalid blocking hook fail mode",
			modify: func(c *Config) {
				c.Hooks.URL = testAuthURL
				c.Hooks.FailMode = "open"
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

func disableMessagingListeners(c *Config) {
	c.Server.MQTT.TCP.V3.Addr = ""
	c.Server.MQTT.TCP.V5.Addr = ""
	c.Server.MQTT.TCP.TLS.Addr = ""
	c.Server.MQTT.TCP.MTLS.Addr = ""
	c.Server.MQTT.WebSocket.V3.Addr = ""
	c.Server.MQTT.WebSocket.V5.Addr = ""
	c.Server.MQTT.WebSocket.TLS.Addr = ""
	c.Server.MQTT.WebSocket.MTLS.Addr = ""
	c.Server.HTTP.Plain.Addr = ""
	c.Server.HTTP.TLS.Addr = ""
	c.Server.HTTP.MTLS.Addr = ""
	c.Server.CoAP.Plain.Addr = ""
	c.Server.CoAP.DTLS.Addr = ""
	c.Server.CoAP.MDTLS.Addr = ""
	c.Server.AMQP.Plain.Addr = ""
	c.Server.AMQP.TLS.Addr = ""
	c.Server.AMQP.MTLS.Addr = ""
	c.Server.AMQP091.Plain.Addr = ""
	c.Server.AMQP091.TLS.Addr = ""
	c.Server.AMQP091.MTLS.Addr = ""
	c.Server.AMQP091.Internal.Addr = ""
}

// TestLoadNonExistent pins the fail-fast contract. A named config file that
// does not exist must be an error: falling back to defaults would turn a typo
// in a unit file or chart into a broker running with none of the operator's
// settings, authentication included.
func TestLoadNonExistent(t *testing.T) {
	cfg, err := Load("nonexistent.yaml")
	if err == nil {
		t.Fatal("Load() must fail when the named config file does not exist")
	}
	if !errors.Is(err, ErrConfigNotFound) {
		t.Fatalf("Load() should report ErrConfigNotFound, got %v", err)
	}
	if cfg != nil {
		t.Fatalf("Load() should return a nil config on error, got %+v", cfg)
	}
}

// TestLoadOptionalNonExistent covers the opt-in fallback behind
// --config-optional.
func TestLoadOptionalNonExistent(t *testing.T) {
	cfg, err := LoadOptional("nonexistent.yaml")
	if err != nil {
		t.Fatalf("LoadOptional() should return the default config when the file is missing, got error: %v", err)
	}
	if cfg == nil {
		t.Fatal("LoadOptional() should return a default config, got nil")
	}

	if cfg.Server.MQTT.TCP.V3.Addr != ":1883" {
		t.Errorf("expected default config, got TCP v3 addr %s", cfg.Server.MQTT.TCP.V3.Addr)
	}
	if cfg.Server.MQTT.TCP.V5.Addr != ":1884" {
		t.Errorf("expected default config, got TCP v5 addr %s", cfg.Server.MQTT.TCP.V5.Addr)
	}
}

func TestSaveLoad(t *testing.T) {
	tmpfile := t.TempDir() + "/config.yaml"

	// Create custom config
	cfg := Default()
	cfg.Server.MQTT.TCP.V3.Addr = ":2883"
	cfg.Server.MQTT.TCP.V5.Addr = ":2884"
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
	if loaded.Server.MQTT.TCP.V3.Addr != ":2883" {
		t.Errorf("expected TCP v3 addr :2883, got %s", loaded.Server.MQTT.TCP.V3.Addr)
	}
	if loaded.Server.MQTT.TCP.V5.Addr != ":2884" {
		t.Errorf("expected TCP v5 addr :2884, got %s", loaded.Server.MQTT.TCP.V5.Addr)
	}
	if loaded.Broker.RetryInterval != 30*time.Second {
		t.Errorf("expected retry interval 30s, got %v", loaded.Broker.RetryInterval)
	}
	if loaded.Log.Level != testLogLevelDebug {
		t.Errorf("expected log level debug, got %s", loaded.Log.Level)
	}
}

func TestExternalAuthEnabledFor(t *testing.T) {
	tests := []struct {
		name     string
		cfg      ExternalAuthConfig
		protocol string
		want     bool
	}{
		{
			name:     "no URL disables all",
			cfg:      ExternalAuthConfig{},
			protocol: protocolMQTT,
			want:     false,
		},
		{
			name:     "URL set, empty protocols enables all",
			cfg:      ExternalAuthConfig{URL: testAuthURL},
			protocol: protocolAMQP091,
			want:     true,
		},
		{
			name:     "protocol explicitly enabled",
			cfg:      ExternalAuthConfig{URL: testAuthURL, Protocols: map[string]bool{protocolMQTT: true, protocolAMQP091: false}},
			protocol: protocolMQTT,
			want:     true,
		},
		{
			name:     "protocol explicitly disabled",
			cfg:      ExternalAuthConfig{URL: testAuthURL, Protocols: map[string]bool{protocolMQTT: true, protocolAMQP091: false}},
			protocol: protocolAMQP091,
			want:     false,
		},
		{
			name:     "protocol not in map defaults to false",
			cfg:      ExternalAuthConfig{URL: testAuthURL, Protocols: map[string]bool{protocolMQTT: true}},
			protocol: "amqp",
			want:     false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.cfg.EnabledFor(tt.protocol); got != tt.want {
				t.Fatalf("EnabledFor(%q) = %v, want %v", tt.protocol, got, tt.want)
			}
		})
	}
}

func TestMQTTMTLSTwoFactorValidation(t *testing.T) {
	configure := func(c *Config, websocket bool) {
		c.Auth.External.URL = testAuthURL
		c.Auth.External.Protocols = map[string]bool{protocolMQTT: true}
		if websocket {
			listener := &c.Server.MQTT.WebSocket.MTLS
			listener.Addr = "127.0.0.1:2885"
			listener.TLS.CertFile = "server.crt"
			listener.TLS.KeyFile = "server.key"
			listener.TLS.ClientCAFile = testCAFile
			listener.TLS.ClientAuth = clientAuthRequire
			return
		}
		listener := &c.Server.MQTT.TCP.MTLS
		listener.Addr = "127.0.0.1:2885"
		listener.TLS.CertFile = "server.crt"
		listener.TLS.KeyFile = "server.key"
		listener.TLS.ClientCAFile = testCAFile
		listener.TLS.ClientAuth = clientAuthRequire
	}

	for _, websocket := range []bool{false, true} {
		transport := "tcp"
		if websocket {
			transport = "websocket"
		}
		t.Run(transport, func(t *testing.T) {
			t.Run("valid default common name binding", func(t *testing.T) {
				cfg := Default()
				configure(cfg, websocket)
				require.NoError(t, cfg.Validate())
			})

			t.Run("external MQTT auth required", func(t *testing.T) {
				cfg := Default()
				configure(cfg, websocket)
				cfg.Auth.External.URL = ""
				require.ErrorContains(t, cfg.Validate(), "requires auth.external with mqtt enabled")
			})

			t.Run("equivalent client auth spellings accepted", func(t *testing.T) {
				for _, mode := range []string{"require", "require_and_verify", "require-and-verify", "requireandverify", ""} {
					name := mode
					if name == "" {
						name = "unset with client CA"
					}
					t.Run(name, func(t *testing.T) {
						cfg := Default()
						configure(cfg, websocket)
						if websocket {
							cfg.Server.MQTT.WebSocket.MTLS.TLS.ClientAuth = mode
						} else {
							cfg.Server.MQTT.TCP.MTLS.TLS.ClientAuth = mode
						}
						require.NoError(t, cfg.Validate())
					})
				}
			})

			t.Run("weak client auth rejected", func(t *testing.T) {
				cfg := Default()
				configure(cfg, websocket)
				if websocket {
					cfg.Server.MQTT.WebSocket.MTLS.TLS.ClientAuth = "verify_if_given"
				} else {
					cfg.Server.MQTT.TCP.MTLS.TLS.ClientAuth = "verify_if_given"
				}
				require.ErrorContains(t, cfg.Validate(), "client_auth must verify the client certificate")
			})
		})
	}
}

func TestHooksEnabledFor(t *testing.T) {
	tests := []struct {
		name     string
		cfg      HooksConfig
		protocol string
		want     bool
	}{
		{
			name:     "no URL disables all",
			cfg:      HooksConfig{},
			protocol: protocolMQTT,
			want:     false,
		},
		{
			name:     "URL set, empty protocols enables all",
			cfg:      HooksConfig{URL: testAuthURL},
			protocol: protocolAMQP091,
			want:     true,
		},
		{
			name:     "protocol explicitly enabled",
			cfg:      HooksConfig{URL: testAuthURL, Protocols: map[string]bool{protocolMQTT: true, protocolAMQP091: false}},
			protocol: protocolMQTT,
			want:     true,
		},
		{
			name:     "protocol explicitly disabled",
			cfg:      HooksConfig{URL: testAuthURL, Protocols: map[string]bool{protocolMQTT: true, protocolAMQP091: false}},
			protocol: protocolAMQP091,
			want:     false,
		},
		{
			name:     "protocol not in map defaults to false",
			cfg:      HooksConfig{URL: testAuthURL, Protocols: map[string]bool{protocolMQTT: true}},
			protocol: protocolAMQP,
			want:     false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.cfg.EnabledFor(tt.protocol); got != tt.want {
				t.Fatalf("EnabledFor(%q) = %v, want %v", tt.protocol, got, tt.want)
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
			loadPath := f
			contents, err := os.ReadFile(f)
			if err != nil {
				t.Fatalf("read example %s: %v", f, err)
			}
			if strings.Contains(string(contents), "/run/secrets/audit_secret_") {
				dir := t.TempDir()
				current := writeSecret(t, dir, "current", strings.Repeat("a", 32))
				previous := writeSecret(t, dir, "previous", strings.Repeat("b", 32))
				rewritten := strings.ReplaceAll(string(contents), "/run/secrets/audit_secret_current", current)
				rewritten = strings.ReplaceAll(rewritten, "/run/secrets/audit_secret_previous", previous)
				loadPath = filepath.Join(dir, filepath.Base(f))
				if err := os.WriteFile(loadPath, []byte(rewritten), 0o600); err != nil {
					t.Fatalf("write rewritten example: %v", err)
				}
			}
			if _, err := Load(loadPath); err != nil {
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

// The capture knobs bound how much unwritten capture a stalled queue store can
// hold, so a negative value is a configuration error rather than a default.
// Zero is not: it selects the built-in default, which is what an unset field
// decodes to.
func TestValidateQueueManagerCaptureBounds(t *testing.T) {
	tests := []struct {
		name      string
		configure func(*QueueManagerConfig)
		wantError string
	}{
		{
			name:      "defaults are valid",
			configure: func(*QueueManagerConfig) {},
		},
		{
			// Omitting a key is what selects the built-in default.
			name: "omitted keys are valid",
			configure: func(q *QueueManagerConfig) {
				q.CaptureWorkers, q.CaptureQueueDepth, q.CaptureDrainTimeout = nil, nil, nil
			},
		},
		{
			name:      "explicit values are valid",
			configure: func(q *QueueManagerConfig) { q.CaptureWorkers, q.CaptureQueueDepth = ptr(2), ptr(64) },
		},
		{
			// A written zero used to select the default silently, so the same
			// literal meant opposite things in different sections.
			name:      "written zero rejected",
			configure: func(q *QueueManagerConfig) { q.CaptureWorkers = ptr(0) },
			wantError: "queue_manager.capture_workers must be greater than zero",
		},
		{
			name:      "negative workers rejected",
			configure: func(q *QueueManagerConfig) { q.CaptureWorkers = ptr(-1) },
			wantError: "queue_manager.capture_workers must be greater than zero",
		},
		{
			name:      "negative depth rejected",
			configure: func(q *QueueManagerConfig) { q.CaptureQueueDepth = ptr(-1) },
			wantError: "queue_manager.capture_queue_depth must be greater than zero",
		},
		{
			name:      "negative drain timeout rejected",
			configure: func(q *QueueManagerConfig) { q.CaptureDrainTimeout = ptr(-1 * time.Second) },
			wantError: "queue_manager.capture_drain_timeout must be greater than zero",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := Default()
			tt.configure(&cfg.QueueManager)
			err := cfg.Validate()
			if tt.wantError == "" {
				if err != nil {
					t.Fatalf("Validate() error = %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tt.wantError) {
				t.Fatalf("Validate() error = %v, want it to contain %q", err, tt.wantError)
			}
		})
	}
}

// A queue bound to a filter that can never match receives nothing, and nothing
// about a silent queue says why. The configuration is refused at load instead.
func TestValidateRejectsMalformedQueueTopicFilters(t *testing.T) {
	tests := []struct {
		name      string
		topics    []string
		wantError string
	}{
		{name: "valid queue address", topics: []string{"$queue/orders/#"}},
		{name: "valid ordinary pattern", topics: []string{"m/+/events"}},
		{name: "multi level wildcard not final", topics: []string{"#/events"}, wantError: `queues[0].topics[0] "#/events" is not a valid topic filter`},
		{name: "second filter malformed", topics: []string{"m/#", "a/#/b"}, wantError: `queues[0].topics[1] "a/#/b" is not a valid topic filter`},
		{name: "empty filter", topics: []string{""}, wantError: `queues[0].topics[0] "" is not a valid topic filter`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := Default()
			cfg.Queues = []QueueConfig{{Name: "q", Topics: tt.topics}}

			err := cfg.Validate()
			if tt.wantError == "" {
				if err != nil {
					t.Fatalf("Validate() error = %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tt.wantError) {
				t.Fatalf("Validate() error = %v, want it to contain %q", err, tt.wantError)
			}
		})
	}
}
