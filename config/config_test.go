// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package config

import (
	"testing"
	"time"
)

func TestDefault(t *testing.T) {
	cfg := Default()

	// Test server defaults
	if cfg.Server.TCP.Plain.Addr != ":1883" {
		t.Errorf("expected default TCP addr :1883, got %s", cfg.Server.TCP.Plain.Addr)
	}
	if cfg.Server.TCP.Plain.MaxConnections != 10000 {
		t.Errorf("expected default max connections 10000, got %d", cfg.Server.TCP.Plain.MaxConnections)
	}

	// Test broker defaults
	if cfg.Broker.RetryInterval != 20*time.Second {
		t.Errorf("expected retry interval 20s, got %v", cfg.Broker.RetryInterval)
	}

	// Test session defaults
	if cfg.Session.MaxSessions != 10000 {
		t.Errorf("expected max sessions 10000, got %d", cfg.Session.MaxSessions)
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
				c.Server.TCP.Plain.Addr = ""
				c.Server.TCP.TLS.Addr = ""
				c.Server.TCP.MTLS.Addr = ""
				c.Server.WebSocket.Plain.Addr = ""
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
			name: "retry interval too short",
			modify: func(c *Config) {
				c.Broker.RetryInterval = 500 * time.Millisecond
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

	if cfg.Server.TCP.Plain.Addr != ":1883" {
		t.Errorf("expected default config, got TCP addr %s", cfg.Server.TCP.Plain.Addr)
	}
}

func TestSaveLoad(t *testing.T) {
	tmpfile := t.TempDir() + "/config.yaml"

	// Create custom config
	cfg := Default()
	cfg.Server.TCP.Plain.Addr = ":8883"
	cfg.Broker.RetryInterval = 30 * time.Second
	cfg.Log.Level = "debug"

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
	if loaded.Server.TCP.Plain.Addr != ":8883" {
		t.Errorf("expected TCP addr :8883, got %s", loaded.Server.TCP.Plain.Addr)
	}
	if loaded.Broker.RetryInterval != 30*time.Second {
		t.Errorf("expected retry interval 30s, got %v", loaded.Broker.RetryInterval)
	}
	if loaded.Log.Level != "debug" {
		t.Errorf("expected log level debug, got %s", loaded.Log.Level)
	}
}
