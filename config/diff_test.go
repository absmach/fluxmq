// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package config

import (
	"slices"
	"testing"
	"time"
)

// TestAllFieldsClassified walks every leaf field in Config via reflection and
// asserts that each has an explicit entry in fieldClassification. This test
// catches new fields that are added without a reload classification.
func TestAllFieldsClassified(t *testing.T) {
	paths := LeafFieldPaths()
	if len(paths) == 0 {
		t.Fatal("LeafFieldPaths returned no paths — reflection walk is broken")
	}

	classified := AllClassifiedFields()
	var unclassified []string

	for _, path := range paths {
		if _, ok := classified[path]; !ok {
			unclassified = append(unclassified, path)
		}
	}
	if len(unclassified) > 0 {
		t.Fatalf("missing classifications for %d fields (first: %q)", len(unclassified), unclassified[0])
	}

	// Verify that all classification entries correspond to real config fields.
	for path := range classified {
		if !slices.Contains(paths, path) {
			t.Errorf("fieldClassification has entry %q that does not match any Config leaf field", path)
		}
	}
}

func TestDiffIdenticalConfigs(t *testing.T) {
	cfg := Default()
	result := Diff(cfg, cfg)
	if result.HasChanges() {
		t.Errorf("identical configs should produce no changes, got %d runtime-safe and %d restart-required",
			len(result.RuntimeSafe), len(result.RestartRequired))
	}
}

func TestDiffRuntimeSafeFields(t *testing.T) {
	tests := []struct {
		name   string
		modify func(cfg *Config)
		path   string
	}{
		{
			name:   "log level",
			modify: func(cfg *Config) { cfg.Log.Level = "debug" },
			path:   "Log.Level",
		},
		{
			name:   "log format",
			modify: func(cfg *Config) { cfg.Log.Format = "json" },
			path:   "Log.Format",
		},
		{
			name:   "ratelimit enabled",
			modify: func(cfg *Config) { cfg.RateLimit.Enabled = true },
			path:   "RateLimit.Enabled",
		},
		{
			name:   "ratelimit connection rate",
			modify: func(cfg *Config) { cfg.RateLimit.Connection.Rate = 999 },
			path:   "RateLimit.Connection.Rate",
		},
		{
			name:   "ratelimit connection burst",
			modify: func(cfg *Config) { cfg.RateLimit.Connection.Burst = 50 },
			path:   "RateLimit.Connection.Burst",
		},
		{
			name:   "ratelimit message rate",
			modify: func(cfg *Config) { cfg.RateLimit.Message.Rate = 500 },
			path:   "RateLimit.Message.Rate",
		},
		{
			name:   "ratelimit subscribe rate",
			modify: func(cfg *Config) { cfg.RateLimit.Subscribe.Rate = 50 },
			path:   "RateLimit.Subscribe.Rate",
		},
		{
			name:   "broker max qos",
			modify: func(cfg *Config) { cfg.Broker.MaxQoS = 1 },
			path:   "Broker.MaxQoS",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			old := Default()
			new := Default()
			tt.modify(new)

			result := Diff(old, new)
			if len(result.RuntimeSafe) != 1 {
				t.Fatalf("expected 1 runtime-safe change, got %d (restart-required: %d)",
					len(result.RuntimeSafe), len(result.RestartRequired))
			}
			if result.RuntimeSafe[0].Path != tt.path {
				t.Errorf("expected path %q, got %q", tt.path, result.RuntimeSafe[0].Path)
			}
			if result.RuntimeSafe[0].Class != RuntimeSafe {
				t.Errorf("expected RuntimeSafe class, got %s", result.RuntimeSafe[0].Class)
			}
			if len(result.RestartRequired) != 0 {
				t.Errorf("expected 0 restart-required changes, got %d", len(result.RestartRequired))
			}
		})
	}
}

func TestDiffRestartRequiredFields(t *testing.T) {
	tests := []struct {
		name   string
		modify func(cfg *Config)
		path   string
	}{
		{
			name:   "tcp listener addr",
			modify: func(cfg *Config) { cfg.Server.TCP.V3.Addr = ":9999" },
			path:   "Server.TCP.V3.Addr",
		},
		{
			name:   "storage type",
			modify: func(cfg *Config) { cfg.Storage.Type = "memory" },
			path:   "Storage.Type",
		},
		{
			name:   "cluster enabled",
			modify: func(cfg *Config) { cfg.Cluster.Enabled = false },
			path:   "Cluster.Enabled",
		},
		{
			name:   "session max sessions",
			modify: func(cfg *Config) { cfg.Session.MaxSessions = 50000 },
			path:   "Session.MaxSessions",
		},
		{
			name:   "auth url",
			modify: func(cfg *Config) { cfg.Auth.URL = "http://auth:9090" },
			path:   "Auth.URL",
		},
		{
			name:   "broker async fanout",
			modify: func(cfg *Config) { cfg.Broker.AsyncFanOut = true },
			path:   "Broker.AsyncFanOut",
		},
		{
			name:   "broker max message size",
			modify: func(cfg *Config) { cfg.Broker.MaxMessageSize = 2048 },
			path:   "Broker.MaxMessageSize",
		},
		{
			name:   "broker max retained messages",
			modify: func(cfg *Config) { cfg.Broker.MaxRetainedMessages = 5000 },
			path:   "Broker.MaxRetainedMessages",
		},
		{
			name:   "broker retry interval",
			modify: func(cfg *Config) { cfg.Broker.RetryInterval = 10 * time.Second },
			path:   "Broker.RetryInterval",
		},
		{
			name:   "broker max retries",
			modify: func(cfg *Config) { cfg.Broker.MaxRetries = 5 },
			path:   "Broker.MaxRetries",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			old := Default()
			new := Default()
			tt.modify(new)

			result := Diff(old, new)
			if len(result.RestartRequired) != 1 {
				t.Fatalf("expected 1 restart-required change, got %d (runtime-safe: %d)",
					len(result.RestartRequired), len(result.RuntimeSafe))
			}
			if result.RestartRequired[0].Path != tt.path {
				t.Errorf("expected path %q, got %q", tt.path, result.RestartRequired[0].Path)
			}
			if result.RestartRequired[0].Reason == "" {
				t.Error("expected non-empty restart reason")
			}
			if len(result.RuntimeSafe) != 0 {
				t.Errorf("expected 0 runtime-safe changes, got %d", len(result.RuntimeSafe))
			}
		})
	}
}

func TestDiffMixedChanges(t *testing.T) {
	old := Default()
	new := Default()
	new.Log.Level = "debug"
	new.Server.TCP.V3.Addr = ":9999"
	new.RateLimit.Message.Rate = 500

	result := Diff(old, new)
	if len(result.RuntimeSafe) != 2 {
		t.Errorf("expected 2 runtime-safe changes, got %d", len(result.RuntimeSafe))
	}
	if len(result.RestartRequired) != 1 {
		t.Errorf("expected 1 restart-required change, got %d", len(result.RestartRequired))
	}
}

func TestClassifyFieldDefault(t *testing.T) {
	if got := ClassifyField("Some.Unknown.Field"); got != RestartRequired {
		t.Errorf("unknown field should be RestartRequired, got %s", got)
	}
}

func TestReloadClassString(t *testing.T) {
	if RuntimeSafe.String() != "runtime_safe" {
		t.Errorf("RuntimeSafe.String() = %q", RuntimeSafe.String())
	}
	if RestartRequired.String() != "restart_required" {
		t.Errorf("RestartRequired.String() = %q", RestartRequired.String())
	}
}
