// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package config

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const (
	testPrincipalName = "atom-audit-publisher"
	testPrincipalSAN  = "spiffe://absmach/atom/audit-publisher"
)

func TestLoadNestedAuth(t *testing.T) {
	dir := t.TempDir()
	current := writeSecret(t, dir, "current", strings.Repeat("a", 32)+"\n")
	previous := writeSecret(t, dir, "previous", strings.Repeat("b", 32)+"\r\n")
	filename := filepath.Join(dir, "config.yaml")
	contents := fmt.Sprintf(`
auth:
  external:
    url: "http://auth.internal:8181"
    transport: "http"
    timeout: 2s
    protocols:
      amqp091: true
    identity_cache_size: 123
    identity_cache_ttl: 1h
  local_principals:
    - name: %q
      certificate_uri_san: %q
      current_secret_file: %q
      previous_secret_file: %q
      permissions:
        publish:
          - exchange: ""
            routing_key: atom-audit
        subscribe: []
`, testPrincipalName, testPrincipalSAN, current, previous)
	if err := os.WriteFile(filename, []byte(contents), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	cfg, err := Load(filename)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.Auth.External.URL != "http://auth.internal:8181" {
		t.Fatalf("external URL = %q", cfg.Auth.External.URL)
	}
	if cfg.Auth.External.Timeout != 2*time.Second {
		t.Fatalf("external timeout = %v", cfg.Auth.External.Timeout)
	}
	if !cfg.Auth.External.EnabledFor(protocolAMQP091) {
		t.Fatal("expected AMQP 0.9.1 external auth to be enabled")
	}
	if len(cfg.Auth.LocalPrincipals) != 1 {
		t.Fatalf("local principal count = %d, want 1", len(cfg.Auth.LocalPrincipals))
	}
	principal := cfg.Auth.LocalPrincipals[0]
	if principal.Name != testPrincipalName || principal.CertificateURISAN != testPrincipalSAN {
		t.Fatalf("unexpected local principal: %+v", principal)
	}
	if len(principal.Permissions.Publish) != 1 || principal.Permissions.Publish[0].RoutingKey != "atom-audit" {
		t.Fatalf("unexpected publish permissions: %+v", principal.Permissions.Publish)
	}
}

func TestLoadRejectsLegacyAuthKeys(t *testing.T) {
	tests := map[string]string{
		"url":                 "auth.external.url",
		"transport":           "auth.external.transport",
		"timeout":             "auth.external.timeout",
		"protocols":           "auth.external.protocols",
		"identity_cache_size": "auth.external.identity_cache_size",
		"identity_cache_ttl":  "auth.external.identity_cache_ttl",
	}

	for key, replacement := range tests {
		t.Run(key, func(t *testing.T) {
			filename := filepath.Join(t.TempDir(), "config.yaml")
			contents := fmt.Sprintf("auth:\n  %s: value\n", key)
			if err := os.WriteFile(filename, []byte(contents), 0o600); err != nil {
				t.Fatalf("write config: %v", err)
			}

			_, err := Load(filename)
			if err == nil {
				t.Fatal("Load() succeeded with a legacy auth key")
			}
			want := fmt.Sprintf("auth.%s is no longer supported; use %s", key, replacement)
			if !strings.Contains(err.Error(), want) {
				t.Fatalf("Load() error = %q, want it to contain %q", err, want)
			}
		})
	}
}

func TestLoadRejectsUnknownAuthFields(t *testing.T) {
	filename := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(filename, []byte("auth:\n  external:\n    unsupported: true\n"), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	_, err := Load(filename)
	if err == nil || !strings.Contains(err.Error(), "field unsupported not found") {
		t.Fatalf("Load() error = %v, want strict unknown-field error", err)
	}
}

func TestValidateLocalPrincipals(t *testing.T) {
	dir := t.TempDir()
	current := writeSecret(t, dir, "current", strings.Repeat("a", 32)+"\n")
	previous := writeSecret(t, dir, "previous", strings.Repeat("b", 32)+"\r\n")
	weak := writeSecret(t, dir, "weak", strings.Repeat("c", 31)+"\n")
	doubleNewline := writeSecret(t, dir, "double-newline", strings.Repeat("d", 32)+"\n\n")
	nul := writeSecret(t, dir, "nul", strings.Repeat("e", 16)+"\x00"+strings.Repeat("e", 16))

	valid := func() LocalPrincipalConfig {
		return LocalPrincipalConfig{
			Name:               testPrincipalName,
			CertificateURISAN:  testPrincipalSAN,
			CurrentSecretFile:  current,
			PreviousSecretFile: previous,
			Permissions: LocalPermissionsConfig{
				Publish: []LocalPublishPermission{{Exchange: "", RoutingKey: "atom-audit"}},
			},
		}
	}

	tests := []struct {
		name       string
		principals func() []LocalPrincipalConfig
		wantError  string
	}{
		{
			name:       "valid current and previous secret",
			principals: func() []LocalPrincipalConfig { return []LocalPrincipalConfig{valid()} },
		},
		{
			name: "duplicate name",
			principals: func() []LocalPrincipalConfig {
				first, second := valid(), valid()
				second.CertificateURISAN = "spiffe://absmach/atom/other"
				return []LocalPrincipalConfig{first, second}
			},
			wantError: ".name \"atom-audit-publisher\" is duplicated",
		},
		{
			name: "duplicate URI SAN",
			principals: func() []LocalPrincipalConfig {
				first, second := valid(), valid()
				second.Name = "other"
				return []LocalPrincipalConfig{first, second}
			},
			wantError: ".certificate_uri_san \"spiffe://absmach/atom/audit-publisher\" is duplicated",
		},
		{
			name: "missing current secret path",
			principals: func() []LocalPrincipalConfig {
				principal := valid()
				principal.CurrentSecretFile = ""
				return []LocalPrincipalConfig{principal}
			},
			wantError: ".current_secret_file cannot be empty",
		},
		{
			name: "missing secret file",
			principals: func() []LocalPrincipalConfig {
				principal := valid()
				principal.CurrentSecretFile = filepath.Join(dir, "missing")
				return []LocalPrincipalConfig{principal}
			},
			wantError: "failed to read secret file",
		},
		{
			name: "weak secret",
			principals: func() []LocalPrincipalConfig {
				principal := valid()
				principal.CurrentSecretFile = weak
				return []LocalPrincipalConfig{principal}
			},
			wantError: "must contain at least 32 bytes",
		},
		{
			name: "more than one terminal newline",
			principals: func() []LocalPrincipalConfig {
				principal := valid()
				principal.CurrentSecretFile = doubleNewline
				return []LocalPrincipalConfig{principal}
			},
			wantError: "may contain only one terminal newline",
		},
		{
			name: "NUL byte in secret",
			principals: func() []LocalPrincipalConfig {
				principal := valid()
				principal.CurrentSecretFile = nul
				return []LocalPrincipalConfig{principal}
			},
			wantError: "secret file must not contain NUL bytes",
		},
		{
			name: "invalid URI SAN",
			principals: func() []LocalPrincipalConfig {
				principal := valid()
				principal.CertificateURISAN = "not-an-absolute-uri"
				return []LocalPrincipalConfig{principal}
			},
			wantError: "must be an absolute URI",
		},
		{
			name: "publish wildcard",
			principals: func() []LocalPrincipalConfig {
				principal := valid()
				principal.Permissions.Publish[0].RoutingKey = "atom-*"
				return []LocalPrincipalConfig{principal}
			},
			wantError: "routing_key must be an exact value without wildcards",
		},
		{
			name: "non-default publish exchange",
			principals: func() []LocalPrincipalConfig {
				principal := valid()
				principal.Permissions.Publish[0].Exchange = "events"
				return []LocalPrincipalConfig{principal}
			},
			wantError: "exchange must be empty; local principals may publish only through the AMQP default exchange",
		},
		{
			name: "subscribe permissions are unsupported",
			principals: func() []LocalPrincipalConfig {
				principal := valid()
				principal.Permissions.Subscribe = []string{"atom-audit"}
				return []LocalPrincipalConfig{principal}
			},
			wantError: "subscribe is unsupported; local principals are publish-only",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := Default()
			cfg.Auth.LocalPrincipals = tt.principals()
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

func TestValidateInternalAMQP091Listener(t *testing.T) {
	tests := []struct {
		name      string
		configure func(*AMQP091ListenerConfig)
		wantError string
	}{
		{
			name:      "disabled by default",
			configure: func(*AMQP091ListenerConfig) {},
		},
		{
			name: "requires certificate",
			configure: func(listener *AMQP091ListenerConfig) {
				listener.Addr = ":5683"
				listener.MaxConnections = 32
			},
			wantError: "server.amqp091.internal.cert_file required",
		},
		{
			name: "requires client CA",
			configure: func(listener *AMQP091ListenerConfig) {
				listener.Addr = ":5683"
				listener.MaxConnections = 32
				listener.TLS.CertFile = "server.crt"
				listener.TLS.KeyFile = "server.key"
			},
			wantError: "server.amqp091.internal.ca_file required",
		},
		{
			name: "requires exact client auth mode",
			configure: func(listener *AMQP091ListenerConfig) {
				listener.Addr = ":5683"
				listener.MaxConnections = 32
				listener.TLS.CertFile = "server.crt"
				listener.TLS.KeyFile = "server.key"
				listener.TLS.ClientCAFile = "clients.crt"
				listener.TLS.ClientAuth = "require-and-verify"
			},
			wantError: "server.amqp091.internal.client_auth must be \"require\"",
		},
		{
			name: "requires a positive connection limit",
			configure: func(listener *AMQP091ListenerConfig) {
				listener.Addr = ":5683"
				listener.TLS.CertFile = "server.crt"
				listener.TLS.KeyFile = "server.key"
				listener.TLS.ClientCAFile = "clients.crt"
				listener.TLS.ClientAuth = "require"
			},
			wantError: "server.amqp091.internal.max_connections must be positive",
		},
		{
			name: "requires a local principal",
			configure: func(listener *AMQP091ListenerConfig) {
				listener.Addr = ":5683"
				listener.MaxConnections = 32
				listener.TLS.CertFile = "server.crt"
				listener.TLS.KeyFile = "server.key"
				listener.TLS.ClientCAFile = "clients.crt"
				listener.TLS.ClientAuth = "require"
			},
			wantError: "auth.local_principals must contain at least one principal",
		},
		{
			name: "valid mandatory mTLS",
			configure: func(listener *AMQP091ListenerConfig) {
				listener.Addr = ":5683"
				listener.MaxConnections = 32
				listener.TLS.CertFile = "server.crt"
				listener.TLS.KeyFile = "server.key"
				listener.TLS.ClientCAFile = "clients.crt"
				listener.TLS.ClientAuth = "require"
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := Default()
			tt.configure(&cfg.Server.AMQP091.Internal)
			if tt.wantError == "" && cfg.Server.AMQP091.Internal.Addr != "" {
				cfg.Auth.LocalPrincipals = []LocalPrincipalConfig{{
					Name:              testPrincipalName,
					CertificateURISAN: testPrincipalSAN,
					CurrentSecretFile: writeSecret(t, t.TempDir(), "current", strings.Repeat("a", 32)),
				}}
			}
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

func writeSecret(t *testing.T, dir, name, contents string) string {
	t.Helper()
	filename := filepath.Join(dir, name)
	if err := os.WriteFile(filename, []byte(contents), 0o600); err != nil {
		t.Fatalf("write secret: %v", err)
	}
	return filename
}
