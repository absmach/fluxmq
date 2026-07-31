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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	testPrincipalName = "atom-audit-publisher"
	testPrincipalSAN  = "spiffe://absmach/atom/audit-publisher"
	testAuditQueue    = "atom-audit"
	testInternalAddr  = ":5683"
	testServiceAddr   = ":5684"
	testServerCert    = "server.crt"
	testServerKey     = "server.key"
	testClientCA      = "clients.crt"
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
	if len(principal.Permissions.Publish) != 1 || principal.Permissions.Publish[0].RoutingKey != testAuditQueue {
		t.Fatalf("unexpected publish permissions: %+v", principal.Permissions.Publish)
	}
}

func TestLoadRejectsLegacyAuthKeys(t *testing.T) {
	tests := map[string]string{
		authURLField:               "auth.external.url",
		authTransportField:         "auth.external.transport",
		authTimeoutField:           "auth.external.timeout",
		authProtocolsField:         "auth.external.protocols",
		authIdentityCacheSizeField: "auth.external.identity_cache_size",
		authIdentityCacheTTLField:  "auth.external.identity_cache_ttl",
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
				Publish: []LocalPublishPermission{{Exchange: "", RoutingKey: testAuditQueue}},
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
			name: "publish permission cannot set both an exact key and a prefix",
			principals: func() []LocalPrincipalConfig {
				principal := valid()
				principal.Permissions.Publish = []LocalPublishPermission{
					{RoutingKey: testAuditQueue, RoutingKeyPrefix: "m."},
				}
				return []LocalPrincipalConfig{principal}
			},
			wantError: "cannot set both routing_key and routing_key_prefix",
		},
		{
			name: "publish permission must set one of them",
			principals: func() []LocalPrincipalConfig {
				principal := valid()
				principal.Permissions.Publish = []LocalPublishPermission{{}}
				return []LocalPrincipalConfig{principal}
			},
			wantError: "must set either routing_key or routing_key_prefix",
		},
		{
			name: "publish prefix cannot contain wildcards",
			principals: func() []LocalPrincipalConfig {
				principal := valid()
				principal.Permissions.Publish = []LocalPublishPermission{{RoutingKeyPrefix: "m.#"}}
				return []LocalPrincipalConfig{principal}
			},
			wantError: "must not contain wildcards",
		},
		{
			name: "publish prefix cannot have surrounding whitespace",
			principals: func() []LocalPrincipalConfig {
				principal := valid()
				principal.Permissions.Publish = []LocalPublishPermission{{RoutingKeyPrefix: " m."}}
				return []LocalPrincipalConfig{principal}
			},
			wantError: "cannot have leading or trailing whitespace",
		},
		{
			name: "subscribe permission cannot be empty",
			principals: func() []LocalPrincipalConfig {
				principal := valid()
				principal.Permissions.Subscribe = []string{""}
				return []LocalPrincipalConfig{principal}
			},
			wantError: "permissions.subscribe[0] cannot be empty",
		},
		{
			name: "subscribe permission cannot contain wildcards",
			principals: func() []LocalPrincipalConfig {
				principal := valid()
				principal.Permissions.Subscribe = []string{testAuditQueue + ".*"}
				return []LocalPrincipalConfig{principal}
			},
			wantError: "must be an exact queue name without wildcards",
		},
		{
			name: "subscribe permission cannot have surrounding whitespace",
			principals: func() []LocalPrincipalConfig {
				principal := valid()
				principal.Permissions.Subscribe = []string{" " + testAuditQueue}
				return []LocalPrincipalConfig{principal}
			},
			wantError: "cannot have leading or trailing whitespace",
		},
		{
			name: "subscribe permission cannot be duplicated",
			principals: func() []LocalPrincipalConfig {
				principal := valid()
				principal.Permissions.Subscribe = []string{testAuditQueue, testAuditQueue}
				return []LocalPrincipalConfig{principal}
			},
			wantError: "duplicates an earlier subscribe permission",
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
		name           string
		configure      func(*AMQP091ListenerConfig)
		clusterEnabled bool
		wantError      string
	}{
		{
			name:      "disabled by default",
			configure: func(*AMQP091ListenerConfig) {},
		},
		{
			name: "requires certificate",
			configure: func(listener *AMQP091ListenerConfig) {
				listener.Addr = testInternalAddr
				listener.MaxConnections = 32
			},
			wantError: "server.amqp091.internal.cert_file required",
		},
		{
			name: "requires client CA",
			configure: func(listener *AMQP091ListenerConfig) {
				listener.Addr = testInternalAddr
				listener.MaxConnections = 32
				listener.TLS.CertFile = testServerCert
				listener.TLS.KeyFile = testServerKey
			},
			wantError: "server.amqp091.internal.ca_file required",
		},
		{
			name: "requires exact client auth mode",
			configure: func(listener *AMQP091ListenerConfig) {
				listener.Addr = testInternalAddr
				listener.MaxConnections = 32
				listener.TLS.CertFile = testServerCert
				listener.TLS.KeyFile = testServerKey
				listener.TLS.ClientCAFile = testClientCA
				listener.TLS.ClientAuth = "require-and-verify"
			},
			wantError: "server.amqp091.internal.client_auth must be \"require\"",
		},
		{
			name: "requires a positive connection limit",
			configure: func(listener *AMQP091ListenerConfig) {
				listener.Addr = testInternalAddr
				listener.TLS.CertFile = testServerCert
				listener.TLS.KeyFile = testServerKey
				listener.TLS.ClientCAFile = testClientCA
				listener.TLS.ClientAuth = clientAuthRequire
			},
			wantError: "server.amqp091.internal.max_connections must be positive",
		},
		{
			name: "requires a local principal",
			configure: func(listener *AMQP091ListenerConfig) {
				listener.Addr = testInternalAddr
				listener.MaxConnections = 32
				listener.TLS.CertFile = testServerCert
				listener.TLS.KeyFile = testServerKey
				listener.TLS.ClientCAFile = testClientCA
				listener.TLS.ClientAuth = clientAuthRequire
			},
			wantError: "auth.local_principals must contain at least one principal",
		},
		{
			name: "rejects clustering",
			configure: func(listener *AMQP091ListenerConfig) {
				listener.Addr = testInternalAddr
				listener.MaxConnections = 32
				listener.TLS.CertFile = testServerCert
				listener.TLS.KeyFile = testServerKey
				listener.TLS.ClientCAFile = testClientCA
				listener.TLS.ClientAuth = clientAuthRequire
			},
			clusterEnabled: true,
			wantError:      "cannot be combined with cluster.enabled",
		},
		{
			name: "valid mandatory mTLS",
			configure: func(listener *AMQP091ListenerConfig) {
				listener.Addr = testInternalAddr
				listener.MaxConnections = 32
				listener.TLS.CertFile = testServerCert
				listener.TLS.KeyFile = testServerKey
				listener.TLS.ClientCAFile = testClientCA
				listener.TLS.ClientAuth = clientAuthRequire
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := Default()
			// Local principals are a single-node feature, so these cases
			// configure clustering explicitly rather than inheriting the
			// clustered default.
			cfg.Cluster.Enabled = tt.clusterEnabled
			tt.configure(&cfg.Server.AMQP091.Internal)
			if (tt.wantError == "" || tt.clusterEnabled) && cfg.Server.AMQP091.Internal.Addr != "" {
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

// The service listener authenticates against the same principal store and
// publishes through the same single-node durable path as the internal one, so
// it carries the same deployment requirements.
func TestValidateServiceAMQP091Listener(t *testing.T) {
	configureValid := func(listener *AMQP091ListenerConfig) {
		listener.Addr = testServiceAddr
		listener.MaxConnections = 32
		listener.TLS.CertFile = testServerCert
		listener.TLS.KeyFile = testServerKey
		listener.TLS.ClientCAFile = testClientCA
		listener.TLS.ClientAuth = clientAuthRequire
	}

	tests := []struct {
		name           string
		configure      func(*AMQP091ListenerConfig)
		clusterEnabled bool
		withPrincipal  bool
		wantError      string
	}{
		{
			name:      "disabled by default",
			configure: func(*AMQP091ListenerConfig) {},
		},
		{
			name: "requires exact client auth mode",
			configure: func(listener *AMQP091ListenerConfig) {
				configureValid(listener)
				listener.TLS.ClientAuth = "verify-if-given"
			},
			wantError: "server.amqp091.service.client_auth must be \"require\"",
		},
		{
			name:      "requires a positive connection limit",
			configure: func(listener *AMQP091ListenerConfig) { configureValid(listener); listener.MaxConnections = 0 },
			wantError: "server.amqp091.service.max_connections must be positive",
		},
		{
			name:      "requires a local principal",
			configure: configureValid,
			wantError: "auth.local_principals must contain at least one principal when server.amqp091.service.addr is configured",
		},
		{
			name:           "rejects clustering",
			configure:      configureValid,
			clusterEnabled: true,
			withPrincipal:  true,
			wantError:      "server.amqp091.service.addr cannot be combined with cluster.enabled",
		},
		{
			name:          "valid mandatory mTLS",
			configure:     configureValid,
			withPrincipal: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := Default()
			cfg.Cluster.Enabled = tt.clusterEnabled
			tt.configure(&cfg.Server.AMQP091.Service)
			if tt.withPrincipal {
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

// The auth subtree is decoded strictly, so a permission field the allowlist
// omits is rejected at parse time however valid the struct would be. Parsing
// real YAML is the only thing that catches that; validating a struct built in
// Go never reaches the decoder.
func TestLoadAcceptsPublishPermissionFields(t *testing.T) {
	tests := []struct {
		name       string
		permission string
	}{
		{name: "exact routing key", permission: "routing_key: \"atom-audit\""},
		{name: "routing key prefix", permission: "routing_key_prefix: \"m.\""},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			secret := filepath.Join(dir, "secret")
			require.NoError(t, os.WriteFile(secret, []byte(strings.Repeat("a", 32)), 0o600))

			filename := filepath.Join(dir, "config.yaml")
			body := "auth:\n" +
				"  local_principals:\n" +
				"    - name: \"svc\"\n" +
				"      certificate_uri_san: \"spiffe://absmach/svc\"\n" +
				"      current_secret_file: \"" + secret + "\"\n" +
				"      permissions:\n" +
				"        publish:\n" +
				"          - " + tc.permission + "\n" +
				"        subscribe:\n" +
				"          - \"m\"\n"
			require.NoError(t, os.WriteFile(filename, []byte(body), 0o600))

			cfg, err := Load(filename)
			require.NoError(t, err)
			require.Len(t, cfg.Auth.LocalPrincipals, 1)
			require.Len(t, cfg.Auth.LocalPrincipals[0].Permissions.Publish, 1)
			assert.Equal(t, []string{"m"}, cfg.Auth.LocalPrincipals[0].Permissions.Subscribe)
		})
	}
}
