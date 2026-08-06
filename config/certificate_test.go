// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func validCertificateConfig(t *testing.T) *Config {
	t.Helper()
	secretFile := filepath.Join(t.TempDir(), "secret")
	require.NoError(t, os.WriteFile(secretFile, []byte("0123456789abcdef0123456789abcdef"), 0o600))
	tokenFile := filepath.Join(t.TempDir(), "token")
	require.NoError(t, os.WriteFile(tokenFile, []byte("atom_service_token"), 0o600))

	config := Default()
	config.Cluster.Enabled = false
	config.Auth.External.URL = "http://atom-auth:8181"
	config.Auth.External.Protocols = map[string]bool{protocolMQTT: true}
	config.Auth.Certificate = CertificateAuthConfig{
		Enabled:                  true,
		ResolverAddress:          "atom:8081",
		ResolverInsecure:         true,
		ServiceTokenFile:         tokenFile,
		TrustBundleURL:           "http://atom:8080/certs/trust-bundle.pem",
		ResolverTimeout:          time.Second,
		CacheTTL:                 30 * time.Second,
		CacheSize:                100,
		TrustRefreshInterval:     time.Minute,
		EventQueue:               "atom.events",
		EventConsumerGroupPrefix: "fluxmq-pki",
		EventSourcePrincipal:     "atom-events",
	}
	config.Server.TCP.MTLS = TCPListenerConfig{
		Addr:           ":8883",
		MaxConnections: 100,
		ReadTimeout:    time.Second,
		WriteTimeout:   time.Second,
		Protocol:       ProtocolModeAuto,
	}
	config.Server.TCP.MTLS.TLS.CertFile = "server.pem"
	config.Server.TCP.MTLS.TLS.KeyFile = "server.key"
	config.Server.TCP.MTLS.TLS.ClientAuth = "require"
	config.Queues = append(config.Queues, QueueConfig{
		Name:     "atom.events",
		Topics:   []string{"$queue/atom.events/#"},
		Reserved: true,
		Type:     "stream",
	})
	config.Auth.LocalPrincipals = []LocalPrincipalConfig{
		{
			Name:              "atom-events",
			CertificateURISAN: "spiffe://example.test/atom/events",
			CurrentSecretFile: secretFile,
			Permissions: LocalPermissionsConfig{Publish: []LocalPublishPermission{
				{Exchange: "", RoutingKey: "atom.events"},
			}},
		},
	}
	return config
}

func TestCertificateAuthenticationConfigurationContract(t *testing.T) {
	require.NoError(t, validCertificateConfig(t).Validate())
}

func TestCertificateAuthenticationRejectsUnsafeCombinations(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*Config)
		match  string
	}{
		{
			name: "normal Atom authorization missing",
			mutate: func(config *Config) {
				config.Auth.External.Protocols = map[string]bool{protocolMQTT: false}
			},
			match: "authorization for mqtt",
		},
		{
			name: "unbounded revocation lag",
			mutate: func(config *Config) {
				config.Auth.Certificate.CacheTTL = certificateMaximumCacheTTL + time.Second
			},
			match: "cache_ttl",
		},
		{
			name: "events are not a durable stream contract",
			mutate: func(config *Config) {
				config.Queues[len(config.Queues)-1].Type = "classic"
			},
			match: "reserved stream queue",
		},
		{
			name: "publisher lacks exact protected target",
			mutate: func(config *Config) {
				config.Auth.LocalPrincipals[0].Permissions.Publish[0].RoutingKey = "other.events"
			},
			match: "exact default-exchange publish grant",
		},
		{
			name: "no applicable MQTT mTLS transport",
			mutate: func(config *Config) {
				config.Server.TCP.MTLS = TCPListenerConfig{}
				config.Server.WebSocket.MTLS = WSListenerConfig{}
			},
			match: "mTLS listener",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			config := validCertificateConfig(t)
			test.mutate(config)
			require.ErrorContains(t, config.Validate(), test.match)
		})
	}
}
