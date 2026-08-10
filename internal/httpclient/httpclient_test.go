// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package httpclient

import (
	"crypto/tls"
	"net/http"
	"testing"
)

func transportOf(t *testing.T, client *http.Client) *http.Transport {
	t.Helper()
	transport, ok := client.Transport.(*http.Transport)
	if !ok {
		t.Fatalf("Transport is %T, want *http.Transport", client.Transport)
	}
	return transport
}

func TestDefaultGRPCUsesUnencryptedHTTP2ForPlaintextURLs(t *testing.T) {
	transport := transportOf(t, DefaultGRPC("http://auth:9090"))
	if transport.Protocols == nil || !transport.Protocols.UnencryptedHTTP2() {
		t.Fatal("plaintext base URL did not enable unencrypted HTTP/2; gRPC needs h2")
	}
}

func TestDefaultGRPCLeavesHTTPSToALPN(t *testing.T) {
	transport := transportOf(t, DefaultGRPC("https://auth:9090"))
	if transport.Protocols != nil && transport.Protocols.UnencryptedHTTP2() {
		t.Fatal("https base URL enabled unencrypted HTTP/2")
	}
}

// A configured client certificate must never end up on a cleartext connection:
// the deployment would look mutually authenticated while being unauthenticated.
func TestGRPCWithTLSNeverDowngradesToCleartext(t *testing.T) {
	transport := transportOf(t, GRPCWithTLS("http://auth:9090", &tls.Config{MinVersion: tls.VersionTLS12}))
	if transport.Protocols != nil && transport.Protocols.UnencryptedHTTP2() {
		t.Fatal("TLS config was set but the transport still enabled unencrypted HTTP/2")
	}
	if transport.TLSClientConfig == nil {
		t.Fatal("TLSClientConfig was not applied")
	}
}

// A non-nil TLSClientConfig otherwise disables Go's automatic HTTP/2 upgrade,
// and gRPC cannot run over HTTP/1.1.
func TestGRPCWithTLSKeepsHTTP2Enabled(t *testing.T) {
	transport := transportOf(t, GRPCWithTLS("https://auth:9090", &tls.Config{MinVersion: tls.VersionTLS12}))
	if !transport.ForceAttemptHTTP2 {
		t.Fatal("ForceAttemptHTTP2 is false with a custom TLS config; gRPC would fall back to HTTP/1.1")
	}
}

func TestWithTLSAppliesConfigAndNilKeepsDefaults(t *testing.T) {
	config := &tls.Config{MinVersion: tls.VersionTLS13}
	if got := transportOf(t, WithTLS(config)).TLSClientConfig; got != config {
		t.Fatalf("TLSClientConfig = %v, want the supplied config", got)
	}

	// http.DefaultTransport.Clone() already carries a TLSClientConfig, so the
	// nil case is about leaving it untouched — no certificate, no CA override.
	got := transportOf(t, WithTLS(nil)).TLSClientConfig
	if got != nil && (len(got.Certificates) != 0 || got.RootCAs != nil) {
		t.Fatalf("nil TLS config left client material behind: %+v", got)
	}
}

func TestClientsKeepTheTunedConnectionPool(t *testing.T) {
	for name, client := range map[string]*http.Client{
		"Default":     Default(),
		"WithTLS":     WithTLS(&tls.Config{MinVersion: tls.VersionTLS12}),
		"DefaultGRPC": DefaultGRPC("http://auth:9090"),
		"GRPCWithTLS": GRPCWithTLS("https://auth:9090", &tls.Config{MinVersion: tls.VersionTLS12}),
	} {
		transport := transportOf(t, client)
		if transport.MaxIdleConnsPerHost != defaultMaxIdleConns {
			t.Fatalf("%s: MaxIdleConnsPerHost = %d, want %d", name, transport.MaxIdleConnsPerHost, defaultMaxIdleConns)
		}
	}
}
