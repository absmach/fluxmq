// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

// Package httpclient provides default HTTP clients for broker callout
// services (auth callout, blocking hooks).
package httpclient

import (
	"crypto/tls"
	"net/http"
	"strings"
	"time"
)

const (
	defaultMaxIdleConns    = 128
	defaultIdleConnTimeout = 90 * time.Second
)

// Default returns an HTTP client tuned for concurrent callouts to a single
// host. http.DefaultTransport keeps only two idle connections per host, which
// churns connections under publish-path concurrency.
func Default() *http.Client {
	return WithTLS(nil)
}

// WithTLS is Default carrying an explicit TLS configuration, which is how a
// callout client presents a certificate to a server that authenticates it by
// one. A nil config keeps the default (system roots, no client certificate).
func WithTLS(tlsConfig *tls.Config) *http.Client {
	return &http.Client{Transport: transportWithTLS(tlsConfig)}
}

// DefaultGRPC returns an HTTP client able to carry gRPC, which requires
// HTTP/2. Plaintext http:// base URLs use unencrypted HTTP/2; https URLs
// negotiate HTTP/2 through TLS ALPN.
func DefaultGRPC(baseURL string) *http.Client {
	return GRPCWithTLS(baseURL, nil)
}

// GRPCWithTLS is DefaultGRPC carrying an explicit TLS configuration.
//
// A non-nil config suppresses the unencrypted-HTTP/2 fallback even for an
// http:// base URL. Silently downgrading there would hand a configured client
// certificate to a cleartext connection — the config would look mutually
// authenticated while being neither. Config validation rejects that pairing up
// front; this is the second line of defence.
func GRPCWithTLS(baseURL string, tlsConfig *tls.Config) *http.Client {
	transport := transportWithTLS(tlsConfig)
	if tlsConfig == nil && strings.HasPrefix(baseURL, "http://") {
		protocols := new(http.Protocols)
		protocols.SetUnencryptedHTTP2(true)
		transport.Protocols = protocols
	}
	return &http.Client{Transport: transport}
}

func transportWithTLS(tlsConfig *tls.Config) *http.Transport {
	transport := defaultTransport()
	if tlsConfig != nil {
		transport.TLSClientConfig = tlsConfig
		// A non-nil TLSClientConfig otherwise disables the automatic HTTP/2
		// upgrade, and gRPC does not work over HTTP/1.1.
		transport.ForceAttemptHTTP2 = true
	}
	return transport
}

func defaultTransport() *http.Transport {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.MaxIdleConns = defaultMaxIdleConns
	transport.MaxIdleConnsPerHost = defaultMaxIdleConns
	transport.IdleConnTimeout = defaultIdleConnTimeout
	return transport
}
