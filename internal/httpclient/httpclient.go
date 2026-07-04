// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

// Package httpclient provides default HTTP clients for broker callout
// services (auth callout, blocking hooks).
package httpclient

import (
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
	return &http.Client{Transport: defaultTransport()}
}

// DefaultGRPC returns an HTTP client able to carry gRPC, which requires
// HTTP/2. Plaintext http:// base URLs use unencrypted HTTP/2; https URLs
// negotiate HTTP/2 through TLS ALPN.
func DefaultGRPC(baseURL string) *http.Client {
	transport := defaultTransport()
	if strings.HasPrefix(baseURL, "http://") {
		protocols := new(http.Protocols)
		protocols.SetUnencryptedHTTP2(true)
		transport.Protocols = protocols
	}
	return &http.Client{Transport: transport}
}

func defaultTransport() *http.Transport {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.MaxIdleConns = defaultMaxIdleConns
	transport.MaxIdleConnsPerHost = defaultMaxIdleConns
	transport.IdleConnTimeout = defaultIdleConnTimeout
	return transport
}
