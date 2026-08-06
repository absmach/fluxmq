// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package pki

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"
)

// RefreshTrustBundle fetches Atom's public trust bundle with ETag
// revalidation, validates every PEM block, then swaps the pool atomically.
// A failed refresh leaves the last known-good pool in service.
func (m *Manager) RefreshTrustBundle(ctx context.Context) error {
	m.trustMu.RLock()
	etag := m.trustETag
	m.trustMu.RUnlock()

	request, err := http.NewRequestWithContext(ctx, http.MethodGet, m.config.TrustBundleURL, nil)
	if err != nil {
		m.metrics.trustRefreshFailures.Add(1)
		return fmt.Errorf("build Atom trust bundle request: %w", err)
	}
	if etag != "" {
		request.Header.Set("If-None-Match", etag)
	}
	response, err := m.httpClient.Do(request)
	if err != nil {
		m.metrics.trustRefreshFailures.Add(1)
		return fmt.Errorf("fetch Atom trust bundle: %w", err)
	}
	defer response.Body.Close()

	if response.StatusCode == http.StatusNotModified {
		m.trustMu.RLock()
		ready := m.trustPool != nil
		m.trustMu.RUnlock()
		if !ready {
			m.metrics.trustRefreshFailures.Add(1)
			return fmt.Errorf("Atom returned an unchanged trust bundle before initial load")
		}
		m.metrics.trustRefreshSuccess.Add(1)
		return nil
	}
	if response.StatusCode != http.StatusOK {
		m.metrics.trustRefreshFailures.Add(1)
		return fmt.Errorf("fetch Atom trust bundle: unexpected HTTP status %d", response.StatusCode)
	}

	limited := io.LimitReader(response.Body, maximumTrustBundleBytes+1)
	bundle, err := io.ReadAll(limited)
	if err != nil {
		m.metrics.trustRefreshFailures.Add(1)
		return fmt.Errorf("read Atom trust bundle: %w", err)
	}
	if len(bundle) == 0 || len(bundle) > maximumTrustBundleBytes {
		m.metrics.trustRefreshFailures.Add(1)
		return fmt.Errorf("Atom trust bundle is empty or exceeds %d bytes", maximumTrustBundleBytes)
	}
	pool, err := parseTrustBundle(bundle)
	if err != nil {
		m.metrics.trustRefreshFailures.Add(1)
		return err
	}

	m.trustMu.Lock()
	m.trustPool = pool
	m.trustETag = strings.TrimSpace(response.Header.Get("ETag"))
	m.trustMu.Unlock()
	m.metrics.trustRefreshSuccess.Add(1)
	return nil
}

func parseTrustBundle(bundle []byte) (*x509.CertPool, error) {
	pool := x509.NewCertPool()
	rest := bundle
	count := 0
	for len(bytes.TrimSpace(rest)) != 0 {
		block, remaining := pem.Decode(rest)
		if block == nil {
			return nil, fmt.Errorf("Atom trust bundle contains malformed PEM data")
		}
		if block.Type != "CERTIFICATE" || len(block.Headers) != 0 {
			return nil, fmt.Errorf("Atom trust bundle contains a non-certificate PEM block")
		}
		certificate, err := x509.ParseCertificate(block.Bytes)
		if err != nil {
			return nil, fmt.Errorf("parse Atom trust bundle certificate: %w", err)
		}
		pool.AddCert(certificate)
		count++
		rest = remaining
	}
	if count == 0 {
		return nil, fmt.Errorf("Atom trust bundle contains no certificates")
	}
	return pool, nil
}

// WrapTLSConfig replaces static client roots with the current Atom-published
// pool for one MQTT mTLS listener while preserving every other TLS option and
// any pre-existing dynamic configuration callback.
func (m *Manager) WrapTLSConfig(base *tls.Config) (*tls.Config, error) {
	if base == nil {
		return nil, fmt.Errorf("cannot attach Atom trust bundle to a nil TLS configuration")
	}
	m.trustMu.RLock()
	initial := m.trustPool
	m.trustMu.RUnlock()
	if initial == nil {
		return nil, fmt.Errorf("Atom trust bundle has not been loaded")
	}

	original := base.GetConfigForClient
	baseCopy := base.Clone()
	wrapped := base.Clone()
	wrapped.ClientCAs = initial
	wrapped.GetConfigForClient = func(hello *tls.ClientHelloInfo) (*tls.Config, error) {
		selected := baseCopy
		if original != nil {
			candidate, err := original(hello)
			if err != nil {
				return nil, err
			}
			if candidate != nil {
				selected = candidate
			}
		}
		m.trustMu.RLock()
		pool := m.trustPool
		m.trustMu.RUnlock()
		if pool == nil {
			return nil, fmt.Errorf("Atom trust bundle is unavailable")
		}
		current := selected.Clone()
		current.ClientCAs = pool
		current.GetConfigForClient = nil
		return current, nil
	}
	return wrapped, nil
}

func (m *Manager) requestTrustRefresh() {
	select {
	case m.refreshCh <- struct{}{}:
	default:
	}
}

func (m *Manager) trustRefreshLoop() {
	defer m.wg.Done()
	ticker := time.NewTicker(m.config.TrustRefreshInterval)
	defer ticker.Stop()
	for {
		select {
		case <-m.stopCh:
			return
		case <-ticker.C:
		case <-m.refreshCh:
		}
		ctx, cancel := context.WithTimeout(context.Background(), m.config.Timeout)
		err := m.RefreshTrustBundle(ctx)
		cancel()
		if err != nil {
			m.logger.Warn("Atom trust bundle refresh failed", slog.String("error", err.Error()))
		}
	}
}
