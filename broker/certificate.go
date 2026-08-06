// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package broker

import (
	"context"
	"time"
)

// CertificateIdentity is the minimum resolver result retained by the broker.
// It deliberately excludes certificate subjects, public keys, and DER.
type CertificateIdentity struct {
	EntityID     string
	TenantID     string
	CredentialID string
	IssuerID     string
	Fingerprint  string
	ExpiresAt    time.Time
}

// PeerCertificate contains the verified TLS material extracted by a transport.
// IssuerDER is optional for chains in which the peer sent only the leaf.
type PeerCertificate struct {
	LeafDER   []byte
	IssuerDER []byte
}

// CertificateAuthenticator authoritatively resolves a peer certificate and
// revalidates its tenant binding before normal operation authorization.
type CertificateAuthenticator interface {
	AuthenticateCertificate(ctx context.Context, peer PeerCertificate) (CertificateIdentity, error)
	AuthorizeCertificate(ctx context.Context, identity CertificateIdentity, topic string) error
}

// PeerCertificateSource is implemented by transports that terminate TLS and
// can expose the verified peer leaf to the authentication layer.
type PeerCertificateSource interface {
	PeerCertificateDER() []byte
	PeerIssuerCertificateDER() []byte
}

// CertificateMetrics is a label-free snapshot suitable for the admin API.
// Identity values are intentionally never exposed as metric labels.
type CertificateMetrics struct {
	ActiveSessions       int    `json:"active_sessions"`
	ResolverRequests     uint64 `json:"resolver_requests"`
	ResolverFailures     uint64 `json:"resolver_failures"`
	ResolverTimeouts     uint64 `json:"resolver_timeouts"`
	CacheHits            uint64 `json:"cache_hits"`
	CacheMisses          uint64 `json:"cache_misses"`
	CacheEvictions       uint64 `json:"cache_evictions"`
	CacheEntries         int    `json:"cache_entries"`
	EventsReceived       uint64 `json:"events_received"`
	EventsRejected       uint64 `json:"events_rejected"`
	CacheInvalidations   uint64 `json:"cache_invalidations"`
	TenantDenials        uint64 `json:"tenant_denials"`
	TrustRefreshSuccess  uint64 `json:"trust_refresh_success"`
	TrustRefreshFailures uint64 `json:"trust_refresh_failures"`
}

// CertificateMetricsProvider supplies a current operational snapshot.
type CertificateMetricsProvider interface {
	CertificateMetrics() CertificateMetrics
}
