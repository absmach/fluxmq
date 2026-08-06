// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

// Package pki implements FluxMQ's resolver verification tier for MQTT mTLS.
// Atom is authoritative for certificate lifecycle state. Successful results
// may be reused only from the bounded cache configured here; cache misses and
// expired entries fail closed when Atom is unavailable.
package pki

import (
	"context"
	"crypto/sha256"
	"crypto/x509"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"math/big"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	corebroker "github.com/absmach/fluxmq/broker"
	"github.com/absmach/fluxmq/topics"
	"github.com/google/uuid"
	"golang.org/x/sync/singleflight"
)

const (
	DefaultResolverTimeout     = 3 * time.Second
	DefaultCacheTTL            = 30 * time.Second
	MaximumCacheTTL            = 5 * time.Minute
	DefaultCacheSize           = 10000
	DefaultTrustRefresh        = time.Minute
	maximumCertificateDERBytes = 64 * 1024
	maximumTrustBundleBytes    = 4 * 1024 * 1024
)

var (
	ErrInvalidPeerCertificate = errors.New("invalid peer certificate")
	ErrResolverIdentity       = errors.New("invalid certificate resolver identity")
	ErrResolutionInvalidated  = errors.New("certificate resolution invalidated by a lifecycle event")
	ErrTenantMismatch         = errors.New("certificate tenant does not match requested scope")
	ErrTenantScopeUnknown     = errors.New("tenant-scoped topic has no canonical tenant UUID")
)

// Config configures resolver-tier certificate authentication. The cache TTL is
// the maximum reviewed outage/revocation window and may not exceed five minutes.
type Config struct {
	ResolverAddress      string
	ResolverInsecure     bool
	ServiceTokenFile     string
	TrustBundleURL       string
	EventSourcePrincipal string
	Timeout              time.Duration
	CacheTTL             time.Duration
	CacheSize            int
	TrustRefreshInterval time.Duration
}

// ResolverRequest contains every selector extracted from the verified TLS
// chain. ExpectedTenantID is set when revalidating an operation scope.
type ResolverRequest struct {
	CertificateDER          []byte
	FingerprintSHA256       string
	IssuerFingerprintSHA256 string
	SerialNumber            string
	ExpectedTenantID        string
}

// ResolverResult mirrors Atom ResolveCertificateV2's identity contract.
type ResolverResult struct {
	EntityID     string
	TenantID     string
	CredentialID string
	IssuerID     string
	ExpiresAt    string
	Status       string
}

// Resolver is injectable so lifecycle and outage behavior can be tested
// without weakening the production gRPC path.
type Resolver interface {
	ResolveCertificateV2(ctx context.Context, request ResolverRequest) (ResolverResult, error)
}

type managerMetrics struct {
	resolverRequests     atomic.Uint64
	resolverFailures     atomic.Uint64
	resolverTimeouts     atomic.Uint64
	cacheHits            atomic.Uint64
	cacheMisses          atomic.Uint64
	cacheEvictions       atomic.Uint64
	eventsReceived       atomic.Uint64
	eventsRejected       atomic.Uint64
	cacheInvalidations   atomic.Uint64
	sessionsDisconnected atomic.Uint64
	tenantDenials        atomic.Uint64
	trustRefreshSuccess  atomic.Uint64
	trustRefreshFailures atomic.Uint64
}

// Manager resolves certificate identities, owns the bounded cache, consumes
// invalidation events, and maintains an atomically refreshed trust bundle.
type Manager struct {
	config     Config
	resolver   Resolver
	httpClient *http.Client
	logger     *slog.Logger
	cache      *resolutionCache
	metrics    managerMetrics
	requests   singleflight.Group
	clock      func() time.Time
	sessions   func(func(corebroker.CertificateIdentity) bool) int
	lifecycle  sync.RWMutex
	generation uint64

	trustMu   sync.RWMutex
	trustPool *x509.CertPool
	trustETag string
	refreshCh chan struct{}
	stopCh    chan struct{}
	stopOnce  sync.Once
	wg        sync.WaitGroup
}

type managerOptions struct {
	resolver   Resolver
	httpClient *http.Client
	logger     *slog.Logger
	clock      func() time.Time
	sessions   func(func(corebroker.CertificateIdentity) bool) int
}

// Option customizes a Manager.
type Option func(*managerOptions)

func WithResolver(resolver Resolver) Option {
	return func(options *managerOptions) { options.resolver = resolver }
}

func WithHTTPClient(client *http.Client) Option {
	return func(options *managerOptions) { options.httpClient = client }
}

func WithLogger(logger *slog.Logger) Option {
	return func(options *managerOptions) { options.logger = logger }
}

// WithSessionInvalidator installs the consuming broker's live-session
// revoker. It is invoked only for lifecycle events that make an existing
// credential, entity, or tenant unusable.
func WithSessionInvalidator(invalidate func(func(corebroker.CertificateIdentity) bool) int) Option {
	return func(options *managerOptions) { options.sessions = invalidate }
}

func withClock(clock func() time.Time) Option {
	return func(options *managerOptions) { options.clock = clock }
}

// NewManager constructs the resolver tier. Start must complete before any mTLS
// listener is exposed so no connection uses a stale file-based trust source.
func NewManager(config Config, opts ...Option) (*Manager, error) {
	applyConfigDefaults(&config)
	if err := validateConfig(config); err != nil {
		return nil, err
	}

	options := managerOptions{
		httpClient: &http.Client{Timeout: config.Timeout},
		logger:     slog.Default(),
		clock:      time.Now,
	}
	for _, option := range opts {
		option(&options)
	}
	if options.httpClient == nil {
		return nil, fmt.Errorf("certificate trust HTTP client is nil")
	}
	if options.logger == nil {
		options.logger = slog.Default()
	}
	if options.clock == nil {
		options.clock = time.Now
	}

	resolver := options.resolver
	if resolver == nil {
		var err error
		resolver, err = newGRPCResolver(config)
		if err != nil {
			return nil, err
		}
	}

	cache := newResolutionCache(config.CacheSize, config.CacheTTL)
	cache.clock = options.clock
	return &Manager{
		config:     config,
		resolver:   resolver,
		httpClient: options.httpClient,
		logger:     options.logger,
		sessions:   options.sessions,
		cache:      cache,
		clock:      options.clock,
		refreshCh:  make(chan struct{}, 1),
		stopCh:     make(chan struct{}),
	}, nil
}

func applyConfigDefaults(config *Config) {
	if config.Timeout == 0 {
		config.Timeout = DefaultResolverTimeout
	}
	if config.CacheTTL == 0 {
		config.CacheTTL = DefaultCacheTTL
	}
	if config.CacheSize == 0 {
		config.CacheSize = DefaultCacheSize
	}
	if config.TrustRefreshInterval == 0 {
		config.TrustRefreshInterval = DefaultTrustRefresh
	}
}

func validateConfig(config Config) error {
	if strings.TrimSpace(config.ResolverAddress) == "" {
		return fmt.Errorf("certificate resolver address is required")
	}
	if strings.TrimSpace(config.ServiceTokenFile) == "" {
		return fmt.Errorf("certificate resolver service token file is required")
	}
	trustURL, err := url.ParseRequestURI(config.TrustBundleURL)
	if err != nil || trustURL.Host == "" || (trustURL.Scheme != "https" && trustURL.Scheme != "http") {
		return fmt.Errorf("Atom trust bundle URL must be an absolute HTTP(S) URL")
	}
	if trustURL.Scheme != "https" && !config.ResolverInsecure {
		return fmt.Errorf("Atom trust bundle URL must use HTTPS unless resolver insecure mode is enabled")
	}
	if strings.TrimSpace(config.EventSourcePrincipal) == "" {
		return fmt.Errorf("Atom event source principal is required")
	}
	if config.Timeout <= 0 {
		return fmt.Errorf("certificate resolver timeout must be positive")
	}
	if config.CacheTTL <= 0 || config.CacheTTL > MaximumCacheTTL {
		return fmt.Errorf("certificate resolver cache TTL must be positive and no greater than %s", MaximumCacheTTL)
	}
	if config.CacheSize <= 0 {
		return fmt.Errorf("certificate resolver cache size must be positive")
	}
	if config.TrustRefreshInterval <= 0 {
		return fmt.Errorf("certificate trust refresh interval must be positive")
	}
	return nil
}

// Start loads Atom's published trust bundle synchronously, then begins periodic
// and event-triggered refresh. Initial failure is fatal and therefore closed.
func (m *Manager) Start(ctx context.Context) error {
	if err := m.RefreshTrustBundle(ctx); err != nil {
		return err
	}
	m.wg.Add(1)
	go m.trustRefreshLoop()
	return nil
}

// Close stops background refresh and closes the owned resolver connection.
func (m *Manager) Close() error {
	m.stopOnce.Do(func() { close(m.stopCh) })
	m.wg.Wait()
	if closer, ok := m.resolver.(interface{ Close() error }); ok {
		return closer.Close()
	}
	return nil
}

// AuthenticateCertificate authoritatively resolves the peer, using a valid
// bounded cache entry when present. On a miss, any Atom error fails closed.
func (m *Manager) AuthenticateCertificate(ctx context.Context, peer corebroker.PeerCertificate) (corebroker.CertificateIdentity, error) {
	request, err := selectorsFromPeer(peer)
	if err != nil {
		return corebroker.CertificateIdentity{}, err
	}
	return m.resolve(ctx, request)
}

// AuthorizeCertificate revalidates lifecycle state from the bounded cache (or
// Atom after expiry/invalidation), then compares the requested tenant before
// FluxMQ calls its existing external authorizer.
func (m *Manager) AuthorizeCertificate(ctx context.Context, identity corebroker.CertificateIdentity, topic string) error {
	tenantID, scoped, err := topicTenant(topic)
	if err != nil {
		m.metrics.tenantDenials.Add(1)
		return err
	}
	if scoped && identity.TenantID != tenantID {
		m.metrics.tenantDenials.Add(1)
		return ErrTenantMismatch
	}

	current, err := m.resolve(ctx, ResolverRequest{
		FingerprintSHA256: identity.Fingerprint,
		ExpectedTenantID:  tenantID,
	})
	if err != nil {
		m.disconnectResolvedSession(identity)
		return err
	}
	if current.EntityID != identity.EntityID || current.TenantID != identity.TenantID || current.CredentialID != identity.CredentialID {
		m.disconnectResolvedSession(identity)
		return ErrResolverIdentity
	}
	return nil
}

func (m *Manager) disconnectResolvedSession(identity corebroker.CertificateIdentity) {
	if m.sessions == nil {
		return
	}
	disconnected := m.sessions(func(candidate corebroker.CertificateIdentity) bool {
		return candidate.CredentialID == identity.CredentialID && candidate.Fingerprint == identity.Fingerprint
	})
	if disconnected > 0 {
		m.metrics.sessionsDisconnected.Add(uint64(disconnected))
	}
}

func (m *Manager) resolve(ctx context.Context, request ResolverRequest) (corebroker.CertificateIdentity, error) {
	if request.FingerprintSHA256 == "" {
		return corebroker.CertificateIdentity{}, ErrInvalidPeerCertificate
	}
	if cached, ok := m.cachedResolution(request.FingerprintSHA256); ok {
		m.metrics.cacheHits.Add(1)
		if request.ExpectedTenantID != "" && cached.TenantID != request.ExpectedTenantID {
			m.metrics.tenantDenials.Add(1)
			return corebroker.CertificateIdentity{}, ErrTenantMismatch
		}
		return cached, nil
	}
	m.metrics.cacheMisses.Add(1)

	value, err, _ := m.requests.Do(request.FingerprintSHA256, func() (any, error) {
		if cached, ok := m.cachedResolution(request.FingerprintSHA256); ok {
			m.metrics.cacheHits.Add(1)
			return cached, nil
		}
		m.lifecycle.RLock()
		generation := m.generation
		m.lifecycle.RUnlock()
		m.metrics.resolverRequests.Add(1)
		callCtx, cancel := context.WithTimeout(ctx, m.config.Timeout)
		defer cancel()
		result, err := m.resolver.ResolveCertificateV2(callCtx, request)
		if err != nil {
			m.metrics.resolverFailures.Add(1)
			if errors.Is(err, context.DeadlineExceeded) || errors.Is(callCtx.Err(), context.DeadlineExceeded) {
				m.metrics.resolverTimeouts.Add(1)
			}
			return corebroker.CertificateIdentity{}, err
		}
		identity, err := validateResolverResult(result, request.FingerprintSHA256, m.clock())
		if err != nil {
			m.metrics.resolverFailures.Add(1)
			return corebroker.CertificateIdentity{}, err
		}
		if request.ExpectedTenantID != "" && identity.TenantID != request.ExpectedTenantID {
			m.metrics.tenantDenials.Add(1)
			return corebroker.CertificateIdentity{}, ErrTenantMismatch
		}
		m.lifecycle.RLock()
		if generation != m.generation {
			m.lifecycle.RUnlock()
			return corebroker.CertificateIdentity{}, ErrResolutionInvalidated
		}
		evicted := m.cache.put(identity)
		m.lifecycle.RUnlock()
		if evicted {
			m.metrics.cacheEvictions.Add(1)
		}
		return identity, nil
	})
	if err != nil {
		return corebroker.CertificateIdentity{}, err
	}
	identity := value.(corebroker.CertificateIdentity)
	if request.ExpectedTenantID != "" && identity.TenantID != request.ExpectedTenantID {
		m.metrics.tenantDenials.Add(1)
		return corebroker.CertificateIdentity{}, ErrTenantMismatch
	}
	return identity, nil
}

func (m *Manager) cachedResolution(fingerprint string) (corebroker.CertificateIdentity, bool) {
	m.lifecycle.RLock()
	defer m.lifecycle.RUnlock()
	return m.cache.get(fingerprint)
}

func selectorsFromPeer(peer corebroker.PeerCertificate) (ResolverRequest, error) {
	if len(peer.LeafDER) == 0 || len(peer.LeafDER) > maximumCertificateDERBytes {
		return ResolverRequest{}, ErrInvalidPeerCertificate
	}
	certificate, err := x509.ParseCertificate(peer.LeafDER)
	if err != nil || certificate.SerialNumber == nil || certificate.SerialNumber.Sign() < 0 {
		return ResolverRequest{}, ErrInvalidPeerCertificate
	}
	leafDigest := sha256.Sum256(peer.LeafDER)
	request := ResolverRequest{
		CertificateDER:    append([]byte(nil), peer.LeafDER...),
		FingerprintSHA256: hex.EncodeToString(leafDigest[:]),
		SerialNumber:      normalizeSerial(certificate.SerialNumber),
	}
	if len(peer.IssuerDER) != 0 {
		if _, err := x509.ParseCertificate(peer.IssuerDER); err != nil {
			return ResolverRequest{}, ErrInvalidPeerCertificate
		}
		issuerDigest := sha256.Sum256(peer.IssuerDER)
		request.IssuerFingerprintSHA256 = hex.EncodeToString(issuerDigest[:])
	}
	return request, nil
}

func normalizeSerial(serial *big.Int) string {
	value := strings.TrimLeft(strings.ToLower(serial.Text(16)), "0")
	if value == "" {
		return "0"
	}
	return value
}

func validateResolverResult(result ResolverResult, fingerprint string, now time.Time) (corebroker.CertificateIdentity, error) {
	if result.Status != "active" {
		return corebroker.CertificateIdentity{}, ErrResolverIdentity
	}
	if _, err := uuid.Parse(result.EntityID); err != nil {
		return corebroker.CertificateIdentity{}, ErrResolverIdentity
	}
	if result.TenantID != "" {
		if _, err := uuid.Parse(result.TenantID); err != nil {
			return corebroker.CertificateIdentity{}, ErrResolverIdentity
		}
	}
	if _, err := uuid.Parse(result.CredentialID); err != nil {
		return corebroker.CertificateIdentity{}, ErrResolverIdentity
	}
	if result.IssuerID != "" {
		if _, err := uuid.Parse(result.IssuerID); err != nil {
			return corebroker.CertificateIdentity{}, ErrResolverIdentity
		}
	}
	expiresAt, err := time.Parse(time.RFC3339, result.ExpiresAt)
	if err != nil || !expiresAt.After(now) {
		return corebroker.CertificateIdentity{}, ErrResolverIdentity
	}
	return corebroker.CertificateIdentity{
		EntityID:     result.EntityID,
		TenantID:     result.TenantID,
		CredentialID: result.CredentialID,
		IssuerID:     result.IssuerID,
		Fingerprint:  strings.ToLower(fingerprint),
		ExpiresAt:    expiresAt,
	}, nil
}

func topicTenant(topic string) (string, bool, error) {
	if _, sharedFilter, shared := topics.ParseShared(topic); shared {
		topic = sharedFilter
	}
	parts := strings.Split(topic, "/")
	if len(parts) == 0 || (parts[0] != "m" && parts[0] != "hc") {
		return "", false, nil
	}
	if len(parts) < 2 || strings.ContainsAny(parts[1], "+#") {
		return "", true, ErrTenantScopeUnknown
	}
	parsed, err := uuid.Parse(parts[1])
	if err != nil {
		return "", true, ErrTenantScopeUnknown
	}
	return parsed.String(), true, nil
}

// CertificateMetrics returns a label-free operational snapshot.
func (m *Manager) CertificateMetrics() corebroker.CertificateMetrics {
	return corebroker.CertificateMetrics{
		ResolverRequests:     m.metrics.resolverRequests.Load(),
		ResolverFailures:     m.metrics.resolverFailures.Load(),
		ResolverTimeouts:     m.metrics.resolverTimeouts.Load(),
		CacheHits:            m.metrics.cacheHits.Load(),
		CacheMisses:          m.metrics.cacheMisses.Load(),
		CacheEvictions:       m.metrics.cacheEvictions.Load(),
		CacheEntries:         m.cache.len(),
		EventsReceived:       m.metrics.eventsReceived.Load(),
		EventsRejected:       m.metrics.eventsRejected.Load(),
		CacheInvalidations:   m.metrics.cacheInvalidations.Load(),
		SessionsDisconnected: m.metrics.sessionsDisconnected.Load(),
		TenantDenials:        m.metrics.tenantDenials.Load(),
		TrustRefreshSuccess:  m.metrics.trustRefreshSuccess.Load(),
		TrustRefreshFailures: m.metrics.trustRefreshFailures.Load(),
	}
}

var (
	_ corebroker.CertificateAuthenticator   = (*Manager)(nil)
	_ corebroker.CertificateMetricsProvider = (*Manager)(nil)
)
