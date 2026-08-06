// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package pki

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"errors"
	"math/big"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	corebroker "github.com/absmach/fluxmq/broker"
	"github.com/stretchr/testify/require"
)

const (
	testEntityID       = "8a0a5c59-4ea8-4fc1-badb-f96cf739b224"
	testTenantID       = "d204f7df-8293-4194-963b-a47a65bc8f04"
	testOtherTenantID  = "88b65e71-e41d-4f12-9800-6c621133af9b"
	testCredentialID   = "ca49950c-3ed2-41b4-a319-896085285686"
	testIssuerID       = "95bdde91-c07a-4fc2-bf7f-cec505475449"
	testEventPrincipal = "atom-events"
)

type resolverStub struct {
	mu        sync.Mutex
	calls     int
	requests  []ResolverRequest
	result    ResolverResult
	results   map[string]ResolverResult
	err       error
	wait      bool
	delay     time.Duration
	started   chan struct{}
	release   chan struct{}
	startOnce sync.Once
}

func (stub *resolverStub) ResolveCertificateV2(ctx context.Context, request ResolverRequest) (ResolverResult, error) {
	stub.mu.Lock()
	stub.calls++
	request.CertificateDER = append([]byte(nil), request.CertificateDER...)
	stub.requests = append(stub.requests, request)
	result := stub.result
	if selected, ok := stub.results[request.FingerprintSHA256]; ok {
		result = selected
	}
	err := stub.err
	wait := stub.wait
	delay := stub.delay
	started := stub.started
	release := stub.release
	stub.mu.Unlock()

	if started != nil {
		stub.startOnce.Do(func() { close(started) })
	}
	if release != nil {
		select {
		case <-release:
		case <-ctx.Done():
			return ResolverResult{}, ctx.Err()
		}
	}
	if wait {
		<-ctx.Done()
		return ResolverResult{}, ctx.Err()
	}
	if delay > 0 {
		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return ResolverResult{}, ctx.Err()
		}
	}
	return result, err
}

func (stub *resolverStub) setError(err error) {
	stub.mu.Lock()
	stub.err = err
	stub.mu.Unlock()
}

func (stub *resolverStub) callCount() int {
	stub.mu.Lock()
	defer stub.mu.Unlock()
	return stub.calls
}

func (stub *resolverStub) lastRequest() ResolverRequest {
	stub.mu.Lock()
	defer stub.mu.Unlock()
	return stub.requests[len(stub.requests)-1]
}

func activeResolverResult(now time.Time) ResolverResult {
	return ResolverResult{
		EntityID:     testEntityID,
		TenantID:     testTenantID,
		CredentialID: testCredentialID,
		IssuerID:     testIssuerID,
		ExpiresAt:    now.Add(time.Hour).Format(time.RFC3339),
		Status:       "active",
	}
}

func newTestManager(t *testing.T, resolver Resolver, now *time.Time, options ...Option) *Manager {
	t.Helper()
	opts := []Option{WithResolver(resolver)}
	if now != nil {
		opts = append(opts, withClock(func() time.Time { return *now }))
	}
	opts = append(opts, options...)
	manager, err := NewManager(Config{
		ResolverAddress:      "atom.invalid:8081",
		ServiceTokenFile:     "unused-by-test-resolver",
		TrustBundleURL:       "https://atom.invalid/certs/trust-bundle.pem",
		EventSourcePrincipal: testEventPrincipal,
		Timeout:              100 * time.Millisecond,
		CacheTTL:             30 * time.Second,
		CacheSize:            64,
		TrustRefreshInterval: time.Hour,
	}, opts...)
	require.NoError(t, err)
	return manager
}

func TestResolverAuthenticationExtractsAllTLSSelectorsAndMapsEntity(t *testing.T) {
	now := time.Now().UTC()
	peer, _ := makePeerCertificate(t, 0x1af)
	resolver := &resolverStub{result: activeResolverResult(now)}
	manager := newTestManager(t, resolver, &now)

	identity, err := manager.AuthenticateCertificate(context.Background(), peer)
	require.NoError(t, err)
	require.Equal(t, testEntityID, identity.EntityID)
	require.Equal(t, testTenantID, identity.TenantID)
	require.Equal(t, testCredentialID, identity.CredentialID)
	require.Equal(t, testIssuerID, identity.IssuerID)

	request := resolver.lastRequest()
	require.Equal(t, peer.LeafDER, request.CertificateDER)
	require.Len(t, request.FingerprintSHA256, 64)
	require.Len(t, request.IssuerFingerprintSHA256, 64)
	require.Equal(t, "1af", request.SerialNumber)
	require.Empty(t, request.ExpectedTenantID)
}

func TestTenantBindingRejectsCrossTenantAndGlobalEscalation(t *testing.T) {
	now := time.Now().UTC()
	peer, _ := makePeerCertificate(t, 1)
	resolver := &resolverStub{result: activeResolverResult(now)}
	manager := newTestManager(t, resolver, &now)
	identity, err := manager.AuthenticateCertificate(context.Background(), peer)
	require.NoError(t, err)

	err = manager.AuthorizeCertificate(context.Background(), identity, "m/"+testOtherTenantID+"/c/channel/messages")
	require.ErrorIs(t, err, ErrTenantMismatch)
	require.ErrorIs(t, manager.AuthorizeCertificate(context.Background(), identity, "$share/workers/m/"+testOtherTenantID+"/c/channel/messages"), ErrTenantMismatch)
	require.Equal(t, 1, resolver.callCount(), "cross-tenant denial must happen before authorization lookup")

	globalResolver := &resolverStub{result: activeResolverResult(now)}
	globalResolver.result.TenantID = ""
	globalManager := newTestManager(t, globalResolver, &now)
	globalIdentity, err := globalManager.AuthenticateCertificate(context.Background(), peer)
	require.NoError(t, err)
	require.Empty(t, globalIdentity.TenantID)
	require.ErrorIs(t, globalManager.AuthorizeCertificate(context.Background(), globalIdentity, "m/"+testTenantID+"/c/channel"), ErrTenantMismatch)
	require.NoError(t, globalManager.AuthorizeCertificate(context.Background(), globalIdentity, "$SYS/global/status"))
}

func TestResolverLifecycleDenialsOccurBeforeSession(t *testing.T) {
	now := time.Now().UTC()
	peer, _ := makePeerCertificate(t, 2)
	tests := []struct {
		name   string
		result ResolverResult
		err    error
	}{
		{name: "unknown", result: activeResolverResult(now), err: errors.New("not found")},
		{name: "revoked", result: func() ResolverResult { value := activeResolverResult(now); value.Status = "revoked"; return value }()},
		{name: "expired", result: func() ResolverResult {
			value := activeResolverResult(now)
			value.ExpiresAt = now.Add(-time.Second).Format(time.RFC3339)
			return value
		}()},
		{name: "inactive entity", result: activeResolverResult(now), err: errors.New("inactive entity")},
		{name: "frozen tenant", result: activeResolverResult(now), err: errors.New("frozen tenant")},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			resolver := &resolverStub{result: test.result, err: test.err}
			manager := newTestManager(t, resolver, &now)
			_, err := manager.AuthenticateCertificate(context.Background(), peer)
			require.Error(t, err)
			require.Zero(t, manager.CertificateMetrics().CacheEntries)
		})
	}
}

func TestResolverTimeoutFailsClosed(t *testing.T) {
	now := time.Now().UTC()
	peer, _ := makePeerCertificate(t, 3)
	resolver := &resolverStub{result: activeResolverResult(now), wait: true}
	manager := newTestManager(t, resolver, &now)
	manager.config.Timeout = 5 * time.Millisecond

	_, err := manager.AuthenticateCertificate(context.Background(), peer)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	metrics := manager.CertificateMetrics()
	require.Equal(t, uint64(1), metrics.ResolverFailures)
	require.Equal(t, uint64(1), metrics.ResolverTimeouts)
}

func TestReviewedCacheAllowsBoundedOutageThenFailsClosed(t *testing.T) {
	now := time.Now().UTC()
	peer, _ := makePeerCertificate(t, 4)
	resolver := &resolverStub{result: activeResolverResult(now)}
	manager := newTestManager(t, resolver, &now)
	manager.config.CacheTTL = 10 * time.Second
	manager.cache.ttl = 10 * time.Second

	first, err := manager.AuthenticateCertificate(context.Background(), peer)
	require.NoError(t, err)
	resolver.setError(errors.New("Atom unavailable"))
	second, err := manager.AuthenticateCertificate(context.Background(), peer)
	require.NoError(t, err)
	require.Equal(t, first, second)

	now = now.Add(11 * time.Second)
	_, err = manager.AuthenticateCertificate(context.Background(), peer)
	require.ErrorContains(t, err, "Atom unavailable")
	require.Equal(t, 2, resolver.callCount())
}

func TestAuthorizationResolverFailureDisconnectsCertificateSession(t *testing.T) {
	now := time.Now().UTC()
	peer, _ := makePeerCertificate(t, 13)
	selectors, err := selectorsFromPeer(peer)
	require.NoError(t, err)
	resolver := &resolverStub{result: activeResolverResult(now)}
	disconnected := false
	manager := newTestManager(t, resolver, &now, WithSessionInvalidator(func(match func(corebroker.CertificateIdentity) bool) int {
		if disconnected || !match(corebroker.CertificateIdentity{
			CredentialID: testCredentialID,
			Fingerprint:  selectors.FingerprintSHA256,
		}) {
			return 0
		}
		disconnected = true
		return 1
	}))
	identity, err := manager.AuthenticateCertificate(context.Background(), peer)
	require.NoError(t, err)
	resolver.setError(errors.New("Atom unavailable"))
	now = now.Add(manager.config.CacheTTL + time.Second)

	err = manager.AuthorizeCertificate(context.Background(), identity, "m/"+testTenantID+"/c/channel")
	require.ErrorContains(t, err, "Atom unavailable")
	require.True(t, disconnected)
	require.Equal(t, uint64(1), manager.CertificateMetrics().SessionsDisconnected)
}

func TestLifecycleEventEvictsRevokedSessionAndDuplicateIsIdempotent(t *testing.T) {
	now := time.Now().UTC()
	peer, _ := makePeerCertificate(t, 5)
	resolver := &resolverStub{result: activeResolverResult(now)}
	var liveIdentity corebroker.CertificateIdentity
	disconnected := false
	manager := newTestManager(t, resolver, &now, WithSessionInvalidator(func(match func(corebroker.CertificateIdentity) bool) int {
		if disconnected || !match(liveIdentity) {
			return 0
		}
		disconnected = true
		return 1
	}))
	identity, err := manager.AuthenticateCertificate(context.Background(), peer)
	require.NoError(t, err)
	liveIdentity = identity

	event := lifecycleEvent(t, "certificate.revoke", "credential", testCredentialID, map[string]any{
		"credential_id": testCredentialID,
	})
	properties := map[string]string{corebroker.ExternalIDProperty: testEventPrincipal}
	require.NoError(t, manager.HandleEvent(event, properties))
	require.NoError(t, manager.HandleEvent(event, properties))
	require.Zero(t, manager.CertificateMetrics().CacheEntries)
	require.True(t, disconnected)
	require.Equal(t, uint64(1), manager.CertificateMetrics().SessionsDisconnected)

	revoked := activeResolverResult(now)
	revoked.Status = "revoked"
	resolver.mu.Lock()
	resolver.result = revoked
	resolver.mu.Unlock()
	require.Error(t, manager.AuthorizeCertificate(context.Background(), identity, "$SYS/global/status"))
	require.Equal(t, uint64(1), manager.CertificateMetrics().CacheInvalidations)
}

func TestLifecycleEventPreventsInflightResolutionFromRepopulatingCache(t *testing.T) {
	now := time.Now().UTC()
	peer, _ := makePeerCertificate(t, 12)
	resolver := &resolverStub{
		result:  activeResolverResult(now),
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	manager := newTestManager(t, resolver, &now)
	result := make(chan error, 1)
	go func() {
		_, err := manager.AuthenticateCertificate(context.Background(), peer)
		result <- err
	}()

	select {
	case <-resolver.started:
	case <-time.After(time.Second):
		t.Fatal("resolver call did not start")
	}
	event := lifecycleEvent(t, "certificate.revoke", "credential", testCredentialID, nil)
	require.NoError(t, manager.HandleEvent(event, map[string]string{corebroker.ExternalIDProperty: testEventPrincipal}))
	close(resolver.release)
	select {
	case err := <-result:
		require.ErrorIs(t, err, ErrResolutionInvalidated)
	case <-time.After(time.Second):
		t.Fatal("in-flight resolver call did not finish")
	}
	require.Zero(t, manager.CertificateMetrics().CacheEntries)
}

func TestCertificateRotationRebindsSameEntity(t *testing.T) {
	now := time.Now().UTC()
	oldPeer, _ := makePeerCertificate(t, 6)
	newPeer, _ := makePeerCertificate(t, 7)
	oldRequest, err := selectorsFromPeer(oldPeer)
	require.NoError(t, err)
	newRequest, err := selectorsFromPeer(newPeer)
	require.NoError(t, err)
	oldResult := activeResolverResult(now)
	newResult := activeResolverResult(now)
	newResult.CredentialID = "05119e28-6260-4a06-8742-f925bcfdccd4"
	resolver := &resolverStub{results: map[string]ResolverResult{
		oldRequest.FingerprintSHA256: oldResult,
		newRequest.FingerprintSHA256: newResult,
	}}
	manager := newTestManager(t, resolver, &now)

	oldIdentity, err := manager.AuthenticateCertificate(context.Background(), oldPeer)
	require.NoError(t, err)
	newIdentity, err := manager.AuthenticateCertificate(context.Background(), newPeer)
	require.NoError(t, err)
	require.Equal(t, oldIdentity.EntityID, newIdentity.EntityID)
	require.NotEqual(t, oldIdentity.CredentialID, newIdentity.CredentialID)
	require.NoError(t, manager.AuthorizeCertificate(context.Background(), newIdentity, "m/"+testTenantID+"/c/channel"))
}

func TestRenewalDisconnectsOnlyRevokedOldCredential(t *testing.T) {
	oldCredentialID := testCredentialID
	newCredentialID := "05119e28-6260-4a06-8742-f925bcfdccd4"
	keys, disconnect := sessionDisconnectionKeys(domainEvent{
		Event:    "certificate.renew",
		TargetID: &oldCredentialID,
		Details: map[string]any{
			"old_credential_id": oldCredentialID,
			"new_credential_id": newCredentialID,
			"revoke_old":        true,
		},
	})
	require.True(t, disconnect)
	require.True(t, keys.matches(corebroker.CertificateIdentity{CredentialID: oldCredentialID}))
	require.False(t, keys.matches(corebroker.CertificateIdentity{CredentialID: newCredentialID}))
}

func TestResolverLoadCollapsesConcurrentConnections(t *testing.T) {
	now := time.Now().UTC()
	peer, _ := makePeerCertificate(t, 8)
	resolver := &resolverStub{result: activeResolverResult(now), delay: 20 * time.Millisecond}
	manager := newTestManager(t, resolver, &now)

	const connections = 250
	start := make(chan struct{})
	errs := make(chan error, connections)
	var workers sync.WaitGroup
	workers.Add(connections)
	for range connections {
		go func() {
			defer workers.Done()
			<-start
			_, err := manager.AuthenticateCertificate(context.Background(), peer)
			errs <- err
		}()
	}
	close(start)
	workers.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
	require.Equal(t, 1, resolver.callCount())
	require.Equal(t, 1, manager.CertificateMetrics().CacheEntries)
}

func TestResolutionCacheEvictsAtConfiguredCapacity(t *testing.T) {
	now := time.Now().UTC()
	firstPeer, _ := makePeerCertificate(t, 15)
	secondPeer, _ := makePeerCertificate(t, 16)
	resolver := &resolverStub{result: activeResolverResult(now)}
	manager := newTestManager(t, resolver, &now)
	manager.cache.capacity = 1

	_, err := manager.AuthenticateCertificate(context.Background(), firstPeer)
	require.NoError(t, err)
	_, err = manager.AuthenticateCertificate(context.Background(), secondPeer)
	require.NoError(t, err)
	require.Equal(t, 1, manager.CertificateMetrics().CacheEntries)
	require.Equal(t, uint64(1), manager.CertificateMetrics().CacheEvictions)

	_, err = manager.AuthenticateCertificate(context.Background(), firstPeer)
	require.NoError(t, err)
	require.Equal(t, 3, resolver.callCount(), "evicted fingerprints must resolve authoritatively again")
}

func TestTrustBundleRefreshAfterAuthorityProvisioningEvent(t *testing.T) {
	now := time.Now().UTC()
	_, firstPEM := makePeerCertificate(t, 9)
	_, secondPEM := makePeerCertificate(t, 10)

	var bundleMu sync.RWMutex
	bundle := firstPEM
	etag := "\"v1\""
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		bundleMu.RLock()
		defer bundleMu.RUnlock()
		if request.Header.Get("If-None-Match") == etag {
			writer.WriteHeader(http.StatusNotModified)
			return
		}
		writer.Header().Set("ETag", etag)
		writer.WriteHeader(http.StatusOK)
		_, _ = writer.Write(bundle)
	}))
	defer server.Close()

	resolver := &resolverStub{result: activeResolverResult(now)}
	manager, err := NewManager(Config{
		ResolverAddress:      "atom.invalid:8081",
		ResolverInsecure:     true,
		ServiceTokenFile:     "unused-by-test-resolver",
		TrustBundleURL:       server.URL,
		EventSourcePrincipal: testEventPrincipal,
		Timeout:              time.Second,
		CacheTTL:             30 * time.Second,
		CacheSize:            64,
		TrustRefreshInterval: time.Hour,
	}, WithResolver(resolver), WithHTTPClient(server.Client()))
	require.NoError(t, err)
	require.NoError(t, manager.Start(context.Background()))
	t.Cleanup(func() { require.NoError(t, manager.Close()) })
	require.Len(t, manager.trustPool.Subjects(), 1)

	bundleMu.Lock()
	bundle = append(append([]byte(nil), firstPEM...), secondPEM...)
	etag = "\"v2\""
	bundleMu.Unlock()
	event := lifecycleEvent(t, "pki.authority.provisioned_automatically", "pki_authority", testIssuerID, nil)
	require.NoError(t, manager.HandleEvent(event, map[string]string{corebroker.ExternalIDProperty: testEventPrincipal}))
	require.Eventually(t, func() bool {
		manager.trustMu.RLock()
		defer manager.trustMu.RUnlock()
		return manager.trustPool != nil && len(manager.trustPool.Subjects()) == 2
	}, time.Second, 10*time.Millisecond)

	wrapped, err := manager.WrapTLSConfig(&tls.Config{ClientAuth: tls.RequireAndVerifyClientCert})
	require.NoError(t, err)
	current, err := wrapped.GetConfigForClient(nil)
	require.NoError(t, err)
	require.Len(t, current.ClientCAs.Subjects(), 2)
}

func TestTrustBundleRejectsNonCA(t *testing.T) {
	peer, _ := makePeerCertificate(t, 14)
	leafPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: peer.LeafDER})
	_, err := parseTrustBundle(leafPEM)
	require.ErrorContains(t, err, "cannot sign certificates")
}

func TestUntrustedEventCannotInvalidateCache(t *testing.T) {
	now := time.Now().UTC()
	peer, _ := makePeerCertificate(t, 11)
	resolver := &resolverStub{result: activeResolverResult(now)}
	manager := newTestManager(t, resolver, &now)
	_, err := manager.AuthenticateCertificate(context.Background(), peer)
	require.NoError(t, err)

	event := lifecycleEvent(t, "certificate.revoke", "credential", testCredentialID, nil)
	require.ErrorIs(t, manager.HandleEvent(event, map[string]string{corebroker.ExternalIDProperty: "attacker"}), ErrUntrustedEvent)
	require.Equal(t, 1, manager.CertificateMetrics().CacheEntries)
	require.Equal(t, uint64(1), manager.CertificateMetrics().EventsRejected)

	unrelated := lifecycleEvent(t, "audit.export", "credential", testCredentialID, nil)
	require.NoError(t, manager.HandleEvent(unrelated, map[string]string{corebroker.ExternalIDProperty: testEventPrincipal}))
	require.Equal(t, 1, manager.CertificateMetrics().CacheEntries)
}

func lifecycleEvent(t *testing.T, event, targetKind, targetID string, details map[string]any) []byte {
	t.Helper()
	payload, err := json.Marshal(map[string]any{
		"schema_version":  1,
		"event_id":        "3025f995-3425-4bc8-a306-e4992cb9cf9d",
		"event":           event,
		"occurred_at":     time.Now().UTC().Format(time.RFC3339),
		"source":          "atom",
		"actor_entity_id": nil,
		"tenant_id":       testTenantID,
		"target_kind":     targetKind,
		"target_id":       targetID,
		"outcome":         "allow",
		"details":         details,
		"request_id":      nil,
	})
	require.NoError(t, err)
	return payload
}

func makePeerCertificate(t *testing.T, serial int64) (corebroker.PeerCertificate, []byte) {
	t.Helper()
	now := time.Now().UTC()
	issuerKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	issuerTemplate := &x509.Certificate{
		SerialNumber:          big.NewInt(serial + 1000),
		Subject:               pkix.Name{CommonName: "test issuer " + big.NewInt(serial).String()},
		NotBefore:             now.Add(-time.Hour),
		NotAfter:              now.Add(24 * time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
	}
	issuerDER, err := x509.CreateCertificate(rand.Reader, issuerTemplate, issuerTemplate, &issuerKey.PublicKey, issuerKey)
	require.NoError(t, err)

	leafKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	leafTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(serial),
		Subject:      pkix.Name{CommonName: "test peer"},
		NotBefore:    now.Add(-time.Minute),
		NotAfter:     now.Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}
	leafDER, err := x509.CreateCertificate(rand.Reader, leafTemplate, issuerTemplate, &leafKey.PublicKey, issuerKey)
	require.NoError(t, err)
	return corebroker.PeerCertificate{LeafDER: leafDER, IssuerDER: issuerDER}, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: issuerDER})
}
