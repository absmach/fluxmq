// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package broker

import (
	"context"
	"sync"
	"time"
)

// Default bounds for the identity cache. These are deliberately conservative;
// operators with large client populations should raise IdentityCacheSize.
const (
	DefaultIdentityCacheSize = 10000
	DefaultIdentityCacheTTL  = 24 * time.Hour
)

// AuthnResult holds the outcome of an authentication attempt.
type AuthnResult struct {
	Authenticated bool
	// ID is the external identity resolved by the auth provider (e.g. a UUID).
	// When non-empty, the AuthEngine stores this and passes it to the
	// Authorizer in place of the protocol-level client ID.
	ID string
}

// Authenticator validates client credentials.
type Authenticator interface {
	Authenticate(clientID, username, secret string) (*AuthnResult, error)
}

// Authorizer checks topic permissions.
// The clientID parameter receives the resolved external identity when
// available, otherwise the protocol-level client ID.
type Authorizer interface {
	CanPublish(clientID string, topic string) bool
	CanSubscribe(clientID string, filter string) bool
}

// AuthEngineOption configures an AuthEngine.
type AuthEngineOption func(*authEngineOptions)

type authEngineOptions struct {
	cacheSize       int
	cacheTTL        time.Duration
	certificateAuth CertificateAuthenticator
}

// WithIdentityCache sets the bounded identity-cache size and TTL. A non-positive
// size disables size-based eviction; a non-positive TTL disables expiry.
func WithIdentityCache(size int, ttl time.Duration) AuthEngineOption {
	return func(o *authEngineOptions) {
		o.cacheSize = size
		o.cacheTTL = ttl
	}
}

// WithCertificateAuthentication enables resolver-tier authentication for MQTT
// transports that present a verified peer leaf certificate.
func WithCertificateAuthentication(auth CertificateAuthenticator) AuthEngineOption {
	return func(o *authEngineOptions) {
		o.certificateAuth = auth
	}
}

// AuthEngine handles authentication and authorization.
// It transparently maps protocol-level client IDs to external identities
// returned by the Authenticator, so protocol handlers don't need to be
// aware of identity resolution.
//
// The identity cache is bounded (TTL + LRU) so misbehaving disconnect
// paths cannot leak memory.
type AuthEngine struct {
	auth            Authenticator
	authz           Authorizer
	identities      *identityCache
	certificateAuth CertificateAuthenticator
	certificateMu   sync.RWMutex
	certificates    map[string]CertificateIdentity
	certificateCap  int
}

// NewAuthEngine creates a new AuthEngine with the given authenticator and authorizer.
// Apply WithIdentityCache to override the default identity-cache bounds.
func NewAuthEngine(auth Authenticator, authz Authorizer, opts ...AuthEngineOption) *AuthEngine {
	o := authEngineOptions{
		cacheSize: DefaultIdentityCacheSize,
		cacheTTL:  DefaultIdentityCacheTTL,
	}
	for _, fn := range opts {
		fn(&o)
	}
	return &AuthEngine{
		auth:            auth,
		authz:           authz,
		identities:      newIdentityCache(o.cacheSize, o.cacheTTL),
		certificateAuth: o.certificateAuth,
		certificates:    make(map[string]CertificateIdentity),
		certificateCap:  o.cacheSize,
	}
}

// Authenticate validates client credentials.
// Returns true if authenticated or if no authenticator is configured.
// On success, also returns the resolved external identity (empty when the
// authenticator did not provide one) and caches it for subsequent
// authorization calls.
func (e *AuthEngine) Authenticate(clientID, username, password string) (bool, string, error) {
	return e.AuthenticateWithPeer(context.Background(), clientID, username, password, PeerCertificate{})
}

// AuthenticateWithPeer authenticates a verified TLS leaf through Atom when a
// certificate resolver is configured. Connections without a leaf follow the
// historical username/secret path unchanged.
func (e *AuthEngine) AuthenticateWithPeer(ctx context.Context, clientID, username, password string, peer PeerCertificate) (bool, string, error) {
	if len(peer.LeafDER) != 0 && e.certificateAuth != nil {
		identity, err := e.certificateAuth.AuthenticateCertificate(ctx, peer)
		if err != nil {
			return false, "", err
		}
		if identity.EntityID == "" || identity.CredentialID == "" || identity.Fingerprint == "" {
			return false, "", nil
		}

		e.certificateMu.Lock()
		existing, exists := e.certificates[clientID]
		if exists && existing.EntityID != identity.EntityID {
			e.certificateMu.Unlock()
			return false, "", ErrCertificateClientIdentityConflict
		}
		if !exists && e.certificateCap > 0 && len(e.certificates) >= e.certificateCap {
			e.certificateMu.Unlock()
			return false, "", ErrCertificateSessionCapacity
		}
		e.certificates[clientID] = identity
		e.certificateMu.Unlock()
		e.identities.Store(clientID, identity.EntityID)
		return true, identity.EntityID, nil
	}

	if e.auth == nil {
		e.removeCertificateIdentity(clientID)
		e.identities.Delete(clientID)
		return true, "", nil
	}
	result, err := e.auth.Authenticate(clientID, username, password)
	if err != nil {
		return false, "", err
	}
	if result == nil || !result.Authenticated {
		return false, "", nil
	}

	// Replace an existing certificate binding only after the alternate
	// credential has authenticated. A rejected takeover must not strip tenant
	// enforcement from the currently connected certificate session.
	e.removeCertificateIdentity(clientID)
	if result.ID != "" {
		e.identities.Store(clientID, result.ID)
		return true, result.ID, nil
	}
	e.identities.Delete(clientID)
	return true, "", nil
}

// CanPublish checks if a client is authorized to publish to a topic.
// Returns true if authorized or if no authorizer is configured.
func (e *AuthEngine) CanPublish(clientID, topic string) bool {
	if !e.authorizeCertificate(clientID, topic) {
		return false
	}
	if e.authz == nil {
		return true
	}
	return e.authz.CanPublish(e.resolveID(clientID), topic)
}

// CanSubscribe checks if a client is authorized to subscribe to a topic filter.
// Returns true if authorized or if no authorizer is configured.
func (e *AuthEngine) CanSubscribe(clientID, filter string) bool {
	if !e.authorizeCertificate(clientID, filter) {
		return false
	}
	if e.authz == nil {
		return true
	}
	return e.authz.CanSubscribe(e.resolveID(clientID), filter)
}

// Forget removes the cached identity mapping for a client.
// Should be called when a client disconnects.
func (e *AuthEngine) Forget(clientID string) {
	e.removeCertificateIdentity(clientID)
	e.identities.Delete(clientID)
}

// SetExternalID stores or replaces the resolved external identity for a client.
func (e *AuthEngine) SetExternalID(clientID, externalID string) {
	if identity, ok := e.certificateIdentity(clientID); ok {
		// A hook may deny the connection, but it cannot replace Atom's resolved
		// certificate identity with a different subject.
		e.identities.Store(clientID, identity.EntityID)
		return
	}
	if externalID == "" {
		e.identities.Delete(clientID)
		return
	}
	e.identities.Store(clientID, externalID)
}

// ExternalID returns the authenticated external identity for a protocol client ID.
func (e *AuthEngine) ExternalID(clientID string) string {
	if identity, ok := e.certificateIdentity(clientID); ok {
		return identity.EntityID
	}
	id, _ := e.identities.Load(clientID)
	return id
}

// IdentityCacheLen returns the current number of cached identity mappings.
// Intended for monitoring; a steadily-growing value relative to live client
// count points to leaked Forget calls.
func (e *AuthEngine) IdentityCacheLen() int {
	return e.identities.Len()
}

// CertificateSessionCount returns bounded certificate session state for
// operational monitoring. It contains no identity labels.
func (e *AuthEngine) CertificateSessionCount() int {
	e.certificateMu.RLock()
	defer e.certificateMu.RUnlock()
	return len(e.certificates)
}

func (e *AuthEngine) authorizeCertificate(clientID, topic string) bool {
	identity, ok := e.certificateIdentity(clientID)
	if !ok {
		return true
	}
	if e.certificateAuth == nil {
		return false
	}
	return e.certificateAuth.AuthorizeCertificate(context.Background(), identity, topic) == nil
}

func (e *AuthEngine) certificateIdentity(clientID string) (CertificateIdentity, bool) {
	e.certificateMu.RLock()
	defer e.certificateMu.RUnlock()
	identity, ok := e.certificates[clientID]
	return identity, ok
}

func (e *AuthEngine) removeCertificateIdentity(clientID string) {
	e.certificateMu.Lock()
	delete(e.certificates, clientID)
	e.certificateMu.Unlock()
}

func (e *AuthEngine) resolveID(clientID string) string {
	if id := e.ExternalID(clientID); id != "" {
		return id
	}
	return clientID
}
