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
// size disables size-based eviction for ordinary identities; certificate
// session bindings retain the default bound. A non-positive TTL disables expiry.
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
	pendingCerts    map[string]CertificateIdentity
	certBindings    map[string]uint64
	nextCertBinding uint64
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
	certificateCap := o.cacheSize
	if o.certificateAuth != nil && certificateCap <= 0 {
		certificateCap = DefaultIdentityCacheSize
	}
	return &AuthEngine{
		auth:            auth,
		authz:           authz,
		identities:      newIdentityCache(o.cacheSize, o.cacheTTL),
		certificateAuth: o.certificateAuth,
		certificates:    make(map[string]CertificateIdentity),
		pendingCerts:    make(map[string]CertificateIdentity),
		certBindings:    make(map[string]uint64),
		certificateCap:  certificateCap,
	}
}

// Authenticate validates client credentials.
// Returns true if authenticated or if no authenticator is configured.
// On success, also returns the resolved external identity (empty when the
// authenticator did not provide one) and caches it for subsequent
// authorization calls.
func (e *AuthEngine) Authenticate(clientID, username, password string) (bool, string, error) {
	return e.AuthenticateContext(context.Background(), clientID, username, password)
}

// AuthenticateContext is the context-aware form of Authenticate for protocol
// adapters that already own a request context.
func (e *AuthEngine) AuthenticateContext(ctx context.Context, clientID, username, password string) (bool, string, error) {
	return e.AuthenticateWithPeer(ctx, clientID, username, password, PeerCertificate{})
}

// AuthenticateWithPeer authenticates a verified TLS leaf through Atom when a
// certificate resolver is configured. Connections without a leaf follow the
// historical username/secret path unchanged: failed ordinary credentials stay
// an ordinary denial, while a successful attempt is rejected before it can
// replace a live certificate binding.
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
		if _, pending := e.pendingCerts[clientID]; pending {
			e.certificateMu.Unlock()
			return false, "", ErrCertificateAuthenticationPending
		}
		existing, exists := e.certificates[clientID]
		if exists && existing.EntityID != identity.EntityID {
			e.certificateMu.Unlock()
			return false, "", ErrCertificateClientIdentityConflict
		}
		if !exists && e.certificateCap > 0 && len(e.certificates)+len(e.pendingCerts) >= e.certificateCap {
			e.certificateMu.Unlock()
			return false, "", ErrCertificateSessionCapacity
		}
		// Authentication is not a live session yet. Hold the resolution pending
		// until the protocol handler has completed hooks and persistent-session
		// ownership checks. This also serializes concurrent CONNECT attempts for
		// one client ID, so neither attempt can commit the other's credential.
		e.pendingCerts[clientID] = identity
		e.certificateMu.Unlock()
		return true, identity.EntityID, nil
	}

	if e.auth == nil {
		if current, pending := e.certificateState(clientID); current || pending {
			if pending && !current {
				return false, "", ErrCertificateAuthenticationPending
			}
			return false, "", ErrCertificateClientIdentityConflict
		}
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

	// Recheck after the external call: a concurrent certificate attempt may
	// have claimed this client ID while ordinary credentials were being
	// validated. Never strip that binding from the other attempt.
	e.certificateMu.RLock()
	_, currentCertificate := e.certificates[clientID]
	_, pendingCertificate := e.pendingCerts[clientID]
	if currentCertificate || pendingCertificate {
		e.certificateMu.RUnlock()
		if pendingCertificate && !currentCertificate {
			return false, "", ErrCertificateAuthenticationPending
		}
		return false, "", ErrCertificateClientIdentityConflict
	}
	if result.ID != "" {
		e.identities.Store(clientID, result.ID)
		e.certificateMu.RUnlock()
		return true, result.ID, nil
	}
	e.identities.Delete(clientID)
	e.certificateMu.RUnlock()
	return true, "", nil
}

// CanPublish checks if a client is authorized to publish to a topic.
// Returns true if authorized or if no authorizer is configured.
func (e *AuthEngine) CanPublish(clientID, topic string) bool {
	return e.CanPublishContext(context.Background(), clientID, topic)
}

// CanPublishContext is the context-aware form of CanPublish.
func (e *AuthEngine) CanPublishContext(ctx context.Context, clientID, topic string) bool {
	if !e.authorizeCertificate(ctx, clientID, topic) {
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
	return e.CanSubscribeContext(context.Background(), clientID, filter)
}

// CanSubscribeContext is the context-aware form of CanSubscribe.
func (e *AuthEngine) CanSubscribeContext(ctx context.Context, clientID, filter string) bool {
	if !e.authorizeCertificate(ctx, clientID, filter) {
		return false
	}
	if e.authz == nil {
		return true
	}
	return e.authz.CanSubscribe(e.resolveID(clientID), filter)
}

// CanPublishPendingCertificate authorizes a publish requested as part of
// CONNECT (notably an MQTT Will) before the certificate binding is committed.
// The pending Atom identity is used directly so a hook or prior client-ID
// mapping cannot change the subject presented to normal authorization.
func (e *AuthEngine) CanPublishPendingCertificate(clientID, topic string) bool {
	return e.CanPublishPendingCertificateContext(context.Background(), clientID, topic)
}

// CanPublishPendingCertificateContext is the context-aware form of
// CanPublishPendingCertificate.
func (e *AuthEngine) CanPublishPendingCertificateContext(ctx context.Context, clientID, topic string) bool {
	e.certificateMu.RLock()
	identity, ok := e.pendingCerts[clientID]
	e.certificateMu.RUnlock()
	if !ok || e.certificateAuth == nil {
		return false
	}
	if e.certificateAuth.AuthorizeCertificate(ctx, identity, topic) != nil {
		return false
	}
	if e.authz == nil {
		return true
	}
	return e.authz.CanPublish(identity.EntityID, topic)
}

// Forget removes the cached identity mapping for a client.
// Should be called when a client disconnects.
func (e *AuthEngine) Forget(clientID string) {
	if e.rejectPendingOrForgetCertificate(clientID) {
		e.identities.Delete(clientID)
	}
}

// SetExternalID stores or replaces the resolved external identity for a client.
func (e *AuthEngine) SetExternalID(clientID, externalID string) {
	if identity, ok := e.certificateAuthenticationIdentity(clientID); ok {
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
	if identity, ok := e.certificateAuthenticationIdentity(clientID); ok {
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

// CertificateAuthenticationEnabled reports whether this engine has an Atom
// certificate resolver configured.
func (e *AuthEngine) CertificateAuthenticationEnabled() bool {
	return e.certificateAuth != nil
}

// CommitCertificateAuthentication promotes a same-entity reconnect that was
// held pending while the old connection remained live. It returns the binding
// generation that the MQTT session must retain for generation-safe cleanup.
func (e *AuthEngine) CommitCertificateAuthentication(clientID string) (uint64, bool) {
	e.certificateMu.Lock()
	pending, ok := e.pendingCerts[clientID]
	if !ok {
		e.certificateMu.Unlock()
		return 0, false
	}
	e.certificates[clientID] = pending
	delete(e.pendingCerts, clientID)
	binding := e.nextCertificateBindingLocked()
	e.certBindings[clientID] = binding
	e.certificateMu.Unlock()
	e.identities.Store(clientID, pending.EntityID)
	return binding, true
}

// CertificateSessionBinding returns the current live binding generation.
func (e *AuthEngine) CertificateSessionBinding(clientID string) (uint64, bool) {
	e.certificateMu.RLock()
	defer e.certificateMu.RUnlock()
	binding, ok := e.certBindings[clientID]
	return binding, ok && binding != 0
}

// ForgetCertificateSession removes a binding only when its generation still
// matches. A delayed disconnect from a replaced connection therefore cannot
// erase the certificate identity of its replacement.
func (e *AuthEngine) ForgetCertificateSession(clientID string, binding uint64) {
	e.certificateMu.Lock()
	if binding == 0 || e.certBindings[clientID] != binding {
		e.certificateMu.Unlock()
		return
	}
	delete(e.certificates, clientID)
	delete(e.certBindings, clientID)
	e.certificateMu.Unlock()
	e.identities.Delete(clientID)
}

// InvalidateCertificateSessions atomically removes certificate bindings that
// match a lifecycle event and returns the affected protocol client IDs. The
// caller can then disconnect those live sessions without holding auth locks.
func (e *AuthEngine) InvalidateCertificateSessions(match func(CertificateIdentity) bool) []string {
	if match == nil {
		return nil
	}
	e.certificateMu.Lock()
	clientIDs := make([]string, 0)
	for clientID, identity := range e.certificates {
		if match(identity) {
			delete(e.certificates, clientID)
			delete(e.certBindings, clientID)
			clientIDs = append(clientIDs, clientID)
		}
	}
	for clientID, identity := range e.pendingCerts {
		if match(identity) {
			delete(e.pendingCerts, clientID)
		}
	}
	e.certificateMu.Unlock()
	for _, clientID := range clientIDs {
		e.identities.Delete(clientID)
	}
	return clientIDs
}

func (e *AuthEngine) authorizeCertificate(ctx context.Context, clientID, topic string) bool {
	identity, ok := e.certificateIdentity(clientID)
	if !ok {
		return true
	}
	if e.certificateAuth == nil {
		return false
	}
	return e.certificateAuth.AuthorizeCertificate(ctx, identity, topic) == nil
}

func (e *AuthEngine) certificateIdentity(clientID string) (CertificateIdentity, bool) {
	e.certificateMu.RLock()
	defer e.certificateMu.RUnlock()
	identity, ok := e.certificates[clientID]
	return identity, ok
}

// certificateAuthenticationIdentity includes an in-progress authentication
// only when there is no committed identity. A reconnect is constrained to the
// same entity before it enters pending state, so the committed value remains
// authoritative for an already-live session.
func (e *AuthEngine) certificateAuthenticationIdentity(clientID string) (CertificateIdentity, bool) {
	e.certificateMu.RLock()
	defer e.certificateMu.RUnlock()
	if identity, ok := e.certificates[clientID]; ok {
		return identity, true
	}
	identity, ok := e.pendingCerts[clientID]
	return identity, ok
}

func (e *AuthEngine) certificateState(clientID string) (current, pending bool) {
	e.certificateMu.RLock()
	defer e.certificateMu.RUnlock()
	_, current = e.certificates[clientID]
	_, pending = e.pendingCerts[clientID]
	return current, pending
}

func (e *AuthEngine) rejectPendingOrForgetCertificate(clientID string) bool {
	e.certificateMu.Lock()
	if _, pending := e.pendingCerts[clientID]; pending {
		delete(e.pendingCerts, clientID)
		_, current := e.certificates[clientID]
		e.certificateMu.Unlock()
		return !current
	}
	delete(e.certificates, clientID)
	delete(e.certBindings, clientID)
	e.certificateMu.Unlock()
	return true
}

func (e *AuthEngine) nextCertificateBindingLocked() uint64 {
	e.nextCertBinding++
	if e.nextCertBinding == 0 {
		e.nextCertBinding++
	}
	return e.nextCertBinding
}

func (e *AuthEngine) resolveID(clientID string) string {
	if id := e.ExternalID(clientID); id != "" {
		return id
	}
	return clientID
}
