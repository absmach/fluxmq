// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package broker

import (
	"context"
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
//
// The context carries the caller's cancellation: an implementation that talks
// to a remote service must abandon the call when the broker closes the
// connection or shuts down.
type Authenticator interface {
	Authenticate(ctx context.Context, clientID, username, secret string) (*AuthnResult, error)
}

// Authorizer checks topic permissions.
// The clientID parameter receives the resolved external identity when
// available, otherwise the protocol-level client ID.
//
// The context carries the caller's cancellation, and on the publish path that
// is the connection's own context: closing or superseding that connection
// releases the callout instead of leaving the broker blocked on a decision
// nobody is waiting for.
type Authorizer interface {
	CanPublish(ctx context.Context, clientID string, topic string) bool
	CanSubscribe(ctx context.Context, clientID string, filter string) bool
}

// AuthEngineOption configures an AuthEngine.
type AuthEngineOption func(*authEngineOptions)

type authEngineOptions struct {
	cacheSize int
	cacheTTL  time.Duration
}

// WithIdentityCache sets the bounded identity-cache size and TTL. A non-positive
// size disables size-based eviction; a non-positive TTL disables expiry.
func WithIdentityCache(size int, ttl time.Duration) AuthEngineOption {
	return func(o *authEngineOptions) {
		o.cacheSize = size
		o.cacheTTL = ttl
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
	auth       Authenticator
	authz      Authorizer
	identities *identityCache
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
		auth:       auth,
		authz:      authz,
		identities: newIdentityCache(o.cacheSize, o.cacheTTL),
	}
}

// Authenticate validates client credentials.
// Returns true if authenticated or if no authenticator is configured.
// On success, also returns the resolved external identity (empty when the
// authenticator did not provide one) and caches it for subsequent
// authorization calls.
func (e *AuthEngine) Authenticate(ctx context.Context, clientID, username, password string) (bool, string, error) {
	if e.auth == nil {
		return true, "", nil
	}
	result, err := e.auth.Authenticate(ctx, clientID, username, password)
	if err != nil {
		return false, "", err
	}
	if result == nil {
		return false, "", nil
	}
	if result.Authenticated && result.ID != "" {
		e.identities.Store(clientID, result.ID)
		return true, result.ID, nil
	}
	return result.Authenticated, "", nil
}

// CanPublish checks if a client is authorized to publish to a topic.
// Returns true if authorized or if no authorizer is configured.
func (e *AuthEngine) CanPublish(ctx context.Context, clientID, topic string) bool {
	if e.authz == nil {
		return true
	}
	return e.authz.CanPublish(ctx, e.resolveID(clientID), topic)
}

// CanSubscribe checks if a client is authorized to subscribe to a topic filter.
// Returns true if authorized or if no authorizer is configured.
func (e *AuthEngine) CanSubscribe(ctx context.Context, clientID, filter string) bool {
	if e.authz == nil {
		return true
	}
	return e.authz.CanSubscribe(ctx, e.resolveID(clientID), filter)
}

// Forget removes the cached identity mapping for a client.
// Should be called when a client disconnects.
func (e *AuthEngine) Forget(clientID string) {
	e.identities.Delete(clientID)
}

// SetExternalID stores or replaces the resolved external identity for a client.
func (e *AuthEngine) SetExternalID(clientID, externalID string) {
	if externalID == "" {
		e.identities.Delete(clientID)
		return
	}
	e.identities.Store(clientID, externalID)
}

// ExternalID returns the authenticated external identity for a protocol client ID.
func (e *AuthEngine) ExternalID(clientID string) string {
	id, _ := e.identities.Load(clientID)
	return id
}

// IdentityCacheLen returns the current number of cached identity mappings.
// Intended for monitoring; a steadily-growing value relative to live client
// count points to leaked Forget calls.
func (e *AuthEngine) IdentityCacheLen() int {
	return e.identities.Len()
}

func (e *AuthEngine) resolveID(clientID string) string {
	if id := e.ExternalID(clientID); id != "" {
		return id
	}
	return clientID
}
