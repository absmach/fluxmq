// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package broker

import "sync"

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

// AuthEngine handles authentication and authorization.
// It transparently maps protocol-level client IDs to external identities
// returned by the Authenticator, so protocol handlers don't need to be
// aware of identity resolution.
type AuthEngine struct {
	auth       Authenticator
	authz      Authorizer
	identities sync.Map // protocol clientID → external ID
}

// NewAuthEngine creates a new AuthEngine with the given authenticator and authorizer.
func NewAuthEngine(auth Authenticator, authz Authorizer) *AuthEngine {
	return &AuthEngine{auth: auth, authz: authz}
}

// Authenticate validates client credentials.
// Returns true if authenticated or if no authenticator is configured.
// On success, caches the resolved external identity for subsequent
// authorization calls.
func (e *AuthEngine) Authenticate(clientID, username, password string) (bool, error) {
	if e.auth == nil {
		return true, nil
	}
	result, err := e.auth.Authenticate(clientID, username, password)
	if err != nil {
		return false, err
	}
	if result == nil {
		return false, nil
	}
	if result.Authenticated && result.ID != "" {
		e.identities.Store(clientID, result.ID)
	}
	return result.Authenticated, nil
}

// CanPublish checks if a client is authorized to publish to a topic.
// Returns true if authorized or if no authorizer is configured.
func (e *AuthEngine) CanPublish(clientID, topic string) bool {
	if e.authz == nil {
		return true
	}
	return e.authz.CanPublish(e.resolveID(clientID), topic)
}

// CanSubscribe checks if a client is authorized to subscribe to a topic filter.
// Returns true if authorized or if no authorizer is configured.
func (e *AuthEngine) CanSubscribe(clientID, filter string) bool {
	if e.authz == nil {
		return true
	}
	return e.authz.CanSubscribe(e.resolveID(clientID), filter)
}

// Forget removes the cached identity mapping for a client.
// Should be called when a client disconnects.
func (e *AuthEngine) Forget(clientID string) {
	e.identities.Delete(clientID)
}

// ExternalID returns the authenticated external identity for a protocol client ID.
func (e *AuthEngine) ExternalID(clientID string) string {
	if id, ok := e.identities.Load(clientID); ok {
		return id.(string)
	}
	return ""
}

func (e *AuthEngine) resolveID(clientID string) string {
	if id := e.ExternalID(clientID); id != "" {
		return id
	}
	return clientID
}
