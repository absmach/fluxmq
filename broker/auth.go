// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package broker

// AuthEngine handles authentication and authorization checks.
type AuthEngine struct {
	auth  Authenticator
	authz Authorizer
}

// NewAuthEngine creates a new auth engine with the given authenticator and authorizer.
func NewAuthEngine(auth Authenticator, authz Authorizer) *AuthEngine {
	return &AuthEngine{
		auth:  auth,
		authz: authz,
	}
}

// CanPublish checks if a client is authorized to publish to a topic.
// Returns true if authorized or if no authorizer is configured.
func (e *AuthEngine) CanPublish(clientID, topic string) bool {
	if e.authz == nil {
		return true
	}
	return e.authz.CanPublish(clientID, topic)
}

// CanSubscribe checks if a client is authorized to subscribe to a topic filter.
// Returns true if authorized or if no authorizer is configured.
func (e *AuthEngine) CanSubscribe(clientID, filter string) bool {
	if e.authz == nil {
		return true
	}
	return e.authz.CanSubscribe(clientID, filter)
}

// Authenticate validates client credentials.
// Returns true if authenticated or if no authenticator is configured.
func (e *AuthEngine) Authenticate(clientID, username, password string) (bool, error) {
	if e.auth == nil {
		return true, nil
	}
	return e.auth.Authenticate(clientID, username, password)
}

// AuthContinuation represents the state of an ongoing enhanced authentication.
// This is used in V5.
type AuthContinuation struct {
	ResponseData []byte
	State        any
	ReasonString string
	ReasonCode   byte
	Complete     bool
}

// AuthenticatorV5 extends Authenticator with MQTT v5 enhanced authentication.
type AuthenticatorV5 interface {
	Authenticator

	StartAuth(clientID, authMethod string, authData []byte) (*AuthContinuation, error)

	ContinueAuth(clientID string, authData []byte, state *AuthContinuation) (*AuthContinuation, error)

	CompleteAuth(clientID string, state *AuthContinuation) (bool, error)
}

// DefaultAuthenticatorV5 wraps a basic Authenticator to provide v5 interface.
type DefaultAuthenticatorV5 struct {
	basic Authenticator
}

// NewDefaultAuthenticatorV5 wraps a basic authenticator.
func NewDefaultAuthenticatorV5(basic Authenticator) *DefaultAuthenticatorV5 {
	if basic == nil {
		return nil
	}
	return &DefaultAuthenticatorV5{basic: basic}
}

// Authenticate delegates to the basic authenticator.
func (a *DefaultAuthenticatorV5) Authenticate(clientID, username, password string) (bool, error) {
	return a.basic.Authenticate(clientID, username, password)
}

// StartAuth returns error for unsupported enhanced auth.
func (a *DefaultAuthenticatorV5) StartAuth(clientID, authMethod string, authData []byte) (*AuthContinuation, error) {
	return nil, ErrNotAuthorized
}

// ContinueAuth returns error for unsupported enhanced auth.
func (a *DefaultAuthenticatorV5) ContinueAuth(clientID string, authData []byte, state *AuthContinuation) (*AuthContinuation, error) {
	return nil, ErrNotAuthorized
}

// CompleteAuth returns error for unsupported enhanced auth.
func (a *DefaultAuthenticatorV5) CompleteAuth(clientID string, state *AuthContinuation) (bool, error) {
	return false, ErrNotAuthorized
}
