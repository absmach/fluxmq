// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package broker

// Authenticator validates client credentials.
type Authenticator interface {
	Authenticate(clientID, username, secret string) (bool, error)
}

// Authorizer checks topic permissions.
type Authorizer interface {
	CanPublish(clientID string, topic string) bool
	CanSubscribe(clientID string, filter string) bool
}

// AuthEngine handles authentication and authorization checks.
type AuthEngine struct {
	auth  Authenticator
	authz Authorizer
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
