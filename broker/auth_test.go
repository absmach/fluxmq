// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package broker

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type stubAuthenticator struct {
	result *AuthnResult
	err    error
}

func (s *stubAuthenticator) Authenticate(clientID, username, secret string) (*AuthnResult, error) {
	return s.result, s.err
}

type stubAuthorizer struct {
	receivedClientID string
	allow            bool
}

func (s *stubAuthorizer) CanPublish(clientID, topic string) bool {
	s.receivedClientID = clientID
	return s.allow
}

func (s *stubAuthorizer) CanSubscribe(clientID, filter string) bool {
	s.receivedClientID = clientID
	return s.allow
}

func TestAuthEngine_Authenticate_NilAuth(t *testing.T) {
	e := NewAuthEngine(nil, nil)
	ok, err := e.Authenticate("c1", "user", "pass")
	require.NoError(t, err)
	assert.True(t, ok)
}

func TestAuthEngine_Authenticate_Success_StoresIdentity(t *testing.T) {
	authn := &stubAuthenticator{result: &AuthnResult{Authenticated: true, ID: "ext-123"}}
	authz := &stubAuthorizer{allow: true}
	e := NewAuthEngine(authn, authz)

	ok, err := e.Authenticate("mqtt-client-1", "user", "pass")
	require.NoError(t, err)
	assert.True(t, ok)

	// Authz should receive the resolved external ID, not the MQTT client ID
	e.CanPublish("mqtt-client-1", "some/topic")
	assert.Equal(t, "ext-123", authz.receivedClientID)
}

func TestAuthEngine_ExternalID_ReturnsStoredIdentity(t *testing.T) {
	authn := &stubAuthenticator{result: &AuthnResult{Authenticated: true, ID: "ext-123"}}
	e := NewAuthEngine(authn, nil)

	ok, err := e.Authenticate("mqtt-client-1", "user", "pass")
	require.NoError(t, err)
	assert.True(t, ok)

	assert.Equal(t, "ext-123", e.ExternalID("mqtt-client-1"))
	assert.Equal(t, "", e.ExternalID("missing-client"))
}

func TestAuthEngine_Authenticate_Failure_NoIdentityStored(t *testing.T) {
	authn := &stubAuthenticator{result: &AuthnResult{Authenticated: false}}
	authz := &stubAuthorizer{allow: true}
	e := NewAuthEngine(authn, authz)

	ok, err := e.Authenticate("mqtt-client-1", "user", "wrong")
	require.NoError(t, err)
	assert.False(t, ok)

	// No identity mapping — authz receives the raw MQTT client ID
	e.CanPublish("mqtt-client-1", "some/topic")
	assert.Equal(t, "mqtt-client-1", authz.receivedClientID)
}

func TestAuthEngine_CanPublish_NilAuthz(t *testing.T) {
	e := NewAuthEngine(nil, nil)
	assert.True(t, e.CanPublish("c1", "topic"))
}

func TestAuthEngine_CanSubscribe_NilAuthz(t *testing.T) {
	e := NewAuthEngine(nil, nil)
	assert.True(t, e.CanSubscribe("c1", "filter"))
}

func TestAuthEngine_Forget_RemovesMapping(t *testing.T) {
	authn := &stubAuthenticator{result: &AuthnResult{Authenticated: true, ID: "ext-456"}}
	authz := &stubAuthorizer{allow: true}
	e := NewAuthEngine(authn, authz)

	_, _ = e.Authenticate("mqtt-client-2", "user", "pass")
	e.Forget("mqtt-client-2")

	// After Forget, authz receives the raw MQTT client ID
	e.CanPublish("mqtt-client-2", "some/topic")
	assert.Equal(t, "mqtt-client-2", authz.receivedClientID)
}

func TestAuthEngine_ResolveID_NoMapping(t *testing.T) {
	authz := &stubAuthorizer{allow: true}
	e := NewAuthEngine(nil, authz)

	e.CanPublish("plain-client", "topic")
	assert.Equal(t, "plain-client", authz.receivedClientID)
}
