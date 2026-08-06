// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package broker

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

const (
	testCertificateClientID     = "mqtt-client"
	testCertificateEntityID     = "8a0a5c59-4ea8-4fc1-badb-f96cf739b224"
	testCertificateCredentialID = "ca49950c-3ed2-41b4-a319-896085285686"
	testRotatedCredentialID     = "05119e28-6260-4a06-8742-f925bcfdccd4"
	testCertificateFingerprint  = "abc123"
	testCertificateGlobalTopic  = "$SYS/global/status"
)

type certificateAuthenticatorStub struct {
	identity               CertificateIdentity
	authenticateErr        error
	authorizeErr           error
	authorizeCalls         int
	lastAuthorizedIdentity CertificateIdentity
}

func (stub *certificateAuthenticatorStub) AuthenticateCertificate(_ context.Context, peer PeerCertificate) (CertificateIdentity, error) {
	if len(peer.LeafDER) == 0 {
		return CertificateIdentity{}, errors.New("missing leaf")
	}
	return stub.identity, stub.authenticateErr
}

func (stub *certificateAuthenticatorStub) AuthorizeCertificate(_ context.Context, identity CertificateIdentity, _ string) error {
	stub.authorizeCalls++
	stub.lastAuthorizedIdentity = identity
	return stub.authorizeErr
}

func TestAuthEngineCertificateIdentityPrecedesNormalAuthorization(t *testing.T) {
	certificateAuth := &certificateAuthenticatorStub{identity: CertificateIdentity{
		EntityID:     testCertificateEntityID,
		TenantID:     "d204f7df-8293-4194-963b-a47a65bc8f04",
		CredentialID: testCertificateCredentialID,
		Fingerprint:  testCertificateFingerprint,
		ExpiresAt:    time.Now().Add(time.Hour),
	}}
	authorizer := &stubAuthorizer{allow: true}
	engine := NewAuthEngine(nil, authorizer, WithCertificateAuthentication(certificateAuth))

	ok, entityID, err := engine.AuthenticateWithPeer(
		context.Background(),
		testCertificateClientID,
		"ignored",
		"ignored",
		PeerCertificate{LeafDER: []byte{1, 2, 3}},
	)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, certificateAuth.identity.EntityID, entityID)
	_, committed := engine.CommitCertificateAuthentication(testCertificateClientID)
	require.True(t, committed)
	require.True(t, engine.CanPublish(testCertificateClientID, "m/d204f7df-8293-4194-963b-a47a65bc8f04/c/channel"))
	require.Equal(t, 1, certificateAuth.authorizeCalls)
	require.Equal(t, certificateAuth.identity.EntityID, authorizer.receivedClientID)
}

func TestAuthEngineCertificateDenialStopsNormalAuthorization(t *testing.T) {
	certificateAuth := &certificateAuthenticatorStub{
		identity: CertificateIdentity{
			EntityID:     testCertificateEntityID,
			CredentialID: testCertificateCredentialID,
			Fingerprint:  testCertificateFingerprint,
		},
		authorizeErr: errTenantMismatchForTest,
	}
	authorizer := &stubAuthorizer{allow: true}
	engine := NewAuthEngine(nil, authorizer, WithCertificateAuthentication(certificateAuth))
	_, _, err := engine.AuthenticateWithPeer(context.Background(), testCertificateClientID, "", "", PeerCertificate{LeafDER: []byte{1}})
	require.NoError(t, err)
	_, committed := engine.CommitCertificateAuthentication(testCertificateClientID)
	require.True(t, committed)

	require.False(t, engine.CanSubscribe(testCertificateClientID, "m/other/#"))
	require.Empty(t, authorizer.receivedClientID, "normal authorization must not run after tenant denial")
}

func TestAuthEngineNormalAuthorizationCanDenyCertificateIdentity(t *testing.T) {
	certificateAuth := &certificateAuthenticatorStub{identity: CertificateIdentity{
		EntityID:     testCertificateEntityID,
		CredentialID: testCertificateCredentialID,
		Fingerprint:  testCertificateFingerprint,
	}}
	authorizer := &stubAuthorizer{allow: false}
	engine := NewAuthEngine(nil, authorizer, WithCertificateAuthentication(certificateAuth))
	_, _, err := engine.AuthenticateWithPeer(context.Background(), testCertificateClientID, "", "", PeerCertificate{LeafDER: []byte{1}})
	require.NoError(t, err)
	_, committed := engine.CommitCertificateAuthentication(testCertificateClientID)
	require.True(t, committed)

	require.False(t, engine.CanPublish(testCertificateClientID, testCertificateGlobalTopic))
	require.Equal(t, certificateAuth.identity.EntityID, authorizer.receivedClientID)
}

func TestAuthEngineCertificateIdentityCannotBeOverriddenByHook(t *testing.T) {
	certificateAuth := &certificateAuthenticatorStub{identity: CertificateIdentity{
		EntityID:     testCertificateEntityID,
		CredentialID: testCertificateCredentialID,
		Fingerprint:  testCertificateFingerprint,
	}}
	engine := NewAuthEngine(nil, nil, WithCertificateAuthentication(certificateAuth))
	_, _, err := engine.AuthenticateWithPeer(context.Background(), testCertificateClientID, "", "", PeerCertificate{LeafDER: []byte{1}})
	require.NoError(t, err)

	engine.SetExternalID(testCertificateClientID, "hook-chosen-identity")
	require.Equal(t, certificateAuth.identity.EntityID, engine.ExternalID(testCertificateClientID))
	engine.Forget(testCertificateClientID)
	require.Zero(t, engine.CertificateSessionCount())
}

func TestAuthEngineCertificateRotationRebindsClientBeforeNextOperation(t *testing.T) {
	certificateAuth := &certificateAuthenticatorStub{identity: CertificateIdentity{
		EntityID:     testCertificateEntityID,
		CredentialID: testCertificateCredentialID,
		Fingerprint:  "old-fingerprint",
	}}
	engine := NewAuthEngine(nil, nil, WithCertificateAuthentication(certificateAuth))
	_, _, err := engine.AuthenticateWithPeer(context.Background(), testCertificateClientID, "", "", PeerCertificate{LeafDER: []byte{1}})
	require.NoError(t, err)
	oldBinding, committed := engine.CommitCertificateAuthentication(testCertificateClientID)
	require.True(t, committed)

	certificateAuth.identity.CredentialID = testRotatedCredentialID
	certificateAuth.identity.Fingerprint = "new-fingerprint"
	_, _, err = engine.AuthenticateWithPeer(context.Background(), testCertificateClientID, "", "", PeerCertificate{LeafDER: []byte{2}})
	require.NoError(t, err)
	newBinding, committed := engine.CommitCertificateAuthentication(testCertificateClientID)
	require.True(t, committed)
	require.NotEqual(t, oldBinding, newBinding)
	engine.ForgetCertificateSession(testCertificateClientID, oldBinding)
	require.True(t, engine.CanPublish(testCertificateClientID, testCertificateGlobalTopic))
	require.Equal(t, "new-fingerprint", certificateAuth.lastAuthorizedIdentity.Fingerprint)
	require.Equal(t, 1, engine.CertificateSessionCount())
}

func TestAuthEngineRejectsCrossEntityCertificateClientIDTakeover(t *testing.T) {
	certificateAuth := &certificateAuthenticatorStub{identity: CertificateIdentity{
		EntityID:     testCertificateEntityID,
		CredentialID: testCertificateCredentialID,
		Fingerprint:  "first-fingerprint",
	}}
	engine := NewAuthEngine(nil, nil, WithCertificateAuthentication(certificateAuth))
	_, _, err := engine.AuthenticateWithPeer(context.Background(), testCertificateClientID, "", "", PeerCertificate{LeafDER: []byte{1}})
	require.NoError(t, err)
	_, committed := engine.CommitCertificateAuthentication(testCertificateClientID)
	require.True(t, committed)

	certificateAuth.identity.EntityID = "ac47c9fd-1d4a-4270-bb11-ab6476a0bd3a"
	certificateAuth.identity.CredentialID = testRotatedCredentialID
	certificateAuth.identity.Fingerprint = "second-fingerprint"
	_, _, err = engine.AuthenticateWithPeer(context.Background(), testCertificateClientID, "", "", PeerCertificate{LeafDER: []byte{2}})
	require.ErrorIs(t, err, ErrCertificateClientIdentityConflict)
	require.Equal(t, testCertificateEntityID, engine.ExternalID(testCertificateClientID))
	require.Equal(t, 1, engine.CertificateSessionCount())
}

func TestAuthEngineNonCertificatePathUnchanged(t *testing.T) {
	authn := &stubAuthenticator{result: &AuthnResult{Authenticated: true, ID: testExternalID}}
	certificateAuth := &certificateAuthenticatorStub{authenticateErr: errors.New("must not be called")}
	engine := NewAuthEngine(authn, nil, WithCertificateAuthentication(certificateAuth))

	ok, externalID, err := engine.Authenticate("plain-client", "user", "password")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, testExternalID, externalID)
}

func TestRejectedPlainTakeoverPreservesCertificateSession(t *testing.T) {
	certificateAuth := &certificateAuthenticatorStub{identity: CertificateIdentity{
		EntityID:     testCertificateEntityID,
		CredentialID: testCertificateCredentialID,
		Fingerprint:  testCertificateFingerprint,
	}}
	authn := &stubAuthenticator{result: &AuthnResult{Authenticated: false}}
	engine := NewAuthEngine(authn, nil, WithCertificateAuthentication(certificateAuth))
	_, _, err := engine.AuthenticateWithPeer(context.Background(), testCertificateClientID, "", "", PeerCertificate{LeafDER: []byte{1}})
	require.NoError(t, err)
	_, committed := engine.CommitCertificateAuthentication(testCertificateClientID)
	require.True(t, committed)

	ok, _, err := engine.Authenticate(testCertificateClientID, "attacker", "wrong")
	require.NoError(t, err)
	require.False(t, ok)
	require.Equal(t, 1, engine.CertificateSessionCount())
	require.Equal(t, certificateAuth.identity.EntityID, engine.ExternalID(testCertificateClientID))
	require.True(t, engine.CanPublish(testCertificateClientID, testCertificateGlobalTopic))
	require.Equal(t, 1, certificateAuth.authorizeCalls)
}

func TestAuthEngineSerializesConcurrentCertificateAuthentication(t *testing.T) {
	certificateAuth := &certificateAuthenticatorStub{identity: CertificateIdentity{
		EntityID:     testCertificateEntityID,
		CredentialID: testCertificateCredentialID,
		Fingerprint:  testCertificateFingerprint,
	}}
	engine := NewAuthEngine(nil, nil, WithCertificateAuthentication(certificateAuth))

	_, _, err := engine.AuthenticateWithPeer(context.Background(), testCertificateClientID, "", "", PeerCertificate{LeafDER: []byte{1}})
	require.NoError(t, err)
	_, _, err = engine.AuthenticateWithPeer(context.Background(), testCertificateClientID, "", "", PeerCertificate{LeafDER: []byte{2}})
	require.ErrorIs(t, err, ErrCertificateAuthenticationPending)

	binding, committed := engine.CommitCertificateAuthentication(testCertificateClientID)
	require.True(t, committed)
	require.NotZero(t, binding)
	require.Equal(t, 1, engine.CertificateSessionCount())
}

func TestAuthEngineBoundsPendingAndCommittedCertificateSessions(t *testing.T) {
	const secondClientID = "client-2"
	certificateAuth := &certificateAuthenticatorStub{identity: CertificateIdentity{
		EntityID:     testCertificateEntityID,
		CredentialID: testCertificateCredentialID,
		Fingerprint:  testCertificateFingerprint,
	}}
	engine := NewAuthEngine(nil, nil,
		WithIdentityCache(1, time.Hour),
		WithCertificateAuthentication(certificateAuth),
	)

	_, _, err := engine.AuthenticateWithPeer(context.Background(), "client-1", "", "", PeerCertificate{LeafDER: []byte{1}})
	require.NoError(t, err)
	_, _, err = engine.AuthenticateWithPeer(context.Background(), secondClientID, "", "", PeerCertificate{LeafDER: []byte{2}})
	require.ErrorIs(t, err, ErrCertificateSessionCapacity, "pending bindings count toward the bound")
	binding, committed := engine.CommitCertificateAuthentication("client-1")
	require.True(t, committed)
	_, _, err = engine.AuthenticateWithPeer(context.Background(), secondClientID, "", "", PeerCertificate{LeafDER: []byte{2}})
	require.ErrorIs(t, err, ErrCertificateSessionCapacity, "committed bindings count toward the bound")

	engine.ForgetCertificateSession("client-1", binding)
	_, _, err = engine.AuthenticateWithPeer(context.Background(), secondClientID, "", "", PeerCertificate{LeafDER: []byte{2}})
	require.NoError(t, err)
}

func TestAuthEngineRejectedReconnectPreservesCommittedCertificate(t *testing.T) {
	certificateAuth := &certificateAuthenticatorStub{identity: CertificateIdentity{
		EntityID:     testCertificateEntityID,
		CredentialID: testCertificateCredentialID,
		Fingerprint:  "old-fingerprint",
	}}
	engine := NewAuthEngine(nil, nil, WithCertificateAuthentication(certificateAuth))
	_, _, err := engine.AuthenticateWithPeer(context.Background(), testCertificateClientID, "", "", PeerCertificate{LeafDER: []byte{1}})
	require.NoError(t, err)
	oldBinding, committed := engine.CommitCertificateAuthentication(testCertificateClientID)
	require.True(t, committed)

	certificateAuth.identity.CredentialID = testRotatedCredentialID
	certificateAuth.identity.Fingerprint = "rejected-fingerprint"
	_, _, err = engine.AuthenticateWithPeer(context.Background(), testCertificateClientID, "", "", PeerCertificate{LeafDER: []byte{2}})
	require.NoError(t, err)
	engine.Forget(testCertificateClientID)

	currentBinding, current := engine.CertificateSessionBinding(testCertificateClientID)
	require.True(t, current)
	require.Equal(t, oldBinding, currentBinding)
	require.Equal(t, testCertificateEntityID, engine.ExternalID(testCertificateClientID))
	require.True(t, engine.CanPublish(testCertificateClientID, testCertificateGlobalTopic))
	require.Equal(t, "old-fingerprint", certificateAuth.lastAuthorizedIdentity.Fingerprint)
	_, committed = engine.CommitCertificateAuthentication(testCertificateClientID)
	require.False(t, committed)
}

var errTenantMismatchForTest = errors.New("tenant mismatch")
