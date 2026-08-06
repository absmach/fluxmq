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
		EntityID:     "8a0a5c59-4ea8-4fc1-badb-f96cf739b224",
		TenantID:     "d204f7df-8293-4194-963b-a47a65bc8f04",
		CredentialID: "ca49950c-3ed2-41b4-a319-896085285686",
		Fingerprint:  "abc123",
		ExpiresAt:    time.Now().Add(time.Hour),
	}}
	authorizer := &stubAuthorizer{allow: true}
	engine := NewAuthEngine(nil, authorizer, WithCertificateAuthentication(certificateAuth))

	ok, entityID, err := engine.AuthenticateWithPeer(
		context.Background(),
		"mqtt-client",
		"ignored",
		"ignored",
		PeerCertificate{LeafDER: []byte{1, 2, 3}},
	)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, certificateAuth.identity.EntityID, entityID)
	require.True(t, engine.CanPublish("mqtt-client", "m/d204f7df-8293-4194-963b-a47a65bc8f04/c/channel"))
	require.Equal(t, 1, certificateAuth.authorizeCalls)
	require.Equal(t, certificateAuth.identity.EntityID, authorizer.receivedClientID)
}

func TestAuthEngineCertificateDenialStopsNormalAuthorization(t *testing.T) {
	certificateAuth := &certificateAuthenticatorStub{
		identity: CertificateIdentity{
			EntityID:     "8a0a5c59-4ea8-4fc1-badb-f96cf739b224",
			CredentialID: "ca49950c-3ed2-41b4-a319-896085285686",
			Fingerprint:  "abc123",
		},
		authorizeErr: ErrTenantMismatchForTest,
	}
	authorizer := &stubAuthorizer{allow: true}
	engine := NewAuthEngine(nil, authorizer, WithCertificateAuthentication(certificateAuth))
	_, _, err := engine.AuthenticateWithPeer(context.Background(), "mqtt-client", "", "", PeerCertificate{LeafDER: []byte{1}})
	require.NoError(t, err)

	require.False(t, engine.CanSubscribe("mqtt-client", "m/other/#"))
	require.Empty(t, authorizer.receivedClientID, "normal authorization must not run after tenant denial")
}

func TestAuthEngineNormalAuthorizationCanDenyCertificateIdentity(t *testing.T) {
	certificateAuth := &certificateAuthenticatorStub{identity: CertificateIdentity{
		EntityID:     "8a0a5c59-4ea8-4fc1-badb-f96cf739b224",
		CredentialID: "ca49950c-3ed2-41b4-a319-896085285686",
		Fingerprint:  "abc123",
	}}
	authorizer := &stubAuthorizer{allow: false}
	engine := NewAuthEngine(nil, authorizer, WithCertificateAuthentication(certificateAuth))
	_, _, err := engine.AuthenticateWithPeer(context.Background(), "mqtt-client", "", "", PeerCertificate{LeafDER: []byte{1}})
	require.NoError(t, err)

	require.False(t, engine.CanPublish("mqtt-client", "$SYS/global/status"))
	require.Equal(t, certificateAuth.identity.EntityID, authorizer.receivedClientID)
}

func TestAuthEngineCertificateIdentityCannotBeOverriddenByHook(t *testing.T) {
	certificateAuth := &certificateAuthenticatorStub{identity: CertificateIdentity{
		EntityID:     "8a0a5c59-4ea8-4fc1-badb-f96cf739b224",
		CredentialID: "ca49950c-3ed2-41b4-a319-896085285686",
		Fingerprint:  "abc123",
	}}
	engine := NewAuthEngine(nil, nil, WithCertificateAuthentication(certificateAuth))
	_, _, err := engine.AuthenticateWithPeer(context.Background(), "mqtt-client", "", "", PeerCertificate{LeafDER: []byte{1}})
	require.NoError(t, err)

	engine.SetExternalID("mqtt-client", "hook-chosen-identity")
	require.Equal(t, certificateAuth.identity.EntityID, engine.ExternalID("mqtt-client"))
	engine.Forget("mqtt-client")
	require.Zero(t, engine.CertificateSessionCount())
}

func TestAuthEngineCertificateRotationRebindsClientBeforeNextOperation(t *testing.T) {
	certificateAuth := &certificateAuthenticatorStub{identity: CertificateIdentity{
		EntityID:     "8a0a5c59-4ea8-4fc1-badb-f96cf739b224",
		CredentialID: "ca49950c-3ed2-41b4-a319-896085285686",
		Fingerprint:  "old-fingerprint",
	}}
	engine := NewAuthEngine(nil, nil, WithCertificateAuthentication(certificateAuth))
	_, _, err := engine.AuthenticateWithPeer(context.Background(), "mqtt-client", "", "", PeerCertificate{LeafDER: []byte{1}})
	require.NoError(t, err)

	certificateAuth.identity.CredentialID = "05119e28-6260-4a06-8742-f925bcfdccd4"
	certificateAuth.identity.Fingerprint = "new-fingerprint"
	_, _, err = engine.AuthenticateWithPeer(context.Background(), "mqtt-client", "", "", PeerCertificate{LeafDER: []byte{2}})
	require.NoError(t, err)
	require.True(t, engine.CanPublish("mqtt-client", "$SYS/global/status"))
	require.Equal(t, "new-fingerprint", certificateAuth.lastAuthorizedIdentity.Fingerprint)
	require.Equal(t, 1, engine.CertificateSessionCount())
}

func TestAuthEngineRejectsCrossEntityCertificateClientIDTakeover(t *testing.T) {
	certificateAuth := &certificateAuthenticatorStub{identity: CertificateIdentity{
		EntityID:     "8a0a5c59-4ea8-4fc1-badb-f96cf739b224",
		CredentialID: "ca49950c-3ed2-41b4-a319-896085285686",
		Fingerprint:  "first-fingerprint",
	}}
	engine := NewAuthEngine(nil, nil, WithCertificateAuthentication(certificateAuth))
	_, _, err := engine.AuthenticateWithPeer(context.Background(), "mqtt-client", "", "", PeerCertificate{LeafDER: []byte{1}})
	require.NoError(t, err)

	certificateAuth.identity.EntityID = "ac47c9fd-1d4a-4270-bb11-ab6476a0bd3a"
	certificateAuth.identity.CredentialID = "05119e28-6260-4a06-8742-f925bcfdccd4"
	certificateAuth.identity.Fingerprint = "second-fingerprint"
	_, _, err = engine.AuthenticateWithPeer(context.Background(), "mqtt-client", "", "", PeerCertificate{LeafDER: []byte{2}})
	require.ErrorIs(t, err, ErrCertificateClientIdentityConflict)
	require.Equal(t, "8a0a5c59-4ea8-4fc1-badb-f96cf739b224", engine.ExternalID("mqtt-client"))
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
		EntityID:     "8a0a5c59-4ea8-4fc1-badb-f96cf739b224",
		CredentialID: "ca49950c-3ed2-41b4-a319-896085285686",
		Fingerprint:  "abc123",
	}}
	authn := &stubAuthenticator{result: &AuthnResult{Authenticated: false}}
	engine := NewAuthEngine(authn, nil, WithCertificateAuthentication(certificateAuth))
	_, _, err := engine.AuthenticateWithPeer(context.Background(), "mqtt-client", "", "", PeerCertificate{LeafDER: []byte{1}})
	require.NoError(t, err)

	ok, _, err := engine.Authenticate("mqtt-client", "attacker", "wrong")
	require.NoError(t, err)
	require.False(t, ok)
	require.Equal(t, 1, engine.CertificateSessionCount())
	require.Equal(t, certificateAuth.identity.EntityID, engine.ExternalID("mqtt-client"))
	require.True(t, engine.CanPublish("mqtt-client", "$SYS/global/status"))
	require.Equal(t, 1, certificateAuth.authorizeCalls)
}

var ErrTenantMismatchForTest = errors.New("tenant mismatch")
