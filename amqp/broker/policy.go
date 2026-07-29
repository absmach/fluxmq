// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package broker

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"

	corebroker "github.com/absmach/fluxmq/broker"
)

// ConnectionPolicyMode selects the authentication and authorization boundary
// for an AMQP 0.9.1 listener.
type ConnectionPolicyMode uint8

const (
	// ConnectionPolicyExternal preserves the existing remote auth-callout and
	// blocking-hook behavior.
	ConnectionPolicyExternal ConnectionPolicyMode = iota
	// ConnectionPolicyLocalPublishOnly enables local-principal authentication
	// and restricts the connection to publisher lifecycle operations.
	ConnectionPolicyLocalPublishOnly
)

// VerifiedPeerIdentity contains identity material from a TLS certificate chain
// that was successfully verified by the listener. Unverified peer certificate
// fields are deliberately never exposed to local authentication.
type VerifiedPeerIdentity struct {
	URISANs                []string
	CertificateFingerprint string
}

// LocalPrincipalAuthenticator authenticates credentials against a verified TLS
// peer identity. certificateURI must name the URI SAN selected from peer.URISANs.
// credentialFingerprint is an opaque, non-secret identifier used to revoke
// sessions after credential rotation.
type LocalPrincipalAuthenticator interface {
	AuthenticateLocal(ctx context.Context, clientID, username, secret string, peer VerifiedPeerIdentity) (principalID, credentialFingerprint, certificateURI string, authenticated bool, err error)
}

// LocalPrincipalAuthorizer makes exact AMQP publish decisions for a fully
// authenticated local session. Implementations must validate both the bound
// session credential and the target against one current policy snapshot.
type LocalPrincipalAuthorizer interface {
	CanPublishLocal(identity LocalSessionIdentity, exchange, routingKey string) bool
}

// LocalSessionValidator checks whether the immutable credential and certificate
// binding established during authentication is still active in the current
// local-principal snapshot. It closes the authenticate-before-register reload
// race; publish authorization remains a separate per-method check.
type LocalSessionValidator interface {
	IsSessionActive(identity LocalSessionIdentity) bool
}

// LocalSessionIdentity is the immutable security identity bound to a local
// AMQP connection after authentication.
type LocalSessionIdentity struct {
	PrincipalID            string
	CredentialFingerprint  string
	CertificateURI         string
	CertificateFingerprint string
}

// ConnectionPolicy is an immutable listener-scoped AMQP security policy.
// Construct policies with NewExternalConnectionPolicy or
// NewLocalPublishOnlyConnectionPolicy and do not mutate them after serving.
type ConnectionPolicy struct {
	mode           ConnectionPolicyMode
	externalAuth   *corebroker.AuthEngine
	hooks          *corebroker.BlockingHookEngine
	localAuth      LocalPrincipalAuthenticator
	localAuthz     LocalPrincipalAuthorizer
	localSessions  LocalSessionValidator
	maxMessageSize uint64
}

// NewExternalConnectionPolicy constructs a policy using only the existing
// external auth engine and blocking hooks.
func NewExternalConnectionPolicy(auth *corebroker.AuthEngine, hooks *corebroker.BlockingHookEngine, maxMessageSize uint64) *ConnectionPolicy {
	return &ConnectionPolicy{
		mode:           ConnectionPolicyExternal,
		externalAuth:   auth,
		hooks:          hooks,
		maxMessageSize: maxMessageSize,
	}
}

// NewLocalPublishOnlyConnectionPolicy constructs a fail-closed local-principal
// policy. It never invokes an external auth engine or blocking hook.
func NewLocalPublishOnlyConnectionPolicy(
	auth LocalPrincipalAuthenticator,
	authz LocalPrincipalAuthorizer,
	sessions LocalSessionValidator,
	maxMessageSize uint64,
) *ConnectionPolicy {
	return &ConnectionPolicy{
		mode:           ConnectionPolicyLocalPublishOnly,
		localAuth:      auth,
		localAuthz:     authz,
		localSessions:  sessions,
		maxMessageSize: maxMessageSize,
	}
}

func verifiedPeerIdentity(conn *tls.Conn) VerifiedPeerIdentity {
	state := conn.ConnectionState()
	if len(state.VerifiedChains) == 0 || len(state.VerifiedChains[0]) == 0 {
		return VerifiedPeerIdentity{}
	}

	leaf := state.VerifiedChains[0][0]
	uriSANs := make([]string, 0, len(leaf.URIs))
	for _, uri := range leaf.URIs {
		uriSANs = append(uriSANs, uri.String())
	}
	fingerprint := sha256.Sum256(leaf.Raw)
	return VerifiedPeerIdentity{
		URISANs:                uriSANs,
		CertificateFingerprint: hex.EncodeToString(fingerprint[:]),
	}
}

func containsURISAN(peer VerifiedPeerIdentity, uri string) bool {
	for _, candidate := range peer.URISANs {
		if candidate == uri {
			return true
		}
	}
	return false
}
