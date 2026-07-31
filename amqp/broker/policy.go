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
// credentialFingerprint and permissionsFingerprint are opaque, non-secret
// identifiers used to revoke sessions after credential or publish-ACL changes.
type LocalPrincipalAuthenticator interface {
	AuthenticateLocal(ctx context.Context, clientID, username, secret string, peer VerifiedPeerIdentity) (principalID, credentialFingerprint, permissionsFingerprint, certificateURI string, authenticated bool, err error)
}

// LocalPrincipalAuthorizer makes exact AMQP publish decisions for a fully
// authenticated local session. Implementations must validate both the bound
// session credential and the target against one current policy snapshot.
type LocalPrincipalAuthorizer interface {
	CanPublishLocal(identity LocalSessionIdentity, exchange, routingKey string) bool
}

// LocalSessionValidator checks whether the immutable credential, permissions,
// and certificate binding established during authentication is still active in
// the current local-principal snapshot. It closes the
// authenticate-before-register reload race; publish authorization remains a
// separate per-method check.
type LocalSessionValidator interface {
	IsSessionActive(identity LocalSessionIdentity) bool
}

// LocalSessionIdentity is the immutable security identity bound to a local
// AMQP connection after authentication.
type LocalSessionIdentity struct {
	PrincipalID            string
	CredentialFingerprint  string
	PermissionsFingerprint string
	CertificateURI         string
	CertificateFingerprint string
}

// ConnectionPolicy is an immutable listener-scoped AMQP security policy.
// Construct policies with NewExternalConnectionPolicy or
// NewLocalPublishOnlyConnectionPolicy and do not mutate them after serving.
type ConnectionPolicy struct {
	mode           ConnectionPolicyMode
	trusted        bool
	externalAuth   *corebroker.AuthEngine
	hooks          *corebroker.BlockingHookEngine
	localAuth      LocalPrincipalAuthenticator
	localAuthz     LocalPrincipalAuthorizer
	localSessions  LocalSessionValidator
	maxMessageSize uint64
}

// carriesReservedProperties reports whether connections under this policy may
// exchange broker-internal properties with the broker.
//
// This is deliberately a field of its own rather than a test on mode: trust is
// a statement about who authenticated the peer, while mode is a statement about
// which operations the peer may perform. A future listener may be trusted and
// still permit subscribe. A nil policy is the embedded-caller compatibility
// path and is never trusted.
func (p *ConnectionPolicy) carriesReservedProperties() bool {
	return p != nil && p.trusted
}

// propagatesOriginIdentity reports whether a publisher under this policy may
// state the external identity and origin protocol of a message it relays,
// rather than having its own authenticated identity stamped on it.
//
// Only a trusted service may: for an externally authenticated client, naming
// another principal is impersonation. A local principal may not either. Its
// identity is fixed by FluxMQ's configuration and its publications are audit
// records, so a relayed origin would make the record disagree with the peer
// that actually authenticated.
func (p *ConnectionPolicy) propagatesOriginIdentity() bool {
	return p.carriesReservedProperties() && p.mode != ConnectionPolicyLocalPublishOnly
}

// NewExternalConnectionPolicy constructs a policy using only the existing
// external auth engine and blocking hooks.
func NewExternalConnectionPolicy(auth *corebroker.AuthEngine, hooks *corebroker.BlockingHookEngine, maxMessageSize uint64) *ConnectionPolicy {
	return &ConnectionPolicy{
		mode:           ConnectionPolicyExternal,
		trusted:        false,
		externalAuth:   auth,
		hooks:          hooks,
		maxMessageSize: maxMessageSize,
	}
}

// NewLocalPublishOnlyConnectionPolicy constructs a fail-closed local-principal
// policy. It never invokes an external auth engine or blocking hook.
//
// The policy is trusted: the listener admits only mTLS peers whose verified
// certificate URI SAN matches a principal declared in FluxMQ's own
// configuration, so a reserved property arriving on it came from a first-party
// service rather than from a tenant or device.
func NewLocalPublishOnlyConnectionPolicy(
	auth LocalPrincipalAuthenticator,
	authz LocalPrincipalAuthorizer,
	sessions LocalSessionValidator,
	maxMessageSize uint64,
) *ConnectionPolicy {
	return &ConnectionPolicy{
		mode:           ConnectionPolicyLocalPublishOnly,
		trusted:        true,
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
