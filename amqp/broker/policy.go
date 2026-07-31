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
	// ConnectionPolicyLocal enables local-principal authentication. It selects
	// the credential store, not the capability: what a session may then do comes
	// from the authenticated principal's own role, so a listener cannot widen a
	// principal and a principal cannot be widened by choosing a listener.
	ConnectionPolicyLocal
)

// LocalPrincipalRole is the capability carried by an authenticated local
// principal. It is bound to the session at authentication and travels with it,
// because nothing binds a principal to a listener: a capability granted by a
// port would be granted to every principal able to reach that port.
type LocalPrincipalRole uint8

const (
	// LocalRolePublisher may publish only. It is the zero value, so an
	// unspecified role is the least privileged one.
	LocalRolePublisher LocalPrincipalRole = iota
	// LocalRoleService may additionally run consumers, subject to its own
	// subscribe ACL.
	LocalRoleService
)

// String returns the role name used in configuration and diagnostics.
func (r LocalPrincipalRole) String() string {
	if r == LocalRoleService {
		return "service"
	}
	return "publisher"
}

// PermitsConsumers reports whether the role may run the consumer lifecycle.
// Whether it may consume a particular queue is a separate ACL decision.
func (r LocalPrincipalRole) PermitsConsumers() bool {
	return r == LocalRoleService
}

// PropagatesOriginIdentity reports whether the role may state the external
// identity and origin protocol of a message it relays.
//
// A service relays messages it did not author, so it may name their true
// origin. A publisher may not: its publications are its own records, and a
// relayed origin would make one disagree with the principal that authenticated.
func (r LocalPrincipalRole) PropagatesOriginIdentity() bool {
	return r == LocalRoleService
}

// VerifiedPeerIdentity contains identity material from a TLS certificate chain
// that was successfully verified by the listener. Unverified peer certificate
// fields are deliberately never exposed to local authentication.
type VerifiedPeerIdentity struct {
	URISANs                []string
	CertificateFingerprint string
}

// LocalAuthentication is what a local-principal authenticator establishes about
// a peer. CertificateURI must name a URI SAN the listener already verified;
// the caller rejects any other value. CredentialFingerprint and
// PermissionsFingerprint are opaque, non-secret identifiers used to revoke
// sessions after a credential, role, or ACL change.
type LocalAuthentication struct {
	PrincipalID            string
	Role                   LocalPrincipalRole
	CredentialFingerprint  string
	PermissionsFingerprint string
	CertificateURI         string
}

// LocalPrincipalAuthenticator authenticates credentials against a verified TLS
// peer identity.
type LocalPrincipalAuthenticator interface {
	AuthenticateLocal(ctx context.Context, clientID, username, secret string, peer VerifiedPeerIdentity) (LocalAuthentication, bool, error)
}

// LocalPublishGrant reports which kind of publish permission authorized a
// publication. The two kinds carry different delivery contracts, so the grant
// decides how the publication is routed.
type LocalPublishGrant uint8

const (
	// LocalPublishGrantNone means no permission matched and the publish is denied.
	LocalPublishGrantNone LocalPublishGrant = iota
	// LocalPublishGrantExactTarget matched an exact routing key. It names a
	// protected durable stream, so the publication is appended and synced before
	// the publisher is confirmed.
	LocalPublishGrantExactTarget
	// LocalPublishGrantPrefix matched a routing-key prefix. It names no queue and
	// is checked against no durability contract, so the publication is routed as
	// an ordinary topic publish.
	LocalPublishGrantPrefix
)

// Allowed reports whether the grant authorizes the publication at all.
func (g LocalPublishGrant) Allowed() bool {
	return g != LocalPublishGrantNone
}

// LocalPrincipalAuthorizer makes exact AMQP publish and subscribe decisions for
// a fully authenticated local session. Implementations must validate both the
// bound session credential and the target against one current policy snapshot.
//
// CanPublishLocal returns the matching grant rather than a bare bool so the
// caller can route by permission kind without a second lookup, which would
// reopen the revocation race a single snapshot read closes.
type LocalPrincipalAuthorizer interface {
	CanPublishLocal(identity LocalSessionIdentity, exchange, routingKey string) LocalPublishGrant
	CanSubscribeLocal(identity LocalSessionIdentity, queue string) bool
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
// AMQP connection after authentication. Role is part of it, so a session's
// capability is fixed by who authenticated rather than by where they connected.
type LocalSessionIdentity struct {
	PrincipalID            string
	Role                   LocalPrincipalRole
	CredentialFingerprint  string
	PermissionsFingerprint string
	CertificateURI         string
	CertificateFingerprint string
}

// ConnectionPolicy is an immutable listener-scoped AMQP security policy.
// Construct policies with NewExternalConnectionPolicy or
// NewLocalConnectionPolicy and do not mutate them after serving.
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

// usesLocalPrincipalAuth reports whether connections under this policy
// authenticate against the local-principal store rather than the external auth
// engine. It is the authentication boundary; what the resulting session may do
// comes from the authenticated principal's role, not from here.
func (p *ConnectionPolicy) usesLocalPrincipalAuth() bool {
	return p != nil && p.mode == ConnectionPolicyLocal
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

// NewLocalConnectionPolicy constructs a fail-closed local-principal policy. It
// never invokes an external auth engine or blocking hook.
//
// The policy is trusted: the listener admits only mTLS peers whose verified
// certificate URI SAN matches a principal declared in FluxMQ's own
// configuration, so a reserved property arriving on it came from a first-party
// service rather than from a tenant or device.
//
// It grants no capability of its own. Publishing, consuming, and relaying an
// origin identity are decided by the authenticated principal's role and ACLs,
// so every local listener is equivalent and a principal cannot widen itself by
// choosing one.
func NewLocalConnectionPolicy(
	auth LocalPrincipalAuthenticator,
	authz LocalPrincipalAuthorizer,
	sessions LocalSessionValidator,
	maxMessageSize uint64,
) *ConnectionPolicy {
	return &ConnectionPolicy{
		mode:           ConnectionPolicyLocal,
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
