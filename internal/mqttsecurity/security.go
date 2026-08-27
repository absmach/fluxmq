// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

// Package mqttsecurity carries verified MQTT transport identity from a TLS
// listener to CONNECT authentication without widening the public MQTT
// connection interface.
package mqttsecurity

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"errors"
	"strings"
)

const externalIDPlaceholder = "{external_id}"

var ErrVerifiedClientCertificateRequired = errors.New("verified client certificate required")

// Policy defines how an externally authenticated identity must appear in the
// verified client certificate.
type Policy struct {
	IdentitySource   string
	IdentityTemplate string
}

// PeerIdentity contains only identity material from a certificate chain that
// the TLS stack has already verified.
type PeerIdentity struct {
	CommonName        string
	URISANs           []string
	SHA256Fingerprint string
}

// Connection binds the listener policy to one verified peer.
type Connection struct {
	Policy Policy
	Peer   PeerIdentity
}

type contextKey struct{}

// FromTLSState extracts the verified leaf certificate. PeerCertificates is not
// used because its presence alone does not prove chain verification.
func FromTLSState(state tls.ConnectionState, policy Policy) (Connection, error) {
	if len(state.VerifiedChains) == 0 || len(state.VerifiedChains[0]) == 0 {
		return Connection{}, ErrVerifiedClientCertificateRequired
	}
	return FromVerifiedCertificate(state.VerifiedChains[0][0], policy)
}

// FromVerifiedCertificate creates a connection identity from a certificate
// already proven to be the verified leaf by the caller.
func FromVerifiedCertificate(cert *x509.Certificate, policy Policy) (Connection, error) {
	if cert == nil {
		return Connection{}, ErrVerifiedClientCertificateRequired
	}

	fingerprint := sha256.Sum256(cert.Raw)
	uriSANs := make([]string, 0, len(cert.URIs))
	for _, uri := range cert.URIs {
		if uri != nil {
			uriSANs = append(uriSANs, uri.String())
		}
	}

	return Connection{
		Policy: policy,
		Peer: PeerIdentity{
			CommonName:        cert.Subject.CommonName,
			URISANs:           uriSANs,
			SHA256Fingerprint: hex.EncodeToString(fingerprint[:]),
		},
	}, nil
}

// WithConnection attaches immutable verified-peer identity to one connection
// context.
func WithConnection(ctx context.Context, connection Connection) context.Context {
	return context.WithValue(ctx, contextKey{}, connection)
}

// FromContext returns the verified-peer policy for an MQTT mTLS connection.
func FromContext(ctx context.Context) (Connection, bool) {
	connection, ok := ctx.Value(contextKey{}).(Connection)
	return connection, ok
}

// Matches reports whether the external identity is represented exactly by the
// configured certificate field and template.
func (c Connection) Matches(externalID string) bool {
	if externalID == "" || strings.Count(c.Policy.IdentityTemplate, externalIDPlaceholder) != 1 {
		return false
	}
	expected := strings.Replace(c.Policy.IdentityTemplate, externalIDPlaceholder, externalID, 1)

	switch c.Policy.IdentitySource {
	case "common_name":
		return c.Peer.CommonName == expected
	case "uri_san":
		for _, uri := range c.Peer.URISANs {
			if uri == expected {
				return true
			}
		}
		return false
	default:
		return false
	}
}
