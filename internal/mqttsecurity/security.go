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
)

var ErrVerifiedClientCertificateRequired = errors.New("verified client certificate required")

// PeerIdentity contains only identity material from a certificate chain that
// the TLS stack has already verified.
type PeerIdentity struct {
	CommonName        string
	SHA256Fingerprint string
}

// Connection carries one verified peer identity.
type Connection struct {
	Peer PeerIdentity
}

type contextKey struct{}

// FromTLSState extracts the verified leaf certificate. PeerCertificates is not
// used because its presence alone does not prove chain verification.
func FromTLSState(state tls.ConnectionState) (Connection, error) {
	if len(state.VerifiedChains) == 0 || len(state.VerifiedChains[0]) == 0 {
		return Connection{}, ErrVerifiedClientCertificateRequired
	}
	return FromVerifiedCertificate(state.VerifiedChains[0][0])
}

// FromVerifiedCertificate creates a connection identity from a certificate
// already proven to be the verified leaf by the caller.
func FromVerifiedCertificate(cert *x509.Certificate) (Connection, error) {
	if cert == nil {
		return Connection{}, ErrVerifiedClientCertificateRequired
	}

	fingerprint := sha256.Sum256(cert.Raw)

	return Connection{
		Peer: PeerIdentity{
			CommonName:        cert.Subject.CommonName,
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

// Matches reports whether the external identity exactly equals the verified
// leaf certificate's subject common name.
func (c Connection) Matches(externalID string) bool {
	return externalID != "" && c.Peer.CommonName == externalID
}
