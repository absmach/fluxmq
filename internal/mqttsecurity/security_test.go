// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package mqttsecurity

import (
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"net/url"
	"testing"

	"github.com/stretchr/testify/require"
)

const testEntityA = "entity-a"

func TestFromTLSStateRequiresVerifiedChain(t *testing.T) {
	cert := &x509.Certificate{Subject: pkix.Name{CommonName: testEntityA}}
	_, err := FromTLSState(tls.ConnectionState{PeerCertificates: []*x509.Certificate{cert}}, Policy{})
	require.ErrorIs(t, err, ErrVerifiedClientCertificateRequired)
}

func TestCertificateIdentityMatch(t *testing.T) {
	entityURI, err := url.Parse("urn:atom:entity:" + testEntityA)
	require.NoError(t, err)
	cert := &x509.Certificate{
		Raw:     []byte("verified-leaf"),
		Subject: pkix.Name{CommonName: "fun_" + testEntityA},
		URIs:    []*url.URL{entityURI},
	}

	tests := []struct {
		name     string
		policy   Policy
		identity string
		want     bool
	}{
		{
			name:     "common name exact template",
			policy:   Policy{IdentitySource: "common_name", IdentityTemplate: "fun_{external_id}"},
			identity: testEntityA,
			want:     true,
		},
		{
			name:     "common name rejects another identity",
			policy:   Policy{IdentitySource: "common_name", IdentityTemplate: "fun_{external_id}"},
			identity: "entity-b",
		},
		{
			name:     "URI SAN exact template",
			policy:   Policy{IdentitySource: "uri_san", IdentityTemplate: "urn:atom:entity:{external_id}"},
			identity: testEntityA,
			want:     true,
		},
		{
			name:     "unknown source fails closed",
			policy:   Policy{IdentitySource: "subject", IdentityTemplate: "{external_id}"},
			identity: testEntityA,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			connection, err := FromVerifiedCertificate(cert, tc.policy)
			require.NoError(t, err)
			require.Equal(t, tc.want, connection.Matches(tc.identity))
		})
	}
}
