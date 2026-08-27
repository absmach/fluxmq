// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package mqttsecurity

import (
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"testing"

	"github.com/stretchr/testify/require"
)

const testEntityA = "entity-a"

func TestFromTLSStateRequiresVerifiedChain(t *testing.T) {
	cert := &x509.Certificate{Subject: pkix.Name{CommonName: testEntityA}}
	_, err := FromTLSState(tls.ConnectionState{PeerCertificates: []*x509.Certificate{cert}})
	require.ErrorIs(t, err, ErrVerifiedClientCertificateRequired)
}

func TestCertificateIdentityMatch(t *testing.T) {
	cert := &x509.Certificate{
		Raw:     []byte("verified-leaf"),
		Subject: pkix.Name{CommonName: testEntityA},
	}

	tests := []struct {
		name     string
		identity string
		want     bool
	}{
		{
			name:     "common name exact match",
			identity: testEntityA,
			want:     true,
		},
		{
			name:     "common name rejects another identity",
			identity: "entity-b",
		},
		{
			name: "empty identity fails closed",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			connection, err := FromVerifiedCertificate(cert)
			require.NoError(t, err)
			require.Equal(t, tc.want, connection.Matches(tc.identity))
		})
	}
}
