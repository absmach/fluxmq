// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	amqpbroker "github.com/absmach/fluxmq/amqp/broker"
	"github.com/absmach/fluxmq/broker/localauth"
	"github.com/absmach/fluxmq/config"
)

// BenchmarkLocalAMQPPolicyCanPublish measures the per-publication authorization
// cost of the local-principal listener, which decodes the session fingerprints
// bound at authentication time.
func BenchmarkLocalAMQPPolicyCanPublish(b *testing.B) {
	dir := b.TempDir()
	secretPath := filepath.Join(dir, "current")
	if err := os.WriteFile(secretPath, []byte(testLocalSecret), 0o600); err != nil {
		b.Fatal(err)
	}
	store, err := localauth.New([]config.LocalPrincipalConfig{{
		Name:              testLocalPrincipal,
		CertificateURISAN: testLocalSAN,
		CurrentSecretFile: secretPath,
		Permissions: config.LocalPermissionsConfig{
			Publish: []config.LocalPublishPermission{{RoutingKey: testAuditQueue}},
		},
	}})
	if err != nil {
		b.Fatal(err)
	}
	adapter := &localAMQPPolicy{store: store}
	principalID, credentialFingerprint, permissionsFingerprint, certificateURI, ok, err := adapter.AuthenticateLocal(
		context.Background(),
		"amqp091:client",
		testLocalPrincipal,
		testLocalSecret,
		amqpbroker.VerifiedPeerIdentity{URISANs: []string{testLocalSAN}},
	)
	if err != nil || !ok {
		b.Fatalf("AuthenticateLocal() authenticated=%v error=%v", ok, err)
	}
	identity := amqpbroker.LocalSessionIdentity{
		PrincipalID:            principalID,
		CredentialFingerprint:  credentialFingerprint,
		PermissionsFingerprint: permissionsFingerprint,
		CertificateURI:         certificateURI,
	}

	b.ReportAllocs()
	for b.Loop() {
		if !adapter.CanPublishLocal(identity, "", testAuditQueue) {
			b.Fatal("configured publish target was denied")
		}
	}
}
