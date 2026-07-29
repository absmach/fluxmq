// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	amqpbroker "github.com/absmach/fluxmq/amqp/broker"
	"github.com/absmach/fluxmq/broker/localauth"
	"github.com/absmach/fluxmq/config"
	queuepkg "github.com/absmach/fluxmq/queue"
	queueStorage "github.com/absmach/fluxmq/queue/storage"
	memoryLog "github.com/absmach/fluxmq/queue/storage/memory/log"
	queueTypes "github.com/absmach/fluxmq/queue/types"
)

const (
	testLocalPrincipal = "atom-audit-publisher"
	testLocalSAN       = "spiffe://absmach/atom/audit-publisher"
	testLocalSecret    = "0123456789abcdef0123456789abcdef"
	testNextSecret     = "abcdef0123456789abcdef0123456789"
	testAuditQueue     = "atom-audit"
)

func TestLocalAMQPPolicyAdapter(t *testing.T) {
	dir := t.TempDir()
	currentPath := filepath.Join(dir, "current")
	if err := os.WriteFile(currentPath, []byte(testLocalSecret+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	principalConfig := config.LocalPrincipalConfig{
		Name:              testLocalPrincipal,
		CertificateURISAN: testLocalSAN,
		CurrentSecretFile: currentPath,
		Permissions: config.LocalPermissionsConfig{
			Publish: []config.LocalPublishPermission{{Exchange: "", RoutingKey: testAuditQueue}},
		},
	}
	store, err := localauth.New([]config.LocalPrincipalConfig{principalConfig})
	if err != nil {
		t.Fatal(err)
	}
	adapter := &localAMQPPolicy{store: store}
	peer := amqpbroker.VerifiedPeerIdentity{
		URISANs:                []string{"spiffe://unrelated/service", testLocalSAN},
		CertificateFingerprint: strings.Repeat("a", 64),
	}

	principalID, credentialFingerprint, permissionsFingerprint, certificateURI, authenticated, err := adapter.AuthenticateLocal(
		context.Background(), "amqp091:client", testLocalPrincipal, testLocalSecret, peer,
	)
	if err != nil {
		t.Fatalf("AuthenticateLocal() error = %v", err)
	}
	if !authenticated || principalID != testLocalPrincipal || certificateURI != testLocalSAN {
		t.Fatalf("unexpected authentication result: principal=%q certificate_uri=%q authenticated=%v", principalID, certificateURI, authenticated)
	}
	decodedCredentialFingerprint, err := hex.DecodeString(credentialFingerprint)
	if err != nil || len(decodedCredentialFingerprint) != len(localauth.CredentialFingerprint{}) {
		t.Fatalf("invalid credential fingerprint %q: %v", credentialFingerprint, err)
	}
	decodedPermissionsFingerprint, err := hex.DecodeString(permissionsFingerprint)
	if err != nil || len(decodedPermissionsFingerprint) != len(localauth.PermissionsFingerprint{}) {
		t.Fatalf("invalid permissions fingerprint %q: %v", permissionsFingerprint, err)
	}
	identity := amqpbroker.LocalSessionIdentity{
		PrincipalID:            principalID,
		CredentialFingerprint:  credentialFingerprint,
		PermissionsFingerprint: permissionsFingerprint,
		CertificateURI:         certificateURI,
	}
	if !adapter.CanPublishLocal(identity, "", testAuditQueue) {
		t.Fatal("exact configured publish target was denied")
	}
	if adapter.CanPublishLocal(identity, "events", testAuditQueue) || adapter.CanPublishLocal(identity, "", "atom-audit.other") {
		t.Fatal("non-exact publish target was allowed")
	}

	if !adapter.IsSessionActive(identity) {
		t.Fatal("freshly authenticated session was not active")
	}

	nextPath := filepath.Join(dir, "next")
	if err := os.WriteFile(nextPath, []byte(testNextSecret), 0o600); err != nil {
		t.Fatal(err)
	}
	principalConfig.CurrentSecretFile = nextPath
	changed, err := store.Reload([]config.LocalPrincipalConfig{principalConfig})
	if err != nil {
		t.Fatalf("Reload() error = %v", err)
	}
	if !changed {
		t.Fatal("credential rotation was reported as unchanged")
	}
	if adapter.IsSessionActive(identity) {
		t.Fatal("session authenticated with the removed secret remains active")
	}
	if adapter.CanPublishLocal(identity, "", testAuditQueue) {
		t.Fatal("session authenticated before reload can publish with the removed secret")
	}
}

func TestLocalAMQPPolicyAdapterFailsClosed(t *testing.T) {
	adapter := &localAMQPPolicy{}
	if _, _, _, _, authenticated, err := adapter.AuthenticateLocal(context.Background(), "client", "user", "secret", amqpbroker.VerifiedPeerIdentity{}); err != nil || authenticated {
		t.Fatalf("nil local store must fail closed: authenticated=%v err=%v", authenticated, err)
	}
	if adapter.CanPublishLocal(amqpbroker.LocalSessionIdentity{PrincipalID: "principal"}, "", testAuditQueue) {
		t.Fatal("nil local store authorized a publication")
	}
	if adapter.IsSessionActive(amqpbroker.LocalSessionIdentity{}) {
		t.Fatal("nil local store reported an active session")
	}
}

func TestLocalAMQPPolicyAdapterRevokesChangedPermissions(t *testing.T) {
	dir := t.TempDir()
	currentPath := filepath.Join(dir, "current")
	if err := os.WriteFile(currentPath, []byte(testLocalSecret), 0o600); err != nil {
		t.Fatal(err)
	}
	principalConfig := config.LocalPrincipalConfig{
		Name:              testLocalPrincipal,
		CertificateURISAN: testLocalSAN,
		CurrentSecretFile: currentPath,
		Permissions: config.LocalPermissionsConfig{
			Publish: []config.LocalPublishPermission{{RoutingKey: testAuditQueue}},
		},
	}
	store, err := localauth.New([]config.LocalPrincipalConfig{principalConfig})
	if err != nil {
		t.Fatal(err)
	}
	adapter := &localAMQPPolicy{store: store}
	principalID, credentialFingerprint, permissionsFingerprint, certificateURI, authenticated, err := adapter.AuthenticateLocal(
		context.Background(),
		"amqp091:client",
		testLocalPrincipal,
		testLocalSecret,
		amqpbroker.VerifiedPeerIdentity{URISANs: []string{testLocalSAN}},
	)
	if err != nil || !authenticated {
		t.Fatalf("AuthenticateLocal() authenticated=%v error=%v", authenticated, err)
	}
	identity := amqpbroker.LocalSessionIdentity{
		PrincipalID:            principalID,
		CredentialFingerprint:  credentialFingerprint,
		PermissionsFingerprint: permissionsFingerprint,
		CertificateURI:         certificateURI,
	}
	if !adapter.IsSessionActive(identity) {
		t.Fatal("freshly authenticated session was not active")
	}

	principalConfig.Permissions.Publish[0].RoutingKey = "atom-audit-v2"
	changed, err := store.Reload([]config.LocalPrincipalConfig{principalConfig})
	if err != nil {
		t.Fatalf("Reload() error = %v", err)
	}
	if !changed {
		t.Fatal("publish ACL replacement was reported as unchanged")
	}
	if adapter.IsSessionActive(identity) {
		t.Fatal("session authenticated against the replaced publish ACL remains active")
	}
	if adapter.CanPublishLocal(identity, "", "atom-audit-v2") {
		t.Fatal("session authenticated against the old ACL used the replacement ACL")
	}
}

type queueStoreWithoutDurableSync struct {
	queueStorage.QueueStore
}

func TestValidateLocalPrincipalPublishTargets(t *testing.T) {
	principal := config.LocalPrincipalConfig{
		Name: testLocalPrincipal,
		Permissions: config.LocalPermissionsConfig{
			Publish: []config.LocalPublishPermission{{RoutingKey: testAuditQueue}},
		},
	}
	expected := queueTypes.DefaultQueueConfig(testAuditQueue, "$queue/atom-audit/#")
	expected.Reserved = true
	expected.Type = queueTypes.QueueTypeStream
	expected.Retention = queueTypes.RetentionPolicy{
		RetentionTime:     30 * 24 * time.Hour,
		RetentionBytes:    10 * 1024 * 1024 * 1024,
		RetentionMessages: 0,
	}
	expected.MaxMessageSize = 1024 * 1024

	tests := []struct {
		name               string
		configured         []queueTypes.QueueConfig
		mutatePersisted    func(*queueTypes.QueueConfig)
		omitPersisted      bool
		withoutDurableSync bool
		wantError          string
	}{
		{name: "valid", configured: []queueTypes.QueueConfig{expected}},
		{
			name:               "durable sync unsupported",
			configured:         []queueTypes.QueueConfig{expected},
			withoutDurableSync: true,
			wantError:          "durable sync support",
		},
		{
			name:       "target absent from configured queues",
			configured: nil,
			wantError:  "has no matching queues entry",
		},
		{
			name:          "target absent from persisted queues",
			configured:    []queueTypes.QueueConfig{expected},
			omitPersisted: true,
			wantError:     "load persisted local principal publish target",
		},
		{
			name:            "classic queue",
			configured:      []queueTypes.QueueConfig{expected},
			mutatePersisted: func(actual *queueTypes.QueueConfig) { actual.Type = queueTypes.QueueTypeClassic },
			wantError:       "must be a stream",
		},
		{
			name:            "ephemeral queue",
			configured:      []queueTypes.QueueConfig{expected},
			mutatePersisted: func(actual *queueTypes.QueueConfig) { actual.Durable = false },
			wantError:       "must be durable",
		},
		{
			name:            "mutable queue",
			configured:      []queueTypes.QueueConfig{expected},
			mutatePersisted: func(actual *queueTypes.QueueConfig) { actual.Reserved = false },
			wantError:       "must be reserved",
		},
		{
			name:       "replicated queue",
			configured: []queueTypes.QueueConfig{expected},
			mutatePersisted: func(actual *queueTypes.QueueConfig) {
				actual.Replication.Enabled = true
			},
			wantError: "must not enable replication",
		},
		{
			name:       "stale retention age",
			configured: []queueTypes.QueueConfig{expected},
			mutatePersisted: func(actual *queueTypes.QueueConfig) {
				actual.Retention.RetentionTime = 24 * time.Hour
			},
			wantError: "retention.max_age",
		},
		{
			name:       "stale retention bytes",
			configured: []queueTypes.QueueConfig{expected},
			mutatePersisted: func(actual *queueTypes.QueueConfig) {
				actual.Retention.RetentionBytes--
			},
			wantError: "retention.max_length_bytes",
		},
		{
			name:       "stale retention messages",
			configured: []queueTypes.QueueConfig{expected},
			mutatePersisted: func(actual *queueTypes.QueueConfig) {
				actual.Retention.RetentionMessages = 1
			},
			wantError: "retention.max_length_messages",
		},
		{
			name:       "stale maximum message size",
			configured: []queueTypes.QueueConfig{expected},
			mutatePersisted: func(actual *queueTypes.QueueConfig) {
				actual.MaxMessageSize++
			},
			wantError: "limits.max_message_size",
		},
		{
			name:       "stale message TTL",
			configured: []queueTypes.QueueConfig{expected},
			mutatePersisted: func(actual *queueTypes.QueueConfig) {
				actual.MessageTTL++
			},
			wantError: "limits.message_ttl",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			store := memoryLog.New()
			if !tc.omitPersisted {
				if err := store.CreateQueue(context.Background(), expected); err != nil {
					t.Fatalf("CreateQueue() error = %v", err)
				}
				if tc.mutatePersisted != nil {
					actual := expected
					tc.mutatePersisted(&actual)
					if err := store.UpdateQueue(context.Background(), actual); err != nil {
						t.Fatalf("UpdateQueue() error = %v", err)
					}
				}
			}

			var queueStore queueStorage.QueueStore = store
			if tc.withoutDurableSync {
				queueStore = queueStoreWithoutDurableSync{QueueStore: store}
			}
			err := validateLocalPrincipalPublishTargets(
				context.Background(),
				[]config.LocalPrincipalConfig{principal},
				tc.configured,
				queueStore,
			)
			if tc.wantError == "" {
				if err != nil {
					t.Fatalf("validateLocalPrincipalPublishTargets() error = %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.wantError) {
				t.Fatalf("validateLocalPrincipalPublishTargets() error = %v, want substring %q", err, tc.wantError)
			}
		})
	}
}

func TestValidateLocalPrincipalPublishTargetsSortsTargetErrors(t *testing.T) {
	principal := config.LocalPrincipalConfig{
		Permissions: config.LocalPermissionsConfig{
			Publish: []config.LocalPublishPermission{
				{RoutingKey: "z-target"},
				{RoutingKey: "a-target"},
			},
		},
	}
	err := validateLocalPrincipalPublishTargets(
		context.Background(),
		[]config.LocalPrincipalConfig{principal},
		nil,
		memoryLog.New(),
	)
	if err == nil || !strings.Contains(err.Error(), `"a-target"`) {
		t.Fatalf("validateLocalPrincipalPublishTargets() error = %v, want first sorted target", err)
	}
}

func TestValidateLocalPrincipalPublishTargetsDisabled(t *testing.T) {
	if err := validateLocalPrincipalPublishTargets(context.Background(), nil, nil, nil); err != nil {
		t.Fatalf("disabled local targets require no queue store: %v", err)
	}
}

func TestReloadLocalPrincipalsRejectsInvalidTargetBeforeSwap(t *testing.T) {
	secretPath := filepath.Join(t.TempDir(), "current")
	if err := os.WriteFile(secretPath, []byte(testLocalSecret), 0o600); err != nil {
		t.Fatal(err)
	}
	principal := config.LocalPrincipalConfig{
		Name:              testLocalPrincipal,
		CertificateURISAN: testLocalSAN,
		CurrentSecretFile: secretPath,
		Permissions: config.LocalPermissionsConfig{
			Publish: []config.LocalPublishPermission{{RoutingKey: testAuditQueue}},
		},
	}
	localStore, err := localauth.New([]config.LocalPrincipalConfig{principal})
	if err != nil {
		t.Fatalf("localauth.New() error = %v", err)
	}
	authentication, ok := localStore.Authenticate(testLocalPrincipal, testLocalSecret, testLocalSAN)
	if !ok {
		t.Fatal("initial principal did not authenticate")
	}

	auditQueue := queueTypes.DefaultQueueConfig(testAuditQueue, "$queue/atom-audit/#")
	auditQueue.Reserved = true
	auditQueue.Type = queueTypes.QueueTypeStream
	queueStore := memoryLog.New()
	if err := queueStore.CreateQueue(context.Background(), auditQueue); err != nil {
		t.Fatalf("CreateQueue() error = %v", err)
	}
	managerConfig := queuepkg.DefaultConfig()
	managerConfig.ProtectedQueueContracts = []queueTypes.QueueConfig{auditQueue}
	queueManager := queuepkg.NewManager(queueStore, nil, nil, managerConfig, nil, nil)

	generation := localStore.Generation()
	principal.Permissions.Publish[0].RoutingKey = "unprovisioned-audit"
	changed, err := reloadLocalPrincipals(
		context.Background(),
		localStore,
		[]config.LocalPrincipalConfig{principal},
		[]queueTypes.QueueConfig{auditQueue},
		queueManager,
	)
	if err == nil || !strings.Contains(err.Error(), "has no matching queues entry") {
		t.Fatalf("reloadLocalPrincipals() error = %v, want invalid target", err)
	}
	if changed {
		t.Fatal("invalid local-principal reload reported a change")
	}
	if localStore.Generation() != generation {
		t.Fatalf("generation changed after rejected reload: got %d, want %d", localStore.Generation(), generation)
	}
	if !localStore.CanPublishAuthenticated(authentication, "", testAuditQueue) {
		t.Fatal("rejected reload replaced the previous valid snapshot")
	}
}

func TestReloadLocalPrincipalsReplacesProtectedTargets(t *testing.T) {
	ctx := context.Background()
	secretPath := filepath.Join(t.TempDir(), "current")
	if err := os.WriteFile(secretPath, []byte(testLocalSecret), 0o600); err != nil {
		t.Fatal(err)
	}
	principal := config.LocalPrincipalConfig{
		Name:              testLocalPrincipal,
		CertificateURISAN: testLocalSAN,
		CurrentSecretFile: secretPath,
		Permissions: config.LocalPermissionsConfig{
			Publish: []config.LocalPublishPermission{{RoutingKey: testAuditQueue}},
		},
	}
	localStore, err := localauth.New([]config.LocalPrincipalConfig{principal})
	if err != nil {
		t.Fatalf("localauth.New() error = %v", err)
	}
	authentication, ok := localStore.Authenticate(testLocalPrincipal, testLocalSecret, testLocalSAN)
	if !ok {
		t.Fatal("initial principal did not authenticate")
	}

	auditQueue := queueTypes.DefaultQueueConfig(testAuditQueue, "$queue/atom-audit/#")
	auditQueue.Reserved = true
	auditQueue.Type = queueTypes.QueueTypeStream
	securityQueue := queueTypes.DefaultQueueConfig("atom-security", "$queue/atom-security/#")
	securityQueue.Reserved = true
	securityQueue.Type = queueTypes.QueueTypeStream
	configuredQueues := []queueTypes.QueueConfig{auditQueue, securityQueue}
	queueStore := memoryLog.New()
	for _, contract := range configuredQueues {
		if err := queueStore.CreateQueue(ctx, contract); err != nil {
			t.Fatalf("CreateQueue(%q) error = %v", contract.Name, err)
		}
	}
	managerConfig := queuepkg.DefaultConfig()
	managerConfig.ProtectedQueueContracts = []queueTypes.QueueConfig{auditQueue}
	queueManager := queuepkg.NewManager(queueStore, nil, nil, managerConfig, nil, nil)

	next := principal
	next.Permissions.Publish = []config.LocalPublishPermission{{RoutingKey: securityQueue.Name}}
	changed, err := reloadLocalPrincipals(
		ctx,
		localStore,
		[]config.LocalPrincipalConfig{next},
		configuredQueues,
		queueManager,
	)
	if err != nil {
		t.Fatalf("reloadLocalPrincipals() error = %v", err)
	}
	if !changed {
		t.Fatal("target replacement did not report a changed snapshot")
	}
	contracts := queueManager.ProtectedQueueContracts()
	if len(contracts) != 1 || contracts[0].Name != securityQueue.Name {
		t.Fatalf("protected contracts = %+v, want only %q", contracts, securityQueue.Name)
	}
	if localStore.CanPublishAuthenticated(authentication, "", auditQueue.Name) {
		t.Fatal("old publish target remained authorized")
	}
	if localStore.IsActive(authentication) {
		t.Fatal("session authenticated against the replaced publish ACL remains active")
	}
	reauthenticated, ok := localStore.Authenticate(testLocalPrincipal, testLocalSecret, testLocalSAN)
	if !ok {
		t.Fatal("principal did not reauthenticate against the replacement publish ACL")
	}
	if !localStore.CanPublishAuthenticated(reauthenticated, "", securityQueue.Name) {
		t.Fatal("new publish target was not authorized")
	}
	if err := queueManager.PublishToDurableStream(ctx, auditQueue.Name, queueTypes.PublishRequest{Payload: []byte("{}")}); !errors.Is(err, queuepkg.ErrQueueNotProtected) {
		t.Fatalf("old target publish error = %v, want ErrQueueNotProtected", err)
	}
	if err := queueManager.PublishToDurableStream(ctx, securityQueue.Name, queueTypes.PublishRequest{Payload: []byte("{}")}); err != nil {
		t.Fatalf("new target publish error = %v", err)
	}
}

func TestReloadLocalPrincipalsRestoresProtectionWhenSecretLoadFails(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	secretPath := filepath.Join(dir, "current")
	if err := os.WriteFile(secretPath, []byte(testLocalSecret), 0o600); err != nil {
		t.Fatal(err)
	}
	principal := config.LocalPrincipalConfig{
		Name:              testLocalPrincipal,
		CertificateURISAN: testLocalSAN,
		CurrentSecretFile: secretPath,
		Permissions: config.LocalPermissionsConfig{
			Publish: []config.LocalPublishPermission{{RoutingKey: testAuditQueue}},
		},
	}
	localStore, err := localauth.New([]config.LocalPrincipalConfig{principal})
	if err != nil {
		t.Fatalf("localauth.New() error = %v", err)
	}

	auditQueue := queueTypes.DefaultQueueConfig(testAuditQueue, "$queue/atom-audit/#")
	auditQueue.Reserved = true
	auditQueue.Type = queueTypes.QueueTypeStream
	securityQueue := queueTypes.DefaultQueueConfig("atom-security", "$queue/atom-security/#")
	securityQueue.Reserved = true
	securityQueue.Type = queueTypes.QueueTypeStream
	configuredQueues := []queueTypes.QueueConfig{auditQueue, securityQueue}
	queueStore := memoryLog.New()
	for _, contract := range configuredQueues {
		if err := queueStore.CreateQueue(ctx, contract); err != nil {
			t.Fatalf("CreateQueue(%q) error = %v", contract.Name, err)
		}
	}
	managerConfig := queuepkg.DefaultConfig()
	managerConfig.ProtectedQueueContracts = []queueTypes.QueueConfig{auditQueue}
	queueManager := queuepkg.NewManager(queueStore, nil, nil, managerConfig, nil, nil)
	generation := localStore.Generation()

	next := principal
	next.CurrentSecretFile = filepath.Join(dir, "missing")
	next.Permissions.Publish = []config.LocalPublishPermission{{RoutingKey: securityQueue.Name}}
	changed, err := reloadLocalPrincipals(ctx, localStore, []config.LocalPrincipalConfig{next}, configuredQueues, queueManager)
	if err == nil {
		t.Fatal("reloadLocalPrincipals() succeeded with a missing secret")
	}
	if changed {
		t.Fatal("failed reload reported a changed snapshot")
	}
	if localStore.Generation() != generation {
		t.Fatalf("generation = %d, want %d", localStore.Generation(), generation)
	}
	contracts := queueManager.ProtectedQueueContracts()
	if len(contracts) != 1 || contracts[0].Name != auditQueue.Name {
		t.Fatalf("protected contracts after rollback = %+v, want only %q", contracts, auditQueue.Name)
	}
}
