// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package localauth

import (
	"crypto/sha256"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/absmach/fluxmq/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	principalName  = "atom-audit-publisher"
	principalSAN   = "spiffe://absmach/atom/audit-publisher"
	currentSecret  = "0123456789abcdef0123456789abcdef"
	previousSecret = "abcdef0123456789abcdef0123456789"
	nextSecret     = "fedcba9876543210fedcba9876543210"
	auditQueue     = "atom-audit"
)

func TestAuthenticateAndAuthorize(t *testing.T) {
	dir := t.TempDir()
	current := writeSecret(t, dir, "current", currentSecret+"\n")
	previous := writeSecret(t, dir, "previous", previousSecret+"\r\n")
	store, err := New([]config.LocalPrincipalConfig{principalConfig(current, previous)})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	authentication, ok := store.Authenticate(principalName, currentSecret, principalSAN)
	if !ok {
		t.Fatal("current secret was rejected")
	}
	if authentication.Principal != principalName || authentication.CertificateURISAN != principalSAN {
		t.Fatalf("unexpected authentication: %+v", authentication)
	}
	wantFingerprint := CredentialFingerprint(sha256.Sum256([]byte(currentSecret)))
	if authentication.CredentialFingerprint != wantFingerprint {
		t.Fatal("credential fingerprint did not identify the current secret")
	}
	if got := authentication.CredentialFingerprint.String(); len(got) != 16 || strings.Contains(got, currentSecret) {
		t.Fatalf("unsafe or malformed diagnostic fingerprint %q", got)
	}

	if _, ok := store.Authenticate(principalName, previousSecret, principalSAN); !ok {
		t.Fatal("previous secret was rejected during rotation overlap")
	}
	if _, ok := store.Authenticate(principalName, "wrong-secret-that-is-at-least-32-bytes", principalSAN); ok {
		t.Fatal("wrong secret was accepted")
	}
	if _, ok := store.Authenticate("unknown", currentSecret, principalSAN); ok {
		t.Fatal("unknown principal was accepted")
	}
	if _, ok := store.Authenticate(principalName, currentSecret, "spiffe://absmach/atom/other"); ok {
		t.Fatal("wrong certificate URI SAN was accepted")
	}

	if !store.CanPublishAuthenticated(authentication, "", "atom-audit") {
		t.Fatal("configured publish target was denied")
	}
	if store.CanPublishAuthenticated(authentication, "events", "atom-audit") {
		t.Fatal("wrong exchange was allowed")
	}
	if store.CanPublishAuthenticated(authentication, "", "atom-audit.other") {
		t.Fatal("prefix routing key was allowed")
	}
}

func TestReloadIsAtomicAndRevokesRemovedCredentials(t *testing.T) {
	dir := t.TempDir()
	current := writeSecret(t, dir, "current", currentSecret)
	previous := writeSecret(t, dir, "previous", previousSecret)
	next := writeSecret(t, dir, "next", nextSecret)
	store, err := New([]config.LocalPrincipalConfig{principalConfig(current, "")})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	oldAuthentication, ok := store.Authenticate(principalName, currentSecret, principalSAN)
	if !ok {
		t.Fatal("initial authentication failed")
	}
	if store.Generation() != 1 {
		t.Fatalf("initial generation = %d, want 1", store.Generation())
	}
	sameSecretDifferentFile := writeSecret(t, dir, "current-copy", currentSecret+"\n")
	changed, err := store.Reload([]config.LocalPrincipalConfig{principalConfig(sameSecretDifferentFile, "")})
	if err != nil {
		t.Fatalf("no-op Reload() error = %v", err)
	}
	if changed {
		t.Fatal("semantically identical credentials were reported as changed")
	}
	if store.Generation() != 1 {
		t.Fatalf("no-op reload changed generation to %d", store.Generation())
	}

	overlap := principalConfig(next, current)
	changed, err = store.Reload([]config.LocalPrincipalConfig{overlap})
	if err != nil {
		t.Fatalf("overlap Reload() error = %v", err)
	}
	if !changed {
		t.Fatal("credential rotation was reported as unchanged")
	}
	if store.Generation() != 2 {
		t.Fatalf("overlap generation = %d, want 2", store.Generation())
	}
	if !store.IsActive(oldAuthentication) {
		t.Fatal("previous credential was revoked during overlap")
	}
	newAuthentication, ok := store.Authenticate(principalName, nextSecret, principalSAN)
	if !ok {
		t.Fatal("rotated current secret was rejected")
	}

	invalid := principalConfig(filepath.Join(dir, "missing"), previous)
	if _, err := store.Reload([]config.LocalPrincipalConfig{invalid}); err == nil {
		t.Fatal("invalid reload succeeded")
	}
	if store.Generation() != 2 {
		t.Fatalf("failed reload changed generation to %d", store.Generation())
	}
	if !store.IsActive(oldAuthentication) || !store.IsActive(newAuthentication) {
		t.Fatal("failed reload changed the active snapshot")
	}

	if _, err := store.Reload([]config.LocalPrincipalConfig{principalConfig(next, "")}); err != nil {
		t.Fatalf("final Reload() error = %v", err)
	}
	if store.IsActive(oldAuthentication) {
		t.Fatal("removed previous credential remains active")
	}
	if store.CanPublishAuthenticated(oldAuthentication, "", "atom-audit") {
		t.Fatal("session authenticated before reload can still publish with a retired credential")
	}
	if !store.IsActive(newAuthentication) {
		t.Fatal("current credential was unexpectedly revoked")
	}

	if _, err := store.Reload(nil); err != nil {
		t.Fatalf("principal-removal Reload() error = %v", err)
	}
	if store.IsActive(newAuthentication) {
		t.Fatal("removed principal remains active")
	}
}

func TestReloadRevokesChangedPublishPermissions(t *testing.T) {
	dir := t.TempDir()
	current := writeSecret(t, dir, "current", currentSecret)
	principal := principalConfig(current, "")
	store, err := New([]config.LocalPrincipalConfig{principal})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	authentication, ok := store.Authenticate(principalName, currentSecret, principalSAN)
	if !ok {
		t.Fatal("initial authentication failed")
	}

	principal.Permissions.Publish[0].RoutingKey = "atom-audit-v2"
	changed, err := store.Reload([]config.LocalPrincipalConfig{principal})
	if err != nil {
		t.Fatalf("Reload() error = %v", err)
	}
	if !changed {
		t.Fatal("publish ACL replacement was reported as unchanged")
	}
	if store.IsActive(authentication) {
		t.Fatal("session authenticated against the replaced publish ACL remains active")
	}
	if store.CanPublishAuthenticated(authentication, "", "atom-audit-v2") {
		t.Fatal("session authenticated against the old ACL used the replacement ACL")
	}

	reauthenticated, ok := store.Authenticate(principalName, currentSecret, principalSAN)
	if !ok {
		t.Fatal("authentication against the replacement ACL failed")
	}
	if !store.IsActive(reauthenticated) || !store.CanPublishAuthenticated(reauthenticated, "", "atom-audit-v2") {
		t.Fatal("session authenticated against the replacement ACL is not active")
	}
}

func TestStoreRejectsInvalidConfiguration(t *testing.T) {
	dir := t.TempDir()
	current := writeSecret(t, dir, "current", currentSecret)
	weak := writeSecret(t, dir, "weak", strings.Repeat("x", 31))
	nul := writeSecret(t, dir, "nul", strings.Repeat("x", 16)+"\x00"+strings.Repeat("x", 16))

	tests := []struct {
		name      string
		configs   func() []config.LocalPrincipalConfig
		wantError string
	}{
		{
			name: "duplicate principal",
			configs: func() []config.LocalPrincipalConfig {
				first, second := principalConfig(current, ""), principalConfig(current, "")
				second.CertificateURISAN = "spiffe://absmach/atom/other"
				return []config.LocalPrincipalConfig{first, second}
			},
			wantError: "name \"atom-audit-publisher\" is duplicated",
		},
		{
			name: "duplicate URI SAN",
			configs: func() []config.LocalPrincipalConfig {
				first, second := principalConfig(current, ""), principalConfig(current, "")
				second.Name = "other"
				return []config.LocalPrincipalConfig{first, second}
			},
			wantError: "certificate_uri_san \"spiffe://absmach/atom/audit-publisher\" is duplicated",
		},
		{
			name: "weak secret",
			configs: func() []config.LocalPrincipalConfig {
				return []config.LocalPrincipalConfig{principalConfig(weak, "")}
			},
			wantError: "must contain at least 32 bytes",
		},
		{
			name: "NUL byte in secret",
			configs: func() []config.LocalPrincipalConfig {
				return []config.LocalPrincipalConfig{principalConfig(nul, "")}
			},
			wantError: "secret file must not contain NUL bytes",
		},
		{
			name: "same current and previous secret",
			configs: func() []config.LocalPrincipalConfig {
				return []config.LocalPrincipalConfig{principalConfig(current, current)}
			},
			wantError: "must contain different secrets",
		},
		{
			name: "wildcard publish ACL",
			configs: func() []config.LocalPrincipalConfig {
				principal := principalConfig(current, "")
				principal.Permissions.Publish[0].RoutingKey = "atom.#"
				return []config.LocalPrincipalConfig{principal}
			},
			wantError: "without wildcards",
		},
		{
			name: "non-default publish exchange",
			configs: func() []config.LocalPrincipalConfig {
				principal := principalConfig(current, "")
				principal.Permissions.Publish[0].Exchange = "events"
				return []config.LocalPrincipalConfig{principal}
			},
			wantError: "exchange must be empty; local principals may publish only through the AMQP default exchange",
		},
		{
			name: "subscribe ACL entry cannot be empty",
			configs: func() []config.LocalPrincipalConfig {
				principal := principalConfig(current, "")
				principal.Permissions.Subscribe = []string{""}
				return []config.LocalPrincipalConfig{principal}
			},
			wantError: "permissions.subscribe[0] cannot be empty",
		},
		{
			name: "subscribe ACL entry cannot be duplicated",
			configs: func() []config.LocalPrincipalConfig {
				principal := principalConfig(current, "")
				principal.Permissions.Subscribe = []string{auditQueue, auditQueue}
				return []config.LocalPrincipalConfig{principal}
			},
			wantError: "duplicates an earlier subscribe permission",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := New(tt.configs())
			if err == nil || !strings.Contains(err.Error(), tt.wantError) {
				t.Fatalf("New() error = %v, want it to contain %q", err, tt.wantError)
			}
		})
	}
}

func TestConcurrentAuthenticationAndReload(t *testing.T) {
	dir := t.TempDir()
	current := writeSecret(t, dir, "current", currentSecret)
	next := writeSecret(t, dir, "next", nextSecret)
	store, err := New([]config.LocalPrincipalConfig{principalConfig(current, next)})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	var workers sync.WaitGroup
	for range 8 {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for range 500 {
				authentication, ok := store.Authenticate(principalName, currentSecret, principalSAN)
				if ok {
					_ = store.CanPublishAuthenticated(authentication, "", "atom-audit")
				}
			}
		}()
	}
	for i := range 100 {
		principal := principalConfig(current, next)
		if i%2 == 1 {
			principal = principalConfig(next, current)
		}
		if _, err := store.Reload([]config.LocalPrincipalConfig{principal}); err != nil {
			t.Fatalf("Reload() error = %v", err)
		}
	}
	workers.Wait()
}

func principalConfig(current, previous string) config.LocalPrincipalConfig {
	return config.LocalPrincipalConfig{
		Name:               principalName,
		CertificateURISAN:  principalSAN,
		CurrentSecretFile:  current,
		PreviousSecretFile: previous,
		Permissions: config.LocalPermissionsConfig{
			Publish: []config.LocalPublishPermission{{Exchange: "", RoutingKey: "atom-audit"}},
		},
	}
}

func writeSecret(t *testing.T, dir, name, contents string) string {
	t.Helper()
	filename := filepath.Join(dir, name)
	if err := os.WriteFile(filename, []byte(contents), 0o600); err != nil {
		t.Fatalf("write secret: %v", err)
	}
	return filename
}

// A principal may consume only the queues its own ACL names, and narrowing that
// ACL must revoke sessions that authenticated under the wider one.
func TestSubscribeACL(t *testing.T) {
	dir := t.TempDir()
	current := writeSecret(t, dir, "current", currentSecret)

	withSubscribe := func(queues ...string) []config.LocalPrincipalConfig {
		principal := principalConfig(current, "")
		principal.Permissions.Subscribe = queues
		return []config.LocalPrincipalConfig{principal}
	}

	store, err := New(withSubscribe(auditQueue, "m"))
	require.NoError(t, err)

	authentication, ok := store.Authenticate(principalName, currentSecret, principalSAN)
	require.True(t, ok, "current secret was rejected")

	t.Run("named queue is allowed", func(t *testing.T) {
		assert.True(t, store.CanSubscribeAuthenticated(authentication, "m"))
	})
	t.Run("unnamed queue is refused", func(t *testing.T) {
		assert.False(t, store.CanSubscribeAuthenticated(authentication, "other"))
	})
	t.Run("empty queue is refused", func(t *testing.T) {
		assert.False(t, store.CanSubscribeAuthenticated(authentication, ""))
	})

	t.Run("narrowing the ACL revokes the session", func(t *testing.T) {
		changed, err := store.Reload(withSubscribe(auditQueue))
		require.NoError(t, err)
		require.True(t, changed, "dropping a subscribe target must be seen as a change")

		assert.False(t, store.IsActive(authentication),
			"a session bound to the wider ACL must not survive it")
		assert.False(t, store.CanSubscribeAuthenticated(authentication, auditQueue),
			"the retired session must not consume even a still-permitted queue")
	})

	t.Run("publish ACL is unaffected by subscribe permissions", func(t *testing.T) {
		reauthenticated, ok := store.Authenticate(principalName, currentSecret, principalSAN)
		require.True(t, ok)
		assert.True(t, store.CanPublishAuthenticated(reauthenticated, "", auditQueue))
		assert.False(t, store.CanSubscribeAuthenticated(reauthenticated, "m"))
	})
}

// The two ACLs share one fingerprint, so swapping a target between them must
// not produce the same digest.
func TestPermissionsFingerprintSeparatesACLs(t *testing.T) {
	dir := t.TempDir()
	current := writeSecret(t, dir, "current", currentSecret)

	fingerprintFor := func(t *testing.T, publish []config.LocalPublishPermission, subscribe []string) PermissionsFingerprint {
		t.Helper()
		principal := principalConfig(current, "")
		principal.Permissions.Publish = publish
		principal.Permissions.Subscribe = subscribe
		store, err := New([]config.LocalPrincipalConfig{principal})
		require.NoError(t, err)
		authentication, ok := store.Authenticate(principalName, currentSecret, principalSAN)
		require.True(t, ok)
		return authentication.PermissionsFingerprint
	}

	publishOnly := fingerprintFor(t, []config.LocalPublishPermission{{RoutingKey: "shared"}}, nil)
	subscribeOnly := fingerprintFor(t, []config.LocalPublishPermission{{RoutingKey: "other"}}, []string{"shared"})

	assert.NotEqual(t, publishOnly, subscribeOnly,
		"a target moved between the publish and subscribe ACLs must change the fingerprint")
}

// A service publishes to topics derived from its own runtime data, so its ACL
// names a routing-key prefix rather than keys that cannot be enumerated in
// configuration. The prefix must bound the grant, not dissolve it.
func TestPublishPrefixACL(t *testing.T) {
	dir := t.TempDir()
	current := writeSecret(t, dir, "current", currentSecret)

	principal := principalConfig(current, "")
	principal.Permissions.Publish = []config.LocalPublishPermission{
		{RoutingKey: auditQueue},
		{RoutingKeyPrefix: "m."},
	}
	store, err := New([]config.LocalPrincipalConfig{principal})
	require.NoError(t, err)

	authentication, ok := store.Authenticate(principalName, currentSecret, principalSAN)
	require.True(t, ok)

	tests := []struct {
		name       string
		exchange   string
		routingKey string
		want       bool
	}{
		{name: "exact permission still matches", routingKey: auditQueue, want: true},
		{name: "key under the prefix", routingKey: "m.domain.c.channel.temp", want: true},
		{name: "prefix itself", routingKey: "m.", want: true},
		{name: "key outside the prefix", routingKey: "other.domain", want: false},
		{name: "prefix must match at the start", routingKey: "x.m.domain", want: false},
		{name: "partial prefix is not enough", routingKey: "m", want: false},
		{name: "prefix does not reach another exchange", exchange: "events", routingKey: "m.domain", want: false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, store.CanPublishAuthenticated(authentication, tc.exchange, tc.routingKey))
		})
	}

	t.Run("dropping the prefix revokes the session", func(t *testing.T) {
		narrowed := principalConfig(current, "")
		narrowed.Permissions.Publish = []config.LocalPublishPermission{{RoutingKey: auditQueue}}
		changed, err := store.Reload([]config.LocalPrincipalConfig{narrowed})
		require.NoError(t, err)
		require.True(t, changed, "dropping a prefix permission must be seen as a change")

		assert.False(t, store.IsActive(authentication))
		assert.False(t, store.CanPublishAuthenticated(authentication, "", "m.domain.c.channel.temp"))
	})
}

// A prefix and an exact key spelling the same grant must not share a digest,
// or narrowing one into the other would leave sessions alive.
func TestPermissionsFingerprintSeparatesPrefixFromExact(t *testing.T) {
	dir := t.TempDir()
	current := writeSecret(t, dir, "current", currentSecret)

	fingerprintFor := func(t *testing.T, publish []config.LocalPublishPermission) PermissionsFingerprint {
		t.Helper()
		principal := principalConfig(current, "")
		principal.Permissions.Publish = publish
		store, err := New([]config.LocalPrincipalConfig{principal})
		require.NoError(t, err)
		authentication, ok := store.Authenticate(principalName, currentSecret, principalSAN)
		require.True(t, ok)
		return authentication.PermissionsFingerprint
	}

	exact := fingerprintFor(t, []config.LocalPublishPermission{{RoutingKey: "m."}})
	prefixed := fingerprintFor(t, []config.LocalPublishPermission{{RoutingKeyPrefix: "m."}})

	assert.NotEqual(t, exact, prefixed,
		"the same string as an exact key and as a prefix are different grants")
}
