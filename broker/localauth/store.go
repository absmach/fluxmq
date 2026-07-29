// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

// Package localauth authenticates and authorizes principals configured locally
// in FluxMQ. It never delegates an unknown or invalid principal to an external
// authentication service.
package localauth

import (
	"bytes"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/absmach/fluxmq/config"
)

const minimumSecretBytes = 32

// CredentialFingerprint identifies the exact local secret used by a session.
// It is comparable, so connection registries can use it to revoke sessions
// after a credential rotation.
type CredentialFingerprint [sha256.Size]byte

// String returns a short non-secret identifier suitable for diagnostics.
func (f CredentialFingerprint) String() string {
	return hex.EncodeToString(f[:8])
}

// PermissionsFingerprint identifies the exact publish ACL bound to a session.
// It is comparable and contains no credential material.
type PermissionsFingerprint [sha256.Size]byte

// Authentication describes a successfully authenticated local principal.
type Authentication struct {
	Principal                     string
	CertificateURISAN             string
	CredentialFingerprint         CredentialFingerprint
	PublishPermissionsFingerprint PermissionsFingerprint
}

// PublishTarget is an exact AMQP exchange and routing-key pair.
type PublishTarget struct {
	Exchange   string
	RoutingKey string
}

// Store holds an atomically replaceable local-principal snapshot.
type Store struct {
	reloadMu sync.Mutex
	current  atomic.Pointer[snapshot]
}

type snapshot struct {
	generation uint64
	principals map[string]*principal
}

type principal struct {
	certificateURISAN  string
	current            CredentialFingerprint
	previous           *CredentialFingerprint
	publish            map[PublishTarget]struct{}
	publishPermissions PermissionsFingerprint
}

// New loads and validates a local-principal snapshot.
func New(configs []config.LocalPrincipalConfig) (*Store, error) {
	store := &Store{}
	if _, err := store.Reload(configs); err != nil {
		return nil, err
	}
	return store, nil
}

// Reload builds and validates a complete replacement before atomically making
// it visible. It reports false without swapping when the loaded credentials
// and ACLs are semantically identical. If loading fails, the current snapshot
// remains unchanged.
func (s *Store) Reload(configs []config.LocalPrincipalConfig) (bool, error) {
	s.reloadMu.Lock()
	defer s.reloadMu.Unlock()

	next, err := buildSnapshot(configs, 0)
	if err != nil {
		return false, err
	}
	current := s.current.Load()
	if current != nil && snapshotsEqual(current, next) {
		return false, nil
	}
	if current == nil {
		next.generation = 1
	} else {
		next.generation = current.generation + 1
	}
	s.current.Store(next)
	return true, nil
}

// Generation returns the active snapshot generation. Every successful reload
// increments it; a failed reload leaves it unchanged.
func (s *Store) Generation() uint64 {
	current := s.current.Load()
	if current == nil {
		return 0
	}
	return current.generation
}

// Authenticate verifies username, SASL secret, and certificate URI SAN
// together. The secret is checked in constant time against both rotation slots.
func (s *Store) Authenticate(username, secret, certificateURISAN string) (Authentication, bool) {
	current := s.current.Load()
	if current == nil {
		return Authentication{}, false
	}
	principal, ok := current.principals[username]
	if !ok || principal.certificateURISAN != certificateURISAN {
		return Authentication{}, false
	}

	candidate := CredentialFingerprint(sha256.Sum256([]byte(secret)))
	matched := subtle.ConstantTimeCompare(candidate[:], principal.current[:])
	if principal.previous != nil {
		matched |= subtle.ConstantTimeCompare(candidate[:], principal.previous[:])
	}
	if matched != 1 {
		return Authentication{}, false
	}

	return Authentication{
		Principal:                     username,
		CertificateURISAN:             certificateURISAN,
		CredentialFingerprint:         candidate,
		PublishPermissionsFingerprint: principal.publishPermissions,
	}, true
}

// IsActive reports whether an authenticated session is still valid in the
// latest snapshot. It detects removed principals, SAN or publish-permission
// changes, and retired credentials.
func (s *Store) IsActive(authentication Authentication) bool {
	current := s.current.Load()
	return authenticationActive(current, authentication)
}

// CanPublishAuthenticated checks the session credential and exact publish ACL
// against one immutable snapshot. Loading both independently would leave a
// revocation race when a reload lands between the two checks.
func (s *Store) CanPublishAuthenticated(authentication Authentication, exchange, routingKey string) bool {
	current := s.current.Load()
	if !authenticationActive(current, authentication) {
		return false
	}
	principal := current.principals[authentication.Principal]
	_, allowed := principal.publish[PublishTarget{Exchange: exchange, RoutingKey: routingKey}]
	return allowed
}

func authenticationActive(current *snapshot, authentication Authentication) bool {
	if current == nil {
		return false
	}
	principal, ok := current.principals[authentication.Principal]
	if !ok || principal.certificateURISAN != authentication.CertificateURISAN {
		return false
	}
	if subtle.ConstantTimeCompare(authentication.PublishPermissionsFingerprint[:], principal.publishPermissions[:]) != 1 {
		return false
	}
	if subtle.ConstantTimeCompare(authentication.CredentialFingerprint[:], principal.current[:]) == 1 {
		return true
	}
	return principal.previous != nil && subtle.ConstantTimeCompare(authentication.CredentialFingerprint[:], principal.previous[:]) == 1
}

// buildSnapshot turns validated configuration into the immutable runtime
// snapshot. The declarative rules live in config.ValidateLocalPrincipals so the
// startup check and this reload path cannot disagree about what is acceptable;
// only the credential material that config deliberately does not retain is
// loaded here.
func buildSnapshot(configs []config.LocalPrincipalConfig, generation uint64) (*snapshot, error) {
	if err := config.ValidateLocalPrincipals(configs); err != nil {
		return nil, err
	}

	principals := make(map[string]*principal, len(configs))

	for i, principalConfig := range configs {
		prefix := fmt.Sprintf("auth.local_principals[%d]", i)

		current, err := loadFingerprint(prefix+".current_secret_file", principalConfig.CurrentSecretFile, true)
		if err != nil {
			return nil, err
		}
		previous, err := loadOptionalFingerprint(prefix+".previous_secret_file", principalConfig.PreviousSecretFile)
		if err != nil {
			return nil, err
		}
		if previous != nil && subtle.ConstantTimeCompare(current[:], previous[:]) == 1 {
			return nil, fmt.Errorf("%s.current_secret_file and previous_secret_file must contain different secrets", prefix)
		}

		publish := make(map[PublishTarget]struct{}, len(principalConfig.Permissions.Publish))
		for _, permission := range principalConfig.Permissions.Publish {
			publish[PublishTarget{Exchange: permission.Exchange, RoutingKey: permission.RoutingKey}] = struct{}{}
		}

		principals[principalConfig.Name] = &principal{
			certificateURISAN:  principalConfig.CertificateURISAN,
			current:            current,
			previous:           previous,
			publish:            publish,
			publishPermissions: fingerprintPublishPermissions(publish),
		}
	}

	return &snapshot{generation: generation, principals: principals}, nil
}

func snapshotsEqual(left, right *snapshot) bool {
	if len(left.principals) != len(right.principals) {
		return false
	}
	for name, leftPrincipal := range left.principals {
		rightPrincipal, ok := right.principals[name]
		if !ok || !principalsEqual(leftPrincipal, rightPrincipal) {
			return false
		}
	}
	return true
}

func principalsEqual(left, right *principal) bool {
	if left.certificateURISAN != right.certificateURISAN || left.current != right.current {
		return false
	}
	if (left.previous == nil) != (right.previous == nil) {
		return false
	}
	if left.previous != nil && *left.previous != *right.previous {
		return false
	}
	if len(left.publish) != len(right.publish) {
		return false
	}
	for target := range left.publish {
		if _, ok := right.publish[target]; !ok {
			return false
		}
	}
	return true
}

func loadOptionalFingerprint(field, filename string) (*CredentialFingerprint, error) {
	if filename == "" {
		return nil, nil
	}
	fingerprint, err := loadFingerprint(field, filename, false)
	if err != nil {
		return nil, err
	}
	return &fingerprint, nil
}

func loadFingerprint(field, filename string, required bool) (CredentialFingerprint, error) {
	var zero CredentialFingerprint
	if strings.TrimSpace(filename) == "" {
		if required || filename != "" {
			return zero, fmt.Errorf("%s cannot be empty", field)
		}
		return zero, nil
	}

	secret, err := os.ReadFile(filename)
	if err != nil {
		return zero, fmt.Errorf("%s: failed to read secret file: %w", field, err)
	}
	if len(secret) > 0 && secret[len(secret)-1] == '\n' {
		secret = secret[:len(secret)-1]
		if len(secret) > 0 && secret[len(secret)-1] == '\r' {
			secret = secret[:len(secret)-1]
		}
	}
	if bytes.ContainsAny(secret, "\r\n") {
		clear(secret)
		return zero, fmt.Errorf("%s: secret file may contain only one terminal newline", field)
	}
	if bytes.IndexByte(secret, 0) >= 0 {
		clear(secret)
		return zero, fmt.Errorf("%s: secret file must not contain NUL bytes", field)
	}
	if len(secret) < minimumSecretBytes {
		clear(secret)
		return zero, fmt.Errorf("%s must contain at least %d bytes", field, minimumSecretBytes)
	}
	fingerprint := CredentialFingerprint(sha256.Sum256(secret))
	clear(secret)
	return fingerprint, nil
}

func fingerprintPublishPermissions(publish map[PublishTarget]struct{}) PermissionsFingerprint {
	targets := make([]PublishTarget, 0, len(publish))
	for target := range publish {
		targets = append(targets, target)
	}
	sort.Slice(targets, func(i, j int) bool {
		if targets[i].Exchange == targets[j].Exchange {
			return targets[i].RoutingKey < targets[j].RoutingKey
		}
		return targets[i].Exchange < targets[j].Exchange
	})

	serialized := make([]byte, 0)
	for _, target := range targets {
		serialized = appendLengthPrefixed(serialized, target.Exchange)
		serialized = appendLengthPrefixed(serialized, target.RoutingKey)
	}
	return PermissionsFingerprint(sha256.Sum256(serialized))
}

func appendLengthPrefixed(destination []byte, value string) []byte {
	destination = binary.BigEndian.AppendUint64(destination, uint64(len(value)))
	return append(destination, value...)
}
