// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package pki

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"

	corebroker "github.com/absmach/fluxmq/broker"
	"github.com/google/uuid"
)

const (
	maximumEventBytes = 1024 * 1024
	issuerIDDetail    = "issuer_id"
)

var ErrUntrustedEvent = errors.New("untrusted Atom certificate lifecycle event")

type domainEvent struct {
	SchemaVersion uint32         `json:"schema_version"`
	EventID       string         `json:"event_id"`
	Event         string         `json:"event"`
	Source        string         `json:"source"`
	TenantID      *string        `json:"tenant_id"`
	TargetKind    *string        `json:"target_kind"`
	TargetID      *string        `json:"target_id"`
	Outcome       string         `json:"outcome"`
	Details       map[string]any `json:"details"`
}

type invalidationKeys struct {
	credentials map[string]struct{}
	entities    map[string]struct{}
	issuers     map[string]struct{}
	tenants     map[string]struct{}
}

// HandleEvent authenticates the broker-stamped publisher identity and Atom's
// envelope before idempotently evicting affected resolver entries. It is safe
// under at-least-once delivery.
func (m *Manager) HandleEvent(payload []byte, properties map[string]string) error {
	if corebroker.ExternalIDFromProperties(properties) != m.config.EventSourcePrincipal {
		m.metrics.eventsRejected.Add(1)
		return ErrUntrustedEvent
	}
	if len(payload) == 0 || len(payload) > maximumEventBytes {
		m.metrics.eventsRejected.Add(1)
		return fmt.Errorf("%w: invalid payload size", ErrUntrustedEvent)
	}
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.UseNumber()
	var event domainEvent
	if err := decoder.Decode(&event); err != nil {
		m.metrics.eventsRejected.Add(1)
		return fmt.Errorf("%w: invalid JSON", ErrUntrustedEvent)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		m.metrics.eventsRejected.Add(1)
		return fmt.Errorf("%w: trailing JSON value", ErrUntrustedEvent)
	}
	if event.SchemaVersion != 1 || event.Source != "atom" || event.Outcome != "allow" || event.Event == "" {
		m.metrics.eventsRejected.Add(1)
		return fmt.Errorf("%w: invalid envelope", ErrUntrustedEvent)
	}
	if _, err := uuid.Parse(event.EventID); err != nil {
		m.metrics.eventsRejected.Add(1)
		return fmt.Errorf("%w: invalid event ID", ErrUntrustedEvent)
	}

	m.metrics.eventsReceived.Add(1)
	if !eventAffectsCertificates(event.Event) {
		return nil
	}
	keys := newInvalidationKeys()
	keys.addEnvelope(event)
	if keys.empty() {
		m.metrics.eventsRejected.Add(1)
		return fmt.Errorf("%w: lifecycle event has no valid invalidation target", ErrUntrustedEvent)
	}
	m.lifecycle.Lock()
	m.generation++
	removed := m.cache.invalidate(func(identity corebroker.CertificateIdentity) bool {
		return keys.matches(identity)
	})
	m.lifecycle.Unlock()
	if removed != 0 {
		m.metrics.cacheInvalidations.Add(uint64(removed))
	}
	disconnectKeys, disconnectSessions := sessionDisconnectionKeys(event)
	if m.sessions != nil && disconnectSessions {
		disconnected := m.sessions(disconnectKeys.matches)
		if disconnected > 0 {
			m.metrics.sessionsDisconnected.Add(uint64(disconnected))
		}
	}
	if strings.HasPrefix(event.Event, "pki.authority.") {
		m.requestTrustRefresh()
	}
	return nil
}

func eventAffectsCertificates(event string) bool {
	return strings.HasPrefix(event, "certificate.") ||
		strings.HasPrefix(event, "entity.") ||
		strings.HasPrefix(event, "tenant.") ||
		strings.HasPrefix(event, "pki.authority.")
}

func sessionDisconnectionKeys(event domainEvent) (invalidationKeys, bool) {
	keys := newInvalidationKeys()
	switch event.Event {
	case "certificate.revoke":
		if event.TargetID != nil {
			keys.add(keys.credentials, *event.TargetID)
		}
		keys.addDetail(keys.credentials, event.Details["credential_id"])
	case "certificate.renew":
		revokeOld, _ := event.Details["revoke_old"].(bool)
		if !revokeOld {
			return keys, false
		}
		if event.TargetID != nil {
			keys.add(keys.credentials, *event.TargetID)
		}
		keys.addDetail(keys.credentials, event.Details["old_credential_id"])
	case "certificate.revoke_entity", "entity.disable", "entity.suspend", "entity.delete", "entity.purge":
		if event.TargetID != nil {
			keys.add(keys.entities, *event.TargetID)
		}
		keys.addDetail(keys.entities, event.Details["entity_id"])
	case "tenant.disable", "tenant.freeze", "tenant.delete", "tenant.purge":
		if event.TargetID != nil {
			keys.add(keys.tenants, *event.TargetID)
		}
		if event.TenantID != nil {
			keys.add(keys.tenants, *event.TenantID)
		}
		keys.addDetail(keys.tenants, event.Details["tenant_id"])
	case "pki.authority.revoke", "pki.authority.revoked",
		"pki.authority.expire", "pki.authority.expired",
		"pki.authority.fail", "pki.authority.failed",
		"pki.authority.disable", "pki.authority.disabled",
		"pki.authority.delete", "pki.authority.deleted",
		"pki.authority.remove", "pki.authority.removed",
		"pki.authority.purge", "pki.authority.purged":
		if event.TargetID != nil {
			keys.add(keys.issuers, *event.TargetID)
		}
		for _, field := range []string{issuerIDDetail, "old_issuer_id"} {
			keys.addDetail(keys.issuers, event.Details[field])
		}
	default:
		return keys, false
	}
	return keys, true
}

func newInvalidationKeys() invalidationKeys {
	return invalidationKeys{
		credentials: make(map[string]struct{}),
		entities:    make(map[string]struct{}),
		issuers:     make(map[string]struct{}),
		tenants:     make(map[string]struct{}),
	}
}

func (keys invalidationKeys) addEnvelope(event domainEvent) {
	if event.TenantID != nil && strings.HasPrefix(event.Event, "tenant.") {
		keys.add(keys.tenants, *event.TenantID)
	}
	if event.TargetKind != nil && event.TargetID != nil {
		switch *event.TargetKind {
		case "credential":
			keys.add(keys.credentials, *event.TargetID)
		case "entity":
			if strings.HasPrefix(event.Event, "entity.") || event.Event == "certificate.revoke_entity" {
				keys.add(keys.entities, *event.TargetID)
			}
		case "authority", "issuer", "pki_authority":
			keys.add(keys.issuers, *event.TargetID)
		case "tenant":
			keys.add(keys.tenants, *event.TargetID)
		}
	}
	for _, field := range []string{"credential_id", "old_credential_id", "new_credential_id"} {
		keys.addDetail(keys.credentials, event.Details[field])
	}
	keys.addDetail(keys.credentials, event.Details["credential_ids"])
	for _, field := range []string{"entity_id"} {
		keys.addDetail(keys.entities, event.Details[field])
	}
	for _, field := range []string{issuerIDDetail, "old_issuer_id", "new_issuer_id"} {
		keys.addDetail(keys.issuers, event.Details[field])
	}
	keys.addDetail(keys.issuers, event.Details["issuer_ids"])
	keys.addDetail(keys.tenants, event.Details["tenant_id"])
}

func (keys invalidationKeys) addDetail(target map[string]struct{}, value any) {
	switch typed := value.(type) {
	case string:
		keys.add(target, typed)
	case []any:
		for _, item := range typed {
			if text, ok := item.(string); ok {
				keys.add(target, text)
			}
		}
	}
}

func (keys invalidationKeys) add(target map[string]struct{}, value string) {
	parsed, err := uuid.Parse(value)
	if err == nil {
		target[parsed.String()] = struct{}{}
	}
}

func (keys invalidationKeys) matches(identity corebroker.CertificateIdentity) bool {
	if _, ok := keys.credentials[identity.CredentialID]; ok {
		return true
	}
	if _, ok := keys.entities[identity.EntityID]; ok {
		return true
	}
	if _, ok := keys.issuers[identity.IssuerID]; ok {
		return true
	}
	if _, ok := keys.tenants[identity.TenantID]; ok {
		return true
	}
	return false
}

func (keys invalidationKeys) empty() bool {
	return len(keys.credentials) == 0 && len(keys.entities) == 0 && len(keys.issuers) == 0 && len(keys.tenants) == 0
}
