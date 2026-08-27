// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package broker

import (
	"context"
	"errors"
	"log/slog"

	corebroker "github.com/absmach/fluxmq/broker"
	"github.com/absmach/fluxmq/internal/mqttsecurity"
)

var (
	errMQTTCredentialsRejected  = errors.New("MQTT credentials rejected")
	errMQTTTwoFactorRejected    = errors.New("MQTT mTLS two-factor authentication rejected")
	errMQTTRegisterHookRejected = errors.New("MQTT registration rejected by hook")
)

type mqttConnectCredentials struct {
	clientID     string
	username     string
	password     string
	usernameFlag bool
	passwordFlag bool
}

// authenticateMQTTConnect validates MQTT credentials without committing an
// identity cache entry, then binds an mTLS peer to the external identity before
// registration hooks are allowed to approve the connection.
func (b *Broker) authenticateMQTTConnect(ctx context.Context, credentials mqttConnectCredentials) (externalID string, boundMTLS bool, err error) {
	security, boundMTLS := mqttsecurity.FromContext(ctx)
	if boundMTLS && (!credentials.usernameFlag || !credentials.passwordFlag || credentials.username == "" || credentials.password == "") {
		return "", true, errMQTTTwoFactorRejected
	}

	if b.auth != nil {
		authenticated, resolvedID, authErr := b.ValidateCredentials(ctx, credentials.clientID, credentials.username, credentials.password)
		if authErr != nil || !authenticated {
			return "", boundMTLS, errMQTTCredentialsRejected
		}
		externalID = resolvedID
	} else if boundMTLS {
		return "", true, errMQTTTwoFactorRejected
	}

	if boundMTLS {
		if externalID == "" || !security.Matches(externalID) {
			b.telemetry.logger.Warn("mqtt_mtls_identity_binding_rejected",
				slog.String("client_id", credentials.clientID),
				slog.String("certificate_fingerprint_sha256", security.Peer.SHA256Fingerprint))
			return "", true, errMQTTTwoFactorRejected
		}
	}

	hookReq, ok := b.applyBlockingHook(ctx, corebroker.BlockingHookRequest{
		Hook:       corebroker.HookAuthOnRegister,
		ClientID:   credentials.clientID,
		ExternalID: externalID,
		Protocol:   corebroker.HookProtocolMQTT,
		Username:   credentials.username,
		Password:   credentials.password,
	})
	if !ok {
		return "", boundMTLS, errMQTTRegisterHookRejected
	}
	if boundMTLS && hookReq.ExternalID != externalID {
		b.telemetry.logger.Warn("mqtt_mtls_register_identity_rewrite_rejected",
			slog.String("client_id", credentials.clientID),
			slog.String("certificate_fingerprint_sha256", security.Peer.SHA256Fingerprint))
		return "", true, errMQTTTwoFactorRejected
	}

	return hookReq.ExternalID, boundMTLS, nil
}
