// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package broker

import (
	"context"
	"crypto/x509"
	"crypto/x509/pkix"
	"errors"
	"io"
	"testing"

	corebroker "github.com/absmach/fluxmq/broker"
	"github.com/absmach/fluxmq/internal/mqttsecurity"
	"github.com/absmach/fluxmq/mqtt/packets"
	v3 "github.com/absmach/fluxmq/mqtt/packets/v3"
	v5 "github.com/absmach/fluxmq/mqtt/packets/v5"
	"github.com/absmach/fluxmq/mqtt/session"
	"github.com/absmach/fluxmq/storage/memory"
	"github.com/stretchr/testify/require"
)

const (
	mtlsEntityA = "345b23ea-3263-4592-bb60-b49df57aa5ac"
	mtlsEntityB = "8dc9cc61-8919-4d9a-8414-91cf71dc65a4"
	mtlsAPIKey  = "api-key"
)

func mqttMTLSContext(t *testing.T, externalID string) context.Context {
	t.Helper()
	security, err := mqttsecurity.FromVerifiedCertificate(&x509.Certificate{
		Raw:     []byte("verified-" + externalID),
		Subject: pkix.Name{CommonName: externalID},
	})
	require.NoError(t, err)
	return mqttsecurity.WithConnection(context.Background(), security)
}

func runMTLSConnect(t *testing.T, b *Broker, version int, ctx context.Context, clientID, username, password string, usernameFlag, passwordFlag, cleanStart bool) error {
	t.Helper()
	conn := &mockConnection{}
	switch version {
	case 4:
		return newV3Handler(b).HandleConnect(ctx, conn, &v3.Connect{
			FixedHeader:     packets.FixedHeader{PacketType: packets.ConnectType},
			ProtocolName:    "MQTT",
			ProtocolVersion: 4,
			ClientID:        clientID,
			CleanSession:    cleanStart,
			KeepAlive:       60,
			UsernameFlag:    usernameFlag,
			PasswordFlag:    passwordFlag,
			Username:        username,
			Password:        []byte(password),
		})
	case 5:
		return newV5Handler(b).HandleConnect(ctx, conn, &v5.Connect{
			FixedHeader:     packets.FixedHeader{PacketType: packets.ConnectType},
			ProtocolName:    "MQTT",
			ProtocolVersion: 5,
			ClientID:        clientID,
			CleanStart:      cleanStart,
			KeepAlive:       60,
			UsernameFlag:    usernameFlag,
			PasswordFlag:    passwordFlag,
			Username:        username,
			Password:        []byte(password),
		})
	default:
		t.Fatalf("unsupported test MQTT version %d", version)
		return nil
	}
}

func TestMQTTMTLSTwoFactorConnect(t *testing.T) {
	for _, version := range []int{4, 5} {
		t.Run(string(rune('0'+version)), func(t *testing.T) {
			tests := []struct {
				name          string
				certificate   string
				authResult    *corebroker.AuthnResult
				configureAuth bool
				username      string
				password      string
				usernameFlag  bool
				passwordFlag  bool
				wantAllowed   bool
			}{
				{
					name:          "matching certificate and credentials",
					certificate:   mtlsEntityA,
					authResult:    &corebroker.AuthnResult{Authenticated: true, ID: mtlsEntityA},
					configureAuth: true,
					username:      mtlsEntityA,
					password:      mtlsAPIKey,
					usernameFlag:  true,
					passwordFlag:  true,
					wantAllowed:   true,
				},
				{
					name:          "certificate and credential identity mismatch",
					certificate:   mtlsEntityA,
					authResult:    &corebroker.AuthnResult{Authenticated: true, ID: mtlsEntityB},
					configureAuth: true,
					username:      mtlsEntityB,
					password:      mtlsAPIKey,
					usernameFlag:  true,
					passwordFlag:  true,
				},
				{
					name:          "prefixed common name rejected",
					certificate:   "fun_" + mtlsEntityA,
					authResult:    &corebroker.AuthnResult{Authenticated: true, ID: mtlsEntityA},
					configureAuth: true,
					username:      mtlsEntityA,
					password:      mtlsAPIKey,
					usernameFlag:  true,
					passwordFlag:  true,
				},
				{
					name:          "username missing",
					certificate:   mtlsEntityA,
					authResult:    &corebroker.AuthnResult{Authenticated: true, ID: mtlsEntityA},
					configureAuth: true,
					password:      mtlsAPIKey,
					passwordFlag:  true,
				},
				{
					name:          "password missing",
					certificate:   mtlsEntityA,
					authResult:    &corebroker.AuthnResult{Authenticated: true, ID: mtlsEntityA},
					configureAuth: true,
					username:      mtlsEntityA,
					usernameFlag:  true,
				},
				{
					name:          "credential provider rejects",
					certificate:   mtlsEntityA,
					authResult:    &corebroker.AuthnResult{Authenticated: false},
					configureAuth: true,
					username:      mtlsEntityA,
					password:      "wrong",
					usernameFlag:  true,
					passwordFlag:  true,
				},
				{
					name:          "empty resolved identity",
					certificate:   mtlsEntityA,
					authResult:    &corebroker.AuthnResult{Authenticated: true},
					configureAuth: true,
					username:      mtlsEntityA,
					password:      mtlsAPIKey,
					usernameFlag:  true,
					passwordFlag:  true,
				},
				{
					name:         "auth provider missing",
					certificate:  mtlsEntityA,
					username:     mtlsEntityA,
					password:     mtlsAPIKey,
					usernameFlag: true,
					passwordFlag: true,
				},
			}

			for _, tc := range tests {
				t.Run(tc.name, func(t *testing.T) {
					b := NewBroker(memory.New(), nil)
					defer b.Close()
					if tc.configureAuth {
						b.SetAuthEngine(corebroker.NewAuthEngine(&externalIDAuthenticator{result: tc.authResult}, nil))
					}

					err := runMTLSConnect(t, b, version, mqttMTLSContext(t, tc.certificate), "client-1", tc.username, tc.password, tc.usernameFlag, tc.passwordFlag, false)
					if tc.wantAllowed {
						require.True(t, err == nil || errors.Is(err, io.EOF), "unexpected connect error: %v", err)
						s := b.Get("client-1")
						require.NotNil(t, s)
						require.Equal(t, mtlsEntityA, s.ExternalIdentity())
						return
					}

					require.True(t, errors.Is(err, ErrNotAuthorized) || errors.Is(err, ErrProtocolViolation), "expected CONNECT rejection, got %v", err)
					require.Empty(t, b.ExternalID("client-1"), "failed factor must not populate the identity cache")
				})
			}
		})
	}
}

func TestMQTTMTLSRegisterHookCannotRewriteIdentity(t *testing.T) {
	b := NewBroker(memory.New(), nil)
	defer b.Close()
	b.SetAuthEngine(corebroker.NewAuthEngine(&externalIDAuthenticator{
		result: &corebroker.AuthnResult{Authenticated: true, ID: mtlsEntityA},
	}, nil))
	b.SetBlockingHooks(corebroker.NewBlockingHookEngine(&registerIdentityHookProvider{externalID: mtlsEntityB}, corebroker.HookFailDeny, nil, nil, nil))

	err := runMTLSConnect(t, b, 5, mqttMTLSContext(t, mtlsEntityA), "client-1", mtlsEntityA, mtlsAPIKey, true, true, true)
	require.ErrorIs(t, err, ErrNotAuthorized)
	require.Empty(t, b.ExternalID("client-1"))
}

func TestMQTTMTLSPersistentSessionRejectsDifferentIdentity(t *testing.T) {
	b := NewBroker(memory.New(), nil)
	defer b.Close()

	_, _, err := b.CreateSession("persistent-client", 5, session.Options{
		ExternalID:     mtlsEntityA,
		CleanStart:     false,
		ExpiryInterval: 300,
	})
	require.NoError(t, err)
	b.SetAuthEngine(corebroker.NewAuthEngine(&externalIDAuthenticator{
		result: &corebroker.AuthnResult{Authenticated: true, ID: mtlsEntityB},
	}, nil))

	err = runMTLSConnect(t, b, 5, mqttMTLSContext(t, mtlsEntityB), "persistent-client", mtlsEntityB, mtlsAPIKey, true, true, false)
	require.ErrorIs(t, err, ErrNotAuthorized)
	require.Equal(t, mtlsEntityA, b.Get("persistent-client").ExternalIdentity())
}

func TestMQTTPlainReconnectCannotTakeOverMTLSIdentity(t *testing.T) {
	b := NewBroker(memory.New(), nil)
	defer b.Close()

	_, _, err := b.CreateSession("persistent-client", 5, session.Options{
		ExternalID:     mtlsEntityA,
		CleanStart:     false,
		ExpiryInterval: 300,
	})
	require.NoError(t, err)
	b.SetAuthEngine(corebroker.NewAuthEngine(&externalIDAuthenticator{
		result: &corebroker.AuthnResult{Authenticated: true, ID: mtlsEntityB},
	}, nil))

	err = runMTLSConnect(t, b, 5, context.Background(), "persistent-client", mtlsEntityB, mtlsAPIKey, true, true, false)
	require.ErrorIs(t, err, ErrNotAuthorized)
	require.Equal(t, mtlsEntityA, b.Get("persistent-client").ExternalIdentity())
}

func TestMQTTPlainCleanStartCanEstablishDifferentIdentity(t *testing.T) {
	b := NewBroker(memory.New(), nil)
	defer b.Close()

	_, _, err := b.CreateSession("persistent-client", 5, session.Options{
		ExternalID:     mtlsEntityA,
		CleanStart:     false,
		ExpiryInterval: 300,
	})
	require.NoError(t, err)
	b.SetAuthEngine(corebroker.NewAuthEngine(&externalIDAuthenticator{
		result: &corebroker.AuthnResult{Authenticated: true, ID: mtlsEntityB},
	}, nil))

	err = runMTLSConnect(t, b, 5, context.Background(), "persistent-client", mtlsEntityB, mtlsAPIKey, true, true, true)
	require.True(t, err == nil || errors.Is(err, io.EOF), "unexpected connect error: %v", err)
	require.Equal(t, mtlsEntityB, b.Get("persistent-client").ExternalIdentity())
}
