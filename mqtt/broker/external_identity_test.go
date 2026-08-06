// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package broker

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	corebroker "github.com/absmach/fluxmq/broker"
	core "github.com/absmach/fluxmq/mqtt"
	"github.com/absmach/fluxmq/mqtt/packets"
	v3 "github.com/absmach/fluxmq/mqtt/packets/v3"
	v5 "github.com/absmach/fluxmq/mqtt/packets/v5"
	"github.com/absmach/fluxmq/mqtt/session"
	"github.com/absmach/fluxmq/storage"
	"github.com/absmach/fluxmq/storage/memory"
	"github.com/stretchr/testify/require"
)

type externalIDAuthenticator struct {
	result *corebroker.AuthnResult
	err    error
}

type certificateResolverAuthenticator struct {
	calls        int
	identity     corebroker.CertificateIdentity
	authorizeErr error
}

func (auth *certificateResolverAuthenticator) AuthenticateCertificate(_ context.Context, peer corebroker.PeerCertificate) (corebroker.CertificateIdentity, error) {
	auth.calls++
	if auth.identity.EntityID == "" {
		auth.identity = corebroker.CertificateIdentity{
			EntityID:     "8a0a5c59-4ea8-4fc1-badb-f96cf739b224",
			TenantID:     "d204f7df-8293-4194-963b-a47a65bc8f04",
			CredentialID: "ca49950c-3ed2-41b4-a319-896085285686",
			Fingerprint:  "certificate-fingerprint",
			ExpiresAt:    time.Now().Add(time.Hour),
		}
	}
	return auth.identity, nil
}

func (auth *certificateResolverAuthenticator) AuthorizeCertificate(context.Context, corebroker.CertificateIdentity, string) error {
	return auth.authorizeErr
}

type certificateMockConnection struct {
	mockConnection
	leafDER   []byte
	issuerDER []byte
}

func (connection *certificateMockConnection) PeerCertificateDER() []byte {
	return append([]byte(nil), connection.leafDER...)
}

func (connection *certificateMockConnection) PeerIssuerCertificateDER() []byte {
	return append([]byte(nil), connection.issuerDER...)
}

func (a *externalIDAuthenticator) Authenticate(clientID, username, secret string) (*corebroker.AuthnResult, error) {
	return a.result, a.err
}

type captureAuthorizer struct {
	publishID string
}

func (a *captureAuthorizer) CanPublish(clientID string, _ string) bool {
	a.publishID = clientID
	return true
}

func (a *captureAuthorizer) CanSubscribe(clientID string, _ string) bool {
	return a.CanPublish(clientID, "")
}

type normalizingHookProvider struct {
	aliasTopic     string
	canonicalTopic string
}

func (n *normalizingHookProvider) HandleHook(_ context.Context, req corebroker.BlockingHookRequest) (corebroker.BlockingHookResult, error) {
	switch req.Topic {
	case n.aliasTopic, n.canonicalTopic:
		return corebroker.BlockingHookResult{Allowed: true, Topic: n.canonicalTopic}, nil
	default:
		return corebroker.BlockingHookResult{Allowed: true}, nil
	}
}

type registerIdentityHookProvider struct {
	externalID string
}

func (p *registerIdentityHookProvider) HandleHook(_ context.Context, req corebroker.BlockingHookRequest) (corebroker.BlockingHookResult, error) {
	if req.Hook == corebroker.HookAuthOnRegister {
		return corebroker.BlockingHookResult{Allowed: true, ExternalID: p.externalID}, nil
	}
	return corebroker.BlockingHookResult{Allowed: true}, nil
}

type qosMutatingHookProvider struct {
	qos byte
}

func (p *qosMutatingHookProvider) HandleHook(_ context.Context, req corebroker.BlockingHookRequest) (corebroker.BlockingHookResult, error) {
	if req.Hook == corebroker.HookAuthOnPublish {
		return corebroker.BlockingHookResult{Allowed: true, QoS: p.qos, QoSSet: true}, nil
	}
	return corebroker.BlockingHookResult{Allowed: true}, nil
}

func TestV5ConnectStoresExternalIDOnSession(t *testing.T) {
	b := NewBroker(memory.New(), nil)
	defer b.Close()

	b.SetAuthEngine(corebroker.NewAuthEngine(&externalIDAuthenticator{
		result: &corebroker.AuthnResult{Authenticated: true, ID: "ext-123"},
	}, nil))

	conn := &mockConnection{}
	handler := newV5Handler(b)
	connect := &v5.Connect{
		FixedHeader:     packets.FixedHeader{PacketType: packets.ConnectType},
		ProtocolName:    protocolNameMQTT,
		ProtocolVersion: 5,
		ClientID:        "test-client",
		CleanStart:      true,
		KeepAlive:       60,
		UsernameFlag:    true,
		PasswordFlag:    true,
		Username:        "user",
		Password:        []byte("pass"),
	}

	err := handler.HandleConnect(conn, connect)
	require.True(t, err == nil || err == io.EOF, "unexpected connect error: %v", err)

	s := b.Get("test-client")
	require.NotNil(t, s)
	require.Equal(t, "ext-123", s.ExternalID)
}

func TestCertificateAuthenticationRunsForMQTTV3AndV5(t *testing.T) {
	tests := []struct {
		name    string
		connect func(clientID string) packets.ControlPacket
		handle  func(*Broker, core.Connection, packets.ControlPacket) error
	}{
		{
			name: "v3 over mTLS transport",
			connect: func(clientID string) packets.ControlPacket {
				return &v3.Connect{
					FixedHeader:     packets.FixedHeader{PacketType: packets.ConnectType},
					ProtocolName:    protocolNameMQTT,
					ProtocolVersion: 4,
					ClientID:        clientID,
					CleanSession:    true,
					KeepAlive:       60,
				}
			},
			handle: func(broker *Broker, connection core.Connection, packet packets.ControlPacket) error {
				return newV3Handler(broker).HandleConnect(connection, packet)
			},
		},
		{
			name: "v5 over mTLS transport",
			connect: func(clientID string) packets.ControlPacket {
				return &v5.Connect{
					FixedHeader:     packets.FixedHeader{PacketType: packets.ConnectType},
					ProtocolName:    protocolNameMQTT,
					ProtocolVersion: 5,
					ClientID:        clientID,
					CleanStart:      true,
					KeepAlive:       60,
				}
			},
			handle: func(broker *Broker, connection core.Connection, packet packets.ControlPacket) error {
				return newV5Handler(broker).HandleConnect(connection, packet)
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			b := NewBroker(memory.New(), nil)
			defer b.Close()
			resolver := &certificateResolverAuthenticator{}
			b.SetAuthEngine(corebroker.NewAuthEngine(nil, nil, corebroker.WithCertificateAuthentication(resolver)))
			connection := &certificateMockConnection{leafDER: []byte{1, 2, 3}, issuerDER: []byte{4, 5, 6}}
			clientID := "certificate-client"

			err := test.handle(b, connection, test.connect(clientID))
			require.True(t, err == nil || err == io.EOF, "unexpected connect error: %v", err)
			require.Equal(t, 1, resolver.calls)
			session := b.Get(clientID)
			require.NotNil(t, session)
			require.Equal(t, "8a0a5c59-4ea8-4fc1-badb-f96cf739b224", session.ExternalID)
		})
	}
}

func TestRejectedCertificateConnectCleansPendingIdentity(t *testing.T) {
	b := NewBroker(memory.New(), nil)
	defer b.Close()
	resolver := &certificateResolverAuthenticator{}
	auth := corebroker.NewAuthEngine(nil, nil, corebroker.WithCertificateAuthentication(resolver))
	b.SetAuthEngine(auth)
	connection := &certificateMockConnection{leafDER: []byte{1, 2, 3}, issuerDER: []byte{4, 5, 6}}
	connect := &v5.Connect{
		FixedHeader:     packets.FixedHeader{PacketType: packets.ConnectType},
		ProtocolName:    protocolNameMQTT,
		ProtocolVersion: 5,
		ClientID:        "rejected-certificate-client",
		CleanStart:      true,
		KeepAlive:       60,
		WillFlag:        true,
		WillTopic:       "invalid/#",
	}

	require.ErrorIs(t, newV5Handler(b).HandleConnect(connection, connect), ErrTopicInvalid)
	require.Zero(t, auth.CertificateSessionCount())
	require.Empty(t, auth.ExternalID(connect.ClientID))
}

func TestCertificateAuthenticationRejectsUnauthorizedWillBeforeSession(t *testing.T) {
	tests := []struct {
		name    string
		connect packets.ControlPacket
		handle  func(*Broker, core.Connection, packets.ControlPacket) error
	}{
		{
			name: "mqtt v3",
			connect: &v3.Connect{
				FixedHeader:     packets.FixedHeader{PacketType: packets.ConnectType},
				ProtocolName:    protocolNameMQTT,
				ProtocolVersion: 4,
				ClientID:        "certificate-will-v3",
				CleanSession:    true,
				WillFlag:        true,
				WillTopic:       "m/88b65e71-e41d-4f12-9800-6c621133af9b/c/will",
			},
			handle: func(broker *Broker, connection core.Connection, packet packets.ControlPacket) error {
				return newV3Handler(broker).HandleConnect(connection, packet)
			},
		},
		{
			name: "mqtt v5",
			connect: &v5.Connect{
				FixedHeader:     packets.FixedHeader{PacketType: packets.ConnectType},
				ProtocolName:    protocolNameMQTT,
				ProtocolVersion: 5,
				ClientID:        "certificate-will-v5",
				CleanStart:      true,
				WillFlag:        true,
				WillTopic:       "m/88b65e71-e41d-4f12-9800-6c621133af9b/c/will",
			},
			handle: func(broker *Broker, connection core.Connection, packet packets.ControlPacket) error {
				return newV5Handler(broker).HandleConnect(connection, packet)
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			b := NewBroker(memory.New(), nil)
			defer b.Close()
			resolver := &certificateResolverAuthenticator{authorizeErr: errors.New("cross-tenant Will")}
			auth := corebroker.NewAuthEngine(nil, nil, corebroker.WithCertificateAuthentication(resolver))
			b.SetAuthEngine(auth)
			connection := &certificateMockConnection{leafDER: []byte{1, 2, 3}}

			require.ErrorIs(t, test.handle(b, connection, test.connect), ErrNotAuthorized)
			require.Zero(t, auth.CertificateSessionCount())
		})
	}
}

func TestCertificateLifecycleInvalidationDisconnectsLiveSession(t *testing.T) {
	b := NewBroker(memory.New(), nil)
	defer b.Close()
	resolver := &certificateResolverAuthenticator{}
	auth := corebroker.NewAuthEngine(nil, nil, corebroker.WithCertificateAuthentication(resolver))
	b.SetAuthEngine(auth)
	clientID := "revoked-certificate-client"
	_, _, err := auth.AuthenticateWithPeer(context.Background(), clientID, "", "", corebroker.PeerCertificate{LeafDER: []byte{1}})
	require.NoError(t, err)
	binding, committed := auth.CommitCertificateAuthentication(clientID)
	require.True(t, committed)
	s, _, err := b.CreateSession(clientID, 5, session.Options{
		CleanStart: true,
		Will: &storage.WillMessage{
			ClientID: clientID,
			Topic:    "m/d204f7df-8293-4194-963b-a47a65bc8f04/c/will",
			Payload:  []byte("must-not-publish"),
		},
	})
	require.NoError(t, err)
	_, _ = s.ConnectWithOptions(&mockConnection{}, session.ConnectOptions{
		Version:            5,
		CertificateBinding: binding,
	})

	disconnected := b.DisconnectCertificateSessions(func(identity corebroker.CertificateIdentity) bool {
		return identity.CredentialID == "ca49950c-3ed2-41b4-a319-896085285686"
	})
	require.Equal(t, 1, disconnected)
	require.False(t, s.IsConnected())
	require.Nil(t, s.Will, "revocation must suppress the certificate session's Will")
	require.Zero(t, auth.CertificateSessionCount())
}

func TestCertificateAuthenticationRejectsPersistentSessionOwnershipTakeover(t *testing.T) {
	b := NewBroker(memory.New(), nil)
	defer b.Close()
	clientID := "owned-persistent-client"
	victimEntityID := "8a0a5c59-4ea8-4fc1-badb-f96cf739b224"
	s, _, err := b.CreateSession(clientID, 5, session.Options{CleanStart: false, ExpiryInterval: 300})
	require.NoError(t, err)
	s.ExternalID = victimEntityID

	resolver := &certificateResolverAuthenticator{identity: corebroker.CertificateIdentity{
		EntityID:     "ac47c9fd-1d4a-4270-bb11-ab6476a0bd3a",
		TenantID:     "88b65e71-e41d-4f12-9800-6c621133af9b",
		CredentialID: "05119e28-6260-4a06-8742-f925bcfdccd4",
		Fingerprint:  "attacker-certificate",
		ExpiresAt:    time.Now().Add(time.Hour),
	}}
	auth := corebroker.NewAuthEngine(nil, nil, corebroker.WithCertificateAuthentication(resolver))
	b.SetAuthEngine(auth)
	connection := &certificateMockConnection{leafDER: []byte{1, 2, 3}}
	connect := &v5.Connect{
		FixedHeader:     packets.FixedHeader{PacketType: packets.ConnectType},
		ProtocolName:    protocolNameMQTT,
		ProtocolVersion: 5,
		ClientID:        clientID,
		CleanStart:      false,
		KeepAlive:       60,
	}

	require.ErrorIs(t, newV5Handler(b).HandleConnect(connection, connect), ErrNotAuthorized)
	require.Equal(t, victimEntityID, s.ExternalID)
	require.Zero(t, auth.CertificateSessionCount())
}

func TestRegisterHookExternalIDOverridesAuthzIdentity(t *testing.T) {
	b := NewBroker(memory.New(), nil)
	defer b.Close()

	authz := &captureAuthorizer{}
	b.SetAuthEngine(corebroker.NewAuthEngine(&externalIDAuthenticator{
		result: &corebroker.AuthnResult{Authenticated: true, ID: "auth-id"},
	}, authz))
	b.SetBlockingHooks(corebroker.NewBlockingHookEngine(&registerIdentityHookProvider{
		externalID: "hook-id",
	}, corebroker.HookFailDeny, nil, nil, nil))

	authenticated, externalID, err := b.Authenticate("client-1", "user", "pass")
	require.NoError(t, err)
	require.True(t, authenticated)
	require.Equal(t, "auth-id", externalID)

	hookID, ok := b.ApplyRegisterHooks(context.Background(), "client-1", externalID, "user", "pass", corebroker.HookProtocolMQTT)
	require.True(t, ok)
	require.Equal(t, "hook-id", hookID)
	require.True(t, b.CanPublish("client-1", "topic"))
	require.Equal(t, "hook-id", authz.publishID)
}

func TestV5PublishSetsExternalIDProperty(t *testing.T) {
	b := NewBroker(nil, nil)
	defer b.Close()

	amqpClientID := corebroker.PrefixedAMQP091ClientID("conn-1")
	require.NoError(t, b.router.Subscribe(amqpClientID, "telemetry/#", 1, storage.SubscribeOptions{}))

	var gotProps map[string]string
	b.SetCrossDeliver(func(ctx context.Context, clientID string, topic string, payload []byte, qos byte, props map[string]string) {
		gotProps = props
	})

	s, _, err := b.CreateSession("mqtt-client", 5, session.Options{CleanStart: true})
	require.NoError(t, err)
	s.ExternalID = "ext-456"

	handler := newV5Handler(b)
	pub := &v5.Publish{
		FixedHeader: packets.FixedHeader{PacketType: packets.PublishType, QoS: 0},
		TopicName:   "telemetry/room1",
		Payload:     []byte("hello"),
		Properties: &v5.PublishProperties{
			User: []v5.User{{Key: corebroker.ExternalIDProperty, Value: "spoofed"}},
		},
	}

	require.NoError(t, handler.HandlePublish(bindConn(s), pub))
	require.NotNil(t, gotProps)
	require.Equal(t, "mqtt-client", gotProps[corebroker.ClientIDProperty])
	require.Equal(t, "ext-456", gotProps[corebroker.ExternalIDProperty])
}

func TestV5PublishRejectsHookQoSMutation(t *testing.T) {
	b := newComplianceTestBroker(t)
	b.SetBlockingHooks(corebroker.NewBlockingHookEngine(&qosMutatingHookProvider{
		qos: 0,
	}, corebroker.HookFailDeny, nil, nil, nil))

	s, _, err := b.CreateSession("publisher", 5, session.Options{CleanStart: true})
	require.NoError(t, err)
	conn := &captureConnection{}
	_, err = s.Connect(conn)
	require.NoError(t, err)

	handler := newV5Handler(b)
	err = handler.HandlePublish(bindConn(s), &v5.Publish{
		FixedHeader: packets.FixedHeader{PacketType: packets.PublishType, QoS: 1},
		ID:          7,
		TopicName:   "telemetry/room1",
		Payload:     []byte("payload"),
	})

	require.NoError(t, err)
	require.Len(t, conn.packets, 1)
	ack, ok := conn.packets[0].(*v5.PubAck)
	require.True(t, ok)
	require.NotNil(t, ack.ReasonCode)
	require.Equal(t, byte(v5.PubAckImplementationSpecificError), *ack.ReasonCode)
}

func TestV5AliasSubscribeReceivesCanonicalPublish(t *testing.T) {
	b := newComplianceTestBroker(t)
	aliasTopic := "m/d1/c/ch1/messages"
	canonicalTopic := "m/26ad5c3f-cd91-4ff0-9685-0c3115643174/c/cdc8f55f-0c54-4a9f-b4aa-8c69d4a8ce15/messages"
	b.SetBlockingHooks(corebroker.NewBlockingHookEngine(&normalizingHookProvider{
		aliasTopic:     aliasTopic,
		canonicalTopic: canonicalTopic,
	}, corebroker.HookFailDeny, nil, nil, nil))

	sub, _, err := b.CreateSession("subscriber", 5, session.Options{CleanStart: true})
	require.NoError(t, err)
	subConn := &captureConnection{}
	_, err = sub.Connect(subConn)
	require.NoError(t, err)

	pub, _, err := b.CreateSession("publisher", 5, session.Options{CleanStart: true})
	require.NoError(t, err)

	handler := newV5Handler(b)
	require.NoError(t, handler.HandleSubscribe(bindConn(sub), &v5.Subscribe{
		FixedHeader: packets.FixedHeader{PacketType: packets.SubscribeType, QoS: 1},
		ID:          1,
		Opts: []v5.SubOption{
			{Topic: aliasTopic, MaxQoS: 0},
		},
	}))
	require.Len(t, subConn.packets, 1)
	_, ok := subConn.packets[0].(*v5.SubAck)
	require.True(t, ok)
	subConn.packets = nil

	require.NoError(t, handler.HandlePublish(bindConn(pub), &v5.Publish{
		FixedHeader: packets.FixedHeader{PacketType: packets.PublishType, QoS: 0},
		TopicName:   canonicalTopic,
		Payload:     []byte("payload"),
	}))

	require.Len(t, subConn.packets, 1)
	got, ok := subConn.packets[0].(*v5.Publish)
	require.True(t, ok)
	require.Equal(t, canonicalTopic, got.TopicName)
	require.Equal(t, []byte("payload"), got.Payload)
}
