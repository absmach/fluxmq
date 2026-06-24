// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package broker

import (
	"context"
	"io"
	"testing"

	corebroker "github.com/absmach/fluxmq/broker"
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

func (a *externalIDAuthenticator) Authenticate(clientID, username, secret string) (*corebroker.AuthnResult, error) {
	return a.result, a.err
}

type willAuthorizer struct {
	allow bool
	topic string
	id    string
}

func (a *willAuthorizer) CanPublish(clientID, topic string) bool {
	a.id = clientID
	a.topic = topic
	return a.allow
}

func (a *willAuthorizer) CanSubscribe(clientID, filter string) bool {
	return true
}

func TestV5ConnectStoresExternalIDOnSession(t *testing.T) {
	b := NewBroker(memory.New(), nil)
	defer b.Close()

	b.SetAuthEngine(corebroker.NewAuthEngine(&externalIDAuthenticator{
		result: &corebroker.AuthnResult{Authenticated: true, ID: "ext-123"},
	}, nil))

	conn := &mockConnection{}
	handler := NewV5Handler(b)
	connect := &v5.Connect{
		FixedHeader:     packets.FixedHeader{PacketType: packets.ConnectType},
		ProtocolName:    "MQTT",
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

	handler := NewV5Handler(b)
	pub := &v5.Publish{
		FixedHeader: packets.FixedHeader{PacketType: packets.PublishType, QoS: 0},
		TopicName:   "telemetry/room1",
		Payload:     []byte("hello"),
		Properties: &v5.PublishProperties{
			User: []v5.User{{Key: corebroker.ExternalIDProperty, Value: "spoofed"}},
		},
	}

	require.NoError(t, handler.HandlePublish(s, pub))
	require.NotNil(t, gotProps)
	require.Equal(t, "mqtt-client", gotProps[corebroker.ClientIDProperty])
	require.Equal(t, "ext-456", gotProps[corebroker.ExternalIDProperty])
}

func TestV3ConnectRejectsUnauthorizedWill(t *testing.T) {
	b := NewBroker(memory.New(), nil)
	defer b.Close()

	authz := &willAuthorizer{allow: false}
	b.SetAuthEngine(corebroker.NewAuthEngine(&externalIDAuthenticator{
		result: &corebroker.AuthnResult{Authenticated: true, ID: "ext-123"},
	}, authz))

	conn := &mockConnection{}
	handler := NewV3Handler(b)
	connect := &v3.Connect{
		FixedHeader:     packets.FixedHeader{PacketType: packets.ConnectType},
		ProtocolName:    "MQTT",
		ProtocolVersion: 4,
		ClientID:        "test-client",
		CleanSession:    true,
		KeepAlive:       60,
		UsernameFlag:    true,
		PasswordFlag:    true,
		Username:        "user",
		Password:        []byte("pass"),
		WillFlag:        true,
		WillTopic:       "m/factory-a/c/telemetry",
		WillMessage:     []byte("offline"),
	}

	err := handler.HandleConnect(conn, connect)
	require.ErrorIs(t, err, ErrNotAuthorized)
	require.Equal(t, "ext-123", authz.id)
	require.Equal(t, "m/factory-a/c/telemetry", authz.topic)
	require.NotEmpty(t, conn.packets)
	ack, ok := conn.packets[0].(*v3.ConnAck)
	require.True(t, ok)
	require.Equal(t, byte(v3.ConnAckNotAuthorized), ack.ReturnCode)
}

func TestV5ConnectRejectsUnauthorizedWill(t *testing.T) {
	b := NewBroker(memory.New(), nil)
	defer b.Close()

	authz := &willAuthorizer{allow: false}
	b.SetAuthEngine(corebroker.NewAuthEngine(&externalIDAuthenticator{
		result: &corebroker.AuthnResult{Authenticated: true, ID: "ext-456"},
	}, authz))

	conn := &mockConnection{}
	handler := NewV5Handler(b)
	connect := &v5.Connect{
		FixedHeader:     packets.FixedHeader{PacketType: packets.ConnectType},
		ProtocolName:    "MQTT",
		ProtocolVersion: 5,
		ClientID:        "test-client",
		CleanStart:      true,
		KeepAlive:       60,
		UsernameFlag:    true,
		PasswordFlag:    true,
		Username:        "user",
		Password:        []byte("pass"),
		WillFlag:        true,
		WillTopic:       "m/factory-a/c/telemetry",
		WillPayload:     []byte("offline"),
	}

	err := handler.HandleConnect(conn, connect)
	require.ErrorIs(t, err, ErrNotAuthorized)
	require.Equal(t, "ext-456", authz.id)
	require.Equal(t, "m/factory-a/c/telemetry", authz.topic)
	require.NotEmpty(t, conn.packets)
	ack, ok := conn.packets[0].(*v5.ConnAck)
	require.True(t, ok)
	require.Equal(t, byte(v5.ConnAckNotAuthorized), ack.ReasonCode)
}
