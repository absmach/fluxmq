// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package broker

import (
	"context"
	"io"
	"testing"

	corebroker "github.com/absmach/fluxmq/broker"
	"github.com/absmach/fluxmq/mqtt/packets"
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
