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
