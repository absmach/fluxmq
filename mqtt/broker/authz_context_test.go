// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package broker

import (
	"context"
	"testing"

	corebroker "github.com/absmach/fluxmq/broker"
	"github.com/absmach/fluxmq/mqtt/packets"
	v3 "github.com/absmach/fluxmq/mqtt/packets/v3"
	v5 "github.com/absmach/fluxmq/mqtt/packets/v5"
	"github.com/absmach/fluxmq/mqtt/session"
	"github.com/absmach/fluxmq/storage/memory"
	"github.com/stretchr/testify/require"
)

type ctxProbeKey struct{}

// ctxCapturingAuthorizer records the context each authorization decision was
// made with, which is what distinguishes a plumbed context from a
// context.Background() the callee invented.
type ctxCapturingAuthorizer struct {
	publishCtx   context.Context
	subscribeCtx context.Context
}

func (a *ctxCapturingAuthorizer) CanPublish(ctx context.Context, _ string, _ string) bool {
	a.publishCtx = ctx
	return true
}

func (a *ctxCapturingAuthorizer) CanSubscribe(ctx context.Context, _ string, _ string) bool {
	a.subscribeCtx = ctx
	return true
}

// TestPublishCarriesConnectionContextToAuthorizer pins the reason the
// interface takes a context at all: the value has to arrive from the
// connection, so cancelling the connection cancels an in-flight callout.
func TestPublishCarriesConnectionContextToAuthorizer(t *testing.T) {
	b := NewBroker(memory.New(), nil)
	t.Cleanup(func() { b.Close() })

	authz := &ctxCapturingAuthorizer{}
	b.SetAuthEngine(corebroker.NewAuthEngine(nil, authz))

	s, _, err := b.CreateSession("publisher", 5, session.Options{CleanStart: true})
	require.NoError(t, err)
	conn := &captureConnection{}
	_, err = s.Connect(conn)
	require.NoError(t, err)

	connCtx, cancel := context.WithCancel(context.WithValue(context.Background(), ctxProbeKey{}, "connection"))
	t.Cleanup(cancel)
	cc := connCtx2(s, connCtx)

	require.NoError(t, newV5Handler(b).HandlePublish(cc, &v5.Publish{
		FixedHeader: packets.FixedHeader{PacketType: packets.PublishType, QoS: 0},
		TopicName:   testTelemetryRoom,
		Payload:     []byte("hello"),
	}))

	require.NotNil(t, authz.publishCtx, "authorizer was never called")
	require.Equal(t, "connection", authz.publishCtx.Value(ctxProbeKey{}),
		"publish authorization ran on a context the connection did not supply")

	cancel()
	require.Error(t, authz.publishCtx.Err(),
		"cancelling the connection must cancel the context the authorizer received")
}

// TestSubscribeCarriesConnectionContextToAuthorizer is the same guarantee on
// the subscribe path, which has its own call site in each handler.
func TestSubscribeCarriesConnectionContextToAuthorizer(t *testing.T) {
	b := NewBroker(memory.New(), nil)
	t.Cleanup(func() { b.Close() })

	authz := &ctxCapturingAuthorizer{}
	b.SetAuthEngine(corebroker.NewAuthEngine(nil, authz))

	s, _, err := b.CreateSession("subscriber", 4, session.Options{CleanStart: true})
	require.NoError(t, err)
	conn := &captureConnection{}
	_, err = s.Connect(conn)
	require.NoError(t, err)

	cc := connCtx2(s, context.WithValue(context.Background(), ctxProbeKey{}, "connection"))

	require.NoError(t, newV3Handler(b).HandleSubscribe(cc, &v3.Subscribe{
		FixedHeader: packets.FixedHeader{PacketType: packets.SubscribeType},
		ID:          1,
		Topics:      []v3.Topic{{Name: "telemetry/#", QoS: 0}},
	}))

	require.NotNil(t, authz.subscribeCtx, "authorizer was never called")
	require.Equal(t, "connection", authz.subscribeCtx.Value(ctxProbeKey{}),
		"subscribe authorization ran on a context the connection did not supply")
}

func connCtx2(s *session.Session, ctx context.Context) *connCtx {
	return &connCtx{Session: s, ctx: ctx, conn: s.Conn(), epoch: s.Epoch()}
}
