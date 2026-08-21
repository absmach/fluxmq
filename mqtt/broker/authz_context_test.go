// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package broker

import (
	"context"
	"io"
	"sync"
	"testing"
	"time"

	corebroker "github.com/absmach/fluxmq/broker"
	"github.com/absmach/fluxmq/mqtt/packets"
	v3 "github.com/absmach/fluxmq/mqtt/packets/v3"
	v5 "github.com/absmach/fluxmq/mqtt/packets/v5"
	"github.com/absmach/fluxmq/mqtt/session"
	"github.com/absmach/fluxmq/storage/memory"
	"github.com/stretchr/testify/require"
)

type ctxProbeKey struct{}

type blockingContextAuthorizer struct {
	entered chan context.Context
}

func (a *blockingContextAuthorizer) CanPublish(ctx context.Context, _ string, _ string) bool {
	a.entered <- ctx
	<-ctx.Done()
	return false
}

func (a *blockingContextAuthorizer) CanSubscribe(context.Context, string, string) bool {
	return true
}

type authzScriptedConnection struct {
	mockConnection
	inbound   []packets.ControlPacket
	next      int
	closed    chan struct{}
	closeOnce sync.Once
}

func (c *authzScriptedConnection) ReadPacket() (packets.ControlPacket, error) {
	if c.next < len(c.inbound) {
		pkt := c.inbound[c.next]
		c.next++
		return pkt, nil
	}

	<-c.closed
	return nil, io.EOF
}

func (c *authzScriptedConnection) Close() error {
	c.closeOnce.Do(func() { close(c.closed) })
	return nil
}

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

func TestConnectionContextCancellationIsGenerationScoped(t *testing.T) {
	parent := context.Background()
	firstCtx, first, cancelFirst := bindConnectionContext(parent, &captureConnection{})
	secondCtx, _, cancelSecond := bindConnectionContext(parent, &captureConnection{})
	t.Cleanup(cancelFirst)
	t.Cleanup(cancelSecond)

	require.NoError(t, first.Close())
	require.ErrorIs(t, firstCtx.Err(), context.Canceled)
	require.NoError(t, secondCtx.Err(), "closing one connection must not cancel another generation")
}

func TestHandleConnectionCloseCancelsInFlightAuthorization(t *testing.T) {
	const clientID = "blocked-publisher"

	b := NewBroker(memory.New(), nil)
	t.Cleanup(func() { b.Close() })

	authz := &blockingContextAuthorizer{entered: make(chan context.Context, 1)}
	b.SetAuthEngine(corebroker.NewAuthEngine(nil, authz))

	conn := &authzScriptedConnection{
		inbound: []packets.ControlPacket{
			&v5.Connect{
				FixedHeader:     packets.FixedHeader{PacketType: packets.ConnectType},
				ProtocolName:    protocolNameMQTT,
				ProtocolVersion: 5,
				ClientID:        clientID,
				CleanStart:      true,
			},
			&v5.Publish{
				FixedHeader: packets.FixedHeader{PacketType: packets.PublishType},
				TopicName:   testTelemetryRoom,
				Payload:     []byte("hello"),
			},
		},
		closed: make(chan struct{}),
	}

	done := make(chan struct{})
	go func() {
		HandleConnection(context.Background(), b, conn)
		close(done)
	}()

	var authCtx context.Context
	select {
	case authCtx = <-authz.entered:
	case <-time.After(time.Second):
		t.Fatal("publish authorization was not reached")
	}

	s := b.Get(clientID)
	require.NotNil(t, s)
	require.NoError(t, s.Conn().Close())

	select {
	case <-authCtx.Done():
		require.ErrorIs(t, authCtx.Err(), context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("closing the MQTT connection did not cancel the authorization context")
	}

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("connection handler remained blocked after the connection closed")
	}
}

func connCtx2(s *session.Session, ctx context.Context) *connCtx {
	return &connCtx{Session: s, ctx: ctx, conn: s.Conn(), epoch: s.Epoch()}
}
