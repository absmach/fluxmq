// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package hook

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"connectrpc.com/connect"
	"github.com/absmach/fluxmq/broker"
	authv1 "github.com/absmach/fluxmq/pkg/proto/auth/v1"
	"github.com/absmach/fluxmq/pkg/proto/auth/v1/authv1connect"
	"github.com/sony/gobreaker"
	"github.com/stretchr/testify/require"
)

type fakeHookServer struct {
	result *authv1.HookRes
	req    *authv1.HookReq
}

func (s *fakeHookServer) Handle(_ context.Context, req *connect.Request[authv1.HookReq]) (*connect.Response[authv1.HookRes], error) {
	s.req = req.Msg
	return connect.NewResponse(s.result), nil
}

func TestGRPCClient_HandleHookParsesMutations(t *testing.T) {
	handler := &fakeHookServer{result: &authv1.HookRes{
		Result:     authv1.HookResult_HookResultOk,
		Topic:      "m/domain-id/c/channel-id/messages",
		Payload:    []byte(`{"v":2}`),
		PayloadSet: true,
		Qos:        1,
		QosSet:     true,
		Properties: map[string]string{"x-hook": "yes"},
	}}
	_, h := authv1connect.NewHookServiceHandler(handler)
	srv := httptest.NewServer(h)
	defer srv.Close()

	client := NewGRPCClient(srv.Client(), srv.URL)
	res, err := client.HandleHook(context.Background(), broker.BlockingHookRequest{
		Hook:       broker.HookAuthOnPublish,
		ClientID:   "cli1",
		ExternalID: "ext1",
		Protocol:   broker.HookProtocolMQTT,
		Topic:      "m/d1/c/ch1/messages",
		Payload:    []byte(`{"v":1}`),
	})

	require.NoError(t, err)
	require.Equal(t, authv1.HookType_AuthOnPublish, handler.req.GetHook())
	require.Equal(t, authv1.Protocol_MQTT, handler.req.GetProtocol())
	require.Equal(t, "m/d1/c/ch1/messages", handler.req.GetTopic())
	require.True(t, res.Allowed)
	require.Equal(t, "m/domain-id/c/channel-id/messages", res.Topic)
	require.Equal(t, []byte(`{"v":2}`), res.Payload)
	require.True(t, res.PayloadSet)
	require.Equal(t, byte(1), res.QoS)
	require.True(t, res.QoSSet)
	require.Equal(t, "yes", res.Properties["x-hook"])
}

func TestGRPCClient_DefaultClientPlaintextH2C(t *testing.T) {
	handler := &fakeHookServer{result: &authv1.HookRes{Result: authv1.HookResult_HookResultOk}}
	_, h := authv1connect.NewHookServiceHandler(handler)
	srv := httptest.NewUnstartedServer(h)
	protocols := new(http.Protocols)
	protocols.SetHTTP1(true)
	protocols.SetUnencryptedHTTP2(true)
	srv.Config.Protocols = protocols
	srv.Start()
	defer srv.Close()

	client := NewGRPCClient(nil, srv.URL)
	res, err := client.HandleHook(context.Background(), broker.BlockingHookRequest{
		Hook:     broker.HookAuthOnPublish,
		Protocol: broker.HookProtocolMQTT,
		Topic:    topic,
	})

	require.NoError(t, err)
	require.True(t, res.Allowed)
}

type failingHookServer struct {
	hits atomic.Int32
}

func (s *failingHookServer) Handle(context.Context, *connect.Request[authv1.HookReq]) (*connect.Response[authv1.HookRes], error) {
	s.hits.Add(1)
	return nil, connect.NewError(connect.CodeInternal, errors.New("boom"))
}

func TestGRPCClient_CircuitBreakerOpensAndFailsFast(t *testing.T) {
	handler := &failingHookServer{}
	_, h := authv1connect.NewHookServiceHandler(handler)
	srv := httptest.NewServer(h)
	defer srv.Close()

	cb := gobreaker.NewCircuitBreaker(gobreaker.Settings{
		Name: "test",
		ReadyToTrip: func(counts gobreaker.Counts) bool {
			return counts.ConsecutiveFailures >= 2
		},
	})
	client := NewGRPCClient(srv.Client(), srv.URL, WithCircuitBreaker(cb))
	req := broker.BlockingHookRequest{
		Hook:     broker.HookAuthOnPublish,
		Protocol: broker.HookProtocolMQTT,
		Topic:    topic,
	}

	for range 2 {
		_, err := client.HandleHook(context.Background(), req)
		require.Error(t, err)
	}
	_, err := client.HandleHook(context.Background(), req)
	require.ErrorIs(t, err, gobreaker.ErrOpenState)
	require.Equal(t, int32(2), handler.hits.Load(), "open breaker must not reach the hook service")
}

func TestGRPCClient_HandleHookRejectsUnknownResult(t *testing.T) {
	handler := &fakeHookServer{result: &authv1.HookRes{}}
	_, h := authv1connect.NewHookServiceHandler(handler)
	srv := httptest.NewServer(h)
	defer srv.Close()

	client := NewGRPCClient(srv.Client(), srv.URL)
	_, err := client.HandleHook(context.Background(), broker.BlockingHookRequest{
		Hook:     broker.HookAuthOnPublish,
		Protocol: broker.HookProtocolMQTT,
		Topic:    topic,
	})

	require.Error(t, err)
}
