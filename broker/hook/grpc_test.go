// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package hook

import (
	"context"
	"net/http/httptest"
	"testing"

	"connectrpc.com/connect"
	"github.com/absmach/fluxmq/broker"
	authv1 "github.com/absmach/fluxmq/pkg/proto/auth/v1"
	"github.com/absmach/fluxmq/pkg/proto/auth/v1/authv1connect"
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
