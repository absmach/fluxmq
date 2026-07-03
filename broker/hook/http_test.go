// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package hook

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/absmach/fluxmq/broker"
	"github.com/stretchr/testify/require"
)

const topic = "topic"

func TestHTTPClient_HandleHookParsesMutations(t *testing.T) {
	var got hookRequest
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/hooks", r.URL.Path)
		require.NoError(t, json.NewDecoder(r.Body).Decode(&got))
		json.NewEncoder(w).Encode(hookResponse{ //nolint:errcheck // best-effort response
			Result:     "ok",
			Topic:      "m/domain-id/c/channel-id/messages",
			Payload:    []byte(`{"v":2}`),
			PayloadSet: true,
			QoS:        1,
			QoSSet:     true,
			Properties: map[string]string{"x-hook": "yes"},
		})
	}))
	defer srv.Close()

	client := NewHTTPClient(srv.Client(), srv.URL)
	res, err := client.HandleHook(context.Background(), broker.BlockingHookRequest{
		Hook:       broker.HookAuthOnPublish,
		ClientID:   "cli1",
		ExternalID: "ext1",
		Protocol:   broker.HookProtocolHTTP,
		Topic:      "m/d1/c/ch1/messages",
		Payload:    []byte(`{"v":1}`),
		QoS:        0,
	})

	require.NoError(t, err)
	require.Equal(t, broker.HookAuthOnPublish, got.Hook)
	require.Equal(t, broker.HookProtocolHTTP, got.Protocol)
	require.Equal(t, "m/d1/c/ch1/messages", got.Topic)
	require.True(t, res.Allowed)
	require.Equal(t, "m/domain-id/c/channel-id/messages", res.Topic)
	require.Equal(t, []byte(`{"v":2}`), res.Payload)
	require.True(t, res.PayloadSet)
	require.Equal(t, byte(1), res.QoS)
	require.True(t, res.QoSSet)
	require.Equal(t, "yes", res.Properties["x-hook"])
}

func TestHTTPClient_HandleHookHTTPError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "nope", http.StatusForbidden)
	}))
	defer srv.Close()

	client := NewHTTPClient(srv.Client(), srv.URL)
	_, err := client.HandleHook(context.Background(), broker.BlockingHookRequest{
		Hook:     broker.HookAuthOnPublish,
		Protocol: broker.HookProtocolHTTP,
		Topic:    topic,
	})

	require.Error(t, err)
}

func TestHTTPClient_HandleHookRejectsUnknownResult(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		json.NewEncoder(w).Encode(hookResponse{}) //nolint:errcheck // best-effort response
	}))
	defer srv.Close()

	client := NewHTTPClient(srv.Client(), srv.URL)
	_, err := client.HandleHook(context.Background(), broker.BlockingHookRequest{
		Hook:     broker.HookAuthOnPublish,
		Protocol: broker.HookProtocolHTTP,
		Topic:    topic,
	})

	require.Error(t, err)
}
