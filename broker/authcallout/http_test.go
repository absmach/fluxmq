// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package authcallout

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/absmach/fluxmq/broker"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newHTTPTestServer(t *testing.T, authnHandler, authzHandler http.HandlerFunc) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	if authnHandler != nil {
		mux.HandleFunc("/auth/authenticate", authnHandler)
	}
	if authzHandler != nil {
		mux.HandleFunc("/auth/authorize", authzHandler)
	}
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func TestHTTPClient_Authenticate_Success(t *testing.T) {
	srv := newHTTPTestServer(t,
		func(w http.ResponseWriter, r *http.Request) {
			var req authnRequest
			require.NoError(t, json.NewDecoder(r.Body).Decode(&req))
			assert.Equal(t, "client-1", req.ClientID)
			assert.Equal(t, "user", req.Username)
			assert.Equal(t, "pass", req.Password)
			assert.Equal(t, "mqtt", req.Protocol)

			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(authnResponse{
				Authenticated: true,
				ID:            "ext-id-1",
			})
		}, nil)

	client := NewHTTPClient(srv.Client(), srv.URL,
		WithLogger(discardLogger()),
		WithProtocol(ProtocolMQTT),
	)

	result, err := client.Authenticate("client-1", "user", "pass")
	require.NoError(t, err)
	assert.True(t, result.Authenticated)
	assert.Equal(t, "ext-id-1", result.ID)
}

func TestHTTPClient_Authenticate_Denied(t *testing.T) {
	srv := newHTTPTestServer(t,
		func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(authnResponse{
				Authenticated: false,
				ReasonCode:    2,
				Reason:        "bad credentials",
			})
		}, nil)

	client := NewHTTPClient(srv.Client(), srv.URL, WithLogger(discardLogger()))

	result, err := client.Authenticate("client-1", "user", "wrong")
	require.NoError(t, err)
	assert.False(t, result.Authenticated)
}

func TestHTTPClient_Authenticate_ServerError(t *testing.T) {
	srv := newHTTPTestServer(t,
		func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte("internal error"))
		}, nil)

	client := NewHTTPClient(srv.Client(), srv.URL, WithLogger(discardLogger()))

	result, err := client.Authenticate("client-1", "user", "pass")
	require.Error(t, err)
	assert.False(t, result.Authenticated)
}

func TestHTTPClient_CanPublish_Allowed(t *testing.T) {
	srv := newHTTPTestServer(t, nil,
		func(w http.ResponseWriter, r *http.Request) {
			var req authzRequest
			require.NoError(t, json.NewDecoder(r.Body).Decode(&req))
			assert.Equal(t, "ext-id-1", req.ExternalID)
			assert.Equal(t, "sensors/temp", req.Topic)
			assert.Equal(t, "publish", req.Action)

			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(authzResponse{Authorized: true})
		})

	client := NewHTTPClient(srv.Client(), srv.URL, WithLogger(discardLogger()))
	assert.True(t, client.CanPublish("ext-id-1", "sensors/temp"))
}

func TestHTTPClient_CanPublish_Denied(t *testing.T) {
	srv := newHTTPTestServer(t, nil,
		func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(authzResponse{
				Authorized: false,
				ReasonCode: 4,
				Reason:     "not authorized",
			})
		})

	client := NewHTTPClient(srv.Client(), srv.URL, WithLogger(discardLogger()))
	assert.False(t, client.CanPublish("ext-id-1", "sensors/temp"))
}

func TestHTTPClient_CanSubscribe_Allowed(t *testing.T) {
	srv := newHTTPTestServer(t, nil,
		func(w http.ResponseWriter, r *http.Request) {
			var req authzRequest
			require.NoError(t, json.NewDecoder(r.Body).Decode(&req))
			assert.Equal(t, "subscribe", req.Action)

			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(authzResponse{Authorized: true})
		})

	client := NewHTTPClient(srv.Client(), srv.URL, WithLogger(discardLogger()))
	assert.True(t, client.CanSubscribe("ext-id-1", "sensors/#"))
}

func TestHTTPClient_CanSubscribe_ServerError(t *testing.T) {
	srv := newHTTPTestServer(t, nil,
		func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusServiceUnavailable)
		})

	client := NewHTTPClient(srv.Client(), srv.URL, WithLogger(discardLogger()))
	assert.False(t, client.CanSubscribe("ext-id-1", "sensors/#"))
}

func TestHTTPClient_ProtocolStrings(t *testing.T) {
	tests := []struct {
		protocol Protocol
		expected string
	}{
		{ProtocolMQTT, "mqtt"},
		{ProtocolAMQP10, "amqp_1_0"},
		{ProtocolAMQP091, "amqp_0_9_1"},
		{ProtocolUnspecified, ""},
	}

	for _, tt := range tests {
		var captured authnRequest
		srv := newHTTPTestServer(t,
			func(w http.ResponseWriter, r *http.Request) {
				json.NewDecoder(r.Body).Decode(&captured)
				w.Header().Set("Content-Type", "application/json")
				json.NewEncoder(w).Encode(authnResponse{Authenticated: true, ID: "x"})
			}, nil)

		client := NewHTTPClient(srv.Client(), srv.URL,
			WithLogger(discardLogger()),
			WithProtocol(tt.protocol),
		)
		_, err := client.Authenticate("c", "u", "p")
		require.NoError(t, err)
		assert.Equal(t, tt.expected, captured.Protocol, "protocol=%v", tt.protocol)
	}
}

func TestHTTPClient_ImplementsInterfaces(t *testing.T) {
	srv := newHTTPTestServer(t, nil, nil)
	client := NewHTTPClient(srv.Client(), srv.URL, WithLogger(discardLogger()))

	var _ broker.Authenticator = client
	var _ broker.Authorizer = client
}
