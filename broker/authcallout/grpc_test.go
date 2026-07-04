// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package authcallout

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"connectrpc.com/connect"
	"github.com/absmach/fluxmq/broker"
	authv1 "github.com/absmach/fluxmq/pkg/proto/auth/v1"
	"github.com/absmach/fluxmq/pkg/proto/auth/v1/authv1connect"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const testExternalID = "ext-id-1"

type fakeAuthServer struct {
	authnResult *authv1.AuthnRes
	authzResult *authv1.AuthzRes
	authnErr    error
	authzErr    error
}

func (f *fakeAuthServer) Authenticate(_ context.Context, req *connect.Request[authv1.AuthnReq]) (*connect.Response[authv1.AuthnRes], error) {
	if f.authnErr != nil {
		return nil, f.authnErr
	}
	return connect.NewResponse(f.authnResult), nil
}

func (f *fakeAuthServer) Authorize(_ context.Context, req *connect.Request[authv1.AuthzReq]) (*connect.Response[authv1.AuthzRes], error) {
	if f.authzErr != nil {
		return nil, f.authzErr
	}
	return connect.NewResponse(f.authzResult), nil
}

func startTestServer(t *testing.T, handler authv1connect.AuthServiceHandler) (*httptest.Server, *GRPCClient) {
	t.Helper()
	mux := http.NewServeMux()
	path, h := authv1connect.NewAuthServiceHandler(handler)
	mux.Handle(path, h)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	client := NewGRPCClient(srv.Client(), srv.URL,
		WithLogger(slog.New(slog.NewTextHandler(io.Discard, nil))),
	)
	return srv, client
}

func TestGRPCClient_DefaultClientPlaintextH2C(t *testing.T) {
	handler := &fakeAuthServer{authnResult: &authv1.AuthnRes{Authenticated: true, Id: testExternalID}}
	mux := http.NewServeMux()
	path, h := authv1connect.NewAuthServiceHandler(handler)
	mux.Handle(path, h)
	srv := httptest.NewUnstartedServer(mux)
	protocols := new(http.Protocols)
	protocols.SetHTTP1(true)
	protocols.SetUnencryptedHTTP2(true)
	srv.Config.Protocols = protocols
	srv.Start()
	t.Cleanup(srv.Close)

	client := NewGRPCClient(nil, srv.URL,
		WithLogger(slog.New(slog.NewTextHandler(io.Discard, nil))),
	)
	result, err := client.Authenticate("mqtt-client", "user", "pass")
	require.NoError(t, err)
	assert.True(t, result.Authenticated)
	assert.Equal(t, testExternalID, result.ID)
}

func TestGRPCClient_Authenticate_Success(t *testing.T) {
	_, client := startTestServer(t, &fakeAuthServer{
		authnResult: &authv1.AuthnRes{
			Authenticated: true,
			Id:            testExternalID,
		},
	})

	result, err := client.Authenticate("mqtt-client", "user", "pass")
	require.NoError(t, err)
	assert.True(t, result.Authenticated)
	assert.Equal(t, testExternalID, result.ID)
}

func TestGRPCClient_Authenticate_Denied(t *testing.T) {
	_, client := startTestServer(t, &fakeAuthServer{
		authnResult: &authv1.AuthnRes{
			Authenticated: false,
			ReasonCode:    2,
			Reason:        "bad credentials",
		},
	})

	result, err := client.Authenticate("mqtt-client", "user", "wrong")
	require.NoError(t, err)
	assert.False(t, result.Authenticated)
}

func TestGRPCClient_Authenticate_ServerError(t *testing.T) {
	_, client := startTestServer(t, &fakeAuthServer{
		authnErr: connect.NewError(connect.CodeInternal, nil),
	})

	result, err := client.Authenticate("mqtt-client", "user", "pass")
	require.Error(t, err)
	assert.False(t, result.Authenticated)
}

func TestGRPCClient_CanPublish_Allowed(t *testing.T) {
	_, client := startTestServer(t, &fakeAuthServer{
		authzResult: &authv1.AuthzRes{Authorized: true},
	})

	assert.True(t, client.CanPublish(testExternalID, "m/domain/c/channel/temp"))
}

func TestGRPCClient_CanPublish_Denied(t *testing.T) {
	_, client := startTestServer(t, &fakeAuthServer{
		authzResult: &authv1.AuthzRes{
			Authorized: false,
			ReasonCode: 4,
			Reason:     "not authorized",
		},
	})

	assert.False(t, client.CanPublish(testExternalID, "m/domain/c/channel/temp"))
}

func TestGRPCClient_CanSubscribe_Allowed(t *testing.T) {
	_, client := startTestServer(t, &fakeAuthServer{
		authzResult: &authv1.AuthzRes{Authorized: true},
	})

	assert.True(t, client.CanSubscribe(testExternalID, "m/domain/c/channel/#"))
}

func TestGRPCClient_CanSubscribe_ServerError(t *testing.T) {
	_, client := startTestServer(t, &fakeAuthServer{
		authzErr: connect.NewError(connect.CodeUnavailable, nil),
	})

	assert.False(t, client.CanSubscribe(testExternalID, "m/domain/c/channel"))
}

func TestGRPCClient_ImplementsInterfaces(t *testing.T) {
	_, client := startTestServer(t, &fakeAuthServer{
		authnResult: &authv1.AuthnRes{Authenticated: true, Id: "x"},
		authzResult: &authv1.AuthzRes{Authorized: true},
	})

	var _ broker.Authenticator = client
	var _ broker.Authorizer = client
}
