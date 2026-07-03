// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package authcallout

import (
	"context"
	"log/slog"
	"net/http"

	"connectrpc.com/connect"
	"github.com/absmach/fluxmq/broker"
	"github.com/absmach/fluxmq/internal/httpclient"
	authv1 "github.com/absmach/fluxmq/pkg/proto/auth/v1"
	"github.com/absmach/fluxmq/pkg/proto/auth/v1/authv1connect"
)

var (
	_ broker.Authenticator = (*GRPCClient)(nil)
	_ broker.Authorizer    = (*GRPCClient)(nil)
)

// GRPCClient implements broker.Authenticator and broker.Authorizer by calling
// a remote AuthService over ConnectRPC (gRPC-compatible).
type GRPCClient struct {
	svc authv1connect.AuthServiceClient
	Options
}

// NewGRPCClient creates a callout client that dials the given base URL.
// The URL should be the ConnectRPC/gRPC server address
// (e.g. "http://localhost:9090"). A nil httpClient selects a default
// transport: unencrypted HTTP/2 for plaintext base URLs, HTTP/2 over TLS
// otherwise.
func NewGRPCClient(httpClient *http.Client, baseURL string, opts ...Option) *GRPCClient {
	if httpClient == nil {
		httpClient = httpclient.DefaultGRPC(baseURL)
	}

	o := DefaultOptions(opts...)
	return &GRPCClient{
		svc:     authv1connect.NewAuthServiceClient(httpClient, baseURL, connect.WithGRPC()),
		Options: o,
	}
}

// Authenticate calls the remote AuthService.Authenticate RPC.
func (c *GRPCClient) Authenticate(clientID, username, secret string) (*broker.AuthnResult, error) {
	req := connect.NewRequest(&authv1.AuthnReq{
		ClientId: clientID,
		Username: username,
		Password: secret,
		Protocol: c.Protocol,
	})

	result, err := c.retryWithBackoff(context.Background(), func() (any, error) {
		ctx, cancel := context.WithTimeout(context.Background(), c.Timeout)
		defer cancel()
		return c.CB.Execute(func() (any, error) {
			return c.svc.Authenticate(ctx, req)
		})
	}, retriableError)
	if err != nil {
		c.Logger.Info("auth_callout_authenticate",
			slog.String("client_id", clientID),
			slog.String("status", "error"),
			slog.String("error", err.Error()))
		return &broker.AuthnResult{}, err
	}

	res := result.(*connect.Response[authv1.AuthnRes])
	msg := res.Msg
	c.Logger.Info("auth_callout_authenticate",
		slog.String("client_id", clientID),
		slog.String("status", "ok"),
		slog.Bool("authenticated", msg.GetAuthenticated()))

	return &broker.AuthnResult{
		Authenticated: msg.GetAuthenticated(),
		ID:            msg.GetId(),
	}, nil
}

// CanPublish calls the remote AuthService.Authorize RPC for publish.
func (c *GRPCClient) CanPublish(clientID string, topic string) bool {
	return c.authorize(clientID, topic, authv1.Action_Publish)
}

// CanSubscribe calls the remote AuthService.Authorize RPC for subscribe.
func (c *GRPCClient) CanSubscribe(clientID string, filter string) bool {
	return c.authorize(clientID, filter, authv1.Action_Subscribe)
}

func (c *GRPCClient) authorize(externalID, topic string, action authv1.Action) bool {
	req := connect.NewRequest(&authv1.AuthzReq{
		ExternalId: externalID,
		Topic:      topic,
		Action:     action,
	})

	result, err := c.retryWithBackoff(context.Background(), func() (any, error) {
		ctx, cancel := context.WithTimeout(context.Background(), c.Timeout)
		defer cancel()
		return c.CB.Execute(func() (any, error) {
			return c.svc.Authorize(ctx, req)
		})
	}, retriableError)
	if err != nil {
		c.Logger.Info("auth_callout_authorize",
			slog.String("external_id", externalID),
			slog.String("topic", topic),
			slog.String("action", action.String()),
			slog.String("status", "error"),
			slog.String("error", err.Error()))
		return false
	}

	res := result.(*connect.Response[authv1.AuthzRes])
	msg := res.Msg
	c.Logger.Info("auth_callout_authorize",
		slog.String("external_id", externalID),
		slog.String("topic", topic),
		slog.String("action", action.String()),
		slog.String("status", "ok"),
		slog.Bool("authorized", msg.GetAuthorized()))

	return msg.GetAuthorized()
}
