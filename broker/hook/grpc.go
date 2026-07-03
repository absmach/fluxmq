// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package hook

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

var _ broker.BlockingHookProvider = (*GRPCClient)(nil)

// GRPCClient calls a remote HookService over ConnectRPC.
type GRPCClient struct {
	svc authv1connect.HookServiceClient
	Options
}

// NewGRPCClient creates a gRPC hook callout client. A nil httpClient selects a
// default transport: h2c for plaintext base URLs, HTTP/2 over TLS otherwise.
func NewGRPCClient(httpClient *http.Client, baseURL string, opts ...Option) *GRPCClient {
	if httpClient == nil {
		httpClient = httpclient.DefaultGRPC(baseURL)
	}
	return &GRPCClient{
		svc:     authv1connect.NewHookServiceClient(httpClient, baseURL, connect.WithGRPC()),
		Options: DefaultOptions(opts...),
	}
}

// HandleHook executes a blocking hook remotely.
func (c *GRPCClient) HandleHook(ctx context.Context, req broker.BlockingHookRequest) (broker.BlockingHookResult, error) {
	connReq := connect.NewRequest(&authv1.HookReq{
		Hook:       hookToProto(req.Hook),
		ClientId:   req.ClientID,
		ExternalId: req.ExternalID,
		Protocol:   protocolToProto(req.Protocol),
		Topic:      req.Topic,
		Payload:    req.Payload,
		Qos:        uint32(req.QoS),
		Retain:     req.Retain,
		Properties: req.Properties,
		Username:   req.Username,
		Password:   req.Password,
	})

	res, err := c.execute(func() (any, error) {
		callCtx, cancel := context.WithTimeout(ctx, c.Timeout)
		defer cancel()
		return c.svc.Handle(callCtx, connReq)
	})
	if err != nil {
		logHookCall(ctx, c.Logger, req, "error", slog.String("error", err.Error()))
		return broker.BlockingHookResult{}, err
	}

	result, err := resultFromProto(res.(*connect.Response[authv1.HookRes]).Msg)
	if err != nil {
		logHookCall(ctx, c.Logger, req, "error", slog.String("error", err.Error()))
		return broker.BlockingHookResult{}, err
	}
	logHookCall(ctx, c.Logger, req, "ok",
		slog.Bool("allowed", result.Allowed),
		slog.String("effective_topic", result.Topic))
	return result, nil
}
