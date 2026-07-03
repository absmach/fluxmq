// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package hookcallout

import (
	"context"
	"log/slog"
	"net/http"

	"connectrpc.com/connect"
	"github.com/absmach/fluxmq/broker"
	authv1 "github.com/absmach/fluxmq/pkg/proto/auth/v1"
	"github.com/absmach/fluxmq/pkg/proto/auth/v1/authv1connect"
)

var _ broker.BlockingHookProvider = (*GRPCClient)(nil)

// GRPCClient calls a remote HookService over ConnectRPC.
type GRPCClient struct {
	svc authv1connect.HookServiceClient
	Options
}

// NewGRPCClient creates a gRPC hook callout client.
func NewGRPCClient(httpClient *http.Client, baseURL string, opts ...Option) *GRPCClient {
	if httpClient == nil {
		httpClient = http.DefaultClient
	}
	return &GRPCClient{
		svc:     authv1connect.NewHookServiceClient(httpClient, baseURL, connect.WithGRPC()),
		Options: DefaultOptions(opts...),
	}
}

// HandleHook executes a blocking hook remotely.
func (c *GRPCClient) HandleHook(ctx context.Context, req broker.BlockingHookRequest) (broker.BlockingHookResult, error) {
	ctx, cancel := context.WithTimeout(ctx, c.Timeout)
	defer cancel()

	res, err := c.svc.Handle(ctx, connect.NewRequest(&authv1.HookReq{
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
	}))
	if err != nil {
		c.Logger.Info("hook_callout",
			slog.String("hook", req.Hook),
			slog.String("client_id", req.ClientID),
			slog.String("protocol", req.Protocol),
			slog.String("topic", req.Topic),
			slog.String("status", "error"),
			slog.String("error", err.Error()))
		return broker.BlockingHookResult{}, err
	}

	result, err := resultFromProto(res.Msg)
	if err != nil {
		c.Logger.Info("hook_callout",
			slog.String("hook", req.Hook),
			slog.String("client_id", req.ClientID),
			slog.String("protocol", req.Protocol),
			slog.String("topic", req.Topic),
			slog.String("status", "error"),
			slog.String("error", err.Error()))
		return broker.BlockingHookResult{}, err
	}
	c.Logger.Info("hook_callout",
		slog.String("hook", req.Hook),
		slog.String("client_id", req.ClientID),
		slog.String("protocol", req.Protocol),
		slog.String("topic", req.Topic),
		slog.String("status", "ok"),
		slog.Bool("allowed", result.Allowed),
		slog.String("effective_topic", result.Topic))
	return result, nil
}
