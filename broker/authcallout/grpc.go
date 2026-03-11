// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package authcallout

import (
	"context"
	"log/slog"
	"net/http"
	"time"

	"connectrpc.com/connect"
	"github.com/absmach/fluxmq/broker"
	authv1 "github.com/absmach/fluxmq/pkg/proto/auth/v1"
	"github.com/absmach/fluxmq/pkg/proto/auth/v1/authv1connect"
	"github.com/sony/gobreaker"
)

const (
	defaultTimeout          = 2 * time.Second
	defaultCBMaxRequests    = 3
	defaultCBOpenInterval   = 10 * time.Second
	defaultCBFailThreshold  = 5
)

var (
	_ broker.Authenticator = (*Client)(nil)
	_ broker.Authorizer    = (*Client)(nil)
)

// Client implements broker.Authenticator and broker.Authorizer by calling
// a remote AuthService over ConnectRPC (gRPC-compatible).
type Client struct {
	svc     authv1connect.AuthServiceClient
	timeout time.Duration
	cb      *gobreaker.CircuitBreaker
	logger  *slog.Logger
}

// Option configures the callout Client.
type Option func(*Client)

// WithTimeout sets the per-call timeout.
func WithTimeout(d time.Duration) Option {
	return func(c *Client) {
		if d > 0 {
			c.timeout = d
		}
	}
}

// WithLogger sets the logger.
func WithLogger(l *slog.Logger) Option {
	return func(c *Client) {
		if l != nil {
			c.logger = l
		}
	}
}

// WithCircuitBreaker overrides the default circuit breaker settings.
func WithCircuitBreaker(cb *gobreaker.CircuitBreaker) Option {
	return func(c *Client) {
		if cb != nil {
			c.cb = cb
		}
	}
}

// NewClient creates a callout client that dials the given base URL.
// The URL should be the ConnectRPC/gRPC server address
// (e.g. "http://localhost:9090").
func NewClient(httpClient *http.Client, baseURL string, opts ...Option) *Client {
	if httpClient == nil {
		httpClient = http.DefaultClient
	}

	c := &Client{
		svc:     authv1connect.NewAuthServiceClient(httpClient, baseURL, connect.WithGRPC()),
		timeout: defaultTimeout,
		logger:  slog.Default(),
	}
	for _, o := range opts {
		o(c)
	}
	if c.cb == nil {
		c.cb = defaultCircuitBreaker(c.logger)
	}
	return c
}

// Authenticate calls the remote AuthService.Authenticate RPC.
func (c *Client) Authenticate(clientID, username, secret string) (*broker.AuthnResult, error) {
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	req := connect.NewRequest(&authv1.AuthnReq{
		ClientId: clientID,
		Username: username,
		Password: secret,
	})

	result, err := c.cb.Execute(func() (any, error) {
		return c.svc.Authenticate(ctx, req)
	})
	if err != nil {
		c.logger.Warn("auth_callout_authenticate_failed",
			slog.String("client_id", clientID),
			slog.String("error", err.Error()))
		return &broker.AuthnResult{}, err
	}

	res := result.(*connect.Response[authv1.AuthnRes])
	msg := res.Msg
	if !msg.GetAuthenticated() {
		c.logger.Debug("auth_callout_authenticate_denied",
			slog.String("client_id", clientID),
			slog.Uint64("reason_code", uint64(msg.GetReasonCode())),
			slog.String("reason", msg.GetReason()))
	}

	return &broker.AuthnResult{
		Authenticated: msg.GetAuthenticated(),
		ID:            msg.GetId(),
	}, nil
}

// CanPublish calls the remote AuthService.Authorize RPC for publish.
func (c *Client) CanPublish(clientID string, topic string) bool {
	return c.authorize(clientID, topic, authv1.Action_ACTION_PUBLISH)
}

// CanSubscribe calls the remote AuthService.Authorize RPC for subscribe.
func (c *Client) CanSubscribe(clientID string, filter string) bool {
	return c.authorize(clientID, filter, authv1.Action_ACTION_SUBSCRIBE)
}

func (c *Client) authorize(externalID, topic string, action authv1.Action) bool {
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	req := connect.NewRequest(&authv1.AuthzReq{
		ExternalId: externalID,
		Topic:      topic,
		Action:     action,
	})

	result, err := c.cb.Execute(func() (any, error) {
		return c.svc.Authorize(ctx, req)
	})
	if err != nil {
		c.logger.Warn("auth_callout_authorize_failed",
			slog.String("external_id", externalID),
			slog.String("topic", topic),
			slog.String("error", err.Error()))
		return false
	}

	res := result.(*connect.Response[authv1.AuthzRes])
	msg := res.Msg
	if !msg.GetAuthorized() {
		c.logger.Debug("auth_callout_authorize_denied",
			slog.String("external_id", externalID),
			slog.String("topic", topic),
			slog.Uint64("reason_code", uint64(msg.GetReasonCode())),
			slog.String("reason", msg.GetReason()))
	}

	return msg.GetAuthorized()
}

func defaultCircuitBreaker(logger *slog.Logger) *gobreaker.CircuitBreaker {
	return gobreaker.NewCircuitBreaker(gobreaker.Settings{
		Name:        "auth-callout",
		MaxRequests: defaultCBMaxRequests,
		Interval:    0,
		Timeout:     defaultCBOpenInterval,
		ReadyToTrip: func(counts gobreaker.Counts) bool {
			return counts.ConsecutiveFailures >= defaultCBFailThreshold
		},
		OnStateChange: func(name string, from gobreaker.State, to gobreaker.State) {
			logger.Warn("auth callout circuit breaker state changed",
				slog.String("from", from.String()),
				slog.String("to", to.String()))
		},
	})
}
