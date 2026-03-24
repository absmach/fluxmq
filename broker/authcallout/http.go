// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package authcallout

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"

	"github.com/absmach/fluxmq/broker"
	authv1 "github.com/absmach/fluxmq/pkg/proto/auth/v1"
)

var (
	_ broker.Authenticator = (*HTTPClient)(nil)
	_ broker.Authorizer    = (*HTTPClient)(nil)
)

type authnRequest struct {
	ClientID string `json:"client_id"`
	Username string `json:"username"`
	Password string `json:"password"`
	Protocol string `json:"protocol"`
}

type authnResponse struct {
	Authenticated bool   `json:"authenticated"`
	ID            string `json:"id"`
	ReasonCode    uint32 `json:"reason_code"`
	Reason        string `json:"reason"`
}

type authzRequest struct {
	ExternalID string `json:"external_id"`
	Topic      string `json:"topic"`
	Action     string `json:"action"`
}

type authzResponse struct {
	Authorized bool   `json:"authorized"`
	ReasonCode uint32 `json:"reason_code"`
	Reason     string `json:"reason"`
}

// HTTPClient implements broker.Authenticator and broker.Authorizer by calling
// a remote auth service over HTTP JSON.
type HTTPClient struct {
	httpClient *http.Client
	baseURL    string
	Options
}

// NewHTTPClient creates an HTTP-based callout client.
// The baseURL should be the auth service address (e.g. "http://localhost:9090").
// Requests are sent as JSON POSTs to {baseURL}/auth/authenticate and
// {baseURL}/auth/authorize.
func NewHTTPClient(httpClient *http.Client, baseURL string, opts ...Option) *HTTPClient {
	if httpClient == nil {
		httpClient = http.DefaultClient
	}

	o := DefaultOptions(opts...)
	return &HTTPClient{
		httpClient: httpClient,
		baseURL:    baseURL,
		Options:    o,
	}
}

// Authenticate calls the remote auth service's authenticate endpoint.
func (c *HTTPClient) Authenticate(clientID, username, secret string) (*broker.AuthnResult, error) {
	ctx, cancel := context.WithTimeout(context.Background(), c.Timeout)
	defer cancel()

	reqBody := authnRequest{
		ClientID: clientID,
		Username: username,
		Password: secret,
		Protocol: protocolToString(c.Protocol),
	}

	var resp authnResponse
	_, err := c.CB.Execute(func() (any, error) {
		return nil, c.doPost(ctx, "/auth/authenticate", reqBody, &resp)
	})
	if err != nil {
		c.Logger.Info("auth_callout_authenticate",
			slog.String("client_id", clientID),
			slog.String("status", "error"),
			slog.String("error", err.Error()))
		return &broker.AuthnResult{}, err
	}

	c.Logger.Info("auth_callout_authenticate",
		slog.String("client_id", clientID),
		slog.String("status", "ok"),
		slog.Bool("authenticated", resp.Authenticated))

	return &broker.AuthnResult{
		Authenticated: resp.Authenticated,
		ID:            resp.ID,
	}, nil
}

// CanPublish calls the remote auth service's authorize endpoint for publish.
func (c *HTTPClient) CanPublish(clientID string, topic string) bool {
	return c.authorize(clientID, topic, "publish")
}

// CanSubscribe calls the remote auth service's authorize endpoint for subscribe.
func (c *HTTPClient) CanSubscribe(clientID string, filter string) bool {
	return c.authorize(clientID, filter, "subscribe")
}

func (c *HTTPClient) authorize(externalID, topic, action string) bool {
	ctx, cancel := context.WithTimeout(context.Background(), c.Timeout)
	defer cancel()

	reqBody := authzRequest{
		ExternalID: externalID,
		Topic:      topic,
		Action:     action,
	}

	var resp authzResponse
	_, err := c.CB.Execute(func() (any, error) {
		return nil, c.doPost(ctx, "/auth/authorize", reqBody, &resp)
	})
	if err != nil {
		c.Logger.Info("auth_callout_authorize",
			slog.String("external_id", externalID),
			slog.String("topic", topic),
			slog.String("action", action),
			slog.String("status", "error"),
			slog.String("error", err.Error()))
		return false
	}

	c.Logger.Info("auth_callout_authorize",
		slog.String("external_id", externalID),
		slog.String("topic", topic),
		slog.String("action", action),
		slog.String("status", "ok"),
		slog.Bool("authorized", resp.Authorized))

	return resp.Authorized
}

func (c *HTTPClient) doPost(ctx context.Context, path string, body, dest any) error {
	payload, err := json.Marshal(body)
	if err != nil {
		return fmt.Errorf("marshal request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+path, bytes.NewReader(payload))
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("http request: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("read response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("auth service returned %d: %s", resp.StatusCode, string(respBody))
	}

	if err := json.Unmarshal(respBody, dest); err != nil {
		return fmt.Errorf("unmarshal response: %w", err)
	}

	return nil
}

func protocolToString(p authv1.Protocol) string {
	switch p {
	case authv1.Protocol_MQTT:
		return "mqtt"
	case authv1.Protocol_AMQP_1_0:
		return "amqp_1_0"
	case authv1.Protocol_AMQP_0_9_1:
		return "amqp_0_9_1"
	default:
		return ""
	}
}
