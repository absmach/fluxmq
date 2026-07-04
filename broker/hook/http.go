// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package hook

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"

	"github.com/absmach/fluxmq/broker"
	"github.com/absmach/fluxmq/internal/httpclient"
)

var _ broker.BlockingHookProvider = (*HTTPClient)(nil)

type hookRequest struct {
	Hook       string            `json:"hook"`
	ClientID   string            `json:"client_id"`
	ExternalID string            `json:"external_id"`
	Protocol   string            `json:"protocol"`
	Topic      string            `json:"topic"`
	Payload    []byte            `json:"payload,omitempty"`
	QoS        uint32            `json:"qos"`
	Retain     bool              `json:"retain"`
	Properties map[string]string `json:"properties,omitempty"`
	Username   string            `json:"username,omitempty"`
	Password   string            `json:"password,omitempty"`
}

type hookResponse struct {
	Result     string            `json:"result"`
	Topic      string            `json:"topic,omitempty"`
	Payload    []byte            `json:"payload,omitempty"`
	PayloadSet bool              `json:"payload_set,omitempty"`
	QoS        uint32            `json:"qos,omitempty"`
	QoSSet     bool              `json:"qos_set,omitempty"`
	Retain     bool              `json:"retain,omitempty"`
	RetainSet  bool              `json:"retain_set,omitempty"`
	Properties map[string]string `json:"properties,omitempty"`
	ExternalID string            `json:"external_id,omitempty"`
	ReasonCode uint32            `json:"reason_code,omitempty"`
	Reason     string            `json:"reason,omitempty"`
}

// HTTPClient calls a remote hook service over HTTP JSON.
type HTTPClient struct {
	httpClient *http.Client
	baseURL    string
	Options
}

// NewHTTPClient creates an HTTP hook callout client. A nil httpClient selects
// a default transport tuned for concurrent callouts to a single host.
func NewHTTPClient(httpClient *http.Client, baseURL string, opts ...Option) *HTTPClient {
	if httpClient == nil {
		httpClient = httpclient.Default()
	}
	return &HTTPClient{
		httpClient: httpClient,
		baseURL:    baseURL,
		Options:    DefaultOptions(opts...),
	}
}

// HandleHook executes a blocking hook remotely.
func (c *HTTPClient) HandleHook(ctx context.Context, req broker.BlockingHookRequest) (broker.BlockingHookResult, error) {
	body := hookRequest{
		Hook:       hookToString(req.Hook),
		ClientID:   req.ClientID,
		ExternalID: req.ExternalID,
		Protocol:   protocolToString(req.Protocol),
		Topic:      req.Topic,
		Payload:    req.Payload,
		QoS:        uint32(req.QoS),
		Retain:     req.Retain,
		Properties: req.Properties,
		Username:   req.Username,
		Password:   req.Password,
	}

	payload, err := json.Marshal(body)
	if err != nil {
		return broker.BlockingHookResult{}, err
	}

	res, err := c.execute(func() (any, error) {
		return c.post(ctx, payload)
	})
	if err != nil {
		logHookCall(ctx, c.Logger, req, "error", slog.String("error", err.Error()))
		return broker.BlockingHookResult{}, err
	}

	result, err := hookResponseToResult(res.(hookResponse))
	if err != nil {
		logHookCall(ctx, c.Logger, req, "error", slog.String("error", err.Error()))
		return broker.BlockingHookResult{}, err
	}
	logHookCall(ctx, c.Logger, req, "ok",
		slog.Bool("allowed", result.Allowed),
		slog.String("effective_topic", result.Topic))
	return result, nil
}

func (c *HTTPClient) post(ctx context.Context, payload []byte) (hookResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, c.Timeout)
	defer cancel()

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/hooks", bytes.NewReader(payload))
	if err != nil {
		return hookResponse{}, err
	}
	httpReq.Header.Set("Content-Type", "application/json")

	res, err := c.httpClient.Do(httpReq)
	if err != nil {
		return hookResponse{}, err
	}
	defer res.Body.Close()

	if res.StatusCode < 200 || res.StatusCode >= 300 {
		msg, _ := io.ReadAll(io.LimitReader(res.Body, 4096))
		return hookResponse{}, fmt.Errorf("hook status %d: %s", res.StatusCode, string(msg))
	}

	var out hookResponse
	if err := json.NewDecoder(res.Body).Decode(&out); err != nil {
		return hookResponse{}, err
	}
	return out, nil
}

func hookResponseToResult(out hookResponse) (broker.BlockingHookResult, error) {
	var allowed bool
	switch strings.ToLower(strings.TrimSpace(out.Result)) {
	case "ok":
		allowed = true
	case "deny":
		allowed = false
	default:
		return broker.BlockingHookResult{}, fmt.Errorf("unknown hook result %q", out.Result)
	}

	return broker.BlockingHookResult{
		Allowed:    allowed,
		Topic:      out.Topic,
		Payload:    append([]byte(nil), out.Payload...),
		PayloadSet: out.PayloadSet,
		QoS:        byte(out.QoS),
		QoSSet:     out.QoSSet,
		Retain:     out.Retain,
		RetainSet:  out.RetainSet,
		Properties: out.Properties,
		ExternalID: out.ExternalID,
		Reason:     out.Reason,
		ReasonCode: out.ReasonCode,
	}, nil
}

// logHookCall logs one hook callout at debug level. The engine owns
// warn/info-level logging; provider logs carry transport detail only.
func logHookCall(ctx context.Context, logger *slog.Logger, req broker.BlockingHookRequest, status string, attrs ...slog.Attr) {
	if logger == nil || !logger.Enabled(ctx, slog.LevelDebug) {
		return
	}
	base := []slog.Attr{
		slog.String("hook", req.Hook),
		slog.String("client_id", req.ClientID),
		slog.String("protocol", req.Protocol),
		slog.String("topic", req.Topic),
		slog.String("status", status),
	}
	logger.LogAttrs(ctx, slog.LevelDebug, "hook_callout", append(base, attrs...)...)
}
