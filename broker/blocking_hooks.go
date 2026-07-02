// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package broker

import (
	"context"
	"log/slog"
)

const (
	HookProtocolMQTT    = "mqtt"
	HookProtocolAMQP10  = "amqp"
	HookProtocolAMQP091 = "amqp091"
	HookProtocolHTTP    = "http"
	HookProtocolCoAP    = "coap"

	HookAuthOnRegister    = "auth_on_register"
	HookAuthOnPublish     = "auth_on_publish"
	HookAuthOnSubscribe   = "auth_on_subscribe"
	HookAuthOnUnsubscribe = "auth_on_unsubscribe"

	HookFailDeny  = "deny"
	HookFailAllow = "allow"
)

// BlockingHookRequest describes a synchronous broker hook request.
type BlockingHookRequest struct {
	Hook       string
	ClientID   string
	ExternalID string
	Protocol   string
	Topic      string
	Payload    []byte
	QoS        byte
	Retain     bool
	Properties map[string]string
	Username   string
	Password   string
}

// BlockingHookResult holds a hook decision and supported mutations.
type BlockingHookResult struct {
	Allowed    bool
	Topic      string
	Payload    []byte
	PayloadSet bool
	QoS        byte
	QoSSet     bool
	Retain     bool
	RetainSet  bool
	Properties map[string]string
	ExternalID string
	Reason     string
	ReasonCode uint32
}

// BlockingHookProvider executes synchronous broker hooks.
type BlockingHookProvider interface {
	HandleHook(ctx context.Context, req BlockingHookRequest) (BlockingHookResult, error)
}

// BlockingHookEngine wraps a hook provider with broker failure policy.
type BlockingHookEngine struct {
	provider  BlockingHookProvider
	failMode  string
	protocols map[string]bool
	hooks     map[string]bool
	logger    *slog.Logger
}

// NewBlockingHookEngine creates a blocking hook engine.
func NewBlockingHookEngine(provider BlockingHookProvider, failMode string, logger *slog.Logger, protocols, hooks map[string]bool) *BlockingHookEngine {
	if failMode == "" {
		failMode = HookFailDeny
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &BlockingHookEngine{
		provider:  provider,
		failMode:  failMode,
		protocols: copyBoolMap(protocols),
		hooks:     copyBoolMap(hooks),
		logger:    logger,
	}
}

// Handle runs a blocking hook and returns the possibly-mutated request plus
// whether processing may continue.
func (e *BlockingHookEngine) Handle(ctx context.Context, req BlockingHookRequest) (BlockingHookRequest, bool) {
	if e == nil || e.provider == nil {
		return req, true
	}
	if len(e.protocols) > 0 && !e.protocols[req.Protocol] {
		return req, true
	}
	if len(e.hooks) > 0 && !e.hooks[req.Hook] {
		return req, true
	}

	res, err := e.provider.HandleHook(ctx, req)
	if err != nil {
		e.logger.Warn("blocking_hook_error",
			slog.String("hook", req.Hook),
			slog.String("client_id", req.ClientID),
			slog.String("external_id", req.ExternalID),
			slog.String("protocol", req.Protocol),
			slog.String("requested_topic", req.Topic),
			slog.String("error", err.Error()))
		return req, e.failMode == HookFailAllow
	}
	if !res.Allowed {
		e.logger.Info("blocking_hook_denied",
			slog.String("hook", req.Hook),
			slog.String("client_id", req.ClientID),
			slog.String("external_id", req.ExternalID),
			slog.String("protocol", req.Protocol),
			slog.String("requested_topic", req.Topic),
			slog.String("reason", res.Reason))
		return req, false
	}

	effective := req
	if res.Topic != "" {
		effective.Topic = res.Topic
	}
	if res.PayloadSet {
		effective.Payload = append([]byte(nil), res.Payload...)
	}
	if res.QoSSet {
		effective.QoS = res.QoS
	}
	if res.RetainSet {
		effective.Retain = res.Retain
	}
	if res.ExternalID != "" {
		effective.ExternalID = res.ExternalID
	}
	effective.Properties = mergeProperties(req.Properties, res.Properties)

	e.logger.Debug("blocking_hook_ok",
		slog.String("hook", req.Hook),
		slog.String("client_id", req.ClientID),
		slog.String("external_id", req.ExternalID),
		slog.String("protocol", req.Protocol),
		slog.String("requested_topic", req.Topic),
		slog.String("effective_topic", effective.Topic))
	return effective, true
}

func copyBoolMap(in map[string]bool) map[string]bool {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]bool, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func mergeProperties(base, updates map[string]string) map[string]string {
	if len(updates) == 0 {
		return base
	}
	merged := make(map[string]string, len(base)+len(updates))
	for k, v := range base {
		merged[k] = v
	}
	for k, v := range updates {
		merged[k] = v
	}
	return merged
}
