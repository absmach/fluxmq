// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package broker

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/require"
)

const testTopic = "topic"

type stubBlockingHookProvider struct {
	result BlockingHookResult
	err    error
	got    BlockingHookRequest
	calls  int
}

func (p *stubBlockingHookProvider) HandleHook(_ context.Context, req BlockingHookRequest) (BlockingHookResult, error) {
	p.calls++
	p.got = req
	return p.result, p.err
}

func TestBlockingHookEngine_AppliesMutations(t *testing.T) {
	provider := &stubBlockingHookProvider{
		result: BlockingHookResult{
			Allowed:    true,
			Topic:      "m/domain-id/c/channel-id/messages",
			Payload:    []byte(`{"v":2}`),
			PayloadSet: true,
			QoS:        1,
			QoSSet:     true,
			Retain:     true,
			RetainSet:  true,
			Properties: map[string]string{"x-hook": "yes"},
			ExternalID: "resolved-id",
		},
	}
	engine := NewBlockingHookEngine(provider, HookFailDeny, discardBlockingHookLogger(), nil, nil)

	req, ok := engine.Handle(context.Background(), BlockingHookRequest{
		Hook:       HookAuthOnPublish,
		ClientID:   "cli1",
		ExternalID: "client-id",
		Protocol:   HookProtocolMQTT,
		Topic:      "m/d1/c/ch1/messages",
		Payload:    []byte(`{"v":1}`),
		QoS:        0,
		Properties: map[string]string{"origin": "mqtt"},
	})

	require.True(t, ok)
	require.Equal(t, HookAuthOnPublish, provider.got.Hook)
	require.Equal(t, "m/domain-id/c/channel-id/messages", req.Topic)
	require.Equal(t, []byte(`{"v":2}`), req.Payload)
	require.Equal(t, byte(1), req.QoS)
	require.True(t, req.Retain)
	require.Equal(t, "mqtt", req.Properties["origin"])
	require.Equal(t, "yes", req.Properties["x-hook"])
	require.Equal(t, "resolved-id", req.ExternalID)
}

func TestBlockingHookEngine_EmptyMutationKeepsOriginal(t *testing.T) {
	engine := NewBlockingHookEngine(&stubBlockingHookProvider{result: BlockingHookResult{Allowed: true}}, HookFailDeny, discardBlockingHookLogger(), nil, nil)

	req, ok := engine.Handle(context.Background(), BlockingHookRequest{
		Hook:     HookAuthOnSubscribe,
		Protocol: HookProtocolMQTT,
		Topic:    "m/d1/c/ch1/#",
	})

	require.True(t, ok)
	require.Equal(t, "m/d1/c/ch1/#", req.Topic)
}

func TestBlockingHookEngine_DenyStopsProcessing(t *testing.T) {
	engine := NewBlockingHookEngine(&stubBlockingHookProvider{result: BlockingHookResult{Allowed: false, Reason: "blocked"}}, HookFailDeny, discardBlockingHookLogger(), nil, nil)

	_, ok := engine.Handle(context.Background(), BlockingHookRequest{Hook: HookAuthOnPublish, Protocol: HookProtocolMQTT, Topic: testTopic})

	require.False(t, ok)
}

func TestBlockingHookEngine_ErrorHonorsFailMode(t *testing.T) {
	provider := &stubBlockingHookProvider{err: errors.New("resolver unavailable")}

	req, ok := NewBlockingHookEngine(provider, HookFailDeny, discardBlockingHookLogger(), nil, nil).
		Handle(context.Background(), BlockingHookRequest{Hook: HookAuthOnPublish, Protocol: HookProtocolMQTT, Topic: testTopic})
	require.False(t, ok)
	require.Equal(t, "topic", req.Topic)

	req, ok = NewBlockingHookEngine(provider, HookFailAllow, discardBlockingHookLogger(), nil, nil).
		Handle(context.Background(), BlockingHookRequest{Hook: HookAuthOnPublish, Protocol: HookProtocolMQTT, Topic: testTopic})
	require.True(t, ok)
	require.Equal(t, testTopic, req.Topic)
}

func TestBlockingHookEngine_FiltersDisabledProtocolAndHook(t *testing.T) {
	provider := &stubBlockingHookProvider{err: errors.New("should not be called")}
	engine := NewBlockingHookEngine(provider, HookFailDeny, discardBlockingHookLogger(),
		map[string]bool{HookProtocolMQTT: true},
		map[string]bool{HookAuthOnPublish: true},
	)

	req, ok := engine.Handle(context.Background(), BlockingHookRequest{
		Hook:     HookAuthOnPublish,
		Protocol: HookProtocolHTTP,
		Topic:    "topic",
	})
	require.True(t, ok)
	require.Equal(t, "topic", req.Topic)

	req, ok = engine.Handle(context.Background(), BlockingHookRequest{
		Hook:     HookAuthOnSubscribe,
		Protocol: HookProtocolMQTT,
		Topic:    "filter",
	})
	require.True(t, ok)
	require.Equal(t, "filter", req.Topic)
	require.Zero(t, provider.calls)
}

func discardBlockingHookLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}
