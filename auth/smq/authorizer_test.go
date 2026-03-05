// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package smq

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"testing"
	"time"

	grpcChannelsV1 "github.com/absmach/supermq/api/grpc/channels/v1"
	"github.com/sony/gobreaker"
	"google.golang.org/grpc"
)

type mockChannelsClient struct {
	authorized bool
	err        error
	lastReq    *grpcChannelsV1.AuthzReq
}

func (m *mockChannelsClient) Authorize(_ context.Context, req *grpcChannelsV1.AuthzReq, _ ...grpc.CallOption) (*grpcChannelsV1.AuthzRes, error) {
	m.lastReq = req
	if m.err != nil {
		return nil, m.err
	}
	return &grpcChannelsV1.AuthzRes{Authorized: m.authorized}, nil
}

func newTestAuthorizer(client channelsClient) *Authorizer {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	return &Authorizer{
		cfg:    Config{Timeout: time.Second, ClientType: "client"},
		client: client,
		cb: gobreaker.NewCircuitBreaker(gobreaker.Settings{
			Name: "test-smq-authorizer",
		}),
		logger: logger,
	}
}

func TestParseSuperMQTopic(t *testing.T) {
	cases := []struct {
		name      string
		topic     string
		domainID  string
		channelID string
		handled   bool
		wantErr   bool
	}{
		{
			name:      "valid topic",
			topic:     "m/domain-1/c/channel-1",
			domainID:  "domain-1",
			channelID: "channel-1",
			handled:   true,
			wantErr:   false,
		},
		{
			name:      "valid topic with subtopic",
			topic:     "m/domain-1/c/channel-1/sub/topic",
			domainID:  "domain-1",
			channelID: "channel-1",
			handled:   true,
			wantErr:   false,
		},
		{
			name:    "non smq topic",
			topic:   "$queue/orders",
			handled: false,
			wantErr: false,
		},
		{
			name:    "smq malformed missing channel",
			topic:   "m/domain-1/c",
			handled: true,
			wantErr: true,
		},
		{
			name:    "smq wildcard in domain",
			topic:   "m/+/c/channel-1",
			handled: true,
			wantErr: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			domainID, channelID, handled, err := parseSuperMQTopic(tc.topic)
			if tc.wantErr && err == nil {
				t.Fatalf("expected error")
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if handled != tc.handled {
				t.Fatalf("handled mismatch: got %v want %v", handled, tc.handled)
			}
			if domainID != tc.domainID {
				t.Fatalf("domain mismatch: got %q want %q", domainID, tc.domainID)
			}
			if channelID != tc.channelID {
				t.Fatalf("channel mismatch: got %q want %q", channelID, tc.channelID)
			}
		})
	}
}

func TestNormalizeClientID(t *testing.T) {
	cases := []struct {
		in   string
		want string
	}{
		{in: "amqp091-1.2.3.4:1234", want: "1.2.3.4:1234"},
		{in: "amqp:container-id", want: "container-id"},
		{in: "http:10.0.0.1:8080", want: "10.0.0.1:8080"},
		{in: "coap:10.0.0.2:5683", want: "10.0.0.2:5683"},
		{in: "plain-client", want: "plain-client"},
	}

	for _, tc := range cases {
		if got := normalizeClientID(tc.in); got != tc.want {
			t.Fatalf("normalizeClientID(%q): got %q want %q", tc.in, got, tc.want)
		}
	}
}

func TestAuthorizerBehavior(t *testing.T) {
	t.Run("publish forwards mapped ids and type", func(t *testing.T) {
		client := &mockChannelsClient{authorized: true}
		a := newTestAuthorizer(client)

		ok := a.CanPublish("amqp091-10.0.0.1:1111", "m/domain-1/c/channel-1/sub")
		if !ok {
			t.Fatalf("expected publish authorization")
		}
		if client.lastReq == nil {
			t.Fatalf("expected authorize call")
		}
		if client.lastReq.ClientId != "10.0.0.1:1111" {
			t.Fatalf("unexpected client id: %q", client.lastReq.ClientId)
		}
		if client.lastReq.DomainId != "domain-1" {
			t.Fatalf("unexpected domain id: %q", client.lastReq.DomainId)
		}
		if client.lastReq.ChannelId != "channel-1" {
			t.Fatalf("unexpected channel id: %q", client.lastReq.ChannelId)
		}
		if client.lastReq.Type != publishConnType {
			t.Fatalf("unexpected auth type: %d", client.lastReq.Type)
		}
	})

	t.Run("subscribe uses subscribe type", func(t *testing.T) {
		client := &mockChannelsClient{authorized: true}
		a := newTestAuthorizer(client)

		ok := a.CanSubscribe("coap:10.0.0.2:5683", "m/domain-2/c/channel-9/#")
		if !ok {
			t.Fatalf("expected subscribe authorization")
		}
		if client.lastReq == nil {
			t.Fatalf("expected authorize call")
		}
		if client.lastReq.ClientId != "10.0.0.2:5683" {
			t.Fatalf("unexpected client id: %q", client.lastReq.ClientId)
		}
		if client.lastReq.Type != subscribeConnType {
			t.Fatalf("unexpected auth type: %d", client.lastReq.Type)
		}
	})

	t.Run("non supermq topics bypass authorizer", func(t *testing.T) {
		client := &mockChannelsClient{authorized: false}
		a := newTestAuthorizer(client)

		if !a.CanPublish("client-1", "$queue/events") {
			t.Fatalf("expected non-supermq topic to bypass auth")
		}
		if client.lastReq != nil {
			t.Fatalf("expected no authorize call")
		}
	})

	t.Run("malformed supermq topic is denied", func(t *testing.T) {
		client := &mockChannelsClient{authorized: true}
		a := newTestAuthorizer(client)

		if a.CanPublish("client-1", "m/domain-1/c") {
			t.Fatalf("expected malformed topic to be denied")
		}
		if client.lastReq != nil {
			t.Fatalf("expected no authorize call for malformed topic")
		}
	})

	t.Run("rpc error denies and is tracked by circuit breaker", func(t *testing.T) {
		client := &mockChannelsClient{err: fmt.Errorf("connection refused")}
		a := newTestAuthorizer(client)

		if a.CanPublish("client-1", "m/domain-1/c/channel-1") {
			t.Fatalf("expected denial on rpc error")
		}
	})

	t.Run("circuit breaker opens after consecutive failures", func(t *testing.T) {
		client := &mockChannelsClient{err: fmt.Errorf("connection refused")}
		a := newTestAuthorizer(client)
		// Default gobreaker trips after 5 consecutive failures.
		a.cb = gobreaker.NewCircuitBreaker(gobreaker.Settings{
			Name: "test-trip",
			ReadyToTrip: func(counts gobreaker.Counts) bool {
				return counts.ConsecutiveFailures >= 3
			},
		})

		for range 3 {
			a.CanPublish("client-1", "m/domain-1/c/channel-1")
		}

		// Next call should be blocked by the open breaker without hitting the client.
		client.lastReq = nil
		if a.CanPublish("client-1", "m/domain-1/c/channel-1") {
			t.Fatalf("expected denial when circuit breaker is open")
		}
		if client.lastReq != nil {
			t.Fatalf("expected no rpc call when circuit breaker is open")
		}
	})
}
