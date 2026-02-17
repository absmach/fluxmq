// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package cluster

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"sync/atomic"
	"testing"

	"connectrpc.com/connect"
	clusterv1 "github.com/absmach/fluxmq/pkg/proto/cluster/v1"
	"github.com/absmach/fluxmq/pkg/proto/cluster/v1/clusterv1connect"
)

// mockBrokerClient implements clusterv1connect.BrokerServiceClient for testing
// the sender-side batch logic. Only the batch RPC methods are wired; the rest panic.
type mockBrokerClient struct {
	clusterv1connect.BrokerServiceClient

	forwardPublishBatchFn func(context.Context, *connect.Request[clusterv1.ForwardPublishBatchRequest]) (*connect.Response[clusterv1.ForwardPublishBatchResponse], error)
	routePublishBatchFn   func(context.Context, *connect.Request[clusterv1.PublishBatchRequest]) (*connect.Response[clusterv1.PublishBatchResponse], error)
	routeQueueBatchFn     func(context.Context, *connect.Request[clusterv1.RouteQueueBatchRequest]) (*connect.Response[clusterv1.RouteQueueBatchResponse], error)
}

func (m *mockBrokerClient) ForwardPublishBatch(ctx context.Context, req *connect.Request[clusterv1.ForwardPublishBatchRequest]) (*connect.Response[clusterv1.ForwardPublishBatchResponse], error) {
	return m.forwardPublishBatchFn(ctx, req)
}

func (m *mockBrokerClient) RoutePublishBatch(ctx context.Context, req *connect.Request[clusterv1.PublishBatchRequest]) (*connect.Response[clusterv1.PublishBatchResponse], error) {
	return m.routePublishBatchFn(ctx, req)
}

func (m *mockBrokerClient) RouteQueueBatch(ctx context.Context, req *connect.Request[clusterv1.RouteQueueBatchRequest]) (*connect.Response[clusterv1.RouteQueueBatchResponse], error) {
	return m.routeQueueBatchFn(ctx, req)
}

func newTestTransport(nodeID string, mock *mockBrokerClient) *Transport {
	t := &Transport{
		nodeID:      "self",
		peerClients: map[string]clusterv1connect.BrokerServiceClient{nodeID: mock},
		breakers:    newPeerBreakers(),
		logger:      slog.New(slog.NewTextHandler(io.Discard, nil)),
		stopCh:      make(chan struct{}),
	}
	return t
}

// --- SendForwardPublishBatch tests ---

func TestSendForwardPublishBatch_AllSucceed(t *testing.T) {
	mock := &mockBrokerClient{
		forwardPublishBatchFn: func(_ context.Context, req *connect.Request[clusterv1.ForwardPublishBatchRequest]) (*connect.Response[clusterv1.ForwardPublishBatchResponse], error) {
			return connect.NewResponse(&clusterv1.ForwardPublishBatchResponse{
				Success:   true,
				Delivered: uint32(len(req.Msg.Messages)),
			}), nil
		},
	}
	tr := newTestTransport("peer1", mock)

	msgs := []*clusterv1.ForwardPublishRequest{
		{Topic: "a/b", Payload: []byte("1")},
		{Topic: "c/d", Payload: []byte("2")},
	}
	if err := tr.SendForwardPublishBatch(context.Background(), "peer1", msgs); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestSendForwardPublishBatch_EmptyBatch(t *testing.T) {
	tr := newTestTransport("peer1", &mockBrokerClient{})
	if err := tr.SendForwardPublishBatch(context.Background(), "peer1", nil); err != nil {
		t.Fatalf("unexpected error for empty batch: %v", err)
	}
}

func TestSendForwardPublishBatch_PartialFailureRetriesSubset(t *testing.T) {
	var callCount atomic.Int32

	mock := &mockBrokerClient{
		forwardPublishBatchFn: func(_ context.Context, req *connect.Request[clusterv1.ForwardPublishBatchRequest]) (*connect.Response[clusterv1.ForwardPublishBatchResponse], error) {
			call := callCount.Add(1)
			msgs := req.Msg.Messages

			if call == 1 {
				// First call: 3 messages, index 1 fails
				if len(msgs) != 3 {
					t.Errorf("call 1: expected 3 messages, got %d", len(msgs))
				}
				return connect.NewResponse(&clusterv1.ForwardPublishBatchResponse{
					Success:   false,
					Delivered: 2,
					Error:     "partial failure",
					Failures: []*clusterv1.ForwardPublishBatchError{
						{Index: 1, Topic: "b/c", Error: "transient"},
					},
				}), nil
			}

			// Second call: only the failed message
			if len(msgs) != 1 {
				t.Errorf("call 2: expected 1 message (retry subset), got %d", len(msgs))
			}
			if msgs[0].Topic != "b/c" {
				t.Errorf("call 2: expected topic b/c, got %s", msgs[0].Topic)
			}
			return connect.NewResponse(&clusterv1.ForwardPublishBatchResponse{
				Success:   true,
				Delivered: 1,
			}), nil
		},
	}
	tr := newTestTransport("peer1", mock)

	msgs := []*clusterv1.ForwardPublishRequest{
		{Topic: "a/b", Payload: []byte("1")},
		{Topic: "b/c", Payload: []byte("2")},
		{Topic: "c/d", Payload: []byte("3")},
	}
	if err := tr.SendForwardPublishBatch(context.Background(), "peer1", msgs); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if c := callCount.Load(); c != 2 {
		t.Fatalf("expected 2 RPC calls, got %d", c)
	}
}

func TestSendForwardPublishBatch_TransportError(t *testing.T) {
	mock := &mockBrokerClient{
		forwardPublishBatchFn: func(context.Context, *connect.Request[clusterv1.ForwardPublishBatchRequest]) (*connect.Response[clusterv1.ForwardPublishBatchResponse], error) {
			return nil, fmt.Errorf("connection refused")
		},
	}
	tr := newTestTransport("peer1", mock)

	msgs := []*clusterv1.ForwardPublishRequest{{Topic: "a/b"}}
	err := tr.SendForwardPublishBatch(context.Background(), "peer1", msgs)
	if err == nil {
		t.Fatal("expected transport error to propagate")
	}
}

func TestSendForwardPublishBatch_ExhaustsPartialRetries(t *testing.T) {
	var callCount atomic.Int32

	mock := &mockBrokerClient{
		forwardPublishBatchFn: func(_ context.Context, req *connect.Request[clusterv1.ForwardPublishBatchRequest]) (*connect.Response[clusterv1.ForwardPublishBatchResponse], error) {
			callCount.Add(1)
			// Always fail index 0
			return connect.NewResponse(&clusterv1.ForwardPublishBatchResponse{
				Success: false,
				Error:   "always fails",
				Failures: []*clusterv1.ForwardPublishBatchError{
					{Index: 0, Error: "persistent error"},
				},
			}), nil
		},
	}
	tr := newTestTransport("peer1", mock)

	msgs := []*clusterv1.ForwardPublishRequest{{Topic: "fail/always", Qos: 0}}
	err := tr.SendForwardPublishBatch(context.Background(), "peer1", msgs)
	if err != nil {
		t.Fatalf("expected nil error for QoS 0 best-effort retries, got: %v", err)
	}
	if c := callCount.Load(); int(c) != maxPartialRetries {
		t.Fatalf("expected %d partial retry calls, got %d", maxPartialRetries, c)
	}
}

func TestSendForwardPublishBatch_ExhaustsPartialRetries_QoS1ReturnsError(t *testing.T) {
	var callCount atomic.Int32

	mock := &mockBrokerClient{
		forwardPublishBatchFn: func(_ context.Context, req *connect.Request[clusterv1.ForwardPublishBatchRequest]) (*connect.Response[clusterv1.ForwardPublishBatchResponse], error) {
			callCount.Add(1)
			return connect.NewResponse(&clusterv1.ForwardPublishBatchResponse{
				Success: false,
				Error:   "always fails",
				Failures: []*clusterv1.ForwardPublishBatchError{
					{Index: 0, Error: "persistent error"},
				},
			}), nil
		},
	}
	tr := newTestTransport("peer1", mock)

	msgs := []*clusterv1.ForwardPublishRequest{{Topic: "fail/always", Qos: 1}}
	if err := tr.SendForwardPublishBatch(context.Background(), "peer1", msgs); err == nil {
		t.Fatal("expected error for QoS > 0 after exhausted partial retries")
	}
	if c := callCount.Load(); int(c) != maxPartialRetries {
		t.Fatalf("expected %d partial retry calls, got %d", maxPartialRetries, c)
	}
}

func TestSendForwardPublishBatch_FailedWithoutIndexedFailures(t *testing.T) {
	mock := &mockBrokerClient{
		forwardPublishBatchFn: func(_ context.Context, _ *connect.Request[clusterv1.ForwardPublishBatchRequest]) (*connect.Response[clusterv1.ForwardPublishBatchResponse], error) {
			return connect.NewResponse(&clusterv1.ForwardPublishBatchResponse{
				Success: false,
				Error:   "no forward publish handler configured",
			}), nil
		},
	}
	tr := newTestTransport("peer1", mock)

	msgs := []*clusterv1.ForwardPublishRequest{{Topic: "a/b", Qos: 1}}
	if err := tr.SendForwardPublishBatch(context.Background(), "peer1", msgs); err == nil {
		t.Fatal("expected error when response is unsuccessful without indexed failures")
	}
}

// --- SendPublishBatch tests ---

func TestSendPublishBatch_AllSucceed(t *testing.T) {
	mock := &mockBrokerClient{
		routePublishBatchFn: func(_ context.Context, req *connect.Request[clusterv1.PublishBatchRequest]) (*connect.Response[clusterv1.PublishBatchResponse], error) {
			return connect.NewResponse(&clusterv1.PublishBatchResponse{
				Success:   true,
				Delivered: uint32(len(req.Msg.Messages)),
			}), nil
		},
	}
	tr := newTestTransport("peer1", mock)

	msgs := []*clusterv1.PublishRequest{
		{ClientId: "c1", Topic: "a/b", Payload: []byte("1")},
		{ClientId: "c2", Topic: "c/d", Payload: []byte("2")},
	}
	if err := tr.SendPublishBatch(context.Background(), "peer1", msgs); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestSendPublishBatch_EmptyBatch(t *testing.T) {
	tr := newTestTransport("peer1", &mockBrokerClient{})
	if err := tr.SendPublishBatch(context.Background(), "peer1", nil); err != nil {
		t.Fatalf("unexpected error for empty batch: %v", err)
	}
}

func TestSendPublishBatch_PartialFailureRetriesSubset(t *testing.T) {
	var callCount atomic.Int32

	mock := &mockBrokerClient{
		routePublishBatchFn: func(_ context.Context, req *connect.Request[clusterv1.PublishBatchRequest]) (*connect.Response[clusterv1.PublishBatchResponse], error) {
			call := callCount.Add(1)
			msgs := req.Msg.Messages

			if call == 1 {
				if len(msgs) != 3 {
					t.Errorf("call 1: expected 3 messages, got %d", len(msgs))
				}
				return connect.NewResponse(&clusterv1.PublishBatchResponse{
					Success:   false,
					Delivered: 2,
					Error:     "partial failure",
					Failures: []*clusterv1.PublishBatchError{
						{Index: 2, ClientId: "c3", Error: "client disconnected"},
					},
				}), nil
			}

			if len(msgs) != 1 {
				t.Errorf("call 2: expected 1 message, got %d", len(msgs))
			}
			if msgs[0].ClientId != "c3" {
				t.Errorf("call 2: expected client c3, got %s", msgs[0].ClientId)
			}
			return connect.NewResponse(&clusterv1.PublishBatchResponse{
				Success:   true,
				Delivered: 1,
			}), nil
		},
	}
	tr := newTestTransport("peer1", mock)

	msgs := []*clusterv1.PublishRequest{
		{ClientId: "c1", Topic: "a"},
		{ClientId: "c2", Topic: "b"},
		{ClientId: "c3", Topic: "c"},
	}
	if err := tr.SendPublishBatch(context.Background(), "peer1", msgs); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if c := callCount.Load(); c != 2 {
		t.Fatalf("expected 2 RPC calls, got %d", c)
	}
}

func TestSendPublishBatch_TransportError(t *testing.T) {
	mock := &mockBrokerClient{
		routePublishBatchFn: func(context.Context, *connect.Request[clusterv1.PublishBatchRequest]) (*connect.Response[clusterv1.PublishBatchResponse], error) {
			return nil, fmt.Errorf("connection refused")
		},
	}
	tr := newTestTransport("peer1", mock)

	msgs := []*clusterv1.PublishRequest{{ClientId: "c1", Topic: "a"}}
	if err := tr.SendPublishBatch(context.Background(), "peer1", msgs); err == nil {
		t.Fatal("expected transport error to propagate")
	}
}

func TestSendPublishBatch_ExhaustsPartialRetries_QoS0BestEffort(t *testing.T) {
	var callCount atomic.Int32
	mock := &mockBrokerClient{
		routePublishBatchFn: func(_ context.Context, _ *connect.Request[clusterv1.PublishBatchRequest]) (*connect.Response[clusterv1.PublishBatchResponse], error) {
			callCount.Add(1)
			return connect.NewResponse(&clusterv1.PublishBatchResponse{
				Success: false,
				Error:   "always fails",
				Failures: []*clusterv1.PublishBatchError{
					{Index: 0, ClientId: "c1", Error: "persistent"},
				},
			}), nil
		},
	}
	tr := newTestTransport("peer1", mock)

	msgs := []*clusterv1.PublishRequest{{ClientId: "c1", Topic: "a", Qos: 0}}
	if err := tr.SendPublishBatch(context.Background(), "peer1", msgs); err != nil {
		t.Fatalf("expected nil error for QoS 0 best-effort retries, got: %v", err)
	}
	if c := callCount.Load(); int(c) != maxPartialRetries {
		t.Fatalf("expected %d partial retry calls, got %d", maxPartialRetries, c)
	}
}

func TestSendPublishBatch_ExhaustsPartialRetries_QoS1ReturnsError(t *testing.T) {
	var callCount atomic.Int32
	mock := &mockBrokerClient{
		routePublishBatchFn: func(_ context.Context, _ *connect.Request[clusterv1.PublishBatchRequest]) (*connect.Response[clusterv1.PublishBatchResponse], error) {
			callCount.Add(1)
			return connect.NewResponse(&clusterv1.PublishBatchResponse{
				Success: false,
				Error:   "always fails",
				Failures: []*clusterv1.PublishBatchError{
					{Index: 0, ClientId: "c1", Error: "persistent"},
				},
			}), nil
		},
	}
	tr := newTestTransport("peer1", mock)

	msgs := []*clusterv1.PublishRequest{{ClientId: "c1", Topic: "a", Qos: 1}}
	if err := tr.SendPublishBatch(context.Background(), "peer1", msgs); err == nil {
		t.Fatal("expected error for QoS > 0 after exhausted partial retries")
	}
	if c := callCount.Load(); int(c) != maxPartialRetries {
		t.Fatalf("expected %d partial retry calls, got %d", maxPartialRetries, c)
	}
}

func TestSendPublishBatch_FailedWithoutIndexedFailures(t *testing.T) {
	mock := &mockBrokerClient{
		routePublishBatchFn: func(_ context.Context, _ *connect.Request[clusterv1.PublishBatchRequest]) (*connect.Response[clusterv1.PublishBatchResponse], error) {
			return connect.NewResponse(&clusterv1.PublishBatchResponse{
				Success: false,
				Error:   "no handler configured",
			}), nil
		},
	}
	tr := newTestTransport("peer1", mock)

	msgs := []*clusterv1.PublishRequest{{ClientId: "c1", Topic: "a", Qos: 1}}
	if err := tr.SendPublishBatch(context.Background(), "peer1", msgs); err == nil {
		t.Fatal("expected error when response is unsuccessful without indexed failures")
	}
}

// --- SendRouteQueueBatch tests ---

func TestSendRouteQueueBatch_AllSucceed(t *testing.T) {
	mock := &mockBrokerClient{
		routeQueueBatchFn: func(_ context.Context, req *connect.Request[clusterv1.RouteQueueBatchRequest]) (*connect.Response[clusterv1.RouteQueueBatchResponse], error) {
			return connect.NewResponse(&clusterv1.RouteQueueBatchResponse{
				Success:   true,
				Delivered: uint32(len(req.Msg.Messages)),
			}), nil
		},
	}
	tr := newTestTransport("peer1", mock)

	deliveries := []QueueDelivery{
		{ClientID: "c1", QueueName: "q1", Message: &QueueMessage{MessageID: "m1", Payload: []byte("1")}},
		{ClientID: "c2", QueueName: "q2", Message: &QueueMessage{MessageID: "m2", Payload: []byte("2")}},
	}
	if err := tr.SendRouteQueueBatch(context.Background(), "peer1", deliveries); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestSendRouteQueueBatch_EmptyBatch(t *testing.T) {
	tr := newTestTransport("peer1", &mockBrokerClient{})
	if err := tr.SendRouteQueueBatch(context.Background(), "peer1", nil); err != nil {
		t.Fatalf("unexpected error for empty batch: %v", err)
	}
}

func TestSendRouteQueueBatch_PartialFailureRetriesSubset(t *testing.T) {
	var callCount atomic.Int32

	mock := &mockBrokerClient{
		routeQueueBatchFn: func(_ context.Context, req *connect.Request[clusterv1.RouteQueueBatchRequest]) (*connect.Response[clusterv1.RouteQueueBatchResponse], error) {
			call := callCount.Add(1)
			msgs := req.Msg.Messages

			if call == 1 {
				if len(msgs) != 2 {
					t.Errorf("call 1: expected 2 messages, got %d", len(msgs))
				}
				return connect.NewResponse(&clusterv1.RouteQueueBatchResponse{
					Success:   false,
					Delivered: 1,
					Error:     "partial",
					Failures: []*clusterv1.RouteQueueBatchError{
						{Index: 0, ClientId: "c1", QueueName: "q1", Error: "busy"},
					},
				}), nil
			}

			if len(msgs) != 1 {
				t.Errorf("call 2: expected 1 message, got %d", len(msgs))
			}
			return connect.NewResponse(&clusterv1.RouteQueueBatchResponse{
				Success:   true,
				Delivered: 1,
			}), nil
		},
	}
	tr := newTestTransport("peer1", mock)

	deliveries := []QueueDelivery{
		{ClientID: "c1", QueueName: "q1", Message: &QueueMessage{MessageID: "m1", Payload: []byte("1")}},
		{ClientID: "c2", QueueName: "q2", Message: &QueueMessage{MessageID: "m2", Payload: []byte("2")}},
	}
	if err := tr.SendRouteQueueBatch(context.Background(), "peer1", deliveries); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if c := callCount.Load(); c != 2 {
		t.Fatalf("expected 2 RPC calls, got %d", c)
	}
}

func TestSendRouteQueueBatch_TransportError(t *testing.T) {
	mock := &mockBrokerClient{
		routeQueueBatchFn: func(context.Context, *connect.Request[clusterv1.RouteQueueBatchRequest]) (*connect.Response[clusterv1.RouteQueueBatchResponse], error) {
			return nil, fmt.Errorf("connection refused")
		},
	}
	tr := newTestTransport("peer1", mock)

	deliveries := []QueueDelivery{
		{ClientID: "c1", QueueName: "q1", Message: &QueueMessage{MessageID: "m1", Payload: []byte("1")}},
	}
	if err := tr.SendRouteQueueBatch(context.Background(), "peer1", deliveries); err == nil {
		t.Fatal("expected transport error to propagate")
	}
}

func TestSendRouteQueueBatch_ExhaustsPartialRetriesReturnsError(t *testing.T) {
	var callCount atomic.Int32
	mock := &mockBrokerClient{
		routeQueueBatchFn: func(_ context.Context, _ *connect.Request[clusterv1.RouteQueueBatchRequest]) (*connect.Response[clusterv1.RouteQueueBatchResponse], error) {
			callCount.Add(1)
			return connect.NewResponse(&clusterv1.RouteQueueBatchResponse{
				Success: false,
				Error:   "always fails",
				Failures: []*clusterv1.RouteQueueBatchError{
					{Index: 0, ClientId: "c1", QueueName: "q1", Error: "persistent"},
				},
			}), nil
		},
	}
	tr := newTestTransport("peer1", mock)

	deliveries := []QueueDelivery{
		{ClientID: "c1", QueueName: "q1", Message: &QueueMessage{MessageID: "m1", Payload: []byte("1")}},
	}
	if err := tr.SendRouteQueueBatch(context.Background(), "peer1", deliveries); err == nil {
		t.Fatal("expected error after exhausted partial retries")
	}
	if c := callCount.Load(); int(c) != maxPartialRetries {
		t.Fatalf("expected %d partial retry calls, got %d", maxPartialRetries, c)
	}
}

func TestSendRouteQueueBatch_FailedWithoutIndexedFailures(t *testing.T) {
	mock := &mockBrokerClient{
		routeQueueBatchFn: func(_ context.Context, _ *connect.Request[clusterv1.RouteQueueBatchRequest]) (*connect.Response[clusterv1.RouteQueueBatchResponse], error) {
			return connect.NewResponse(&clusterv1.RouteQueueBatchResponse{
				Success: false,
				Error:   "no queue handler configured",
			}), nil
		},
	}
	tr := newTestTransport("peer1", mock)

	deliveries := []QueueDelivery{
		{ClientID: "c1", QueueName: "q1", Message: &QueueMessage{MessageID: "m1", Payload: []byte("1")}},
	}
	if err := tr.SendRouteQueueBatch(context.Background(), "peer1", deliveries); err == nil {
		t.Fatal("expected error when response is unsuccessful without indexed failures")
	}
}

func TestSendRouteQueueBatch_PartialFailureWithNilDeliveryRetriesCorrectSubset(t *testing.T) {
	var callCount atomic.Int32
	mock := &mockBrokerClient{
		routeQueueBatchFn: func(_ context.Context, req *connect.Request[clusterv1.RouteQueueBatchRequest]) (*connect.Response[clusterv1.RouteQueueBatchResponse], error) {
			call := callCount.Add(1)

			switch call {
			case 1:
				if len(req.Msg.Messages) != 2 {
					t.Errorf("call 1: expected 2 wire messages, got %d", len(req.Msg.Messages))
				}
				return connect.NewResponse(&clusterv1.RouteQueueBatchResponse{
					Success: false,
					Error:   "partial",
					Failures: []*clusterv1.RouteQueueBatchError{
						{Index: 0, ClientId: "c1", QueueName: "q1", Error: "transient"},
					},
				}), nil
			case 2:
				if len(req.Msg.Messages) != 1 {
					t.Errorf("call 2: expected 1 wire message, got %d", len(req.Msg.Messages))
				}
				if len(req.Msg.Messages) == 1 && req.Msg.Messages[0].ClientId != "c1" {
					t.Errorf("call 2: expected retry for client c1, got %s", req.Msg.Messages[0].ClientId)
				}
				return connect.NewResponse(&clusterv1.RouteQueueBatchResponse{
					Success: true,
				}), nil
			default:
				t.Errorf("unexpected call %d", call)
				return connect.NewResponse(&clusterv1.RouteQueueBatchResponse{
					Success: true,
				}), nil
			}
		},
	}
	tr := newTestTransport("peer1", mock)

	deliveries := []QueueDelivery{
		{ClientID: "nil", QueueName: "q0", Message: nil},
		{ClientID: "c1", QueueName: "q1", Message: &QueueMessage{MessageID: "m1", Payload: []byte("1")}},
		{ClientID: "c2", QueueName: "q2", Message: &QueueMessage{MessageID: "m2", Payload: []byte("2")}},
	}

	if err := tr.SendRouteQueueBatch(context.Background(), "peer1", deliveries); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if c := callCount.Load(); c != 2 {
		t.Fatalf("expected 2 RPC calls, got %d", c)
	}
}

// --- Multi-index partial failure ---

func TestSendForwardPublishBatch_MultipleFailuresRetried(t *testing.T) {
	var callCount atomic.Int32

	mock := &mockBrokerClient{
		forwardPublishBatchFn: func(_ context.Context, req *connect.Request[clusterv1.ForwardPublishBatchRequest]) (*connect.Response[clusterv1.ForwardPublishBatchResponse], error) {
			call := callCount.Add(1)
			msgs := req.Msg.Messages

			if call == 1 {
				// 5 messages, indices 0, 2, 4 fail
				if len(msgs) != 5 {
					t.Errorf("call 1: expected 5 messages, got %d", len(msgs))
				}
				return connect.NewResponse(&clusterv1.ForwardPublishBatchResponse{
					Success:   false,
					Delivered: 2,
					Error:     "partial",
					Failures: []*clusterv1.ForwardPublishBatchError{
						{Index: 0, Error: "fail"},
						{Index: 2, Error: "fail"},
						{Index: 4, Error: "fail"},
					},
				}), nil
			}

			// Second call: exactly the 3 failed messages
			if len(msgs) != 3 {
				t.Errorf("call 2: expected 3 messages, got %d", len(msgs))
			}
			wantTopics := []string{"t0", "t2", "t4"}
			for i, want := range wantTopics {
				if msgs[i].Topic != want {
					t.Errorf("call 2: msg[%d] topic = %s, want %s", i, msgs[i].Topic, want)
				}
			}
			return connect.NewResponse(&clusterv1.ForwardPublishBatchResponse{
				Success:   true,
				Delivered: 3,
			}), nil
		},
	}
	tr := newTestTransport("peer1", mock)

	msgs := make([]*clusterv1.ForwardPublishRequest, 5)
	for i := range msgs {
		msgs[i] = &clusterv1.ForwardPublishRequest{Topic: fmt.Sprintf("t%d", i)}
	}
	if err := tr.SendForwardPublishBatch(context.Background(), "peer1", msgs); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if c := callCount.Load(); c != 2 {
		t.Fatalf("expected 2 RPC calls, got %d", c)
	}
}

func TestSendForwardPublishBatch_ShrinkingRetries(t *testing.T) {
	// Each retry fixes one message, batch shrinks: 3 → 2 → 1 → success
	var callCount atomic.Int32

	mock := &mockBrokerClient{
		forwardPublishBatchFn: func(_ context.Context, req *connect.Request[clusterv1.ForwardPublishBatchRequest]) (*connect.Response[clusterv1.ForwardPublishBatchResponse], error) {
			call := callCount.Add(1)
			msgs := req.Msg.Messages

			switch call {
			case 1:
				if len(msgs) != 3 {
					t.Errorf("call 1: expected 3, got %d", len(msgs))
				}
				// All 3 fail
				failures := make([]*clusterv1.ForwardPublishBatchError, len(msgs))
				for i := range msgs {
					failures[i] = &clusterv1.ForwardPublishBatchError{Index: uint32(i), Error: "fail"}
				}
				return connect.NewResponse(&clusterv1.ForwardPublishBatchResponse{
					Success: false, Failures: failures, Error: "all failed",
				}), nil
			case 2:
				if len(msgs) != 3 {
					t.Errorf("call 2: expected 3, got %d", len(msgs))
				}
				// Index 0 succeeds, 1 and 2 still fail
				return connect.NewResponse(&clusterv1.ForwardPublishBatchResponse{
					Success: false, Delivered: 1, Error: "partial",
					Failures: []*clusterv1.ForwardPublishBatchError{
						{Index: 1, Error: "fail"},
						{Index: 2, Error: "fail"},
					},
				}), nil
			case 3:
				if len(msgs) != 2 {
					t.Errorf("call 3: expected 2, got %d", len(msgs))
				}
				return connect.NewResponse(&clusterv1.ForwardPublishBatchResponse{
					Success: true, Delivered: 2,
				}), nil
			default:
				t.Errorf("unexpected call %d", call)
				return connect.NewResponse(&clusterv1.ForwardPublishBatchResponse{Success: true}), nil
			}
		},
	}
	tr := newTestTransport("peer1", mock)

	msgs := []*clusterv1.ForwardPublishRequest{
		{Topic: "a"}, {Topic: "b"}, {Topic: "c"},
	}
	if err := tr.SendForwardPublishBatch(context.Background(), "peer1", msgs); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if c := callCount.Load(); c != 3 {
		t.Fatalf("expected 3 calls, got %d", c)
	}
}
