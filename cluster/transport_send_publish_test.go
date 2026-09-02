// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package cluster

import (
	"context"
	"errors"
	"testing"

	"connectrpc.com/connect"
	corebroker "github.com/absmach/fluxmq/broker"
	"github.com/absmach/fluxmq/message"
	clusterv1 "github.com/absmach/fluxmq/pkg/proto/cluster/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// sendPublishTransport wires a transport to a peer that answers every directed
// publish with resp, or with err when err is set, counting the calls.
func sendPublishTransport(resp *clusterv1.PublishResponse, err error) (*Transport, *int) {
	calls := 0
	mock := &mockBrokerClient{
		routePublishFn: func(context.Context, *connect.Request[clusterv1.PublishRequest]) (*connect.Response[clusterv1.PublishResponse], error) {
			calls++
			if err != nil {
				return nil, err
			}
			return connect.NewResponse(resp), nil
		},
	}

	return newTestTransport(testNodeA, mock), &calls
}

func TestSendPublishSucceeds(t *testing.T) {
	tr, calls := sendPublishTransport(&clusterv1.PublishResponse{Success: true}, nil)

	msg := message.New(testTasksTopic, []byte("work"))
	defer message.Release(msg)

	require.NoError(t, tr.SendPublish(context.Background(), testNodeA, testWorkerA, msg))
	assert.Equal(t, 1, *calls)
}

// TestSendPublishDoesNotRetry guards the latency of a share group's choice: the
// group has other members waiting, so a member that cannot take the message
// must cost one round trip, not the retry budget's several seconds.
func TestSendPublishDoesNotRetry(t *testing.T) {
	cases := []struct {
		name string
		resp *clusterv1.PublishResponse
		err  error
	}{
		{
			name: "client-not-connected",
			resp: &clusterv1.PublishResponse{Success: false, Error: testClientGone, ClientNotConnected: true},
		},
		{
			name: "handler-failure",
			resp: &clusterv1.PublishResponse{Success: false, Error: "boom"},
		},
		{
			name: "transport-error",
			err:  errors.New("dial failed"),
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tr, calls := sendPublishTransport(tc.resp, tc.err)

			msg := message.New(testTasksTopic, []byte("work"))
			defer message.Release(msg)

			require.Error(t, tr.SendPublish(context.Background(), testNodeA, testWorkerA, msg))
			assert.Equal(t, 1, *calls, "one attempt, no retry")
		})
	}
}

// TestSendPublishReportsClientNotConnected checks the signal survives the wire,
// so the caller can tell a member that moved from a peer that is failing.
func TestSendPublishReportsClientNotConnected(t *testing.T) {
	tr, _ := sendPublishTransport(&clusterv1.PublishResponse{Success: false, Error: testClientGone, ClientNotConnected: true}, nil)

	msg := message.New(testTasksTopic, []byte("work"))
	defer message.Release(msg)

	err := tr.SendPublish(context.Background(), testNodeA, testWorkerA, msg)
	assert.True(t, corebroker.IsErrClientNotConnected(err), "got %v", err)
}

// TestSendPublishKeepsCircuitClosedForMovedClient is the reason the signal is
// carried structurally. A client that reconnects elsewhere leaves a stale owner
// entry behind, and the group keeps choosing it until the entry catches up.
// Counting those answers against the peer would open its circuit over a healthy
// node and take every other delivery to it down too.
func TestSendPublishKeepsCircuitClosedForMovedClient(t *testing.T) {
	notConnected := &clusterv1.PublishResponse{Success: false, Error: testClientGone, ClientNotConnected: true}

	calls := 0
	answer := notConnected
	mock := &mockBrokerClient{
		routePublishFn: func(context.Context, *connect.Request[clusterv1.PublishRequest]) (*connect.Response[clusterv1.PublishResponse], error) {
			calls++
			return connect.NewResponse(answer), nil
		},
	}
	tr := newTestTransport(testNodeA, mock)

	msg := message.New(testTasksTopic, []byte("work"))
	defer message.Release(msg)

	// Well past failureThreshold, which is what a stale owner entry produces.
	for range failureThreshold * 2 {
		require.Error(t, tr.SendPublish(context.Background(), testNodeA, testWorkerA, msg))
	}

	answer = &clusterv1.PublishResponse{Success: true}
	require.NoError(t, tr.SendPublish(context.Background(), testNodeA, testWorkerA, msg),
		"the peer was never at fault, so its circuit must still be closed")
	assert.Equal(t, failureThreshold*2+1, calls)
}

// TestSendPublishOpensCircuitForFailingPeer is the other half: a peer that
// really is failing must stop being chosen.
func TestSendPublishOpensCircuitForFailingPeer(t *testing.T) {
	calls := 0
	mock := &mockBrokerClient{
		routePublishFn: func(context.Context, *connect.Request[clusterv1.PublishRequest]) (*connect.Response[clusterv1.PublishResponse], error) {
			calls++
			return nil, errors.New("dial failed")
		},
	}
	tr := newTestTransport(testNodeA, mock)

	msg := message.New(testTasksTopic, []byte("work"))
	defer message.Release(msg)

	for range failureThreshold {
		require.Error(t, tr.SendPublish(context.Background(), testNodeA, testWorkerA, msg))
	}
	attempted := calls

	err := tr.SendPublish(context.Background(), testNodeA, testWorkerA, msg)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "circuit open")
	assert.Equal(t, attempted, calls, "an open circuit is not dialled")
}
