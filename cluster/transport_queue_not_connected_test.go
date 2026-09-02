// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package cluster

import (
	"context"
	"errors"
	"testing"

	"connectrpc.com/connect"
	corebroker "github.com/absmach/fluxmq/broker"
	clusterv1 "github.com/absmach/fluxmq/pkg/proto/cluster/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSendRouteQueueMessageDoesNotRetryMovedConsumer guards the cost of a
// consumer that has gone. The caller's answer to this reply is to unregister
// the consumer, so the retry budget buys nothing and spends seconds.
func TestSendRouteQueueMessageDoesNotRetryMovedConsumer(t *testing.T) {
	calls := 0
	mock := &mockBrokerClient{
		routeQueueMessageFn: func(context.Context, *connect.Request[clusterv1.RouteQueueMessageRequest]) (*connect.Response[clusterv1.RouteQueueMessageResponse], error) {
			calls++

			return connect.NewResponse(&clusterv1.RouteQueueMessageResponse{
				Success:            false,
				Error:              testNoSession,
				ClientNotConnected: true,
			}), nil
		},
	}
	tr := newTestTransport(testNodeA, mock)

	delivery := newQueueDelivery(testWorkerA, testOrders, "m1", "1")
	err := tr.SendRouteQueueMessage(context.Background(), testNodeA, testWorkerA, delivery.Message)

	require.Error(t, err)
	assert.True(t, corebroker.IsErrClientNotConnected(err), "got %v", err)
	assert.Equal(t, 1, calls, "one attempt, no retry")
}

// TestSendRouteQueueMessageKeepsCircuitClosedForMovedConsumer is the other half
// of not retrying: the peer answered correctly, so counting the answer against
// it would open its circuit and stop deliveries to consumers that are there.
func TestSendRouteQueueMessageKeepsCircuitClosedForMovedConsumer(t *testing.T) {
	answer := &clusterv1.RouteQueueMessageResponse{
		Success:            false,
		Error:              testNoSession,
		ClientNotConnected: true,
	}
	mock := &mockBrokerClient{
		routeQueueMessageFn: func(context.Context, *connect.Request[clusterv1.RouteQueueMessageRequest]) (*connect.Response[clusterv1.RouteQueueMessageResponse], error) {
			return connect.NewResponse(answer), nil
		},
	}
	tr := newTestTransport(testNodeA, mock)

	delivery := newQueueDelivery(testWorkerA, testOrders, "m1", "1")
	for range failureThreshold * 2 {
		require.Error(t, tr.SendRouteQueueMessage(context.Background(), testNodeA, testWorkerA, delivery.Message))
	}

	answer = &clusterv1.RouteQueueMessageResponse{Success: true}
	require.NoError(t, tr.SendRouteQueueMessage(context.Background(), testNodeA, testWorkerA, delivery.Message),
		"the peer was never at fault, so its circuit must still be closed")
}

// TestSendRouteQueueMessageStillRetriesPeerFailure keeps the retry budget where
// it belongs: a peer that fails to answer may answer the next attempt.
func TestSendRouteQueueMessageStillRetriesPeerFailure(t *testing.T) {
	calls := 0
	mock := &mockBrokerClient{
		routeQueueMessageFn: func(context.Context, *connect.Request[clusterv1.RouteQueueMessageRequest]) (*connect.Response[clusterv1.RouteQueueMessageResponse], error) {
			calls++

			return nil, errors.New("dial failed")
		},
	}
	tr := newTestTransport(testNodeA, mock)

	delivery := newQueueDelivery(testWorkerA, testOrders, "m1", "1")
	require.Error(t, tr.SendRouteQueueMessage(context.Background(), testNodeA, testWorkerA, delivery.Message))
	assert.Equal(t, maxRetries, calls, "a peer that did not answer is worth retrying")
}

// TestSendRouteQueueBatchStopsWhenEveryTargetIsGone covers the batch path, where
// the wasted rounds are whole RPCs rather than retries of one message.
func TestSendRouteQueueBatchStopsWhenEveryTargetIsGone(t *testing.T) {
	calls := 0
	mock := &mockBrokerClient{
		routeQueueBatchFn: func(context.Context, *connect.Request[clusterv1.RouteQueueBatchRequest]) (*connect.Response[clusterv1.RouteQueueBatchResponse], error) {
			calls++

			return connect.NewResponse(&clusterv1.RouteQueueBatchResponse{
				Success: false,
				Error:   testAlwaysFails,
				Failures: []*clusterv1.RouteQueueBatchError{
					{Index: 0, ClientId: testWorkerA, QueueName: testOrders, Error: testNoSession, ClientNotConnected: true},
				},
			}), nil
		},
	}
	tr := newTestTransport(testNodeA, mock)

	deliveries := []QueueDelivery{newQueueDelivery(testWorkerA, testOrders, "m1", "1")}
	err := tr.SendRouteQueueBatch(context.Background(), testNodeA, deliveries)

	require.Error(t, err)
	assert.True(t, corebroker.IsErrClientNotConnected(err), "got %v", err)
	assert.Equal(t, 1, calls, "one round, not the whole partial retry budget")
}
