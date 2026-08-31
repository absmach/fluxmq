// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package queue

import (
	"context"
	"fmt"
	"testing"
	"time"

	clusterv1 "github.com/absmach/fluxmq/pkg/proto/cluster/v1"
	"github.com/stretchr/testify/require"
)

type recordingGroupForwarder struct {
	nodeID    string
	queueName string
	op        *clusterv1.GroupOperation
}

func (f *recordingGroupForwarder) ForwardGroupOp(_ context.Context, nodeID, queueName string, op *clusterv1.GroupOperation) error {
	f.nodeID, f.queueName, f.op = nodeID, queueName, op
	return nil
}

func TestRaftGroupStoreRequeueRoutesThroughLeader(t *testing.T) {
	ctx := context.Background()
	attemptedAt := time.Date(2026, 8, 31, 12, 0, 0, 123, time.UTC)
	coordinator := &mockQueueCoordinator{
		enabled:           true,
		replicatedByQueue: map[string]bool{testQueueEvents: true},
		leaderByQueue:     map[string]bool{testQueueEvents: true},
	}
	store := newRaftGroupStore(newMockGroupStore())
	store.SetCoordinator(coordinator)

	require.NoError(t, store.RequeuePendingEntry(ctx, testQueueEvents, testGroupWorkers, testGroupConsumerA, 42, attemptedAt))
	require.Equal(t, []string{
		fmt.Sprintf("%s/%s/%s/%d/%d", testQueueEvents, testGroupWorkers, testGroupConsumerA, 42, attemptedAt.UnixNano()),
	}, coordinator.requeueCalls)
}

func TestRaftGroupStoreRequeueForwardsFromFollower(t *testing.T) {
	ctx := context.Background()
	attemptedAt := time.Date(2026, 8, 31, 12, 0, 0, 123, time.UTC)
	coordinator := &mockQueueCoordinator{
		enabled:           true,
		replicatedByQueue: map[string]bool{testQueueEvents: true},
		leaderByQueue:     map[string]bool{testQueueEvents: false},
		leaderIDByQueue:   map[string]string{testQueueEvents: testNode2},
	}
	forwarder := new(recordingGroupForwarder)
	store := newRaftGroupStore(newMockGroupStore())
	store.SetCoordinator(coordinator)
	store.SetForwarder(forwarder)

	require.NoError(t, store.RequeuePendingEntry(ctx, testQueueEvents, testGroupWorkers, testGroupConsumerA, 42, attemptedAt))
	require.Equal(t, testNode2, forwarder.nodeID)
	require.Equal(t, testQueueEvents, forwarder.queueName)
	require.NotNil(t, forwarder.op)
	require.True(t, attemptedAt.Equal(forwarder.op.GetTimestamp().AsTime()))
	payload := forwarder.op.GetRequeuePending()
	require.NotNil(t, payload)
	require.Equal(t, testGroupConsumerA, payload.GetConsumerId())
	require.Equal(t, uint64(42), payload.GetOffset())
}
