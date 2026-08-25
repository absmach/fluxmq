// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package queue

import (
	"testing"
	"time"

	clusterv1 "github.com/absmach/fluxmq/pkg/proto/cluster/v1"
	"github.com/absmach/fluxmq/queue/raft"
	"github.com/absmach/fluxmq/queue/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Every consumer group mutation that can be forwarded to the leader must survive
// the wire unchanged. A field added to raft.Operation without a matching wire
// field would silently drop here, which is exactly what the previous
// JSON-blob-in-a-protobuf encoding made invisible.
func TestGroupOperationRoundTrip(t *testing.T) {
	claimedAt := time.Date(2026, 8, 25, 12, 0, 0, 0, time.UTC)

	tests := []struct {
		name string
		op   *raft.Operation
	}{
		{
			name: "update cursor",
			op:   &raft.Operation{Type: raft.OpUpdateCursor, QueueName: testQueueJobs, GroupID: testGroupWorkers, Cursor: 17},
		},
		{
			name: "update committed",
			op:   &raft.Operation{Type: raft.OpUpdateCommitted, QueueName: testQueueJobs, GroupID: testGroupWorkers, Committed: 9},
		},
		{
			name: "delete group",
			op:   &raft.Operation{Type: raft.OpDeleteGroup, QueueName: testQueueJobs, GroupID: testGroupWorkers},
		},
		{
			name: "add pending",
			op: &raft.Operation{
				Type: raft.OpAddPending, QueueName: testQueueJobs, GroupID: testGroupWorkers,
				PendingEntry: &types.PendingEntry{
					Offset: 42, ConsumerID: testGroupConsumerA, ClaimedAt: claimedAt, DeliveryCount: 3,
				},
			},
		},
		{
			name: "remove pending",
			op: &raft.Operation{
				Type: raft.OpRemovePending, QueueName: testQueueJobs, GroupID: testGroupWorkers,
				ConsumerID: testGroupConsumerA, Offset: 42,
			},
		},
		{
			name: "transfer pending",
			op: &raft.Operation{
				Type: raft.OpTransferPending, QueueName: testQueueJobs, GroupID: testGroupWorkers,
				Offset: 42, FromConsumer: testGroupConsumerA, ToConsumer: testGroupConsumerB,
			},
		},
		{
			name: "register consumer",
			op: &raft.Operation{
				Type: raft.OpRegisterConsumer, QueueName: testQueueJobs, GroupID: testGroupWorkers,
				ConsumerInfo: &types.ConsumerInfo{
					ID: testGroupConsumerA, ClientID: "client-1", ProxyNodeID: testNode2,
					RegisteredAt: claimedAt, LastHeartbeat: claimedAt.Add(time.Minute),
				},
			},
		},
		{
			name: "unregister consumer",
			op: &raft.Operation{
				Type: raft.OpUnregisterConsumer, QueueName: testQueueJobs, GroupID: testGroupWorkers, ConsumerID: testGroupConsumerA,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			wire, err := encodeGroupOperation(tt.op)
			require.NoError(t, err)
			require.NotNil(t, wire.Operation, "the oneof must name the mutation")

			decoded, err := decodeGroupOperation(wire)
			require.NoError(t, err)

			assert.Equal(t, tt.op.Type, decoded.Type)
			assert.Equal(t, tt.op.QueueName, decoded.QueueName)
			assert.Equal(t, tt.op.GroupID, decoded.GroupID)
			assert.Equal(t, tt.op.ConsumerID, decoded.ConsumerID)
			assert.Equal(t, tt.op.Cursor, decoded.Cursor)
			assert.Equal(t, tt.op.Committed, decoded.Committed)
			assert.Equal(t, tt.op.Offset, decoded.Offset)
			assert.Equal(t, tt.op.FromConsumer, decoded.FromConsumer)
			assert.Equal(t, tt.op.ToConsumer, decoded.ToConsumer)
			assert.Equal(t, tt.op.PendingEntry, decoded.PendingEntry)
			assert.Equal(t, tt.op.ConsumerInfo, decoded.ConsumerInfo)
		})
	}
}

// A group's replicated state includes its cursor, pending list and membership.
// The wire form flattens the pending list, so the per-consumer grouping has to
// be rebuilt exactly.
func TestGroupStateRoundTripPreservesPELAndMembership(t *testing.T) {
	claimedAt := time.Date(2026, 8, 25, 12, 0, 0, 0, time.UTC)

	group := types.NewConsumerGroupState(testQueueJobs, testGroupWorkers, testQueueJobs+"/#")
	group.Mode = types.GroupModeQueue
	group.AutoCommit = true
	group.Cursor = &types.QueueCursor{Cursor: 12, Committed: 7}
	group.ReplacePEL(map[string][]*types.PendingEntry{
		testGroupConsumerA: {
			{Offset: 1, ConsumerID: testGroupConsumerA, ClaimedAt: claimedAt, DeliveryCount: 1},
			{Offset: 2, ConsumerID: testGroupConsumerA, ClaimedAt: claimedAt, DeliveryCount: 2},
		},
		testGroupConsumerB: {
			{Offset: 3, ConsumerID: testGroupConsumerB, ClaimedAt: claimedAt, DeliveryCount: 1},
		},
	})
	group.ReplaceConsumers(map[string]*types.ConsumerInfo{
		testGroupConsumerA: {ID: testGroupConsumerA, ClientID: "client-1", RegisteredAt: claimedAt},
		testGroupConsumerB: {ID: testGroupConsumerB, ClientID: "client-2", ProxyNodeID: testNode2, RegisteredAt: claimedAt},
	})

	wire, err := encodeGroupOperation(&raft.Operation{
		Type: raft.OpUpdateGroup, QueueName: testQueueJobs, GroupID: testGroupWorkers, GroupState: group,
	})
	require.NoError(t, err)

	decoded, err := decodeGroupOperation(wire)
	require.NoError(t, err)
	require.NotNil(t, decoded.GroupState)

	before, after := group.Snapshot(), decoded.GroupState.Snapshot()
	assert.Equal(t, before.ID, after.ID)
	assert.Equal(t, before.QueueName, after.QueueName)
	assert.Equal(t, before.Pattern, after.Pattern)
	assert.Equal(t, before.Mode, after.Mode)
	assert.Equal(t, before.AutoCommit, after.AutoCommit)
	assert.Equal(t, before.Cursor, after.Cursor)
	assert.Equal(t, before.PEL, after.PEL, "pending entries must regroup by consumer")
	assert.Equal(t, before.Consumers, after.Consumers)
}

// Only group mutations travel this RPC. The leader applies appends, truncations
// and queue config changes through Raft directly, so forwarding one is a bug
// and must be reported rather than encoded into a nil oneof.
func TestGroupOperationRejectsNonGroupTypes(t *testing.T) {
	for _, opType := range []raft.OpType{raft.OpAppend, raft.OpAppendBatch, raft.OpTruncate, raft.OpCreateQueue} {
		_, err := encodeGroupOperation(&raft.Operation{Type: opType, QueueName: testQueueJobs})
		assert.ErrorIs(t, err, ErrUnsupportedGroupOp, "op type %d must not be forwardable", opType)
	}

	_, err := encodeGroupOperation(nil)
	assert.ErrorIs(t, err, ErrUnsupportedGroupOp)
}

func TestGroupOperationRejectsMissingPayload(t *testing.T) {
	_, err := decodeGroupOperation(nil)
	assert.ErrorIs(t, err, ErrUnsupportedGroupOp)

	// A wire message whose oneof was never set names no mutation.
	_, err = decodeGroupOperation(&clusterv1.GroupOperation{QueueName: testQueueJobs, GroupId: testGroupWorkers})
	assert.ErrorIs(t, err, ErrUnsupportedGroupOp)
}

// Snapshot must copy, not alias: a caller that serializes a group and then holds
// the result cannot be allowed to mutate live group state through it.
func TestSnapshotDoesNotAliasGroupState(t *testing.T) {
	group := types.NewConsumerGroupState(testQueueJobs, testGroupWorkers, "")
	group.ReplacePEL(map[string][]*types.PendingEntry{
		testGroupConsumerA: {{Offset: 1, ConsumerID: testGroupConsumerA, DeliveryCount: 1}},
	})

	snapshot := group.Snapshot()
	snapshot.PEL[testGroupConsumerA][0].DeliveryCount = 99
	snapshot.PEL[testGroupConsumerB] = []*types.PendingEntry{{Offset: 5, ConsumerID: testGroupConsumerB}}

	fresh := group.Snapshot()
	require.Len(t, fresh.PEL, 1, "snapshot must not add consumers to the group")
	assert.Equal(t, 1, fresh.PEL[testGroupConsumerA][0].DeliveryCount, "snapshot must not mutate live entries")
}
