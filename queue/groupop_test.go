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
	"google.golang.org/protobuf/types/known/timestamppb"
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
					ID: testGroupConsumerA, ClientID: testClientOneID, ProxyNodeID: testNode2,
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
		{
			name: "requeue pending",
			op: &raft.Operation{
				Type: raft.OpRequeuePending, Timestamp: claimedAt, QueueName: testQueueJobs, GroupID: testGroupWorkers,
				ConsumerID: testGroupConsumerA, Offset: 42,
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
			assert.True(t, tt.op.Timestamp.Equal(decoded.Timestamp))
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
		testGroupConsumerA: {ID: testGroupConsumerA, ClientID: testClientOneID, RegisteredAt: claimedAt},
		testGroupConsumerB: {ID: testGroupConsumerB, ClientID: testClientTwoID, ProxyNodeID: testNode2, RegisteredAt: claimedAt},
	})
	group.CreatedAt = claimedAt.Add(-time.Hour)
	group.UpdatedAt = claimedAt

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
	assert.True(t, before.CreatedAt.Equal(after.CreatedAt))
	assert.True(t, before.UpdatedAt.Equal(after.UpdatedAt), "decoding membership must not replace the replicated timestamp")
}

// Only group mutations travel this RPC. The leader applies appends, truncations
// and queue config changes through Raft directly, so forwarding one is a bug
// and must be reported rather than encoded into a nil oneof.
func TestGroupOperationRejectsNonGroupTypes(t *testing.T) {
	for _, opType := range []raft.OpType{raft.OpAppend, raft.OpTruncate, raft.OpCreateQueue} {
		_, err := encodeGroupOperation(&raft.Operation{Type: opType, QueueName: testQueueJobs})
		assert.ErrorIs(t, err, ErrUnsupportedGroupOp, "op type %d must not be forwardable", opType)
	}

	_, err := encodeGroupOperation(nil)
	assert.ErrorIs(t, err, ErrMalformedGroupOp)
}

func TestGroupOperationRejectsMissingPayload(t *testing.T) {
	// An absent operation is malformed input, distinct from an operation whose
	// type this node does not handle.
	_, err := decodeGroupOperation(nil)
	assert.ErrorIs(t, err, ErrMalformedGroupOp)

	// A wire message whose oneof was never set names no mutation.
	_, err = decodeGroupOperation(&clusterv1.GroupOperation{QueueName: testQueueJobs, GroupId: testGroupWorkers})
	assert.ErrorIs(t, err, ErrMalformedGroupOp)
}

func TestGroupOperationRejectsMalformedOutboundOperations(t *testing.T) {
	validGroup := types.NewConsumerGroupState(testQueueJobs, testGroupWorkers, "")
	invalidTime := time.Date(10000, time.January, 1, 0, 0, 0, 0, time.UTC)

	tests := []struct {
		name string
		op   *raft.Operation
	}{
		{
			name: "missing queue",
			op:   &raft.Operation{Type: raft.OpDeleteGroup, GroupID: testGroupWorkers},
		},
		{
			name: "missing group",
			op:   &raft.Operation{Type: raft.OpDeleteGroup, QueueName: testQueueJobs},
		},
		{
			name: "invalid operation timestamp",
			op: &raft.Operation{
				Type: raft.OpDeleteGroup, QueueName: testQueueJobs, GroupID: testGroupWorkers, Timestamp: invalidTime,
			},
		},
		{
			name: "create without state",
			op:   &raft.Operation{Type: raft.OpCreateGroup, QueueName: testQueueJobs, GroupID: testGroupWorkers},
		},
		{
			name: "nested identity mismatch",
			op: &raft.Operation{
				Type: raft.OpUpdateGroup, QueueName: testQueueJobs, GroupID: "other-group", GroupState: validGroup,
			},
		},
		{
			name: "add pending without entry",
			op:   &raft.Operation{Type: raft.OpAddPending, QueueName: testQueueJobs, GroupID: testGroupWorkers},
		},
		{
			name: "register without consumer",
			op:   &raft.Operation{Type: raft.OpRegisterConsumer, QueueName: testQueueJobs, GroupID: testGroupWorkers},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			wire, err := encodeGroupOperation(tt.op)
			assert.ErrorIs(t, err, ErrMalformedGroupOp)
			assert.Nil(t, wire)
		})
	}
}

func TestGroupOperationRejectsInvalidWireTimestamps(t *testing.T) {
	invalid := &timestamppb.Timestamp{Seconds: 253402300800}
	base := func() *clusterv1.GroupOperation {
		return &clusterv1.GroupOperation{
			QueueName: testQueueJobs,
			GroupId:   testGroupWorkers,
			Operation: &clusterv1.GroupOperation_DeleteGroup{DeleteGroup: &clusterv1.DeleteGroupOp{}},
		}
	}

	tests := []struct {
		name string
		wire func() *clusterv1.GroupOperation
	}{
		{
			name: "operation timestamp",
			wire: func() *clusterv1.GroupOperation {
				wire := base()
				wire.Timestamp = invalid
				return wire
			},
		},
		{
			name: "group created at",
			wire: func() *clusterv1.GroupOperation {
				wire := base()
				wire.Operation = &clusterv1.GroupOperation_CreateGroup{CreateGroup: &clusterv1.CreateGroupOp{
					Group: &clusterv1.ConsumerGroupState{
						Id: testGroupWorkers, QueueName: testQueueJobs, CreatedAt: invalid,
					},
				}}
				return wire
			},
		},
		{
			name: "pending claimed at",
			wire: func() *clusterv1.GroupOperation {
				wire := base()
				wire.Operation = &clusterv1.GroupOperation_AddPending{AddPending: &clusterv1.AddPendingOp{
					Entry: &clusterv1.PendingEntryState{
						Offset: 1, ConsumerId: testGroupConsumerA, ClaimedAt: invalid,
					},
				}}
				return wire
			},
		},
		{
			name: "consumer registered at",
			wire: func() *clusterv1.GroupOperation {
				wire := base()
				wire.Operation = &clusterv1.GroupOperation_RegisterConsumer{RegisterConsumer: &clusterv1.RegisterConsumerOp{
					Consumer: &clusterv1.ConsumerState{Id: testGroupConsumerA, RegisteredAt: invalid},
				}}
				return wire
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			op, err := decodeGroupOperation(tt.wire())
			assert.ErrorIs(t, err, ErrMalformedGroupOp)
			assert.Nil(t, op)
		})
	}
}

// A typed oneof stops a field from being misread; it does not stop a required
// payload from being absent. The leader applies whatever it decodes, so a
// schema-valid but semantically empty mutation must be rejected here rather
// than dereferenced downstream.
func TestGroupOperationRejectsSemanticallyEmptyPayloads(t *testing.T) {
	group := func() *clusterv1.ConsumerGroupState {
		return &clusterv1.ConsumerGroupState{Id: testGroupWorkers, QueueName: testQueueJobs}
	}

	tests := []struct {
		name string
		wire *clusterv1.GroupOperation
	}{
		{
			name: "envelope without queue name",
			wire: &clusterv1.GroupOperation{
				GroupId:   testGroupWorkers,
				Operation: &clusterv1.GroupOperation_DeleteGroup{DeleteGroup: &clusterv1.DeleteGroupOp{}},
			},
		},
		{
			name: "envelope without group id",
			wire: &clusterv1.GroupOperation{
				QueueName: testQueueJobs,
				Operation: &clusterv1.GroupOperation_DeleteGroup{DeleteGroup: &clusterv1.DeleteGroupOp{}},
			},
		},
		{
			name: "create group without a group",
			wire: &clusterv1.GroupOperation{
				QueueName: testQueueJobs, GroupId: testGroupWorkers,
				Operation: &clusterv1.GroupOperation_CreateGroup{CreateGroup: &clusterv1.CreateGroupOp{}},
			},
		},
		{
			name: "update group without a group",
			wire: &clusterv1.GroupOperation{
				QueueName: testQueueJobs, GroupId: testGroupWorkers,
				Operation: &clusterv1.GroupOperation_UpdateGroup{UpdateGroup: &clusterv1.UpdateGroupOp{}},
			},
		},
		{
			name: "group state naming a different group",
			wire: &clusterv1.GroupOperation{
				QueueName: testQueueJobs, GroupId: testGroupWorkers,
				Operation: &clusterv1.GroupOperation_CreateGroup{CreateGroup: &clusterv1.CreateGroupOp{
					Group: &clusterv1.ConsumerGroupState{Id: "other-group", QueueName: testQueueJobs},
				}},
			},
		},
		{
			name: "group state naming a different queue",
			wire: &clusterv1.GroupOperation{
				QueueName: testQueueJobs, GroupId: testGroupWorkers,
				Operation: &clusterv1.GroupOperation_CreateGroup{CreateGroup: &clusterv1.CreateGroupOp{
					Group: &clusterv1.ConsumerGroupState{Id: testGroupWorkers, QueueName: "other-queue"},
				}},
			},
		},
		{
			name: "add pending without an entry",
			wire: &clusterv1.GroupOperation{
				QueueName: testQueueJobs, GroupId: testGroupWorkers,
				Operation: &clusterv1.GroupOperation_AddPending{AddPending: &clusterv1.AddPendingOp{}},
			},
		},
		{
			name: "pending entry without a consumer",
			wire: &clusterv1.GroupOperation{
				QueueName: testQueueJobs, GroupId: testGroupWorkers,
				Operation: &clusterv1.GroupOperation_AddPending{AddPending: &clusterv1.AddPendingOp{
					Entry: &clusterv1.PendingEntryState{Offset: 1},
				}},
			},
		},
		{
			name: "remove pending without a consumer",
			wire: &clusterv1.GroupOperation{
				QueueName: testQueueJobs, GroupId: testGroupWorkers,
				Operation: &clusterv1.GroupOperation_RemovePending{RemovePending: &clusterv1.RemovePendingOp{Offset: 1}},
			},
		},
		{
			name: "transfer pending without consumers",
			wire: &clusterv1.GroupOperation{
				QueueName: testQueueJobs, GroupId: testGroupWorkers,
				Operation: &clusterv1.GroupOperation_TransferPending{TransferPending: &clusterv1.TransferPendingOp{Offset: 1}},
			},
		},
		{
			name: "register consumer without a consumer",
			wire: &clusterv1.GroupOperation{
				QueueName: testQueueJobs, GroupId: testGroupWorkers,
				Operation: &clusterv1.GroupOperation_RegisterConsumer{RegisterConsumer: &clusterv1.RegisterConsumerOp{}},
			},
		},
		{
			name: "register consumer without an id",
			wire: &clusterv1.GroupOperation{
				QueueName: testQueueJobs, GroupId: testGroupWorkers,
				Operation: &clusterv1.GroupOperation_RegisterConsumer{RegisterConsumer: &clusterv1.RegisterConsumerOp{
					Consumer: &clusterv1.ConsumerState{ClientId: testClientOneID},
				}},
			},
		},
		{
			name: "unregister consumer without an id",
			wire: &clusterv1.GroupOperation{
				QueueName: testQueueJobs, GroupId: testGroupWorkers,
				Operation: &clusterv1.GroupOperation_UnregisterConsumer{UnregisterConsumer: &clusterv1.UnregisterConsumerOp{}},
			},
		},
		{
			name: "requeue pending without an id",
			wire: &clusterv1.GroupOperation{
				QueueName: testQueueJobs, GroupId: testGroupWorkers,
				Operation: &clusterv1.GroupOperation_RequeuePending{RequeuePending: &clusterv1.RequeuePendingOp{Offset: 1}},
			},
		},
		{
			name: "group state carrying a pending entry without a consumer",
			wire: &clusterv1.GroupOperation{
				QueueName: testQueueJobs, GroupId: testGroupWorkers,
				Operation: &clusterv1.GroupOperation_UpdateGroup{UpdateGroup: &clusterv1.UpdateGroupOp{
					Group: &clusterv1.ConsumerGroupState{
						Id: testGroupWorkers, QueueName: testQueueJobs,
						Pending: []*clusterv1.PendingEntryState{{Offset: 1}},
					},
				}},
			},
		},
		{
			name: "group state carrying a consumer without an id",
			wire: &clusterv1.GroupOperation{
				QueueName: testQueueJobs, GroupId: testGroupWorkers,
				Operation: &clusterv1.GroupOperation_UpdateGroup{UpdateGroup: &clusterv1.UpdateGroupOp{
					Group: &clusterv1.ConsumerGroupState{
						Id: testGroupWorkers, QueueName: testQueueJobs,
						Consumers: []*clusterv1.ConsumerState{{ClientId: testClientOneID}},
					},
				}},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.NotPanics(t, func() {
				op, err := decodeGroupOperation(tt.wire)
				assert.ErrorIs(t, err, ErrMalformedGroupOp)
				assert.Nil(t, op)
			})
		})
	}

	// The well-formed shape those cases vary from must still decode.
	ok, err := decodeGroupOperation(&clusterv1.GroupOperation{
		QueueName: testQueueJobs, GroupId: testGroupWorkers,
		Operation: &clusterv1.GroupOperation_CreateGroup{CreateGroup: &clusterv1.CreateGroupOp{Group: group()}},
	})
	require.NoError(t, err)
	require.NotNil(t, ok.GroupState)
}

// The nil group that reached raft.Manager.ApplyCreateGroup was dereferenced as
// group.ID. The decoder rejects it now, and the manager reports it rather than
// trusting its caller.
func TestRaftManagerRejectsNilGroup(t *testing.T) {
	// A disabled manager returns ErrRaftDisabled for everything, so asserting
	// only "an error" would pass whether or not the nil check exists. The nil
	// check runs before the enabled check precisely so this can be asserted
	// specifically.
	manager := &raft.Manager{}
	assert.ErrorIs(t, manager.ApplyCreateGroup(t.Context(), testQueueJobs, nil), raft.ErrInvalidOperation)
	assert.ErrorIs(t, manager.ApplyUpdateGroup(t.Context(), testQueueJobs, nil), raft.ErrInvalidOperation)
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
