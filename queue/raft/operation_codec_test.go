// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package raft

import (
	"encoding/json"
	"testing"
	"time"

	raftv1 "github.com/absmach/fluxmq/pkg/proto/raft/v1"
	"github.com/absmach/fluxmq/queue/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	testOperationQueue     = "jobs"
	testOperationGroup     = "workers"
	testOperationConsumerA = "consumer-a"
	testOperationConsumerB = "consumer-b"

	// Shared subtest names: each wire format rejects the same two shapes.
	caseUnsupportedVersion = "unsupported version"
	caseInvalidTimestamp   = "invalid timestamp"
)

func TestOperationCodecRoundTripsEveryCommand(t *testing.T) {
	now := conformanceTime
	group := conformanceConsumerGroup(now)
	queueConfig := conformanceQueueConfig()

	operations := map[string]*Operation{
		"append":              {Type: OpAppend, Timestamp: now, QueueName: testOperationQueue, Message: []byte{0, 1, 2, 255}, DedupeKey: "transfer-1"},
		"truncate":            {Type: OpTruncate, Timestamp: now, QueueName: testOperationQueue, MinOffset: 10},
		"create group":        {Type: OpCreateGroup, Timestamp: now, QueueName: testOperationQueue, GroupID: testOperationGroup, GroupState: group},
		"update group":        {Type: OpUpdateGroup, Timestamp: now, QueueName: testOperationQueue, GroupID: testOperationGroup, GroupState: group},
		"delete group":        {Type: OpDeleteGroup, Timestamp: now, QueueName: testOperationQueue, GroupID: testOperationGroup},
		"update cursor":       {Type: OpUpdateCursor, Timestamp: now, QueueName: testOperationQueue, GroupID: testOperationGroup, Cursor: 20},
		"update committed":    {Type: OpUpdateCommitted, Timestamp: now, QueueName: testOperationQueue, GroupID: testOperationGroup, Committed: 19},
		"add pending":         {Type: OpAddPending, Timestamp: now, QueueName: testOperationQueue, GroupID: testOperationGroup, PendingEntry: &types.PendingEntry{Offset: 20, ConsumerID: testOperationConsumerA, ClaimedAt: now, DeliveryCount: 3}},
		"remove pending":      {Type: OpRemovePending, Timestamp: now, QueueName: testOperationQueue, GroupID: testOperationGroup, ConsumerID: testOperationConsumerA, Offset: 20},
		"transfer pending":    {Type: OpTransferPending, Timestamp: now, QueueName: testOperationQueue, GroupID: testOperationGroup, Offset: 20, FromConsumer: testOperationConsumerA, ToConsumer: testOperationConsumerB},
		"register consumer":   {Type: OpRegisterConsumer, Timestamp: now, QueueName: testOperationQueue, GroupID: testOperationGroup, ConsumerInfo: &types.ConsumerInfo{ID: "consumer-c", ClientID: "client-c", ProxyNodeID: "node-c", RegisteredAt: now, LastHeartbeat: now}},
		"unregister consumer": {Type: OpUnregisterConsumer, Timestamp: now, QueueName: testOperationQueue, GroupID: testOperationGroup, ConsumerID: "consumer-c"},
		"create queue":        {Type: OpCreateQueue, Timestamp: now, QueueName: testOperationQueue, QueueConfig: &queueConfig},
		"update queue":        {Type: OpUpdateQueue, Timestamp: now, QueueName: testOperationQueue, QueueConfig: &queueConfig},
		"delete queue":        {Type: OpDeleteQueue, Timestamp: now, QueueName: testOperationQueue},
	}

	for name, operation := range operations {
		t.Run(name, func(t *testing.T) {
			encoded, err := marshalOperation(operation)
			require.NoError(t, err)
			require.NotEmpty(t, encoded)

			decoded, err := unmarshalOperation(encoded)
			require.NoError(t, err)
			assert.Equal(t, operation.Type, decoded.Type)
			assert.True(t, operation.Timestamp.Equal(decoded.Timestamp))

			reencoded, err := marshalOperation(decoded)
			require.NoError(t, err)
			assert.Equal(t, encoded, reencoded, "protobuf encoding must be deterministic and lossless")

			if operation.GroupState != nil {
				assert.Equal(t, operation.GroupState.Snapshot(), decoded.GroupState.Snapshot())
			}
			if operation.QueueConfig != nil {
				assert.Equal(t, *operation.QueueConfig, *decoded.QueueConfig)
			}
		})
	}
}

func TestOperationCodecRejectsMalformedWire(t *testing.T) {
	tests := map[string]*raftv1.Operation{
		caseUnsupportedVersion: {Version: operationWireVersion + 1, Command: &raftv1.Operation_DeleteQueue{DeleteQueue: &raftv1.DeleteQueueOperation{QueueName: testOperationQueue}}},
		"missing command":      {Version: operationWireVersion},
		caseInvalidTimestamp: {
			Version: operationWireVersion, Timestamp: &timestamppb.Timestamp{Seconds: 253402300800},
			Command: &raftv1.Operation_DeleteQueue{DeleteQueue: &raftv1.DeleteQueueOperation{QueueName: testOperationQueue}},
		},
		"missing group": {
			Version: operationWireVersion,
			Command: &raftv1.Operation_CreateGroup{CreateGroup: &raftv1.CreateGroupOperation{QueueName: testOperationQueue, GroupId: testOperationGroup}},
		},
		"unknown queue enum": {
			Version: operationWireVersion,
			Command: &raftv1.Operation_CreateQueue{CreateQueue: &raftv1.CreateQueueOperation{QueueName: testOperationQueue, Config: completeWireQueueConfig()}},
		},
		"invalid duration": {
			Version: operationWireVersion,
			Command: &raftv1.Operation_UpdateQueue{UpdateQueue: &raftv1.UpdateQueueOperation{QueueName: testOperationQueue, Config: completeWireQueueConfig()}},
		},
	}
	tests["unknown queue enum"].GetCreateQueue().Config.Type = raftv1.QueueType(99)
	tests["invalid duration"].GetUpdateQueue().Config.ExpiresAfter = &durationpb.Duration{Seconds: 315576000001}

	for name, wire := range tests {
		t.Run(name, func(t *testing.T) {
			data, err := proto.Marshal(wire)
			require.NoError(t, err)
			_, err = unmarshalOperation(data)
			assert.ErrorIs(t, err, errMalformedOperation)
		})
	}

	_, err := unmarshalOperation(nil)
	assert.ErrorIs(t, err, errMalformedOperation)

	legacyJSON, err := json.Marshal(&Operation{Type: OpDeleteQueue, QueueName: testOperationQueue})
	require.NoError(t, err)
	_, err = unmarshalOperation(legacyJSON)
	assert.ErrorIs(t, err, errMalformedOperation, "the new storage contract must not carry a legacy JSON fallback")
}

// A field this binary does not know is a field a newer peer added. The entry
// carrying it is already committed, so refusing it would apply the mutation
// everywhere except here. Tolerating it is what keeps a rolling upgrade from
// diverging; operationWireVersion is the gate for a change that must not be
// applied blindly.
func TestOperationCodecToleratesUnknownFields(t *testing.T) {
	wire := &raftv1.Operation{
		Version: operationWireVersion,
		Command: &raftv1.Operation_DeleteQueue{DeleteQueue: &raftv1.DeleteQueueOperation{QueueName: testOperationQueue}},
	}
	data, err := proto.Marshal(wire)
	require.NoError(t, err)
	data = append(data, 0x98, 0x06, 0x01) // field 99, varint 1

	decoded, err := unmarshalOperation(data)
	require.NoError(t, err)
	assert.Equal(t, OpDeleteQueue, decoded.Type)
	assert.Equal(t, testOperationQueue, decoded.QueueName)
}

func TestOperationCodecRejectsIncompleteDomainOperations(t *testing.T) {
	tests := map[string]*Operation{
		"nil":               nil,
		"unknown type":      {Type: OpType(255)},
		"create group":      {Type: OpCreateGroup},
		"update group":      {Type: OpUpdateGroup},
		"add pending":       {Type: OpAddPending},
		"register consumer": {Type: OpRegisterConsumer},
		"create queue":      {Type: OpCreateQueue},
		"update queue":      {Type: OpUpdateQueue},
	}
	for name, operation := range tests {
		t.Run(name, func(t *testing.T) {
			_, err := marshalOperation(operation)
			assert.ErrorIs(t, err, errMalformedOperation)
		})
	}
}

func TestOperationCodecSortsMapBackedGroupState(t *testing.T) {
	now := time.Date(2026, 8, 26, 10, 0, 0, 0, time.UTC)
	group := &types.ConsumerGroup{
		ID: testOperationGroup, QueueName: testOperationQueue, Mode: types.GroupModeQueue, Cursor: &types.QueueCursor{},
		PEL: map[string][]*types.PendingEntry{
			"z": {{Offset: 2, ConsumerID: "z", ClaimedAt: now}},
			"a": {{Offset: 1, ConsumerID: "a", ClaimedAt: now}},
		},
		Consumers: map[string]*types.ConsumerInfo{
			"z": {ID: "z"},
			"a": {ID: "a"},
		},
	}
	wire, err := encodeOperation(&Operation{Type: OpCreateGroup, GroupState: group})
	require.NoError(t, err)
	state := wire.GetCreateGroup().Group
	require.Len(t, state.Pending, 2)
	require.Len(t, state.Consumers, 2)
	assert.Equal(t, "a", state.Pending[0].ConsumerId)
	assert.Equal(t, "a", state.Consumers[0].Id)
}

func completeWireQueueConfig() *raftv1.QueueConfigState {
	return &raftv1.QueueConfigState{
		RetryPolicy: &raftv1.RetryPolicyState{},
		DeadLetter:  &raftv1.DeadLetterState{},
		Replication: &raftv1.ReplicationState{},
		Retention:   &raftv1.RetentionState{},
	}
}
