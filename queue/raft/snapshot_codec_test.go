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
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestSnapshotCodecRoundTripsQueuesAndGroups(t *testing.T) {
	now := time.Date(2026, 8, 26, 10, 11, 12, 13, time.UTC)
	config := conformanceQueueConfig()
	snapshot := &GlobalSnapshotData{
		Timestamp: now,
		Queues: []QueueSnapshotData{
			{
				QueueName:   testOperationQueue,
				QueueConfig: &config,
				Groups:      []*types.ConsumerGroup{conformanceConsumerGroup(now)},
			},
			{QueueName: "config-less", Groups: nil},
		},
	}

	data, err := marshalSnapshot(snapshot)
	require.NoError(t, err)

	decoded, err := unmarshalSnapshot(data)
	require.NoError(t, err)
	require.Len(t, decoded.Queues, 2)
	assert.True(t, snapshot.Timestamp.Equal(decoded.Timestamp))

	assert.Equal(t, testOperationQueue, decoded.Queues[0].QueueName)
	require.NotNil(t, decoded.Queues[0].QueueConfig)
	assert.Equal(t, config, *decoded.Queues[0].QueueConfig)
	require.Len(t, decoded.Queues[0].Groups, 1)
	assert.Equal(t, snapshot.Queues[0].Groups[0].Snapshot(), decoded.Queues[0].Groups[0].Snapshot())

	assert.Equal(t, "config-less", decoded.Queues[1].QueueName)
	assert.Nil(t, decoded.Queues[1].QueueConfig)
	assert.Empty(t, decoded.Queues[1].Groups)

	reencoded, err := marshalSnapshot(decoded)
	require.NoError(t, err)
	assert.Equal(t, data, reencoded, "snapshot encoding must be deterministic and lossless")
}

// The snapshot and the log carry the same queue and group state. Encoding them
// through the same functions is what keeps the two from drifting, so this pins
// that a config written into a snapshot decodes to what an operation would.
func TestSnapshotCodecSharesStateEncodingWithOperations(t *testing.T) {
	config := conformanceQueueConfig()

	viaSnapshot, err := marshalSnapshot(&GlobalSnapshotData{
		Queues: []QueueSnapshotData{{QueueName: testOperationQueue, QueueConfig: &config}},
	})
	require.NoError(t, err)
	fromSnapshot, err := unmarshalSnapshot(viaSnapshot)
	require.NoError(t, err)

	viaOperation, err := marshalOperation(&Operation{Type: OpCreateQueue, QueueName: testOperationQueue, QueueConfig: &config})
	require.NoError(t, err)
	fromOperation, err := unmarshalOperation(viaOperation)
	require.NoError(t, err)

	assert.Equal(t, *fromOperation.QueueConfig, *fromSnapshot.Queues[0].QueueConfig)
}

func TestSnapshotCodecRejectsMalformedWire(t *testing.T) {
	tests := map[string]*raftv1.Snapshot{
		caseUnsupportedVersion: {Version: snapshotWireVersion + 1},
		caseInvalidTimestamp:   {Version: snapshotWireVersion, Timestamp: &timestamppb.Timestamp{Seconds: 253402300800}},
		"incomplete config": {
			Version: snapshotWireVersion,
			Queues:  []*raftv1.QueueSnapshot{{QueueName: testOperationQueue, Config: &raftv1.QueueConfigState{}}},
		},
		"unknown queue enum": {
			Version: snapshotWireVersion,
			Queues:  []*raftv1.QueueSnapshot{{QueueName: testOperationQueue, Config: unknownTypeWireQueueConfig()}},
		},
		"group without cursor": {
			Version: snapshotWireVersion,
			Queues:  []*raftv1.QueueSnapshot{{QueueName: testOperationQueue, Groups: []*raftv1.ConsumerGroupState{{Id: testOperationGroup}}}},
		},
	}
	for name, wire := range tests {
		t.Run(name, func(t *testing.T) {
			data, err := proto.Marshal(wire)
			require.NoError(t, err)
			_, err = unmarshalSnapshot(data)
			assert.ErrorIs(t, err, errMalformedSnapshot)
		})
	}

	_, err := unmarshalSnapshot(nil)
	assert.ErrorIs(t, err, errMalformedSnapshot)
	_, err = marshalSnapshot(nil)
	assert.ErrorIs(t, err, errMalformedSnapshot)

	legacyJSON, err := json.Marshal(map[string]any{"queues": []any{}, "timestamp": time.Now()})
	require.NoError(t, err)
	_, err = unmarshalSnapshot(legacyJSON)
	assert.ErrorIs(t, err, errMalformedSnapshot, "the snapshot contract must not carry a legacy JSON fallback")
}

func unknownTypeWireQueueConfig() *raftv1.QueueConfigState {
	config := completeWireQueueConfig()
	config.Type = raftv1.QueueType(99)
	return config
}
