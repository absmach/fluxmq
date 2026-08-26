// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package raft

import (
	"errors"
	"fmt"
	"time"

	raftv1 "github.com/absmach/fluxmq/pkg/proto/raft/v1"
	"github.com/absmach/fluxmq/queue/types"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const snapshotWireVersion uint32 = 1

var errMalformedSnapshot = errors.New("malformed queue raft snapshot")

// QueueSnapshotData holds snapshot data for a single queue.
type QueueSnapshotData struct {
	QueueName   string
	QueueConfig *types.QueueConfig
	Groups      []*types.ConsumerGroup
}

// GlobalSnapshotData is the FSM state a restoring node rebuilds from.
//
// It is encoded by the same codec as the operations in the log, over the same
// QueueConfigState and ConsumerGroupState messages. A second serializer here
// would be a second place for every queue and group field to be spelled out,
// and the two would disagree the first time one of them was updated alone.
type GlobalSnapshotData struct {
	Queues    []QueueSnapshotData
	Timestamp time.Time
}

func marshalSnapshot(snapshot *GlobalSnapshotData) ([]byte, error) {
	if snapshot == nil {
		return nil, fmt.Errorf("%w: snapshot is missing", errMalformedSnapshot)
	}

	wire := &raftv1.Snapshot{
		Version: snapshotWireVersion,
		Queues:  make([]*raftv1.QueueSnapshot, 0, len(snapshot.Queues)),
	}
	if !snapshot.Timestamp.IsZero() {
		wire.Timestamp = timestamppb.New(snapshot.Timestamp)
	}

	for _, queue := range snapshot.Queues {
		entry := &raftv1.QueueSnapshot{QueueName: queue.QueueName}
		if queue.QueueConfig != nil {
			config, err := encodeOperationQueueConfig(queue.QueueConfig)
			if err != nil {
				return nil, fmt.Errorf("%w: queue %q: %w", errMalformedSnapshot, queue.QueueName, err)
			}
			entry.Config = config
		}
		for _, group := range queue.Groups {
			if group == nil {
				continue
			}
			state, err := encodeOperationGroup(group)
			if err != nil {
				return nil, fmt.Errorf("%w: queue %q group: %w", errMalformedSnapshot, queue.QueueName, err)
			}
			entry.Groups = append(entry.Groups, state)
		}
		wire.Queues = append(wire.Queues, entry)
	}

	data, err := (proto.MarshalOptions{Deterministic: true}).Marshal(wire)
	if err != nil {
		return nil, fmt.Errorf("marshal queue raft snapshot: %w", err)
	}
	return data, nil
}

func unmarshalSnapshot(data []byte) (*GlobalSnapshotData, error) {
	if len(data) == 0 {
		return nil, fmt.Errorf("%w: empty payload", errMalformedSnapshot)
	}

	wire := new(raftv1.Snapshot)
	if err := proto.Unmarshal(data, wire); err != nil {
		return nil, fmt.Errorf("%w: decode protobuf: %w", errMalformedSnapshot, err)
	}
	if wire.Version != snapshotWireVersion {
		return nil, fmt.Errorf("%w: unsupported version %d", errMalformedSnapshot, wire.Version)
	}

	timestamp, err := decodeOperationTime(wire.Timestamp, "timestamp")
	if err != nil {
		return nil, fmt.Errorf("%w: %w", errMalformedSnapshot, err)
	}

	snapshot := &GlobalSnapshotData{
		Queues:    make([]QueueSnapshotData, 0, len(wire.Queues)),
		Timestamp: timestamp,
	}
	for _, entry := range wire.Queues {
		if entry == nil {
			return nil, fmt.Errorf("%w: queue entry is missing", errMalformedSnapshot)
		}
		queue := QueueSnapshotData{QueueName: entry.QueueName}
		if entry.Config != nil {
			config, decodeErr := decodeOperationQueueConfig(entry.Config)
			if decodeErr != nil {
				return nil, fmt.Errorf("%w: queue %q: %w", errMalformedSnapshot, entry.QueueName, decodeErr)
			}
			queue.QueueConfig = config
		}
		for _, state := range entry.Groups {
			group, decodeErr := decodeOperationGroup(state)
			if decodeErr != nil {
				return nil, fmt.Errorf("%w: queue %q group: %w", errMalformedSnapshot, entry.QueueName, decodeErr)
			}
			queue.Groups = append(queue.Groups, group)
		}
		snapshot.Queues = append(snapshot.Queues, queue)
	}
	return snapshot, nil
}
