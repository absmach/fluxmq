// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package raft

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"testing"
	"time"

	raftv1 "github.com/absmach/fluxmq/pkg/proto/raft/v1"
	"github.com/absmach/fluxmq/queue/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protodelim"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestSnapshotCodecRoundTripsQueuesGroupsAndRecords(t *testing.T) {
	now := conformanceTime
	config := conformanceQueueConfig()

	var buf bytes.Buffer
	writer := newSnapshotWriter(&buf)
	require.NoError(t, writer.WriteHeader(now))
	require.NoError(t, writer.WriteQueue(QueueSnapshotData{
		QueueName:   testOperationQueue,
		QueueConfig: &config,
		Groups:      []*types.ConsumerGroup{conformanceConsumerGroup(now)},
		Head:        7,
		Tail:        9,
	}))
	require.NoError(t, writer.WriteRecord(7, []byte("record-seven")))
	require.NoError(t, writer.WriteRecord(8, []byte("record-eight")))
	require.NoError(t, writer.WriteQueue(QueueSnapshotData{QueueName: "config-less"}))

	reader := newSnapshotReader(bytes.NewReader(buf.Bytes()))
	require.NoError(t, reader.ReadHeader())
	assert.True(t, now.Equal(reader.timestamp))

	entries := drainSnapshot(t, reader)
	require.Len(t, entries, 4)

	require.NotNil(t, entries[0].Queue)
	assert.Equal(t, testOperationQueue, entries[0].Queue.QueueName)
	assert.Equal(t, uint64(7), entries[0].Queue.Head)
	assert.Equal(t, uint64(9), entries[0].Queue.Tail)
	require.NotNil(t, entries[0].Queue.QueueConfig)
	assert.Equal(t, config, *entries[0].Queue.QueueConfig)
	require.Len(t, entries[0].Queue.Groups, 1)
	assert.Equal(t, conformanceConsumerGroup(now).Snapshot(), entries[0].Queue.Groups[0].Snapshot())

	require.NotNil(t, entries[1].Record)
	assert.Equal(t, uint64(7), entries[1].Record.Offset)
	assert.Equal(t, []byte("record-seven"), entries[1].Record.Envelope)
	require.NotNil(t, entries[2].Record)
	assert.Equal(t, uint64(8), entries[2].Record.Offset)

	require.NotNil(t, entries[3].Queue)
	assert.Equal(t, "config-less", entries[3].Queue.QueueName)
	assert.Nil(t, entries[3].Queue.QueueConfig)
}

// The snapshot and the log carry the same queue and group state. Encoding both
// through the same functions is what keeps them from drifting, so this pins
// that a config written into a snapshot decodes to what an operation would.
func TestSnapshotCodecSharesStateEncodingWithOperations(t *testing.T) {
	config := conformanceQueueConfig()

	var buf bytes.Buffer
	writer := newSnapshotWriter(&buf)
	require.NoError(t, writer.WriteHeader(conformanceTime))
	require.NoError(t, writer.WriteQueue(QueueSnapshotData{QueueName: testOperationQueue, QueueConfig: &config}))

	reader := newSnapshotReader(bytes.NewReader(buf.Bytes()))
	require.NoError(t, reader.ReadHeader())
	entries := drainSnapshot(t, reader)
	require.Len(t, entries, 1)

	viaOperation, err := marshalOperation(&Operation{Type: OpCreateQueue, QueueName: testOperationQueue, QueueConfig: &config})
	require.NoError(t, err)
	fromOperation, err := unmarshalOperation(viaOperation)
	require.NoError(t, err)

	assert.Equal(t, *fromOperation.QueueConfig, *entries[0].Queue.QueueConfig)
}

// Framing is what keeps a snapshot's cost independent of what the queues hold,
// so the bytes on the wire have to stay a stream of length-delimited frames
// rather than becoming one message again.
func TestSnapshotCodecWritesOneFramePerEntry(t *testing.T) {
	var buf bytes.Buffer
	writer := newSnapshotWriter(&buf)
	require.NoError(t, writer.WriteHeader(conformanceTime))
	require.NoError(t, writer.WriteQueue(QueueSnapshotData{QueueName: testOperationQueue}))
	for offset := range uint64(3) {
		require.NoError(t, writer.WriteRecord(offset, []byte("payload")))
	}

	source := bufio.NewReader(bytes.NewReader(buf.Bytes()))
	frames := 0
	for {
		var frame raftv1.SnapshotFrame
		if err := (protodelim.UnmarshalOptions{}).UnmarshalFrom(source, &frame); err != nil {
			break
		}
		frames++
	}
	assert.Equal(t, 5, frames, "header, queue, and one frame per record")
	assert.Equal(t, int64(buf.Len()), writer.written)
}

func TestSnapshotCodecRejectsMalformedStream(t *testing.T) {
	t.Run("empty", func(t *testing.T) {
		reader := newSnapshotReader(bytes.NewReader(nil))
		assert.ErrorIs(t, reader.ReadHeader(), errMalformedSnapshot)
	})

	t.Run("no header", func(t *testing.T) {
		data := frameBytes(t, &raftv1.SnapshotFrame{
			Frame: &raftv1.SnapshotFrame_Queue{Queue: &raftv1.QueueSnapshot{QueueName: testOperationQueue}},
		})
		reader := newSnapshotReader(bytes.NewReader(data))
		assert.ErrorIs(t, reader.ReadHeader(), errMalformedSnapshot)
	})

	t.Run("unsupported version", func(t *testing.T) {
		data := frameBytes(t, &raftv1.SnapshotFrame{
			Frame: &raftv1.SnapshotFrame_Header{Header: &raftv1.SnapshotHeader{Version: snapshotWireVersion + 1}},
		})
		reader := newSnapshotReader(bytes.NewReader(data))
		assert.ErrorIs(t, reader.ReadHeader(), errMalformedSnapshot)
	})

	t.Run("invalid timestamp", func(t *testing.T) {
		data := frameBytes(t, &raftv1.SnapshotFrame{
			Frame: &raftv1.SnapshotFrame_Header{Header: &raftv1.SnapshotHeader{
				Version:   snapshotWireVersion,
				Timestamp: &timestamppb.Timestamp{Seconds: 253402300800},
			}},
		})
		reader := newSnapshotReader(bytes.NewReader(data))
		assert.ErrorIs(t, reader.ReadHeader(), errMalformedSnapshot)
	})

	t.Run("tail before head", func(t *testing.T) {
		reader := newSnapshotReader(bytes.NewReader(headerAnd(t, &raftv1.SnapshotFrame{
			Frame: &raftv1.SnapshotFrame_Queue{Queue: &raftv1.QueueSnapshot{QueueName: testOperationQueue, Head: 9, Tail: 4}},
		})))
		require.NoError(t, reader.ReadHeader())
		_, err := reader.Next()
		assert.ErrorIs(t, err, errMalformedSnapshot)
	})

	t.Run("incomplete config", func(t *testing.T) {
		reader := newSnapshotReader(bytes.NewReader(headerAnd(t, &raftv1.SnapshotFrame{
			Frame: &raftv1.SnapshotFrame_Queue{Queue: &raftv1.QueueSnapshot{
				QueueName: testOperationQueue, Config: &raftv1.QueueConfigState{},
			}},
		})))
		require.NoError(t, reader.ReadHeader())
		_, err := reader.Next()
		assert.ErrorIs(t, err, errMalformedSnapshot)
	})

	t.Run("unknown field", func(t *testing.T) {
		frame := &raftv1.SnapshotFrame{
			Frame: &raftv1.SnapshotFrame_Queue{Queue: &raftv1.QueueSnapshot{
				QueueName: testOperationQueue, Config: completeWireQueueConfig(),
			}},
		}
		frame.GetQueue().Config.ProtoReflect().SetUnknown([]byte{0x98, 0x06, 0x07})
		reader := newSnapshotReader(bytes.NewReader(headerAnd(t, frame)))
		require.NoError(t, reader.ReadHeader())
		_, err := reader.Next()
		assert.ErrorIs(t, err, errMalformedSnapshot, "state this build cannot read must not restore as a zero value")
	})

	t.Run("legacy json", func(t *testing.T) {
		legacy, err := json.Marshal(map[string]any{"queues": []any{}, "timestamp": time.Now()})
		require.NoError(t, err)
		reader := newSnapshotReader(bytes.NewReader(legacy))
		assert.Error(t, reader.ReadHeader(), "the snapshot contract must not carry a legacy JSON fallback")
	})
}

func drainSnapshot(t *testing.T, reader *snapshotReader) []snapshotEntry {
	t.Helper()

	var entries []snapshotEntry
	for {
		entry, err := reader.Next()
		if errors.Is(err, io.EOF) {
			return entries
		}
		require.NoError(t, err)
		entries = append(entries, entry)
	}
}

func frameBytes(t *testing.T, frames ...*raftv1.SnapshotFrame) []byte {
	t.Helper()

	var buf bytes.Buffer
	for _, frame := range frames {
		_, err := protodelim.MarshalTo(&buf, frame)
		require.NoError(t, err)
	}
	return buf.Bytes()
}

func headerAnd(t *testing.T, frames ...*raftv1.SnapshotFrame) []byte {
	t.Helper()

	header := &raftv1.SnapshotFrame{
		Frame: &raftv1.SnapshotFrame_Header{Header: &raftv1.SnapshotHeader{Version: snapshotWireVersion}},
	}
	return frameBytes(t, append([]*raftv1.SnapshotFrame{header}, frames...)...)
}

func completeWireQueueConfig() *raftv1.QueueConfigState {
	return &raftv1.QueueConfigState{
		RetryPolicy: &raftv1.RetryPolicyState{},
		DeadLetter:  &raftv1.DeadLetterState{},
		Replication: &raftv1.ReplicationState{},
		Retention:   &raftv1.RetentionState{},
	}
}
