// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package raft

import (
	"bytes"
	"context"
	"io"
	"testing"

	"github.com/absmach/fluxmq/queue/storage"
	memlog "github.com/absmach/fluxmq/queue/storage/memory/log"
	"github.com/absmach/fluxmq/queue/types"
	hraft "github.com/hashicorp/raft"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

// recordingGroupStore keeps what Restore writes so the snapshot round trip can
// be checked. It borrows every method it does not care about from the noop.
type recordingGroupStore struct {
	noopGroupStore
	groups map[string]*types.ConsumerGroup
}

func newRecordingGroupStore() *recordingGroupStore {
	return &recordingGroupStore{groups: make(map[string]*types.ConsumerGroup)}
}

func (s *recordingGroupStore) key(queueName, groupID string) string { return queueName + "/" + groupID }

func (s *recordingGroupStore) CreateConsumerGroup(_ context.Context, group *types.ConsumerGroup) error {
	s.groups[s.key(group.QueueName, group.ID)] = group
	return nil
}

func (s *recordingGroupStore) GetConsumerGroup(_ context.Context, queueName, groupID string) (*types.ConsumerGroup, error) {
	group, ok := s.groups[s.key(queueName, groupID)]
	if !ok {
		return nil, storage.ErrConsumerNotFound
	}
	return group, nil
}

func (s *recordingGroupStore) ListConsumerGroups(_ context.Context, queueName string) ([]*types.ConsumerGroup, error) {
	var groups []*types.ConsumerGroup
	for _, group := range s.groups {
		if group.QueueName == queueName {
			groups = append(groups, group)
		}
	}
	return groups, nil
}

// memSink collects what Persist writes so the bytes can be handed straight to
// Restore, which is the trip a restoring node actually makes.
type memSink struct {
	bytes.Buffer
	cancelled bool
}

func (s *memSink) ID() string { return "conformance-snapshot" }
func (s *memSink) Close() error {
	return nil
}

func (s *memSink) Cancel() error {
	s.cancelled = true
	return nil
}

func TestLogFSMSnapshotRestoresQueuesAndGroups(t *testing.T) {
	ctx := context.Background()
	source := memlog.New()
	sourceGroups := newRecordingGroupStore()
	fsm := NewLogFSM(source, sourceGroups, discardLogger())

	config := conformanceQueueConfig()
	require.NoError(t, source.CreateQueue(ctx, config))
	require.NoError(t, sourceGroups.CreateConsumerGroup(ctx, conformanceConsumerGroup(conformanceTime)))

	snapshot, err := fsm.Snapshot()
	require.NoError(t, err)

	sink := new(memSink)
	require.NoError(t, snapshot.Persist(sink))
	require.False(t, sink.cancelled)
	snapshot.Release()

	restored := memlog.New()
	restoredGroups := newRecordingGroupStore()
	target := NewLogFSM(restored, restoredGroups, discardLogger())
	require.NoError(t, target.Restore(io.NopCloser(bytes.NewReader(sink.Bytes()))))

	restoredConfig, err := restored.GetQueue(ctx, testOperationQueue)
	require.NoError(t, err)
	assert.Equal(t, config, *restoredConfig, "the snapshot must carry the queue config field for field")

	restoredGroup, err := restoredGroups.GetConsumerGroup(ctx, testOperationQueue, testOperationGroup)
	require.NoError(t, err)
	expected := conformanceConsumerGroup(conformanceTime).Snapshot()
	got := restoredGroup.Snapshot()
	assert.Equal(t, expected.Cursor, got.Cursor)
	assert.Equal(t, expected.Mode, got.Mode)
	assert.Equal(t, expected.Pattern, got.Pattern)
	assert.Equal(t, expected.PEL, got.PEL, "the pending list must survive a snapshot")
	assert.Equal(t, expected.Consumers, got.Consumers)
}

func TestLogFSMRestoreRejectsMalformedSnapshot(t *testing.T) {
	store := memlog.New()
	fsm := NewLogFSM(store, newRecordingGroupStore(), discardLogger())

	err := fsm.Restore(io.NopCloser(bytes.NewReader([]byte("{\"queues\":[]}"))))
	assert.ErrorIs(t, err, errMalformedSnapshot)
}

// A committed entry this binary cannot decode is an entry every peer applied
// and this node did not. Continuing would leave the replica silently behind, so
// Apply must stop the process instead of reporting the error and moving on.
func TestLogFSMApplyStopsOnUndecodableCommittedEntry(t *testing.T) {
	store := memlog.New()
	fsm := NewLogFSM(store, newRecordingGroupStore(), discardLogger())

	assert.Panics(t, func() {
		fsm.Apply(&hraft.Log{Index: 12, Term: 3, Type: hraft.LogCommand, Data: []byte("not protobuf at all")})
	}, "an undecodable committed entry must not be skipped")

	future, err := proto.Marshal(futureVersionOperation())
	require.NoError(t, err)
	assert.Panics(t, func() {
		fsm.Apply(&hraft.Log{Index: 13, Term: 3, Type: hraft.LogCommand, Data: future})
	}, "an entry from an unsupported wire version must not be skipped")
}

// An additive field from a newer peer is not divergence: the operation still
// decodes and still applies, which is what keeps a rolling upgrade whole.
func TestLogFSMApplyToleratesUnknownFieldsFromNewerPeer(t *testing.T) {
	store := memlog.New()
	fsm := NewLogFSM(store, newRecordingGroupStore(), discardLogger())

	config := conformanceQueueConfig()
	data, err := marshalOperation(&Operation{Type: OpCreateQueue, QueueName: testOperationQueue, QueueConfig: &config})
	require.NoError(t, err)
	data = append(data, 0x98, 0x06, 0x01) // field 99, varint 1

	result, ok := fsm.Apply(&hraft.Log{Index: 14, Term: 3, Type: hraft.LogCommand, Data: data}).(*ApplyResult)
	require.True(t, ok)
	require.NoError(t, result.Error)

	stored, err := store.GetQueue(context.Background(), testOperationQueue)
	require.NoError(t, err)
	assert.Equal(t, testOperationQueue, stored.Name)
}

func futureVersionOperation() proto.Message {
	config := conformanceQueueConfig()
	wire, err := encodeOperation(&Operation{Type: OpCreateQueue, QueueName: testOperationQueue, QueueConfig: &config})
	if err != nil {
		panic(err)
	}
	wire.Version = operationWireVersion + 1
	return wire
}
