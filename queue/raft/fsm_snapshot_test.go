// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package raft

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"

	"github.com/absmach/fluxmq/message"
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

func TestLogFSMSnapshotRestoresQueuesGroupsAndRecords(t *testing.T) {
	ctx := context.Background()
	source := memlog.New()
	sourceGroups := newRecordingGroupStore()
	fsm := NewLogFSM(source, sourceGroups, discardLogger())

	config := conformanceQueueConfig()
	require.NoError(t, source.CreateQueue(ctx, config))
	require.NoError(t, sourceGroups.CreateConsumerGroup(ctx, conformanceConsumerGroup(conformanceTime)))

	payloads := []string{"first", "second", "third"}
	for i, payload := range payloads {
		envelope := newQueuedEnvelope(payload, "$queue/"+config.Name, []byte(payload))
		offset, err := source.Append(ctx, config.Name, envelope)
		require.NoError(t, err)
		require.Equal(t, uint64(i), offset)
	}
	// Truncation moves the log's start away from zero, which is the case a
	// restore that rebuilt from zero would silently get wrong.
	require.NoError(t, source.Truncate(ctx, config.Name, 1))

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

	head, err := restored.Head(ctx, config.Name)
	require.NoError(t, err)
	assert.Equal(t, uint64(1), head, "a truncated log must not be rebuilt from zero")
	tail, err := restored.Tail(ctx, config.Name)
	require.NoError(t, err)
	assert.Equal(t, uint64(3), tail)

	for offset, want := range map[uint64]string{1: "second", 2: "third"} {
		got, readErr := restored.Read(ctx, config.Name, offset)
		require.NoError(t, readErr, "offset %d must survive the snapshot", offset)
		assert.Equal(t, want, string(got.PayloadBytes()), "offset %d", offset)
		message.Release(got)
	}
	_, err = restored.Read(ctx, config.Name, 0)
	assert.Error(t, err, "a truncated record must not come back")

	restoredGroup, err := restoredGroups.GetConsumerGroup(ctx, testOperationQueue, testOperationGroup)
	require.NoError(t, err)
	expected := conformanceConsumerGroup(conformanceTime).Snapshot()
	got := restoredGroup.Snapshot()
	assert.Equal(t, expected.Cursor, got.Cursor)
	assert.Equal(t, expected.Mode, got.Mode)
	assert.Equal(t, expected.PEL, got.PEL, "the pending list must survive a snapshot")
	assert.Equal(t, expected.Consumers, got.Consumers)
}

// A snapshot is the group's state at the index it was taken, not a patch. What
// this node held before describes a past the group compacted away, so it must
// not survive underneath the restored state.
func TestLogFSMRestoreReplacesExistingState(t *testing.T) {
	ctx := context.Background()

	source := memlog.New()
	fsm := NewLogFSM(source, newRecordingGroupStore(), discardLogger())
	config := conformanceQueueConfig()
	require.NoError(t, source.CreateQueue(ctx, config))

	snapshot, err := fsm.Snapshot()
	require.NoError(t, err)
	sink := new(memSink)
	require.NoError(t, snapshot.Persist(sink))
	snapshot.Release()

	// The target holds a queue the snapshot never mentions, and records in the
	// queue it does.
	target := memlog.New()
	targetFSM := NewLogFSM(target, newRecordingGroupStore(), discardLogger())
	require.NoError(t, target.CreateQueue(ctx, config))
	_, err = target.Append(ctx, config.Name, newQueuedEnvelope("stale", "$queue/"+config.Name, []byte("stale")))
	require.NoError(t, err)

	localOnly := conformanceQueueConfig()
	localOnly.Name = "local-only"
	localOnly.Reserved = false
	require.NoError(t, target.CreateQueue(ctx, localOnly))

	require.NoError(t, targetFSM.Restore(io.NopCloser(bytes.NewReader(sink.Bytes()))))

	count, err := target.Count(ctx, config.Name)
	require.NoError(t, err)
	assert.Zero(t, count, "a record absent from the snapshot must not survive the restore")

	_, err = target.GetQueue(ctx, "local-only")
	assert.Error(t, err, "a queue the snapshot never mentions must not survive the restore")
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

// A newer peer's additive field reaches Apply as an entry this build cannot
// faithfully apply. It has to stop for the same reason an undecodable entry
// does: the alternative is applying a zero value where every other replica
// applied the real one, with nothing left in the next snapshot to show it.
func TestLogFSMApplyStopsOnUnknownFieldFromNewerPeer(t *testing.T) {
	store := memlog.New()
	fsm := NewLogFSM(store, newRecordingGroupStore(), discardLogger())

	config := conformanceQueueConfig()
	wire, err := encodeOperation(&Operation{Type: OpCreateQueue, QueueName: testOperationQueue, QueueConfig: &config})
	require.NoError(t, err)
	wire.GetCreateQueue().Config.ProtoReflect().SetUnknown([]byte{0x98, 0x06, 0x07})
	data, err := proto.Marshal(wire)
	require.NoError(t, err)

	assert.Panics(t, func() {
		fsm.Apply(&hraft.Log{Index: 14, Term: 3, Type: hraft.LogCommand, Data: data})
	}, "an entry carrying state this build cannot read must not be applied")

	_, err = store.GetQueue(context.Background(), testOperationQueue)
	assert.Error(t, err, "nothing may be applied from a refused entry")
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

// failingQueueStore fails one named operation the way a local disk would.
type failingQueueStore struct {
	storage.QueueStore
	failAppend bool
	failCreate bool
}

func (s failingQueueStore) Append(ctx context.Context, queueName string, msg *message.Envelope) (uint64, error) {
	if s.failAppend {
		return 0, errors.New("disk is on fire")
	}
	return s.QueueStore.Append(ctx, queueName, msg)
}

func (s failingQueueStore) CreateQueue(ctx context.Context, config types.QueueConfig) error {
	if s.failCreate {
		return errors.New("disk is on fire")
	}
	return s.QueueStore.CreateQueue(ctx, config)
}

// A store failure is local to one node: the entry committed, the peers applied
// it, and this replica did not. Raft advances the applied index as soon as
// Apply returns and no follower reads the result, so reporting the error and
// carrying on is exactly the silent gap the decode path already refuses.
func TestLogFSMApplyStopsOnLocalStoreFailure(t *testing.T) {
	t.Run("append", func(t *testing.T) {
		backing := memlog.New()
		fsm := NewLogFSM(failingQueueStore{QueueStore: backing, failAppend: true}, newRecordingGroupStore(), discardLogger())

		config := conformanceQueueConfig()
		require.NoError(t, backing.CreateQueue(context.Background(), config))

		data, err := marshalOperation(&Operation{
			Type: OpAppend, QueueName: config.Name,
			Message: encodeOperationEnvelope(t, newQueuedEnvelope("m1", "$queue/"+config.Name, []byte("payload"))),
		})
		require.NoError(t, err)

		assert.Panics(t, func() {
			fsm.Apply(&hraft.Log{Index: 20, Term: 4, Type: hraft.LogCommand, Data: data})
		}, "a committed append the local store refused must not be stepped over")
	})

	t.Run("create queue", func(t *testing.T) {
		backing := memlog.New()
		fsm := NewLogFSM(failingQueueStore{QueueStore: backing, failCreate: true}, newRecordingGroupStore(), discardLogger())

		config := conformanceQueueConfig()
		data, err := marshalOperation(&Operation{Type: OpCreateQueue, QueueName: config.Name, QueueConfig: &config})
		require.NoError(t, err)

		assert.Panics(t, func() {
			fsm.Apply(&hraft.Log{Index: 21, Term: 4, Type: hraft.LogCommand, Data: data})
		})
	})
}

// A payload every replica decodes the same way is not divergence: each one
// refuses it identically and the group stays consistent, so this still reports
// rather than stopping.
func TestLogFSMApplyReportsDeterministicRefusal(t *testing.T) {
	store := memlog.New()
	fsm := NewLogFSM(store, newRecordingGroupStore(), discardLogger())

	data, err := marshalOperation(&Operation{Type: OpAppend, QueueName: testOperationQueue, Message: []byte("not an envelope")})
	require.NoError(t, err)

	result, ok := fsm.Apply(&hraft.Log{Index: 22, Term: 4, Type: hraft.LogCommand, Data: data}).(*ApplyResult)
	require.True(t, ok)
	assert.Error(t, result.Error, "an undecodable payload is refused the same way on every replica")
}
