// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package raft

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/absmach/fluxmq/message"
	"github.com/absmach/fluxmq/queue/storage"
	memlog "github.com/absmach/fluxmq/queue/storage/memory/log"
	"github.com/absmach/fluxmq/queue/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const testDedupeKey = "dlq-abc123"

// plainQueueStore exposes only QueueStore's method set, so the FSM's assertion
// to storage.DeduplicatingQueueStore fails the way a third-party store would.
type plainQueueStore struct {
	storage.QueueStore
}

// replicate encodes an operation the way Raft does and hands each replica its
// own decoded copy, so no replica can observe another's envelope or result.
func replicate(t *testing.T, op *Operation, replicas ...*LogFSM) []*ApplyResult {
	t.Helper()

	data, err := json.Marshal(op)
	require.NoError(t, err)

	results := make([]*ApplyResult, 0, len(replicas))
	for _, fsm := range replicas {
		var decoded Operation
		require.NoError(t, json.Unmarshal(data, &decoded))
		results = append(results, fsm.applyAppend(context.Background(), &decoded))
	}
	return results
}

func dedupeOperation(t *testing.T, queueName, key, payload string) *Operation {
	return &Operation{
		Type:      OpAppend,
		QueueName: queueName,
		DedupeKey: key,
		Message:   encodeOperationEnvelope(t, newQueuedEnvelope("msg", "$queue/"+queueName, []byte(payload))),
	}
}

// A repeated key must not append twice, and must report the offset the first
// attempt landed on so the caller can settle against it.
func TestLogFSMAppendOnceDeduplicatesRepeatedKey(t *testing.T) {
	fsm, store := newTestLogFSM()
	ctx := context.Background()
	queueName := "dedupe-events"

	first := fsm.applyAppend(ctx, dedupeOperation(t, queueName, testDedupeKey, "one"))
	require.NoError(t, first.Error)
	assert.False(t, first.Deduplicated)

	second := fsm.applyAppend(ctx, dedupeOperation(t, queueName, testDedupeKey, "two"))
	require.NoError(t, second.Error)
	assert.True(t, second.Deduplicated, "a repeated key must be recognised")
	assert.Equal(t, first.Offset, second.Offset, "the caller must learn where the record is")

	count, err := store.Count(ctx, queueName)
	require.NoError(t, err)
	assert.Equal(t, uint64(1), count, "exactly one record may exist")
}

// The property that matters for replication: every replica decides the same
// way, so the stores hold the same records. A decision taken only on the leader
// would let a follower append a second copy of the retried transfer.
func TestLogFSMAppendOnceKeepsReplicasIdentical(t *testing.T) {
	ctx := context.Background()
	queueName := "dedupe-replicated"

	leader, leaderStore := newTestLogFSM()
	follower, followerStore := newTestLogFSM()

	// The transfer, then a retry of it after a settlement that never landed.
	for _, payload := range []string{"transfer", "retry"} {
		results := replicate(t, dedupeOperation(t, queueName, testDedupeKey, payload), leader, follower)
		require.NoError(t, results[0].Error)
		require.NoError(t, results[1].Error)
		assert.Equal(t, results[0].Deduplicated, results[1].Deduplicated,
			"replicas must agree on whether the record was already present")
		assert.Equal(t, results[0].Offset, results[1].Offset)
	}

	// A distinct key is a distinct record on both.
	results := replicate(t, dedupeOperation(t, queueName, "dlq-other", "second"), leader, follower)
	require.NoError(t, results[0].Error)
	require.NoError(t, results[1].Error)
	assert.False(t, results[0].Deduplicated)

	leaderCount, err := leaderStore.Count(ctx, queueName)
	require.NoError(t, err)
	followerCount, err := followerStore.Count(ctx, queueName)
	require.NoError(t, err)
	require.Equal(t, uint64(2), leaderCount)
	assert.Equal(t, leaderCount, followerCount, "replicas diverged on record count")

	for offset := range leaderCount {
		onLeader, err := leaderStore.Read(ctx, queueName, offset)
		require.NoError(t, err)
		onFollower, err := followerStore.Read(ctx, queueName, offset)
		require.NoError(t, err)
		assert.Equal(t, string(onLeader.PayloadBytes()), string(onFollower.PayloadBytes()),
			"replicas diverged at offset %d", offset)
		assert.Equal(t, onLeader.BrokerMeta.Transfer.ID, onFollower.BrokerMeta.Transfer.ID)
	}
}

// The key travels as replicated state. A dropped struct tag would leave every
// follower doing a plain append while the leader deduplicated, which is exactly
// the divergence this design exists to prevent.
func TestLogFSMAppendOnceCarriesKeyThroughJSON(t *testing.T) {
	data, err := json.Marshal(dedupeOperation(t, "dedupe-json", testDedupeKey, "one"))
	require.NoError(t, err)

	var decoded Operation
	require.NoError(t, json.Unmarshal(data, &decoded))
	require.NotEmpty(t, decoded.Message, "the encoded envelope must survive replication")

	assert.Equal(t, testDedupeKey, decoded.DedupeKey, "the key must survive replication")
}

// A store that cannot deduplicate must refuse rather than fall back: falling
// back is a per-node decision, and a cluster where only some nodes can perform
// the check would disagree about what the queue holds.
func TestLogFSMAppendOnceRefusesStoreWithoutCapability(t *testing.T) {
	ctx := context.Background()
	queueName := "dedupe-unsupported"
	backing := memlog.New()
	require.NoError(t, backing.CreateQueue(ctx, types.DefaultQueueConfig(queueName, queueName+"/#")))
	fsm := NewLogFSM(plainQueueStore{QueueStore: backing}, noopGroupStore{}, discardLogger())

	result := fsm.applyAppend(ctx, dedupeOperation(t, queueName, testDedupeKey, "one"))
	require.ErrorIs(t, result.Error, storage.ErrDeduplicationUnsupported)

	count, err := backing.Count(ctx, queueName)
	require.NoError(t, err)
	assert.Zero(t, count, "a refused append must not write a record")
}

// An append with no key stays a plain append; only a keyed one is conditional.
func TestLogFSMAppendWithoutKeyIsNotDeduplicated(t *testing.T) {
	fsm, store := newTestLogFSM()
	ctx := context.Background()
	queueName := "dedupe-absent"

	for range 2 {
		result := fsm.applyAppend(ctx, &Operation{
			Type:      OpAppend,
			QueueName: queueName,
			Message:   encodeOperationEnvelope(t, newQueuedEnvelope("msg", "$queue/"+queueName, []byte("payload"))),
		})
		require.NoError(t, result.Error)
		assert.False(t, result.Deduplicated)
	}

	count, err := store.Count(ctx, queueName)
	require.NoError(t, err)
	assert.Equal(t, uint64(2), count)
}

// Parity with the plain append path: a keyed append to a queue the replica has
// not created yet must still land.
func TestLogFSMAppendOnceAutoCreatesMissingQueue(t *testing.T) {
	fsm, store := newTestLogFSM()
	ctx := context.Background()
	queueName := "dedupe-autocreate"

	result := fsm.applyAppend(ctx, dedupeOperation(t, queueName, testDedupeKey, "one"))
	require.NoError(t, result.Error)

	stored, err := store.Read(ctx, queueName, result.Offset)
	require.NoError(t, err)
	t.Cleanup(func() { message.Release(stored) })
	assert.Equal(t, testDedupeKey, stored.BrokerMeta.Transfer.ID,
		"the key must reach the record so a rebuild can recover it")
}
