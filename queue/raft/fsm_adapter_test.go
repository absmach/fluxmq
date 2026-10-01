// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package raft

import (
	"bytes"
	"context"
	"io"
	"testing"

	"github.com/absmach/fluxmq/logstorage"
	"github.com/absmach/fluxmq/message"
	"github.com/absmach/fluxmq/queue/storage"
	hraft "github.com/hashicorp/raft"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newAdapterFSM(t *testing.T) (*LogFSM, *logstorage.Adapter) {
	t.Helper()

	adapter, err := logstorage.NewAdapter(t.TempDir(), logstorage.DefaultAdapterConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = adapter.Close() })

	return NewLogFSM(testFSMGroup, adapter, adapter, discardLogger()), adapter
}

// The broker runs on logstorage.Adapter, never on the memory store. An FSM that
// cannot snapshot refuses outright, which stops raft from ever compacting the
// log, so the production store has to satisfy the capture contract.
func TestLogFSMSnapshotsThroughProductionAdapter(t *testing.T) {
	ctx := context.Background()
	fsm, source := newAdapterFSM(t)

	config := conformanceQueueConfig()
	require.NoError(t, source.CreateQueue(ctx, config))

	payloads := []string{payloadFirst, payloadSecond}
	for _, payload := range payloads {
		data, err := marshalOperation(&Operation{
			Type: OpAppend, QueueName: config.Name,
			Message: encodeOperationEnvelope(t, newQueuedEnvelope(payload, "$queue/"+config.Name, []byte(payload))),
		})
		require.NoError(t, err)

		result, ok := fsm.Apply(&hraft.Log{Index: 1, Term: 1, Type: hraft.LogCommand, Data: data}).(*ApplyResult)
		require.True(t, ok)
		require.NoError(t, result.Error)
	}

	snapshot, err := fsm.Snapshot()
	require.NoError(t, err, "the production queue store must be snapshottable")

	sink := new(memSink)
	require.NoError(t, snapshot.Persist(sink))
	require.False(t, sink.cancelled)
	snapshot.Release()

	targetFSM, target := newAdapterFSM(t)
	require.NoError(t, targetFSM.Restore(io.NopCloser(bytes.NewReader(sink.Bytes()))))

	count, err := target.Count(ctx, config.Name)
	require.NoError(t, err)
	assert.Equal(t, uint64(2), count, "the records must cross the snapshot")

	for offset, want := range map[uint64]string{0: payloadFirst, 1: payloadSecond} {
		got, readErr := target.Read(ctx, config.Name, offset)
		require.NoError(t, readErr, "offset %d", offset)
		assert.Equal(t, want, string(got.PayloadBytes()), "offset %d", offset)
		message.Release(got)
	}
}

// Two deletes of one queue can both commit: each caller checks the queue
// exists before its delete is applied. Every replica applies the second
// against a queue the first removed, which must be a no-op on the
// production store rather than a local failure that stops the node.
func TestLogFSMRepeatedDeleteThroughProductionAdapter(t *testing.T) {
	ctx := context.Background()
	fsm, adapter := newAdapterFSM(t)

	config := conformanceQueueConfig()
	require.NoError(t, adapter.CreateQueue(ctx, config))

	for index := uint64(1); index <= 2; index++ {
		require.NotPanics(t, func() {
			result := applyLogged(t, fsm, index, &Operation{Type: OpDeleteQueue, QueueName: config.Name})
			require.NoError(t, result.Error)
		}, "delete %d", index)
	}

	_, err := adapter.GetQueue(ctx, config.Name)
	assert.ErrorIs(t, err, storage.ErrQueueNotFound)
}
