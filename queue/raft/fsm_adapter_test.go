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
