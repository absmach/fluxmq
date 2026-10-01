// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package raft

import (
	"context"
	"testing"

	"github.com/absmach/fluxmq/queue/storage"
	"github.com/absmach/fluxmq/queue/types"
	hraft "github.com/hashicorp/raft"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func applyLogged(t *testing.T, fsm *LogFSM, index uint64, op *Operation) *ApplyResult {
	t.Helper()
	data, err := marshalOperation(op)
	require.NoError(t, err)
	result, ok := fsm.Apply(&hraft.Log{Index: index, Term: 1, Type: hraft.LogCommand, Data: data}).(*ApplyResult)
	require.True(t, ok)
	return result
}

// Recording a created queue's settings takes a create and then an update, and a
// delete can commit between them. The update must then find nothing to update
// on every replica, not bring back a queue whose log the delete removed.
func TestLogFSMUpdateAfterDeleteLeavesQueueDeleted(t *testing.T) {
	cases := []struct {
		name string
		fsm  func(t *testing.T) (*LogFSM, storage.QueueStore)
	}{
		{name: "store/disk-adapter", fsm: func(t *testing.T) (*LogFSM, storage.QueueStore) { return newAdapterFSM(t) }},
		{name: "store/memory", fsm: func(*testing.T) (*LogFSM, storage.QueueStore) { return newTestLogFSM() }},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			fsm, store := tc.fsm(t)
			config := types.DefaultQueueConfig(testOperationQueue, "jobs/#")
			config.Replication.Enabled = true
			config.Replication.Group = testFSMGroup

			require.NoError(t, applyLogged(t, fsm, 1, &Operation{Type: OpCreateQueue, QueueName: config.Name, QueueConfig: &config}).Error)
			require.NoError(t, applyLogged(t, fsm, 2, &Operation{Type: OpDeleteQueue, QueueName: config.Name}).Error)

			var result *ApplyResult
			require.NotPanics(t, func() {
				result = applyLogged(t, fsm, 3, &Operation{Type: OpUpdateQueue, QueueName: config.Name, QueueConfig: &config})
			})
			require.ErrorIs(t, result.Error, storage.ErrQueueNotFound)
			assert.False(t, fsm.IsQueueConfigRecorded(config.Name))
			_, err := store.GetQueue(ctx, config.Name)
			require.ErrorIs(t, err, storage.ErrQueueNotFound, "the update must not recreate the deleted queue")

			require.NotPanics(t, func() {
				result = applyLogged(t, fsm, 4, dedupeOperation(t, config.Name, "key-1", "after delete"))
			})
			require.NoError(t, result.Error, "an append to the deleted queue takes the auto-create path")
		})
	}
}
