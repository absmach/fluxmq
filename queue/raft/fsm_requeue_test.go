// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package raft

import (
	"context"
	"testing"
	"time"

	memlog "github.com/absmach/fluxmq/queue/storage/memory/log"
	"github.com/stretchr/testify/require"
)

type recordingRequeueStore struct {
	noopGroupStore
	queueName  string
	groupID    string
	consumerID string
	offset     uint64
	attempted  time.Time
}

func (s *recordingRequeueStore) RequeuePendingEntry(_ context.Context, queueName, groupID, consumerID string, offset uint64, attemptedAt time.Time) error {
	s.queueName, s.groupID, s.consumerID = queueName, groupID, consumerID
	s.offset, s.attempted = offset, attemptedAt
	return nil
}

func TestLogFSMApplyRequeuePending(t *testing.T) {
	store := new(recordingRequeueStore)
	fsm := NewLogFSM(testFSMGroup, memlog.New(), store, discardLogger())
	op := &Operation{
		Type: OpRequeuePending, Timestamp: conformanceTime,
		QueueName: testOperationQueue, GroupID: testOperationGroup,
		ConsumerID: testOperationConsumerA, Offset: 42,
	}

	result := fsm.applyRequeuePending(context.Background(), op)
	require.NoError(t, result.Error)
	require.Equal(t, testOperationQueue, store.queueName)
	require.Equal(t, testOperationGroup, store.groupID)
	require.Equal(t, testOperationConsumerA, store.consumerID)
	require.Equal(t, uint64(42), store.offset)
	require.True(t, conformanceTime.Equal(store.attempted))
}
