// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package queue

import (
	"context"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/absmach/fluxmq/logstorage"
	coremessage "github.com/absmach/fluxmq/message"
	queuev1 "github.com/absmach/fluxmq/pkg/proto/queue/v1"
	queuepkg "github.com/absmach/fluxmq/queue"
	memlog "github.com/absmach/fluxmq/queue/storage/memory/log"
	"github.com/absmach/fluxmq/queue/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const testQueueAppends = "appends"

func newAppendHandler(t *testing.T) (*Handler, *memlog.Store, context.Context) {
	t.Helper()

	ctx := context.Background()
	store := memlog.New()
	manager := queuepkg.NewManager(store, noopGroupStore{}, nil, queuepkg.DefaultConfig(), nil, nil)
	require.NoError(t, manager.CreateQueue(ctx, types.DefaultQueueConfig(testQueueAppends, "appends/#")))

	return NewHandler(manager, store, noopGroupStore{}, nil), store, ctx
}

// queueErrorDetail extracts the typed detail every QueueService error carries.
func queueErrorDetail(t *testing.T, err error) *queuev1.QueueErrorDetail {
	t.Helper()

	var connectErr *connect.Error
	require.ErrorAs(t, err, &connectErr)
	for _, detail := range connectErr.Details() {
		value, valueErr := detail.Value()
		require.NoError(t, valueErr)
		if typed, ok := value.(*queuev1.QueueErrorDetail); ok {
			return typed
		}
	}
	t.Fatalf("error carried no QueueErrorDetail: %v", err)
	return nil
}

// The append responses report the timestamp the record was assigned, not the
// time the call returned. A batch reports the last record's timestamp, which is
// the one that pairs with last_offset.
func TestAppendResponsesCarryRecordTimestamps(t *testing.T) {
	h, store, ctx := newAppendHandler(t)

	before := time.Now()
	resp, err := h.Append(ctx, connect.NewRequest(&queuev1.AppendRequest{
		QueueName: testQueueAppends,
		Value:     []byte("one"),
	}))
	require.NoError(t, err)
	after := time.Now()

	stored, err := store.Read(ctx, testQueueAppends, resp.Msg.Offset)
	require.NoError(t, err)
	t.Cleanup(func() { coremessage.Release(stored) })

	assert.WithinDuration(t, stored.Broker.Queue.CreatedAt, resp.Msg.Timestamp.AsTime(), time.Millisecond,
		"append timestamp must be the record's, not the response time")
	assert.False(t, resp.Msg.Timestamp.AsTime().Before(before.Add(-time.Second)))
	assert.False(t, resp.Msg.Timestamp.AsTime().After(after.Add(time.Second)))

	batch, err := h.AppendBatch(ctx, connect.NewRequest(&queuev1.AppendBatchRequest{
		QueueName: testQueueAppends,
		Messages: []*queuev1.BatchMessage{
			{Value: []byte("two")},
			{Value: []byte("three")},
		},
	}))
	require.NoError(t, err)

	last, err := store.Read(ctx, testQueueAppends, batch.Msg.LastOffset)
	require.NoError(t, err)
	t.Cleanup(func() { coremessage.Release(last) })

	assert.WithinDuration(t, last.Broker.Queue.CreatedAt, batch.Msg.Timestamp.AsTime(), time.Millisecond,
		"batch timestamp must be the last appended record's")
}

// Offset 0 is a valid offset, so an append that writes nothing must not answer
// with a zero-valued success.
func TestAppendRejectsEmptyRequests(t *testing.T) {
	h, _, ctx := newAppendHandler(t)

	t.Run("batch", func(t *testing.T) {
		_, err := h.AppendBatch(ctx, connect.NewRequest(&queuev1.AppendBatchRequest{
			QueueName: testQueueAppends,
			Messages:  nil,
		}))
		require.Error(t, err)
		assert.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))
		assert.Equal(t, queuev1.QueueErrorCode_QUEUE_ERROR_CODE_INVALID_ARGUMENT, queueErrorDetail(t, err).Code)
	})

	t.Run("state machine command", func(t *testing.T) {
		_, err := h.manager.StateMachine().Append(ctx, queuepkg.AppendCommand{
			QueueName: testQueueAppends,
		})
		require.ErrorIs(t, err, queuepkg.ErrInvalidCommand)
	})
}

// A multi-offset settlement stops at its first failure. The error must report
// the prefix it did settle, so the client re-acknowledges only what remains
// instead of re-sending the whole set.
func TestAckPartialFailureReportsProgress(t *testing.T) {
	ctx := context.Background()
	store, err := logstorage.NewAdapter(t.TempDir(), logstorage.DefaultAdapterConfig())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })

	manager := queuepkg.NewManager(store, store, nil, queuepkg.DefaultConfig(), nil, nil)
	h := NewHandler(manager, nil, nil, nil)
	require.NoError(t, manager.CreateQueue(ctx, types.DefaultQueueConfig(testQueueAppends, "appends/#")))
	require.NoError(t, store.CreateConsumerGroup(ctx, types.NewConsumerGroupState(testQueueAppends, testGroupWorkers, "")))
	require.NoError(t, store.RegisterConsumer(ctx, testQueueAppends, testGroupWorkers,
		&types.ConsumerInfo{ID: testConsumer1, ClientID: testConsumer1}))

	_, err = h.AppendBatch(ctx, connect.NewRequest(&queuev1.AppendBatchRequest{
		QueueName: testQueueAppends,
		Messages: []*queuev1.BatchMessage{
			{Value: []byte("zero")},
			{Value: []byte("one")},
		},
	}))
	require.NoError(t, err)

	// Claim only offset 0 into the pending list. Offset 1 is never delivered, so
	// acknowledging it must fail after offset 0 has already settled.
	consumed, err := h.Consume(ctx, connect.NewRequest(&queuev1.ConsumeRequest{
		QueueName: testQueueAppends, GroupId: testGroupWorkers, ConsumerId: testConsumer1, MaxMessages: 1,
	}))
	require.NoError(t, err)
	require.Len(t, consumed.Msg.Messages, 1)
	require.Equal(t, uint64(0), consumed.Msg.Messages[0].Offset)

	_, err = h.Ack(ctx, connect.NewRequest(&queuev1.AckRequest{
		QueueName:  testQueueAppends,
		GroupId:    testGroupWorkers,
		ConsumerId: testConsumer1,
		Offsets:    []uint64{0, 1},
	}))
	require.Error(t, err, "acking an undelivered offset must fail")

	detail := queueErrorDetail(t, err)
	require.NotNil(t, detail.Progress, "a partial settlement must report its committed prefix")
	assert.Equal(t, uint32(1), detail.Progress.ProcessedCount, "offset 0 settled before the failure")
	assert.Equal(t, uint64(1), detail.Progress.FailedOffset, "offset 1 is where the command stopped")
	assert.Equal(t, uint64(1), detail.Progress.Committed, "committed cursor advanced past the settled prefix")
}
