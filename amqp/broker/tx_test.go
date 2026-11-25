// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package broker

import (
	"bytes"
	"context"
	"testing"

	"github.com/absmach/fluxmq/amqp/codec"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	txExchange   = "tx-events"
	txRoutingKey = "k"
)

// settlementRecorder records the settlements that reach the queue manager.
type settlementRecorder struct {
	mockChannelQueueManager
	acks, nacks, rejects []uint64
}

func (m *settlementRecorder) Ack(_ context.Context, _, _ string, offset uint64) error {
	m.acks = append(m.acks, offset)
	return nil
}

func (m *settlementRecorder) Nack(_ context.Context, _, _ string, offset uint64) error {
	m.nacks = append(m.nacks, offset)
	return nil
}

func (m *settlementRecorder) Reject(_ context.Context, _, _ string, offset uint64, _ string) error {
	m.rejects = append(m.rejects, offset)
	return nil
}

// newTxChannel returns a channel whose exchange routes txRoutingKey to a
// queue, so a completed publish reaches the queue manager.
func newTxChannel(t *testing.T) (*Channel, *bytes.Buffer, *settlementRecorder) {
	t.Helper()
	ch, buf := newTestChannel(t)
	qm := &settlementRecorder{}
	ch.conn.broker.queueManager = qm
	ch.exchanges[txExchange] = &exchange{name: txExchange, typ: "direct"}
	ch.bindings = append(ch.bindings, binding{queue: testOrders, exchange: txExchange, routingKey: txRoutingKey})
	return ch, buf, qm
}

func publishBody(ch *Channel, body []byte) {
	ch.pendingMethod = &codec.BasicPublish{Exchange: txExchange, RoutingKey: txRoutingKey}
	ch.pendingHeader = &codec.ContentHeader{ClassID: codec.ClassBasic, BodySize: uint64(len(body))}
	ch.pendingBody = body
	ch.completePublish()
}

// lastMethod decodes the most recent method the channel wrote.
func lastMethod(t *testing.T, buf *bytes.Buffer) any {
	t.Helper()
	frames := readFramesFrom(t, buf, 0)
	require.NotEmpty(t, frames, "the channel wrote nothing")
	decoded, err := frames[len(frames)-1].Decode()
	require.NoError(t, err)
	return decoded
}

func TestTransactionPublishesOnlyOnCommit(t *testing.T) {
	tests := []struct {
		name      string
		finish    []any
		published int
	}{
		{"commit", []any{&codec.TxCommit{}}, 2},
		{"rollback", []any{&codec.TxRollback{}}, 0},
		{"rollback_then_commit", []any{&codec.TxRollback{}, &codec.TxCommit{}}, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ch, _, qm := newTxChannel(t)
			require.NoError(t, ch.handleMethod(&codec.TxSelect{}))

			publishBody(ch, []byte("one"))
			publishBody(ch, []byte("two"))
			require.Zero(t, qm.publishCalls, "a transactional publish reached the queue before commit")

			for _, method := range tt.finish {
				require.NoError(t, ch.handleMethod(method))
			}
			assert.Equal(t, tt.published, qm.publishCalls)
		})
	}
}

// A settlement in a transaction is applied at commit and discarded on
// rollback, and the delivery stays unacked until then, so the client can
// settle it again after a rollback.
func TestTransactionDefersSettlement(t *testing.T) {
	tests := []struct {
		name   string
		settle any
		record func(*settlementRecorder) []uint64
	}{
		{"ack", &codec.BasicAck{DeliveryTag: 1}, func(r *settlementRecorder) []uint64 { return r.acks }},
		{"nack", &codec.BasicNack{DeliveryTag: 1, Requeue: true}, func(r *settlementRecorder) []uint64 { return r.nacks }},
		{"reject", &codec.BasicReject{DeliveryTag: 1}, func(r *settlementRecorder) []uint64 { return r.rejects }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ch, _, qm := newTxChannel(t)
			ch.unacked[1] = &unackedDelivery{deliveryTag: 1, queueName: testOrders, groupID: "g", offset: 42, queued: true}
			require.NoError(t, ch.handleMethod(&codec.TxSelect{}))

			require.NoError(t, ch.handleMethod(tt.settle))
			assert.Empty(t, tt.record(qm), "settled before commit")
			require.NoError(t, ch.handleMethod(&codec.TxRollback{}))
			assert.Empty(t, tt.record(qm), "a rolled-back settlement was applied")
			assert.Contains(t, ch.unacked, uint64(1), "a rolled-back settlement removed the delivery")

			require.NoError(t, ch.handleMethod(tt.settle))
			require.NoError(t, ch.handleMethod(&codec.TxCommit{}))
			assert.Equal(t, []uint64{42}, tt.record(qm))
			assert.NotContains(t, ch.unacked, uint64(1))
		})
	}
}

func TestTransactionAcknowledgesEachMethod(t *testing.T) {
	ch, buf, _ := newTxChannel(t)
	for method, want := range map[any]any{
		&codec.TxSelect{}:   &codec.TxSelectOk{},
		&codec.TxCommit{}:   &codec.TxCommitOk{},
		&codec.TxRollback{}: &codec.TxRollbackOk{},
	} {
		buf.Reset()
		if _, ok := method.(*codec.TxSelect); !ok {
			require.NoError(t, ch.handleMethod(&codec.TxSelect{}))
			buf.Reset()
		}
		require.NoError(t, ch.handleMethod(method))
		assert.IsType(t, want, lastMethod(t, buf))
	}
}

func TestTransactionPreconditions(t *testing.T) {
	tests := []struct {
		name    string
		methods []any
	}{
		{"commit_without_select", []any{&codec.TxCommit{}}},
		{"rollback_without_select", []any{&codec.TxRollback{}}},
		{"select_on_confirm_channel", []any{&codec.ConfirmSelect{NoWait: true}, &codec.TxSelect{}}},
		{"confirm_on_tx_channel", []any{&codec.TxSelect{}, &codec.ConfirmSelect{}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ch, buf, _ := newTxChannel(t)
			for _, method := range tt.methods {
				require.NoError(t, ch.handleMethod(method))
			}
			closeMethod, ok := lastMethod(t, buf).(*codec.ChannelClose)
			require.True(t, ok, "expected the channel to close")
			assert.Equal(t, uint16(codec.PreconditionFailed), closeMethod.ReplyCode)
		})
	}
}

// A transaction that is never committed must not make the broker hold an
// unbounded backlog.
func TestTransactionSizeIsBounded(t *testing.T) {
	ch, buf, qm := newTxChannel(t)
	require.NoError(t, ch.handleMethod(&codec.TxSelect{}))

	body := make([]byte, maxTxBufferedBytes/2+1)
	publishBody(ch, body)
	publishBody(ch, body)

	closeMethod, ok := lastMethod(t, buf).(*codec.ChannelClose)
	require.True(t, ok, "expected the channel to close")
	assert.Equal(t, uint16(codec.PreconditionFailed), closeMethod.ReplyCode)
	assert.Zero(t, qm.publishCalls)
}
