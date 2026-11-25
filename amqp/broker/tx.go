// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package broker

import "github.com/absmach/fluxmq/amqp/codec"

// maxTxBufferedBytes caps the message bodies one channel may hold in an open
// transaction. Nothing reaches a queue before tx.commit, so without a cap a
// client that never commits could make the broker hold an unbounded backlog.
const maxTxBufferedBytes = 64 << 20

// txOp is one deferred action of an open transaction: a completed publish, or
// a basic.ack, basic.nack or basic.reject.
type txOp struct {
	publish *txPublish
	settle  any
}

type txPublish struct {
	method *codec.BasicPublish
	header *codec.ContentHeader
	body   []byte
}

// txState is a channel's transaction, after tx.select. Publishes and
// settlements are held, in the order the client sent them, until tx.commit
// applies them or tx.rollback discards them. A settled delivery stays unacked
// meanwhile, so after a rollback the client can settle it again.
//
// Commit applies the actions in order but is not atomic: an action that fails
// on commit, or a broker that stops partway through, leaves the earlier ones
// applied. That is the guarantee AMQP 0.9.1 brokers give for transactions
// spanning several queues. What it does guarantee is that nothing rolled back
// is ever published or settled.
//
// A txState belongs to its channel's frame loop and is not safe for concurrent
// use.
type txState struct {
	selected bool
	ops      []txOp
	bytes    int
}

// bufferPublish holds a completed publish for commit. It reports false when
// the transaction would exceed maxTxBufferedBytes.
func (t *txState) bufferPublish(method *codec.BasicPublish, header *codec.ContentHeader, body []byte) bool {
	if t.bytes+len(body) > maxTxBufferedBytes {
		return false
	}
	t.bytes += len(body)
	t.ops = append(t.ops, txOp{publish: &txPublish{method: method, header: header, body: body}})
	return true
}

func (t *txState) bufferSettlement(method any) {
	t.ops = append(t.ops, txOp{settle: method})
}

// take ends the open transaction, returning its actions; the channel stays
// transactional.
func (t *txState) take() []txOp {
	ops := t.ops
	t.ops, t.bytes = nil, 0
	return ops
}

func (ch *Channel) handleTxSelect() error {
	if ch.confirmMode {
		return ch.sendChannelClose(codec.PreconditionFailed,
			"cannot switch from confirm to tx mode", codec.ClassTx, codec.MethodTxSelect)
	}
	ch.tx.selected = true
	return ch.conn.writeMethod(ch.id, &codec.TxSelectOk{})
}

func (ch *Channel) handleTxCommit() error {
	if !ch.tx.selected {
		return ch.sendChannelClose(codec.PreconditionFailed,
			"channel is not transactional", codec.ClassTx, codec.MethodTxCommit)
	}
	for _, op := range ch.tx.take() {
		if op.publish != nil {
			ch.publish(op.publish.method, op.publish.header, op.publish.body)
		} else {
			ch.applySettlement(op.settle)
		}
		// A publish that is refused closes the channel; the rest of the
		// transaction goes with it.
		if ch.serverClosing.Load() {
			return nil
		}
	}
	return ch.conn.writeMethod(ch.id, &codec.TxCommitOk{})
}

func (ch *Channel) handleTxRollback() error {
	if !ch.tx.selected {
		return ch.sendChannelClose(codec.PreconditionFailed,
			"channel is not transactional", codec.ClassTx, codec.MethodTxRollback)
	}
	ch.tx.take()
	return ch.conn.writeMethod(ch.id, &codec.TxRollbackOk{})
}

// applySettlement applies a basic.ack, basic.nack or basic.reject that a
// transaction held back.
func (ch *Channel) applySettlement(method any) {
	switch m := method.(type) {
	case *codec.BasicAck:
		ch.applyBasicAck(m)
	case *codec.BasicNack:
		ch.applyBasicNack(m)
	case *codec.BasicReject:
		ch.applyBasicReject(m)
	}
}
