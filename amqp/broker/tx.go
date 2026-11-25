// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package broker

import "github.com/absmach/fluxmq/amqp/codec"

// An open transaction holds everything it will apply at tx.commit. Nothing
// reaches a queue before then, so without bounds a client that never commits
// could make the broker hold an unbounded backlog: maxTxBufferedBytes caps the
// bytes held, counting txOpOverhead for every action so that empty publishes
// and settlements count too, and maxTxOps caps the number of actions.
const (
	maxTxBufferedBytes = 64 << 20
	maxTxOps           = 1 << 16
	txOpOverhead       = 256
)

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
// the transaction would exceed its bounds.
func (t *txState) bufferPublish(method *codec.BasicPublish, header *codec.ContentHeader, body []byte) bool {
	if !t.admit(len(body)) {
		return false
	}
	t.ops = append(t.ops, txOp{publish: &txPublish{method: method, header: header, body: body}})
	return true
}

// bufferSettlement holds a basic.ack, basic.nack or basic.reject for commit.
// It reports false when the transaction would exceed its bounds.
func (t *txState) bufferSettlement(method any) bool {
	if !t.admit(0) {
		return false
	}
	t.ops = append(t.ops, txOp{settle: method})
	return true
}

func (t *txState) admit(bodyLen int) bool {
	size := bodyLen + txOpOverhead
	if len(t.ops) >= maxTxOps || t.bytes+size > maxTxBufferedBytes {
		return false
	}
	t.bytes += size
	return true
}

// take ends the open transaction, returning its actions; the channel stays
// transactional.
func (t *txState) take() []txOp {
	ops := t.ops
	t.ops, t.bytes = nil, 0
	return ops
}

// abortTransaction closes the channel because an open transaction outgrew
// its bounds, and releases what it held.
func (ch *Channel) abortTransaction(classID, methodID uint16) error {
	ch.tx.take()
	return ch.sendChannelClose(codec.PreconditionFailed,
		"transaction exceeds the uncommitted size limit", classID, methodID)
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
			// The exchange was checked when the publish arrived; it may have
			// been deleted since.
			if name := normalizeExchange(op.publish.method.Exchange); name != "" && !ch.exchangeExists(name) {
				return ch.sendChannelClose(codec.NotFound,
					"exchange not found", codec.ClassTx, codec.MethodTxCommit)
			}
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
