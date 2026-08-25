// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package broker

import (
	"context"
	"testing"

	"github.com/absmach/fluxmq/message"
	"github.com/absmach/fluxmq/mqtt/session"
	"github.com/absmach/fluxmq/storage"
	"github.com/absmach/fluxmq/storage/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func queueDeliveryBroker(t *testing.T) (*Broker, *mockQueueManager, *session.Session) {
	t.Helper()

	b := NewBroker(memory.New(), nil)
	t.Cleanup(func() { b.Close() })

	qm := &mockQueueManager{}
	require.NoError(t, b.SetQueueManager(qm))

	s, _, err := b.CreateSession("consumer", 4, session.Options{CleanStart: true})
	require.NoError(t, err)
	_, err = s.Connect(&captureConnection{})
	require.NoError(t, err)

	return b, qm, s
}

func queueDelivery() *message.Envelope {
	msg := message.NewDelivery("$queue/m/acme/temp", []byte("reading"), 1, false)
	msg.Broker.Queue.Name = "m"
	msg.Broker.Queue.MessageID = "m:42"
	msg.Broker.Queue.GroupID = "workers"
	msg.Broker.Queue.Offset = 42
	return msg
}

// TestPubAckSettlesQueueDelivery is what makes MQTT 3.1.1 a usable queue
// consumer. Settling normally requires publishing to <address>/$ack with the
// message and group identifiers in properties, which 3.1.1 cannot encode, so
// its messages would redeliver until they exhausted their delivery budget. The
// broker stamped those identifiers onto the delivery, so it settles the message
// when the client acknowledges the packet.
func TestPubAckSettlesQueueDelivery(t *testing.T) {
	b, qm, s := queueDeliveryBroker(t)

	packetID, err := b.DeliverToSession(context.Background(), s, queueDelivery())
	require.NoError(t, err)
	require.NotZero(t, packetID, "a QoS 1 delivery must be tracked in flight")
	require.Empty(t, qm.ackCalls, "the queue must not be settled before the client acknowledges")

	require.NoError(t, b.AckMessage(s, packetID))

	require.Len(t, qm.ackCalls, 1)
	assert.Equal(t, ackCall{queueName: "m", messageID: "m:42", groupID: "workers"}, qm.ackCalls[0])
}

// TestPubAckIgnoresOrdinaryDelivery keeps the settlement scoped to queue
// traffic: an ordinary pub/sub message carries none of that metadata and must
// not reach the queue manager.
func TestPubAckIgnoresOrdinaryDelivery(t *testing.T) {
	b, qm, s := queueDeliveryBroker(t)

	msg := message.NewDelivery(testTelemetryRoom, []byte("reading"), 1, false)

	packetID, err := b.DeliverToSession(context.Background(), s, msg)
	require.NoError(t, err)
	require.NoError(t, b.AckMessage(s, packetID))

	assert.Empty(t, qm.ackCalls)
}

// TestClassicQueueSubscriptionRequiresQoS refuses what the broker cannot
// honour. A classic queue holds each delivery until the consumer settles it,
// and this broker settles on PUBACK; QoS 0 sends none, so accepting the
// subscription would quietly discard the work it delivered.
func TestClassicQueueSubscriptionRequiresQoS(t *testing.T) {
	b, _, s := queueDeliveryBroker(t)

	err := b.subscribe(s, "$queue/m/#", 0, storage.SubscribeOptions{})
	require.ErrorIs(t, err, ErrQueueSubscriptionRequiresQoS)

	require.NoError(t, b.subscribe(s, "$queue/m/#", 1, storage.SubscribeOptions{}))
}

// TestOrdinaryQoSZeroSubscriptionIsUnaffected keeps the refusal narrow: only
// classic queue addresses need an acknowledgement to settle.
func TestOrdinaryQoSZeroSubscriptionIsUnaffected(t *testing.T) {
	b, _, s := queueDeliveryBroker(t)

	require.NoError(t, b.subscribe(s, "telemetry/#", 0, storage.SubscribeOptions{}))
}
