// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package cluster

import "testing"

func TestRouteQueueMessageWirePreservesDeliveryTopic(t *testing.T) {
	const deliveryTopic = "$queue/m/domain/c/channel/tst"

	wire := encodeRouteQueueMessage("consumer", "m", &QueueMessage{
		MessageID: "m:1",
		QueueName: "m",
		GroupID:   "rules-engine",
		Topic:     deliveryTopic,
		Payload:   []byte("payload"),
		Sequence:  1,
		Stream:    true,
	})
	if wire.Topic != deliveryTopic {
		t.Fatalf("wire topic = %q, want %q", wire.Topic, deliveryTopic)
	}

	decoded := decodeRouteQueueMessage(wire)
	if decoded.Topic != deliveryTopic {
		t.Fatalf("decoded topic = %q, want %q", decoded.Topic, deliveryTopic)
	}
}
