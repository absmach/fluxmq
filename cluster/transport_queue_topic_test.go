// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package cluster

import (
	"testing"

	queueTypes "github.com/absmach/fluxmq/queue/types"
)

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

// The delivery address cannot be reversed into a source topic, so a consumer on
// another node depends entirely on the source topic surviving the wire.
func TestRouteQueueMessageWirePreservesSourceTopic(t *testing.T) {
	const sourceTopic = "domain/c/channel/tst"

	wire := encodeRouteQueueMessage("consumer", "m", &QueueMessage{
		MessageID:      "m:1",
		QueueName:      "m",
		GroupID:        "rules-engine",
		Topic:          "$queue/m/domain/c/channel/tst",
		SourceTopic:    sourceTopic,
		Payload:        []byte("payload"),
		Sequence:       1,
		UserProperties: map[string]string{"user": "kept"},
	})

	decoded := decodeRouteQueueMessage(wire)
	if decoded.SourceTopic != sourceTopic {
		t.Fatalf("decoded source topic = %q, want %q", decoded.SourceTopic, sourceTopic)
	}
	// Queue-owned metadata must not leak into the user properties a consumer
	// sees as its own.
	if _, leaked := decoded.UserProperties[queueTypes.PropSourceTopic]; leaked {
		t.Fatal("the source topic leaked into user properties")
	}
	if decoded.UserProperties["user"] != "kept" {
		t.Fatalf("an ordinary user property was dropped: %v", decoded.UserProperties)
	}
}
