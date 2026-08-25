// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package cluster

import (
	"testing"

	"github.com/absmach/fluxmq/message"
	clusterv1 "github.com/absmach/fluxmq/pkg/proto/cluster/v1"
)

const (
	testQueueMessageID  = "m:1"
	testUserPropertyVal = "kept"
)

func TestRouteQueueMessageWirePreservesDeliveryTopic(t *testing.T) {
	const deliveryTopic = "$queue/m/domain/c/channel/tst"

	envelope := queueTestEnvelope(deliveryTopic, "domain/c/channel/tst")
	defer message.Release(envelope)
	wire := encodeRouteQueueMessage("consumer", envelope)
	if wire.Topic != deliveryTopic {
		t.Fatalf("wire topic = %q, want %q", wire.Topic, deliveryTopic)
	}

	decoded, err := decodeRouteQueueMessage(wire)
	if err != nil {
		t.Fatalf("decode queue message: %v", err)
	}
	defer message.Release(decoded)
	if decoded.Topic != deliveryTopic {
		t.Fatalf("decoded topic = %q, want %q", decoded.Topic, deliveryTopic)
	}
}

// The delivery address cannot be reversed into a source topic, so a consumer on
// another node depends entirely on the source topic surviving the wire.
func TestRouteQueueMessageWirePreservesSourceTopic(t *testing.T) {
	const sourceTopic = "domain/c/channel/tst"

	envelope := queueTestEnvelope("$queue/m/domain/c/channel/tst", sourceTopic)
	defer message.Release(envelope)
	envelope.User.Properties = map[string]string{"user": testUserPropertyVal}
	wire := encodeRouteQueueMessage("consumer", envelope)

	decoded, err := decodeRouteQueueMessage(wire)
	if err != nil {
		t.Fatalf("decode queue message: %v", err)
	}
	defer message.Release(decoded)
	if decoded.Broker.Source.Topic != sourceTopic {
		t.Fatalf("decoded source topic = %q, want %q", decoded.Broker.Source.Topic, sourceTopic)
	}
	// Queue-owned metadata must not leak into the user properties a consumer
	// sees as its own.
	if _, leaked := decoded.User.Properties[message.PropertySourceTopic]; leaked {
		t.Fatal("the source topic leaked into user properties")
	}
	if decoded.User.Properties["user"] != testUserPropertyVal {
		t.Fatalf("an ordinary user property was dropped: %v", decoded.User.Properties)
	}
}

// Source topic is broker-owned even when its real value is empty. The encoder
// must overwrite a publisher-supplied value before the decoder promotes the
// reserved property into the typed source namespace.
func TestRouteQueueMessageWireClearsForgedEmptySourceTopic(t *testing.T) {
	envelope := queueTestEnvelope("$queue/m", "")
	defer message.Release(envelope)
	envelope.User.Properties = map[string]string{
		message.PropertySourceTopic: "forged/topic",
		"user":                      testUserPropertyVal,
	}
	wire := encodeRouteQueueMessage("consumer", envelope)

	if sourceTopic, ok := wire.Properties[message.PropertySourceTopic]; !ok || sourceTopic != "" {
		t.Fatalf("wire source topic = %q, present=%t; want an explicit empty broker value", sourceTopic, ok)
	}

	decoded, err := decodeRouteQueueMessage(wire)
	if err != nil {
		t.Fatalf("decode queue message: %v", err)
	}
	defer message.Release(decoded)
	if decoded.Broker.Source.Topic != "" {
		t.Fatalf("decoded source topic = %q, want the real empty source topic", decoded.Broker.Source.Topic)
	}
	if _, leaked := decoded.User.Properties[message.PropertySourceTopic]; leaked {
		t.Fatal("the source topic leaked into user properties")
	}
	if decoded.User.Properties["user"] != testUserPropertyVal {
		t.Fatalf("an ordinary user property was dropped: %v", decoded.User.Properties)
	}
}

func TestDecodeRouteQueueMessageRequiresCanonicalTopic(t *testing.T) {
	if _, err := decodeRouteQueueMessage(&clusterv1.RouteQueueMessageRequest{Payload: []byte("payload")}); err == nil {
		t.Fatal("missing canonical queue delivery topic was accepted")
	}
}

func queueTestEnvelope(topic, sourceTopic string) *message.Envelope {
	envelope := message.New(topic, []byte("payload"))
	envelope.Broker.Source.Topic = sourceTopic
	envelope.Broker.Queue.MessageID = testQueueMessageID
	envelope.Broker.Queue.Name = "m"
	envelope.Broker.Queue.GroupID = "rules-engine"
	envelope.Broker.Queue.Offset = 1
	envelope.Broker.Queue.Stream = &message.StreamMetadata{}
	return envelope
}
