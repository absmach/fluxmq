// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package cluster

import (
	"testing"
	"time"

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

	decoded := roundTripQueueMessage(t, envelope)
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
	envelope.PublisherMeta.Properties = message.NewPropertyMap(map[string]string{"user": testUserPropertyVal})

	decoded := roundTripQueueMessage(t, envelope)
	defer message.Release(decoded)
	if decoded.BrokerMeta.Source.Topic != sourceTopic {
		t.Fatalf("decoded source topic = %q, want %q", decoded.BrokerMeta.Source.Topic, sourceTopic)
	}
	// Queue-owned metadata must not leak into the user properties a consumer
	// sees as its own.
	if _, leaked := decoded.PublisherMeta.Properties.Get(message.PropertySourceTopic); leaked {
		t.Fatal("the source topic leaked into user properties")
	}
	if user, _ := decoded.PublisherMeta.Properties.Get("user"); user != testUserPropertyVal {
		t.Fatalf("an ordinary user property was dropped: %v", decoded.PublisherMeta.Properties)
	}
}

// A publisher used to be able to forge broker-owned state by naming a reserved
// property, because the wire flattened both into one string map and the decoder
// promoted the reserved names back into typed fields. The typed envelope keeps
// the two in separate namespaces, so the forged value stays a user property.
func TestRouteQueueMessageWireIgnoresForgedSourceTopic(t *testing.T) {
	envelope := queueTestEnvelope("$queue/m", "")
	defer message.Release(envelope)
	envelope.PublisherMeta.Properties = message.NewPropertyMap(map[string]string{
		message.PropertySourceTopic: "forged/topic",
		"user":                      testUserPropertyVal,
	})

	decoded := roundTripQueueMessage(t, envelope)
	defer message.Release(decoded)
	if decoded.BrokerMeta.Source.Topic != "" {
		t.Fatalf("decoded source topic = %q, want the real empty source topic", decoded.BrokerMeta.Source.Topic)
	}
	if user, _ := decoded.PublisherMeta.Properties.Get("user"); user != testUserPropertyVal {
		t.Fatalf("an ordinary user property was dropped: %v", decoded.PublisherMeta.Properties)
	}
}

// Everything the flattened property map could not represent. Each of these was
// silently dropped on every cluster hop: a message arrived at its new node with
// a zero delivery count, no retry deadline and no expiry, so redelivery limits
// and TTLs restarted from scratch.
func TestRouteQueueMessageWirePreservesBrokerNamespaces(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)

	envelope := queueTestEnvelope("$queue/m/a/b", "a/b")
	defer message.Release(envelope)
	envelope.BrokerMeta.Queue.State = message.QueueStateDelivered
	envelope.BrokerMeta.Queue.CreatedAt = now
	envelope.BrokerMeta.Queue.DeliveredAt = now.Add(time.Second)
	envelope.BrokerMeta.Queue.NextRetryAt = now.Add(time.Minute)
	envelope.BrokerMeta.Queue.ExpiresAt = now.Add(time.Hour)
	envelope.BrokerMeta.Queue.RetryCount = 3
	envelope.BrokerMeta.Transfer.ID = "transfer-1"
	envelope.BrokerMeta.Transfer.FailureReason = "poison"
	envelope.BrokerMeta.Delivery.PublishedAt = now
	envelope.PublisherMeta.ContentType = "application/json"
	envelope.PublisherMeta.Key = message.NewBinary([]byte("partition-key"))
	envelope.PublisherMeta.Headers = message.NewHeaderMap(map[string][]byte{"h": []byte("v")})

	decoded := roundTripQueueMessage(t, envelope)
	defer message.Release(decoded)

	queue := decoded.BrokerMeta.Queue
	if queue.State != message.QueueStateDelivered {
		t.Errorf("queue state = %q, want %q", queue.State, message.QueueStateDelivered)
	}
	if queue.RetryCount != 3 {
		t.Errorf("retry count = %d, want 3", queue.RetryCount)
	}
	if !queue.NextRetryAt.Equal(now.Add(time.Minute)) {
		t.Errorf("next retry = %v, want %v", queue.NextRetryAt, now.Add(time.Minute))
	}
	if !queue.ExpiresAt.Equal(now.Add(time.Hour)) {
		t.Errorf("expiry = %v, want %v", queue.ExpiresAt, now.Add(time.Hour))
	}
	if decoded.BrokerMeta.Transfer.ID != "transfer-1" || decoded.BrokerMeta.Transfer.FailureReason != "poison" {
		t.Errorf("transfer metadata = %+v", decoded.BrokerMeta.Transfer)
	}
	if decoded.PublisherMeta.ContentType != "application/json" {
		t.Errorf("content type = %q", decoded.PublisherMeta.ContentType)
	}
	if !decoded.PublisherMeta.Key.Equal([]byte("partition-key")) {
		t.Errorf("key = %q", decoded.PublisherMeta.Key)
	}
	header, ok := decoded.PublisherMeta.Headers.Get("h")
	if !ok || !header.Equal([]byte("v")) {
		t.Errorf("headers = %v", decoded.PublisherMeta.Headers)
	}
}

func TestDecodeRouteQueueMessageRejectsEmptyEnvelope(t *testing.T) {
	if _, err := decodeRouteQueueMessage(&clusterv1.RouteQueueMessageRequest{}); err == nil {
		t.Fatal("a request with no envelope was accepted")
	}
}

func TestDecodeRouteQueueMessageRequiresCanonicalTopic(t *testing.T) {
	untopiced := message.New("", []byte("payload"))
	defer message.Release(untopiced)
	encoded, err := message.MarshalBinary(untopiced)
	if err != nil {
		t.Fatalf("encode envelope: %v", err)
	}

	if _, err := decodeRouteQueueMessage(&clusterv1.RouteQueueMessageRequest{Envelope: encoded}); err == nil {
		t.Fatal("missing canonical queue delivery topic was accepted")
	}
}

func roundTripQueueMessage(t *testing.T, envelope *message.Envelope) *message.Envelope {
	t.Helper()
	wire, err := encodeRouteQueueMessage("consumer", envelope)
	if err != nil {
		t.Fatalf("encode queue message: %v", err)
	}
	decoded, err := decodeRouteQueueMessage(wire)
	if err != nil {
		t.Fatalf("decode queue message: %v", err)
	}
	return decoded
}

func queueTestEnvelope(topic, sourceTopic string) *message.Envelope {
	envelope := message.New(topic, []byte("payload"))
	envelope.BrokerMeta.Source.Topic = sourceTopic
	envelope.PublisherMeta.MessageID = testQueueMessageID
	envelope.BrokerMeta.Queue.Name = "m"
	envelope.BrokerMeta.Queue.GroupID = "rules-engine"
	envelope.BrokerMeta.Queue.Offset = 1
	envelope.BrokerMeta.Queue.Stream = message.Some(message.StreamMetadata{})
	return envelope
}
