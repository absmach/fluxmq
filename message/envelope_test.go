// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package message

import (
	"testing"

	"github.com/absmach/fluxmq/payload"
)

const (
	testClientID      = "client"
	testQueueName     = "queue"
	testPropertyValue = "value"
	testSubject       = "subject"
)

func TestEnvelopeCloneSharesOnlyImmutablePayload(t *testing.T) {
	original := New("devices/1", []byte("payload"))
	original.PublisherMeta.Headers = map[string][]byte{"trace": []byte("one")}
	original.PublisherMeta.Properties = map[string]string{"key": testPropertyValue}
	original.BrokerMeta.Queue.Stream = &StreamMetadata{Offset: 7}
	clone := original.Clone()

	if refs := original.Payload.RefCount(); refs != 2 {
		t.Fatalf("payload references = %d, want 2", refs)
	}
	clone.PublisherMeta.Headers["trace"][0] = 'X'
	clone.PublisherMeta.Properties["key"] = "changed"
	clone.BrokerMeta.Queue.Stream.Offset = 8
	if string(original.PublisherMeta.Headers["trace"]) != "one" || original.PublisherMeta.Properties["key"] != testPropertyValue || original.BrokerMeta.Queue.Stream.Offset != 7 {
		t.Fatal("clone aliases mutable metadata")
	}

	Release(clone)
	if refs := original.Payload.RefCount(); refs != 1 {
		t.Fatalf("payload references after clone release = %d, want 1", refs)
	}
	Release(original)
}

func TestEnvelopePoolReset(t *testing.T) {
	envelope := Acquire()
	envelope.Topic = "devices/1"
	envelope.Payload = payload.FromBytes([]byte("payload"))
	envelope.PublisherMeta.Properties = map[string]string{"key": testPropertyValue}
	Release(envelope)

	reused := Acquire()
	defer Release(reused)
	if reused.Version != Version1 || reused.Topic != "" || reused.Payload != nil || reused.PublisherMeta.Properties != nil {
		t.Fatalf("pooled envelope was not reset: %#v", reused)
	}
}

func TestPropertyProjectionTrustBoundary(t *testing.T) {
	envelope := New("devices/1", nil)
	defer Release(envelope)
	envelope.PublisherMeta.Properties = map[string]string{
		"user":             "visible",
		PropertyExternalID: "forged",
		PropertyTraceID:    "forged",
	}
	envelope.BrokerMeta.Source = SourceMetadata{ClientID: testClientID, ExternalID: testSubject, Protocol: ProtocolMQTT}
	envelope.BrokerMeta.Queue = QueueMetadata{Name: testQueueName, Offset: 3}
	envelope.BrokerMeta.Trace.TraceID = "trusted"

	public := ProjectProperties(envelope, PublicProjection)
	if public["user"] != "visible" || public[PropertyMessageID] != testQueueName+":3" {
		t.Fatalf("public projection lost delivery metadata: %#v", public)
	}
	if _, ok := public[PropertyExternalID]; ok {
		t.Fatalf("public projection leaked source metadata: %#v", public)
	}
	if _, ok := public[PropertyTraceID]; ok {
		t.Fatalf("public projection leaked trace metadata: %#v", public)
	}

	trusted := ProjectProperties(envelope, TrustedServiceProjection)
	if trusted[PropertyExternalID] != testSubject || trusted[PropertyTraceID] != "trusted" {
		t.Fatalf("trusted projection = %#v", trusted)
	}
}

func TestEmptyPropertyProjectionDoesNotAllocate(t *testing.T) {
	envelope := New("devices/1", nil)
	defer Release(envelope)
	if allocations := testing.AllocsPerRun(1000, func() {
		if properties := ProjectProperties(envelope, PublicProjection); properties != nil {
			t.Fatalf("empty projection = %#v, want nil", properties)
		}
	}); allocations != 0 {
		t.Fatalf("empty projection allocations = %v, want 0", allocations)
	}
}

func TestEnvelopeCloneDoesNotAllocateWithoutMutableMetadata(t *testing.T) {
	envelope := New("devices/1", make([]byte, 1024))
	defer Release(envelope)
	if allocations := testing.AllocsPerRun(1000, func() {
		clone := envelope.Clone()
		Release(clone)
	}); allocations != 0 {
		t.Fatalf("envelope clone allocations = %v, want 0", allocations)
	}
}

func BenchmarkEnvelopeClone(b *testing.B) {
	envelope := New("devices/1", make([]byte, 1024))
	defer Release(envelope)
	b.ReportAllocs()
	for range b.N {
		clone := envelope.Clone()
		Release(clone)
	}
}
