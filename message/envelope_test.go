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
	testTenant        = "acme"
	testTenantKey     = "tenant"
	testTraceKey      = "trace"
)

func TestEnvelopeCloneSharesImmutableMetadataAndCopiesOnWrite(t *testing.T) {
	original := New("devices/1", []byte("payload"))
	original.PublisherMeta.Headers = NewHeaderMap(map[string][]byte{testTraceKey: []byte("one")})
	original.PublisherMeta.Properties = NewPropertyMap(map[string]string{"key": testPropertyValue})
	original.BrokerMeta.Queue.Stream = Some(StreamMetadata{Offset: 7})
	clone := original.Clone()

	if refs := original.payload.RefCount(); refs != 2 {
		t.Fatalf("payload references = %d, want 2", refs)
	}
	clone.PublisherMeta.Headers = clone.PublisherMeta.Headers.With(testTraceKey, []byte("Xne"))
	clone.PublisherMeta.Properties = clone.PublisherMeta.Properties.With("key", "changed")
	stream, ok := clone.BrokerMeta.Queue.Stream.Value()
	if !ok {
		t.Fatal("clone lost stream metadata")
	}
	stream.Offset = 8
	clone.BrokerMeta.Queue.Stream = Some(stream)

	originalHeader, _ := original.PublisherMeta.Headers.Get(testTraceKey)
	originalProperty, _ := original.PublisherMeta.Properties.Get("key")
	originalStream, _ := original.BrokerMeta.Queue.Stream.Value()
	if !originalHeader.Equal([]byte("one")) || originalProperty != testPropertyValue || originalStream.Offset != 7 {
		t.Fatal("copy-on-write mutation changed the source envelope")
	}

	Release(clone)
	if refs := original.payload.RefCount(); refs != 1 {
		t.Fatalf("payload references after clone release = %d, want 1", refs)
	}
	Release(original)
}

func TestEnvelopePoolReset(t *testing.T) {
	envelope := Acquire()
	envelope.Topic = "devices/1"
	envelope.payload = payload.FromBytes([]byte("payload"))
	envelope.PublisherMeta.Properties = NewPropertyMap(map[string]string{"key": testPropertyValue})
	Release(envelope)

	reused := Acquire()
	defer Release(reused)
	if reused.Version != Version1 || reused.Topic != "" || reused.payload != nil || reused.PublisherMeta.Properties.Len() != 0 {
		t.Fatalf("pooled envelope was not reset: %#v", reused)
	}
}

func TestPropertyProjectionTrustBoundary(t *testing.T) {
	envelope := New("devices/1", nil)
	defer Release(envelope)
	envelope.PublisherMeta.Properties = NewPropertyMap(map[string]string{
		"user":             "visible",
		PropertyExternalID: "forged",
		PropertyTraceID:    "forged",
	})
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

// Clone allocates nothing for metadata regardless of shape: all collection
// storage is immutable and writers replace it through copy-on-write values.
func TestEnvelopeCloneAllocatesNothing(t *testing.T) {
	envelope := New("devices/1", make([]byte, 1024))
	envelope.PublisherMeta.Headers = NewHeaderMap(map[string][]byte{testTraceKey: []byte("one")})
	envelope.PublisherMeta.Properties = NewPropertyMap(map[string]string{testTenantKey: testTenant})
	envelope.BrokerMeta.Delivery.SubscriptionIDs = NewUint32List(1, 2, 3)
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
