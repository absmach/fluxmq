// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package message

import (
	"testing"
	"time"
)

// The codec's cost depends almost entirely on whether the envelope carries
// metadata, and the in-tree benchmark that existed before this file did not.
//
// BenchmarkEnvelopeBinaryCodec uses NewDelivery with no headers, properties,
// source, queue, transfer or trace metadata, so every nested encodeX returns
// nil and appendBytes skips it: the nested-buffer structure the encoder is
// built around is never exercised. These benchmarks split marshal from
// unmarshal and metadata-free from realistic, so the difference is visible and
// reproducible with benchstat rather than argued about.
//
// "Realistic" is what a durable queue append actually carries — see
// logstorage.encodeMessage, which marshals metadata for every append, and
// cluster/etcd.go, which marshals it for every retained write.

const (
	benchPayloadSize    = 256
	testGroupID         = "workers"
	testContentEncoding = "gzip"
	testTelemetryQueue  = "telemetry"
)

// benchEmptyEnvelope carries a payload and nothing else.
func benchEmptyEnvelope() *Envelope {
	return NewDelivery("devices/1", make([]byte, benchPayloadSize), 1, false)
}

// benchRichEnvelope carries what a queued, delivered, dead-lettered record
// holds: user metadata, all four broker namespaces, and stream state.
func benchRichEnvelope() *Envelope {
	envelope := NewDelivery("devices/sensor-1/telemetry", make([]byte, benchPayloadSize), 1, false)

	envelope.PublisherMeta.Key = NewBinary([]byte("partition-key"))
	envelope.PublisherMeta.Headers = NewHeaderMap(map[string][]byte{
		"x-tenant": []byte(testTenant),
		"x-region": []byte("eu-central-1"),
	})
	envelope.PublisherMeta.Properties = NewPropertyMap(map[string]string{
		"content-version": "3",
		"schema":          "telemetry.v2",
	})
	envelope.PublisherMeta.ContentType = "application/json"
	envelope.PublisherMeta.ContentEncoding = testContentEncoding
	envelope.PublisherMeta.ResponseTopic = "devices/sensor-1/reply"
	envelope.PublisherMeta.CorrelationData = NewBinary([]byte("correlation-0123456789"))
	envelope.PublisherMeta.PayloadFormat = Some(byte(1))
	envelope.PublisherMeta.MessageExpiry = Some(uint32(3600))

	now := time.Now().UTC()
	envelope.BrokerMeta.Source = SourceMetadata{
		ClientID:   "sensor-1",
		ExternalID: "ext-sensor-1",
		Protocol:   ProtocolMQTT,
		Topic:      "devices/sensor-1/telemetry",
	}
	envelope.BrokerMeta.Queue = QueueMetadata{
		Name:        testTelemetryQueue,
		GroupID:     testGroupID,
		Offset:      4096,
		State:       QueueStateDelivered,
		CreatedAt:   now,
		DeliveredAt: now,
		NextRetryAt: now.Add(time.Minute),
		RetryCount:  2,
		ExpiresAt:   now.Add(time.Hour),
		Stream: Some(StreamMetadata{
			Offset:             4096,
			Timestamp:          now.UnixNano(),
			CommittedOffset:    4000,
			HasCommittedOffset: true,
			WorkAcknowledged:   true,
			WorkGroup:          testGroupID,
		}),
	}
	envelope.BrokerMeta.Transfer = TransferMetadata{
		ID:            "dlq-0f1e2d3c4b5a69788796a5b4c3d2e1f0",
		FailureReason: "max delivery count exceeded",
		FirstAttempt:  now.Add(-time.Hour),
		LastAttempt:   now,
		CompletedAt:   now,
		SourceQueue:   testTelemetryQueue,
		SourceGroup:   testGroupID,
		SourceOffset:  4096,
		DeliveryCount: 5,
	}
	envelope.BrokerMeta.Trace = TraceMetadata{
		TraceParent: "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
		TraceState:  "fluxmq=t61rcWkgMzE",
		TraceID:     "0af7651916cd43dd8448eb211c80319c",
	}

	return envelope
}

func benchmarkMarshal(b *testing.B, envelope *Envelope) {
	b.Helper()
	defer Release(envelope)

	b.ReportAllocs()
	for b.Loop() {
		if _, err := MarshalBinary(envelope); err != nil {
			b.Fatalf("marshal: %v", err)
		}
	}
}

func benchmarkUnmarshal(b *testing.B, envelope *Envelope) {
	b.Helper()
	defer Release(envelope)

	encoded, err := MarshalBinary(envelope)
	if err != nil {
		b.Fatalf("marshal: %v", err)
	}
	b.SetBytes(int64(len(encoded)))

	b.ReportAllocs()
	for b.Loop() {
		decoded, err := UnmarshalBinary(encoded)
		if err != nil {
			b.Fatalf("unmarshal: %v", err)
		}
		Release(decoded)
	}
}

func BenchmarkMarshalBinaryEmpty(b *testing.B) { benchmarkMarshal(b, benchEmptyEnvelope()) }
func BenchmarkMarshalBinaryRich(b *testing.B)  { benchmarkMarshal(b, benchRichEnvelope()) }

func BenchmarkUnmarshalBinaryEmpty(b *testing.B) { benchmarkUnmarshal(b, benchEmptyEnvelope()) }
func BenchmarkUnmarshalBinaryRich(b *testing.B)  { benchmarkUnmarshal(b, benchRichEnvelope()) }

// MarshalMetadata is the mode the queue log uses: the record already owns the
// payload and key, so the codec writes neither.
func BenchmarkMarshalMetadataRich(b *testing.B) {
	envelope := benchRichEnvelope()
	defer Release(envelope)

	b.ReportAllocs()
	for b.Loop() {
		if _, err := MarshalMetadata(envelope); err != nil {
			b.Fatalf("marshal metadata: %v", err)
		}
	}
}

// Clone shares both the payload buffer and immutable metadata. This rich shape
// guards the O(1) clone cost that queue fan-out depends on.
func BenchmarkEnvelopeCloneRich(b *testing.B) {
	envelope := benchRichEnvelope()
	defer Release(envelope)

	b.ReportAllocs()
	for b.Loop() {
		Release(envelope.Clone())
	}
}
