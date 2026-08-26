// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package message

import (
	"flag"
	"os"
	"path/filepath"
	"testing"
	"time"

	messagev1 "github.com/absmach/fluxmq/pkg/proto/message/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

var updateGolden = flag.Bool("update-envelope-golden", false,
	"rewrite message/testdata/envelope-v1.bin from the current codec")

const goldenPath = "testdata/envelope-v1.bin"

// proto/message/v1/envelope.proto is the schema of record for every stored
// message; the hand-written codec in codec.go is its implementation. Nothing
// makes the two agree except this test: it decodes the hand codec's bytes with
// the generated type and checks every field, so a field added to one and not
// the other fails here rather than in a log nobody can read back.
func TestHandCodecMatchesTheSchema(t *testing.T) {
	envelope := conformanceEnvelope()
	defer Release(envelope)

	encoded, err := MarshalBinary(envelope)
	require.NoError(t, err)

	var schema messagev1.Envelope
	require.NoError(t, proto.Unmarshal(encoded, &schema),
		"the hand codec wrote bytes the schema cannot parse")

	assert.Equal(t, uint32(Version1), schema.Version)
	assert.Equal(t, envelope.Topic, schema.Topic)
	assert.Equal(t, envelope.PayloadBytes(), schema.Payload)

	publisher := schema.Publisher
	require.NotNil(t, publisher)
	assert.Equal(t, envelope.PublisherMeta.Key, publisher.Key)
	assert.Equal(t, envelope.PublisherMeta.Headers, publisher.Headers)
	assert.Equal(t, envelope.PublisherMeta.Properties, publisher.Properties)
	assert.Equal(t, envelope.PublisherMeta.ContentType, publisher.ContentType)
	assert.Equal(t, envelope.PublisherMeta.ContentEncoding, publisher.ContentEncoding)
	assert.Equal(t, envelope.PublisherMeta.ResponseTopic, publisher.ResponseTopic)
	assert.Equal(t, envelope.PublisherMeta.CorrelationData, publisher.CorrelationData)
	require.NotNil(t, publisher.PayloadFormat)
	assert.Equal(t, uint32(*envelope.PublisherMeta.PayloadFormat), *publisher.PayloadFormat)
	require.NotNil(t, publisher.MessageExpiry)
	assert.Equal(t, *envelope.PublisherMeta.MessageExpiry, *publisher.MessageExpiry)
	assert.Equal(t, envelope.PublisherMeta.MessageID, publisher.MessageId)

	broker := schema.Broker
	require.NotNil(t, broker)

	source := envelope.BrokerMeta.Source
	require.NotNil(t, broker.Source)
	assert.Equal(t, source.ClientID, broker.Source.ClientId)
	assert.Equal(t, source.ExternalID, broker.Source.ExternalId)
	assert.Equal(t, string(source.Protocol), broker.Source.Protocol)
	assert.Equal(t, source.Topic, broker.Source.Topic)

	delivery := envelope.BrokerMeta.Delivery
	require.NotNil(t, broker.Delivery)
	assertTime(t, "published_at", delivery.PublishedAt, broker.Delivery.PublishedAt)
	assertTime(t, "expires_at", delivery.ExpiresAt, broker.Delivery.ExpiresAt)
	assert.Equal(t, delivery.SubscriptionIDs, broker.Delivery.SubscriptionIds)
	assert.Equal(t, uint32(delivery.PacketID), broker.Delivery.PacketId)
	assert.Equal(t, uint32(delivery.QoS), broker.Delivery.Qos)
	assert.Equal(t, uint32(delivery.InflightDirection), broker.Delivery.InflightDirection)
	assert.Equal(t, uint32(delivery.InflightState), broker.Delivery.InflightState)
	assert.Equal(t, delivery.Retain, broker.Delivery.Retain)
	assert.Equal(t, delivery.Duplicate, broker.Delivery.Duplicate)

	queue := envelope.BrokerMeta.Queue
	require.NotNil(t, broker.Queue)
	assert.Equal(t, queue.Name, broker.Queue.Name)
	assert.Equal(t, queue.GroupID, broker.Queue.GroupId)
	assert.Equal(t, queue.Offset, broker.Queue.Offset)
	assert.Equal(t, string(queue.State), broker.Queue.State)
	assertTime(t, "created_at", queue.CreatedAt, broker.Queue.CreatedAt)
	assertTime(t, "delivered_at", queue.DeliveredAt, broker.Queue.DeliveredAt)
	assertTime(t, "next_retry_at", queue.NextRetryAt, broker.Queue.NextRetryAt)
	assert.Equal(t, uint32(queue.RetryCount), broker.Queue.RetryCount)
	assertTime(t, "queue expires_at", queue.ExpiresAt, broker.Queue.ExpiresAt)

	require.NotNil(t, queue.Stream)
	require.NotNil(t, broker.Queue.Stream)
	assert.Equal(t, queue.Stream.Offset, broker.Queue.Stream.Offset)
	assert.Equal(t, queue.Stream.Timestamp, broker.Queue.Stream.Timestamp)
	assert.Equal(t, queue.Stream.CommittedOffset, broker.Queue.Stream.CommittedOffset)
	assert.Equal(t, queue.Stream.HasCommittedOffset, broker.Queue.Stream.HasCommittedOffset)
	assert.Equal(t, queue.Stream.WorkAcknowledged, broker.Queue.Stream.WorkAcknowledged)
	assert.Equal(t, queue.Stream.WorkGroup, broker.Queue.Stream.WorkGroup)

	transfer := envelope.BrokerMeta.Transfer
	require.NotNil(t, broker.Transfer)
	assert.Equal(t, transfer.ID, broker.Transfer.Id)
	assert.Equal(t, transfer.FailureReason, broker.Transfer.FailureReason)
	assertTime(t, "first_attempt", transfer.FirstAttempt, broker.Transfer.FirstAttempt)
	assertTime(t, "last_attempt", transfer.LastAttempt, broker.Transfer.LastAttempt)
	assertTime(t, "completed_at", transfer.CompletedAt, broker.Transfer.CompletedAt)
	assert.Equal(t, transfer.SourceQueue, broker.Transfer.SourceQueue)
	assert.Equal(t, transfer.SourceGroup, broker.Transfer.SourceGroup)
	assert.Equal(t, transfer.SourceOffset, broker.Transfer.SourceOffset)
	assert.Equal(t, uint32(transfer.DeliveryCount), broker.Transfer.DeliveryCount)

	trace := envelope.BrokerMeta.Trace
	require.NotNil(t, broker.Trace)
	assert.Equal(t, trace.TraceParent, broker.Trace.TraceParent)
	assert.Equal(t, trace.TraceState, broker.Trace.TraceState)
	assert.Equal(t, trace.TraceID, broker.Trace.TraceId)
}

// The other direction: bytes written from the schema have to decode into the
// same envelope. A field the hand codec writes but does not read back would
// pass the check above and fail here.
func TestSchemaBytesDecodeThroughTheHandCodec(t *testing.T) {
	envelope := conformanceEnvelope()
	defer Release(envelope)

	encoded, err := MarshalBinary(envelope)
	require.NoError(t, err)

	var schema messagev1.Envelope
	require.NoError(t, proto.Unmarshal(encoded, &schema))

	// Re-encode from the schema's own marshaller, then read it with the codec
	// under test.
	reencoded, err := proto.Marshal(&schema)
	require.NoError(t, err)

	decoded, err := UnmarshalBinary(reencoded)
	require.NoError(t, err, "the codec cannot read what the schema writes")
	defer Release(decoded)

	assert.Equal(t, envelope.Topic, decoded.Topic)
	assert.Equal(t, envelope.PayloadBytes(), decoded.PayloadBytes())
	assert.Equal(t, envelope.PublisherMeta, decoded.PublisherMeta)
	assert.Equal(t, envelope.BrokerMeta, decoded.BrokerMeta)
}

// The golden encoding pins the bytes themselves. The two tests above would both
// pass if the schema and the codec changed together in a way that rewrote every
// stored record; this one fails when the format on disk moves at all.
func TestGoldenEnvelopeEncoding(t *testing.T) {
	envelope := conformanceEnvelope()
	defer Release(envelope)

	encoded, err := MarshalBinary(envelope)
	require.NoError(t, err)

	if *updateGolden {
		require.NoError(t, os.MkdirAll(filepath.Dir(goldenPath), 0o755))
		require.NoError(t, os.WriteFile(goldenPath, encoded, 0o644))
		t.Logf("wrote %s (%d bytes)", goldenPath, len(encoded))
		return
	}

	golden, err := os.ReadFile(goldenPath)
	require.NoError(t, err)
	assert.Equal(t, golden, encoded,
		"the stored envelope format changed.\n"+
			"Every record already written uses the old bytes. If the change is intended, run:\n"+
			"  go test ./message -run TestGoldenEnvelopeEncoding -update-envelope-golden\n"+
			"and put the schema change and the golden diff in the same review.")
}

// conformanceEnvelope is fixed rather than time-dependent so the golden bytes
// are reproducible. It carries every field the schema declares, which is what
// makes the comparisons above exhaustive.
func conformanceEnvelope() *Envelope {
	format := byte(1)
	expiry := uint32(3600)
	base := time.Unix(1700000000, 123456789).UTC()

	envelope := NewDelivery("devices/sensor-1/telemetry", []byte("conformance-payload"), 1, true)
	envelope.BrokerMeta.Delivery.SubscriptionIDs = []uint32{7, 9}
	envelope.BrokerMeta.Delivery.PacketID = 42
	envelope.BrokerMeta.Delivery.InflightDirection = 1
	envelope.BrokerMeta.Delivery.InflightState = 1
	envelope.BrokerMeta.Delivery.Duplicate = true
	envelope.BrokerMeta.Delivery.PublishedAt = base
	envelope.BrokerMeta.Delivery.ExpiresAt = base.Add(time.Hour)

	envelope.PublisherMeta.Key = []byte("partition-key")
	envelope.PublisherMeta.Headers = map[string][]byte{"x-tenant": []byte("acme")}
	envelope.PublisherMeta.Properties = map[string]string{"schema": "telemetry.v2"}
	envelope.PublisherMeta.ContentType = "application/json"
	envelope.PublisherMeta.ContentEncoding = testContentEncoding
	envelope.PublisherMeta.ResponseTopic = "devices/sensor-1/reply"
	envelope.PublisherMeta.CorrelationData = []byte("correlation-0123456789")
	envelope.PublisherMeta.PayloadFormat = &format
	envelope.PublisherMeta.MessageExpiry = &expiry
	envelope.PublisherMeta.MessageID = "publisher-message-1"

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
		CreatedAt:   base,
		DeliveredAt: base.Add(time.Second),
		NextRetryAt: base.Add(time.Minute),
		RetryCount:  2,
		ExpiresAt:   base.Add(time.Hour),
		Stream: &StreamMetadata{
			Offset:             4096,
			Timestamp:          base.UnixNano(),
			CommittedOffset:    4000,
			HasCommittedOffset: true,
			WorkAcknowledged:   true,
			WorkGroup:          testGroupID,
		},
	}
	envelope.BrokerMeta.Transfer = TransferMetadata{
		ID:            "dlq-0f1e2d3c4b5a69788796a5b4c3d2e1f0",
		FailureReason: "max delivery count exceeded",
		FirstAttempt:  base.Add(-time.Hour),
		LastAttempt:   base,
		CompletedAt:   base.Add(time.Second),
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

func assertTime(t *testing.T, name string, want time.Time, gotNanos int64) {
	t.Helper()
	if want.IsZero() {
		assert.Zero(t, gotNanos, "%s: a zero time must be omitted, not written as the epoch", name)
		return
	}
	assert.Equal(t, want.UnixNano(), gotNanos, "%s", name)
}
