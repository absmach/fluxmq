// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package types

import (
	"reflect"
	"testing"
	"time"

	"github.com/absmach/fluxmq/message"
	"github.com/stretchr/testify/require"
)

// The two halves of the conversion are the whole cluster publish wire. A field
// added to PublishRequest and to only one half is a field that crosses a node
// boundary in one direction and vanishes in the other, which is exactly the
// class of loss the flattened property map used to cause. This walks the struct
// by reflection so a new field fails here rather than in production.
func TestPublishRequestEnvelopeRoundTrip(t *testing.T) {
	format := byte(1)
	expiry := uint32(90)
	published := time.Now().UTC().Truncate(time.Second)

	original := PublishRequest{
		Source: message.SourceMetadata{
			ClientID:   "mqtt:sensor-1",
			ExternalID: "ext-1",
			Protocol:   message.ProtocolMQTT,
			Topic:      "sensors/temperature",
		},
		Trace: message.TraceMetadata{
			TraceParent: "00-trace-span-01",
			TraceState:  "vendor=1",
			TraceID:     "trace-1",
		},
		Topic:           "$queue/readings/sensors/temperature",
		Payload:         []byte("21.5"),
		Key:             []byte("sensor-1"),
		Headers:         map[string][]byte{"unit": []byte("celsius")},
		Properties:      map[string]string{"tenant": "acme"},
		ContentType:     "application/json",
		ContentEncoding: "gzip",
		ResponseTopic:   "replies/sensor-1",
		CorrelationData: []byte("corr-1"),
		PayloadFormat:   &format,
		MessageExpiry:   &expiry,
		PublishedAt:     published,
		ExpiresAt:       published.Add(time.Hour),
	}
	requireNoZeroFields(t, original)

	envelope := original.Envelope()
	defer message.Release(envelope)

	// Cross the wire for real: a field the codec cannot carry is as lost as a
	// field the conversion forgets.
	encoded, err := message.MarshalBinary(envelope)
	require.NoError(t, err)
	decoded, err := message.UnmarshalBinary(encoded)
	require.NoError(t, err)
	defer message.Release(decoded)

	round := PublishFromEnvelope(decoded)
	require.Equal(t, original.Source, round.Source)
	require.Equal(t, original.Trace, round.Trace)
	require.Equal(t, original.Topic, round.Topic)
	require.Equal(t, original.Payload, round.Payload)
	require.Equal(t, original.Key, round.Key)
	require.Equal(t, original.Headers, round.Headers)
	require.Equal(t, original.Properties, round.Properties)
	require.Equal(t, original.ContentType, round.ContentType)
	require.Equal(t, original.ContentEncoding, round.ContentEncoding)
	require.Equal(t, original.ResponseTopic, round.ResponseTopic)
	require.Equal(t, original.CorrelationData, round.CorrelationData)
	require.Equal(t, original.PayloadFormat, round.PayloadFormat)
	require.Equal(t, original.MessageExpiry, round.MessageExpiry)
	require.True(t, original.PublishedAt.Equal(round.PublishedAt),
		"published at %v, want %v", round.PublishedAt, original.PublishedAt)
	require.True(t, original.ExpiresAt.Equal(round.ExpiresAt),
		"expires at %v, want %v", round.ExpiresAt, original.ExpiresAt)

	// ForwardTargetQueues is deliberately not carried: it is a routing control
	// that travels beside the envelope, so a publisher cannot forge it.
	require.Nil(t, round.ForwardTargetQueues)
}

// requireNoZeroFields fails when the fixture above stops covering the struct,
// which is what makes the round-trip assertions above exhaustive.
func requireNoZeroFields(t *testing.T, publish PublishRequest) {
	t.Helper()
	value := reflect.ValueOf(publish)
	for i := range value.NumField() {
		name := value.Type().Field(i).Name
		if name == "ForwardTargetQueues" {
			continue
		}
		require.Falsef(t, value.Field(i).IsZero(),
			"PublishRequest.%s is not covered by the round-trip fixture", name)
	}
}

func TestPublishFromNilEnvelope(t *testing.T) {
	require.Equal(t, PublishRequest{}, PublishFromEnvelope(nil))
}
