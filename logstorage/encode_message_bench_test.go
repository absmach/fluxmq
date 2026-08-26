// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package logstorage

import (
	"testing"
	"time"

	"github.com/absmach/fluxmq/message"
)

// C11 asks what the per-append header map costs. It is a map because that is
// the log record's own format on disk — a count followed by key/value pairs,
// see Batch.encode — and a reader keeps the map the record was decoded with.
// Replacing it with two typed fields is therefore a change to the durable log
// format, not to one allocation, which is what this measures.
func BenchmarkEncodeMessage(b *testing.B) {
	for _, tc := range []struct {
		name     string
		envelope func() *message.Envelope
	}{
		{"plain", benchAppendEnvelope},
		{"deduplicated", benchDeduplicatedEnvelope},
	} {
		b.Run(tc.name, func(b *testing.B) {
			envelope := tc.envelope()
			defer message.Release(envelope)

			b.ReportAllocs()
			for b.Loop() {
				if _, _, _, err := encodeMessage(envelope); err != nil {
					b.Fatalf("encode: %v", err)
				}
			}
		})
	}
}

func benchAppendEnvelope() *message.Envelope {
	envelope := message.NewDelivery("$queue/telemetry/readings", make([]byte, 256), 1, false)
	envelope.PublisherMeta.Key = message.NewBinary([]byte("partition-key"))
	envelope.PublisherMeta.Properties = message.NewPropertyMap(map[string]string{"schema": "telemetry.v2"})
	envelope.BrokerMeta.Source.ClientID = "sensor-1"
	envelope.BrokerMeta.Queue.Name = "telemetry"
	envelope.BrokerMeta.Queue.State = message.QueueStateQueued
	envelope.BrokerMeta.Queue.CreatedAt = time.Now()
	return envelope
}

func benchDeduplicatedEnvelope() *message.Envelope {
	envelope := benchAppendEnvelope()
	envelope.BrokerMeta.Transfer.ID = "dlq-0f1e2d3c4b5a69788796a5b4c3d2e1f0"
	return envelope
}
