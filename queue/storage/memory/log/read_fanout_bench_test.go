// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package log

import (
	"context"
	"testing"
	"time"

	"github.com/absmach/fluxmq/message"
	"github.com/absmach/fluxmq/queue/types"
)

// A record read by N consumer groups still needs N independently releasable
// envelopes, but their payload and metadata storage are immutable and shared.
// These benchmarks keep both a bare and realistic record in the fan-out matrix
// so a future mutable field cannot quietly restore per-reader deep copies.
func BenchmarkReadFanOut(b *testing.B) {
	for _, shape := range []struct {
		name  string
		build func() *message.Envelope
	}{
		{"bare", benchBareRecord},
		{"realistic", benchRealisticRecord},
	} {
		for _, groups := range []int{1, 4, 16} {
			b.Run(shape.name+"/"+itoa(groups)+"_groups", func(b *testing.B) {
				ctx := context.Background()
				store := New()
				if err := store.CreateQueue(ctx, types.DefaultQueueConfig("fanout", "fanout/#")); err != nil {
					b.Fatalf("create queue: %v", err)
				}
				if _, err := store.Append(ctx, "fanout", shape.build()); err != nil {
					b.Fatalf("append: %v", err)
				}

				b.ReportAllocs()
				for b.Loop() {
					for range groups {
						msg, err := store.Read(ctx, "fanout", 0)
						if err != nil {
							b.Fatalf("read: %v", err)
						}
						message.Release(msg)
					}
				}
			})
		}
	}
}

func benchBareRecord() *message.Envelope {
	return message.NewDelivery("fanout/readings", make([]byte, 256), 1, false)
}

func benchRealisticRecord() *message.Envelope {
	envelope := benchBareRecord()
	envelope.PublisherMeta.Key = message.NewBinary([]byte("partition-key"))
	envelope.PublisherMeta.Headers = message.NewHeaderMap(map[string][]byte{
		"x-tenant": []byte("acme"),
		"x-region": []byte("eu-central-1"),
	})
	envelope.PublisherMeta.Properties = message.NewPropertyMap(map[string]string{
		"schema":          "telemetry.v2",
		"content-version": "3",
	})
	envelope.PublisherMeta.CorrelationData = message.NewBinary([]byte("correlation-0123456789"))
	envelope.BrokerMeta.Source.ClientID = "sensor-1"
	envelope.BrokerMeta.Queue.Name = "fanout"
	envelope.BrokerMeta.Queue.State = message.QueueStateQueued
	envelope.BrokerMeta.Queue.CreatedAt = time.Now()
	envelope.BrokerMeta.Queue.Stream = message.Some(message.StreamMetadata{Offset: 1, WorkGroup: "workers"})
	return envelope
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var digits [8]byte
	i := len(digits)
	for n > 0 {
		i--
		digits[i] = byte('0' + n%10)
		n /= 10
	}
	return string(digits[i:])
}
