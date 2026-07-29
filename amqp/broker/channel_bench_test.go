// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package broker

import (
	"bufio"
	"bytes"
	"context"
	"io"
	"log/slog"
	"testing"

	"github.com/absmach/fluxmq/amqp/codec"
	qtypes "github.com/absmach/fluxmq/queue/types"
)

func newBenchChannel(b *testing.B, cfg *qtypes.QueueConfig) *Channel {
	b.Helper()

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	broker := New(nil, logger)
	broker.queueManager = &mockChannelQueueManager{queueCfg: cfg}
	conn := &Connection{
		broker:   broker,
		ctx:      context.Background(),
		writer:   bufio.NewWriter(&bytes.Buffer{}),
		frameMax: defaultFrameMax,
		logger:   logger,
		connID:   testConnectionID,
		channels: make(map[uint16]*Channel),
	}
	return newChannel(conn, 1)
}

// BenchmarkIsStreamQueue covers the classic-queue path, where every
// channel-local and queue-manager lookup misses. That is the worst case a
// remote publisher on the default exchange pays per publication.
func BenchmarkIsStreamQueue(b *testing.B) {
	b.Run("classic/miss", func(b *testing.B) {
		ch := newBenchChannel(b, &qtypes.QueueConfig{Name: testOrders, Type: qtypes.QueueTypeClassic})
		b.ReportAllocs()
		for b.Loop() {
			if ch.isStreamQueue(testOrders) {
				b.Fatal("classic queue reported as stream")
			}
		}
	})

	b.Run("stream/manager", func(b *testing.B) {
		ch := newBenchChannel(b, &qtypes.QueueConfig{Name: testAuditQueue, Type: qtypes.QueueTypeStream})
		b.ReportAllocs()
		for b.Loop() {
			if !ch.isStreamQueue(testAuditQueue) {
				b.Fatal("configured stream not detected")
			}
		}
	})

	b.Run("stream/channel-local", func(b *testing.B) {
		ch := newBenchChannel(b, nil)
		ch.queues[testAuditQueue] = &queueInfo{queueType: string(qtypes.QueueTypeStream)}
		b.ReportAllocs()
		for b.Loop() {
			if !ch.isStreamQueue(testAuditQueue) {
				b.Fatal("declared stream not detected")
			}
		}
	})

	b.Run("queue-filter/miss", func(b *testing.B) {
		ch := newBenchChannel(b, &qtypes.QueueConfig{Name: testOrders, Type: qtypes.QueueTypeClassic})
		b.ReportAllocs()
		for b.Loop() {
			if ch.isStreamQueue("$queue/" + testOrders + "/events") {
				b.Fatal("classic queue reported as stream")
			}
		}
	})
}

// BenchmarkCompletePublishRemoteDefaultExchange measures the full remote
// publish path through the default exchange, which is where the stream lookup
// sits.
func BenchmarkCompletePublishRemoteDefaultExchange(b *testing.B) {
	ch := newBenchChannel(b, &qtypes.QueueConfig{Name: testOrders, Type: qtypes.QueueTypeClassic})
	body := []byte(`{"event":"benchmark"}`)

	b.ReportAllocs()
	for b.Loop() {
		ch.pendingMethod = &codec.BasicPublish{RoutingKey: testOrders}
		ch.pendingHeader = &codec.ContentHeader{ClassID: codec.ClassBasic, BodySize: uint64(len(body))}
		ch.pendingBody = body
		ch.completePublish()
	}
}
