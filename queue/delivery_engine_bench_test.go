// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package queue

import (
	"bytes"
	"testing"

	core "github.com/absmach/fluxmq/mqtt"
	"github.com/absmach/fluxmq/queue/types"
)

func BenchmarkCreateRoutedQueueMessage(b *testing.B) {
	payload := bytes.Repeat([]byte("x"), 1024)

	b.Run("plain_payload", func(b *testing.B) {
		msg := &types.Message{
			ID:       "bench",
			Topic:    "$queue/bench",
			Sequence: 42,
			Payload:  payload,
		}
		b.ReportAllocs()
		for b.Loop() {
			_ = createRoutedQueueMessage(msg, "group", "bench", false, 0, false, "")
		}
	})

	b.Run("buffer_backed", func(b *testing.B) {
		msg := &types.Message{
			ID:       "bench",
			Topic:    "$queue/bench",
			Sequence: 42,
		}
		msg.SetPayloadFromBuffer(core.GetBufferWithData(payload))
		defer msg.ReleasePayload()
		b.ReportAllocs()
		for b.Loop() {
			_ = createRoutedQueueMessage(msg, "group", "bench", false, 0, false, "")
		}
	})
}
