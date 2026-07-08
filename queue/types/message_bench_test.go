// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package types_test

import (
	"bytes"
	"encoding/json"
	"testing"

	core "github.com/absmach/fluxmq/mqtt"
	"github.com/absmach/fluxmq/queue/types"
)

func BenchmarkMessageMarshalJSON(b *testing.B) {
	payload := bytes.Repeat([]byte("x"), 1024)

	b.Run("plain_payload", func(b *testing.B) {
		msg := &types.Message{ID: "bench", Topic: "$queue/bench", Payload: payload}
		b.ReportAllocs()
		for b.Loop() {
			if _, err := json.Marshal(msg); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("buffer_backed", func(b *testing.B) {
		msg := &types.Message{ID: "bench", Topic: "$queue/bench"}
		msg.SetPayloadFromBuffer(core.GetBufferWithData(payload))
		defer msg.ReleasePayload()
		b.ReportAllocs()
		for b.Loop() {
			if _, err := json.Marshal(msg); err != nil {
				b.Fatal(err)
			}
		}
	})
}
