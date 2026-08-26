// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package message

import (
	"testing"

	messagev1 "github.com/absmach/fluxmq/pkg/proto/message/v1"
	"google.golang.org/protobuf/proto"
)

// C2 leaves one question open: whether the generated codec beats the
// hand-written one on the shape a durable record actually has. These benchmark
// the same bytes through both. The hand codec stays unless the generated one
// wins.
func BenchmarkSchemaMarshalRich(b *testing.B) {
	envelope := benchRichEnvelope()
	defer Release(envelope)

	encoded, err := MarshalBinary(envelope)
	if err != nil {
		b.Fatalf("seed the schema message: %v", err)
	}
	var schema messagev1.Envelope
	if err := proto.Unmarshal(encoded, &schema); err != nil {
		b.Fatalf("seed the schema message: %v", err)
	}

	b.ReportAllocs()
	for b.Loop() {
		if _, err := proto.Marshal(&schema); err != nil {
			b.Fatalf("marshal: %v", err)
		}
	}
}

func BenchmarkSchemaUnmarshalRich(b *testing.B) {
	envelope := benchRichEnvelope()
	defer Release(envelope)

	encoded, err := MarshalBinary(envelope)
	if err != nil {
		b.Fatalf("seed: %v", err)
	}

	b.ReportAllocs()
	b.SetBytes(int64(len(encoded)))
	for b.Loop() {
		var schema messagev1.Envelope
		if err := proto.Unmarshal(encoded, &schema); err != nil {
			b.Fatalf("unmarshal: %v", err)
		}
	}
}
