// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package message

import (
	"bytes"
	"testing"
)

// FuzzUnmarshalBinary feeds the envelope decoder arbitrary bytes. Cluster
// peers send envelopes in this encoding, so its input is whatever a faulty or
// hostile node puts on the wire: it must reject what it cannot decode without
// panicking, and an envelope it accepts must survive re-encoding. Header and
// property maps are written in map iteration order, so two encodings of one
// envelope can differ in field order but not in length.
func FuzzUnmarshalBinary(f *testing.F) {
	for _, envelope := range []*Envelope{
		New("", nil),
		NewDelivery("devices/1", []byte("payload"), 1, true),
		benchRichEnvelope(),
	} {
		encoded, err := MarshalBinary(envelope)
		if err != nil {
			f.Fatal(err)
		}
		f.Add(encoded)
		f.Add(encoded[:len(encoded)/2])
		Release(envelope)
	}
	f.Add([]byte{})
	f.Add([]byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff})

	f.Fuzz(func(t *testing.T, data []byte) {
		decoded, err := UnmarshalBinary(data)
		if err != nil {
			return
		}
		defer Release(decoded)

		first, err := MarshalBinary(decoded)
		if err != nil {
			t.Fatalf("decoded envelope does not re-encode: %v", err)
		}
		again, err := UnmarshalBinary(first)
		if err != nil {
			t.Fatalf("re-encoded envelope does not decode: %v", err)
		}
		defer Release(again)

		second, err := MarshalBinary(again)
		if err != nil {
			t.Fatalf("second re-encode failed: %v", err)
		}
		if len(first) != len(second) {
			t.Fatalf("re-encoding changed the envelope:\n first %x\nsecond %x", first, second)
		}
		if !bytes.Equal(decoded.PayloadBytes(), again.PayloadBytes()) || decoded.Topic != again.Topic {
			t.Fatalf("re-encoding changed topic or payload")
		}
	})
}
