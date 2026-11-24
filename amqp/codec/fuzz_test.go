// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package codec

import (
	"bytes"
	"fmt"
	"runtime"
	"testing"
)

// startOkPayload builds a Connection.StartOk method payload, the first thing a
// client sends and something the broker decodes before authentication.
func startOkPayload(t testing.TB, props map[string]any) []byte {
	t.Helper()
	var b bytes.Buffer
	for _, step := range []error{
		WriteShort(&b, 10), WriteShort(&b, 11),
		WriteTable(&b, props),
		WriteShortStr(&b, "PLAIN"),
		WriteLongStr(&b, "\x00user\x00pass"),
		WriteShortStr(&b, "en_US"),
	} {
		if step != nil {
			t.Fatal(step)
		}
	}
	return b.Bytes()
}

// FuzzFrameDecode decodes arbitrary method and content-header payloads.
// Declared lengths inside a payload must never be trusted for allocation:
// readSized keeps every allocation within the bytes actually present.
func FuzzFrameDecode(f *testing.F) {
	f.Add(FrameMethod, startOkPayload(f, map[string]any{"product": "x", "capabilities": map[string]any{"publisher_confirms": true}}))
	f.Add(FrameMethod, []byte{0, 60, 0, 40, 0, 0, 3, 'e', 'x', 'c', 1, 'k', 0})
	f.Add(FrameHeader, []byte{0, 60, 0, 0, 0, 0, 0, 0, 0, 0, 0, 5, 0x10, 0x00, 0, 0, 0, 0})
	// A tiny table claiming a 4 GiB long string reserved 4 GiB before it was fixed.
	f.Add(FrameMethod, []byte{0, 10, 0, 11, 0, 0, 0, 7, 1, 'k', 'S', 0xff, 0xff, 0xff, 0xff})

	f.Fuzz(func(t *testing.T, frameType byte, payload []byte) {
		frame := &Frame{Type: frameType, Channel: 1, Payload: payload}
		_, _ = frame.Decode()
	})
}

// FuzzTableRoundTrip checks that any table the reader accepts is written back
// in a form that reads as the same table.
func FuzzTableRoundTrip(f *testing.F) {
	var seed bytes.Buffer
	if err := WriteTable(&seed, map[string]any{
		"s": "v", "i": int32(-7), "l": int64(1 << 40), "b": true, "t": map[string]any{"n": nil}, "a": []any{"x", int8(1)},
	}); err != nil {
		f.Fatal(err)
	}
	f.Add(seed.Bytes())
	f.Add([]byte{0, 0, 0, 0})

	f.Fuzz(func(t *testing.T, data []byte) {
		table, err := ReadTable(bytes.NewReader(data))
		if err != nil {
			return
		}
		var out bytes.Buffer
		if err := WriteTable(&out, table); err != nil {
			return // a value the reader accepts but the writer does not produce
		}
		again, err := ReadTable(bytes.NewReader(out.Bytes()))
		if err != nil {
			t.Fatalf("written table % x does not read back: %v", out.Bytes(), err)
		}
		// fmt sorts map keys and prints NaN as NaN, so equal tables print equal.
		if fmt.Sprint(again) != fmt.Sprint(table) {
			t.Fatalf("table changed in a round trip:\n first  %v\n second %v", table, again)
		}
	})
}

func TestDeclaredLengthsDoNotDriveAllocation(t *testing.T) {
	cases := []struct {
		name string
		read func(*bytes.Reader) error
		data []byte
	}{
		{
			"table/long string", func(r *bytes.Reader) error { _, err := ReadTable(r); return err },
			[]byte{0, 0, 0, 7, 1, 'k', 'S', 0xff, 0xff, 0xff, 0xff},
		},
		{
			"table/byte array", func(r *bytes.Reader) error { _, err := ReadTable(r); return err },
			[]byte{0, 0, 0, 7, 1, 'k', 'x', 0xff, 0xff, 0xff, 0xff},
		},
		{
			"table/declared size", func(r *bytes.Reader) error { _, err := ReadTable(r); return err },
			[]byte{0xff, 0xff, 0xff, 0xff},
		},
		{
			"array/declared size", func(r *bytes.Reader) error { _, err := ReadArray(r); return err },
			[]byte{0xff, 0xff, 0xff, 0xff},
		},
		{
			"long string", func(r *bytes.Reader) error { _, err := ReadLongStr(r); return err },
			[]byte{0xff, 0xff, 0xff, 0xff, 'a'},
		},
		{
			"frame/declared size", func(r *bytes.Reader) error { _, err := ReadFrame(r); return err },
			[]byte{FrameMethod, 0, 0, 0xff, 0xff, 0xff, 0xff, 1, 2},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var before, after runtime.MemStats
			runtime.ReadMemStats(&before)
			err := tc.read(bytes.NewReader(tc.data))
			runtime.ReadMemStats(&after)

			if err == nil {
				t.Fatal("a length beyond the input must be rejected")
			}
			if grew := after.TotalAlloc - before.TotalAlloc; grew > 1<<20 {
				t.Fatalf("%d input bytes allocated %d MiB", len(tc.data), grew>>20)
			}
		})
	}
}
