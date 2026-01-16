// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package v5_test

import (
	"bytes"
	"io"
	"testing"

	"github.com/absmach/fluxmq/core/codec"
	. "github.com/absmach/fluxmq/core/packets/v5"
)

func BenchmarkReadInline(b *testing.B) {
	buf := bytes.NewReader([]byte{0x42})
	var arr [1]byte
	for i := 0; i < b.N; i++ {
		buf.Reset([]byte{0x42})
		io.ReadAtLeast(buf, arr[:], 1)
	}
}

func BenchmarkReadInlineFull(b *testing.B) {
	buf := bytes.NewReader([]byte{0x42})
	var arr [1]byte
	for i := 0; i < b.N; i++ {
		buf.Reset([]byte{0x42})
		io.ReadFull(buf, arr[:])
	}
}

func BenchmarkReadMake(b *testing.B) {
	buf := bytes.NewReader([]byte{0x42})
	for i := 0; i < b.N; i++ {
		buf.Reset([]byte{0x42})
		data := make([]byte, 1)
		io.ReadAtLeast(buf, data, 1)
	}
}

func BenchmarkU16(b *testing.B) {
	for b.Loop() {
		val := func() *ConnAckProperties {
			a := uint16(1)
			ret := ConnAckProperties{
				ReceiveMax: &a,
			}
			return &ret
		}()

		ret := codec.EncodeUint16(*val.ReceiveMax)
		_ = ret
	}
}

func BenchmarkInlineU16(b *testing.B) {
	for b.Loop() {
		val := func() *ConnAckProperties {
			a := uint16(1)
			ret := ConnAckProperties{
				ReceiveMax: &a,
			}
			return &ret
		}()

		ret := []byte{byte(*val.ReceiveMax >> 8), byte(*val.ReceiveMax)}
		_ = ret
	}
}
