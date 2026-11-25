// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package packets

import (
	"io"
	"sync"
)

// maxPooledEncodeBuffer caps the buffers WriteEncoded keeps. A packet larger
// than this is encoded into a buffer that is dropped afterwards, so one large
// message does not pin its size in the pool.
const maxPooledEncodeBuffer = 64 << 10

var encodeBufferPool = sync.Pool{
	New: func() any {
		buf := make([]byte, 0, 1024)
		return &buf
	},
}

// AppendEncoder is a packet that can append its wire form to a buffer.
type AppendEncoder interface {
	EncodeTo(dst []byte) []byte
}

// WriteEncoded encodes pkt into a pooled buffer and writes it to w in one
// call, so a packet that is written once per subscriber does not allocate its
// wire form each time. Writing it in one call keeps a WebSocket connection,
// which frames each write, sending one frame per packet.
func WriteEncoded(w io.Writer, pkt AppendEncoder) error {
	buf := encodeBufferPool.Get().(*[]byte)
	*buf = pkt.EncodeTo((*buf)[:0])
	_, err := w.Write(*buf)
	if cap(*buf) <= maxPooledEncodeBuffer {
		encodeBufferPool.Put(buf)
	}
	return err
}
