// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package bufpool

import (
	"bytes"
	"sync"
)

const maxPooledCap = 64 * 1024

var pool = sync.Pool{New: func() any { return new(bytes.Buffer) }}

func Get() *bytes.Buffer {
	b := pool.Get().(*bytes.Buffer)
	b.Reset()
	return b
}

func Put(b *bytes.Buffer) {
	if b.Cap() > maxPooledCap {
		return
	}
	pool.Put(b)
}
