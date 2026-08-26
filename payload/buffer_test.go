// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package payload

import (
	"bytes"
	"testing"
)

func TestBufferReferenceLifetime(t *testing.T) {
	pool := NewPoolWithCapacity(1, 0, 0)
	buf := pool.FromBytes([]byte("payload"))
	buf.Retain()

	buf.Release()
	if got := string(buf.Bytes()); got != "payload" {
		t.Fatalf("payload after first release = %q", got)
	}
	buf.Release()

	reused := pool.get(len("payload"))
	defer reused.Release()
	if reused != buf {
		t.Fatal("released buffer was not returned to its size-class pool")
	}
}

func TestFromBytesCopiesInput(t *testing.T) {
	input := []byte("payload")
	buf := FromBytes(input)
	defer buf.Release()

	input[0] = 'X'
	if !bytes.Equal(buf.Bytes(), []byte("payload")) {
		t.Fatalf("buffer aliases caller input: %q", buf.Bytes())
	}
}

func TestOversizedBufferIsNotReused(t *testing.T) {
	pool := NewPoolWithCapacity(1, 1, 1)
	buf := pool.get(1048577)
	buf.Release()

	next := pool.get(1048577)
	defer next.Release()
	if next == buf {
		t.Fatal("oversized buffer must not be retained by the pool")
	}
}
