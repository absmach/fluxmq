// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package bufpool

import (
	"sync"
	"testing"
)

func TestGetReturnsResetBuffer(t *testing.T) {
	b := Get()
	b.WriteString("hello")
	Put(b)

	b2 := Get()
	if b2.Len() != 0 {
		t.Fatalf("expected empty buffer, got %d bytes", b2.Len())
	}
	Put(b2)
}

func TestPutDiscardsOversizedBuffer(t *testing.T) {
	b := Get()
	b.Grow(maxPooledCap + 1)
	Put(b) // should be discarded, not panic
}

func TestConcurrentGetPut(t *testing.T) {
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			b := Get()
			b.WriteString("concurrent test data")
			Put(b)
		}()
	}
	wg.Wait()
}

func TestGetReturnsUsableBuffer(t *testing.T) {
	b := Get()
	defer Put(b)

	n, err := b.WriteString("test")
	if err != nil {
		t.Fatal(err)
	}
	if n != 4 {
		t.Fatalf("expected 4 bytes written, got %d", n)
	}
	if b.String() != "test" {
		t.Fatalf("expected %q, got %q", "test", b.String())
	}
}
