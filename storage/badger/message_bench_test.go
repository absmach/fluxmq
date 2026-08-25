// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package badger

import (
	"testing"

	"github.com/absmach/fluxmq/message"
)

func benchMessageStore(b *testing.B) *MessageStore {
	b.Helper()

	store, err := New(Config{Dir: b.TempDir()})
	if err != nil {
		b.Fatalf("failed to open store: %v", err)
	}
	b.Cleanup(func() {
		store.Close() //nolint:errcheck // benchmark teardown
	})

	return store.Messages().(*MessageStore)
}

func BenchmarkMessageStore_Store(b *testing.B) {
	store := benchMessageStore(b)
	msg := message.NewDelivery("bench/topic", make([]byte, 256), 1, false)

	b.ReportAllocs()
	for b.Loop() {
		if err := store.Store("bench-client/queue/0", msg); err != nil {
			b.Fatalf("store failed: %v", err)
		}
	}
}

func BenchmarkMessageStore_Get(b *testing.B) {
	store := benchMessageStore(b)
	msg := message.NewDelivery("bench/topic", make([]byte, 256), 1, false)
	if err := store.Store("bench-client/queue/0", msg); err != nil {
		b.Fatalf("store failed: %v", err)
	}

	b.ReportAllocs()
	for b.Loop() {
		if _, err := store.Get("bench-client/queue/0"); err != nil {
			b.Fatalf("get failed: %v", err)
		}
	}
}
