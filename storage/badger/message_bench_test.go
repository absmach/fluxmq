// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package badger

import (
	"testing"

	"github.com/absmach/fluxmq/storage"
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
	msg := &storage.Message{Topic: "bench/topic", Payload: make([]byte, 256), QoS: 1}

	b.ReportAllocs()
	for b.Loop() {
		if err := store.Store("bench-client/queue/0", msg); err != nil {
			b.Fatalf("store failed: %v", err)
		}
	}
}

func BenchmarkMessageStore_Get(b *testing.B) {
	store := benchMessageStore(b)
	msg := &storage.Message{Topic: "bench/topic", Payload: make([]byte, 256), QoS: 1}
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
