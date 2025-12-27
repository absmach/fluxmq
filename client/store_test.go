// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package client

import (
	"sync"
	"testing"
)

func TestMemoryStore(t *testing.T) {
	store := NewMemoryStore()
	if store == nil {
		t.Fatal("NewMemoryStore should not return nil")
	}
}

func TestMemoryStoreOutbound(t *testing.T) {
	store := NewMemoryStore()
	msg := NewMessage("test/topic", []byte("payload"), 1, false)
	msg.PacketID = 1

	// Store
	err := store.StoreOutbound(1, msg)
	if err != nil {
		t.Fatalf("StoreOutbound failed: %v", err)
	}

	// Get
	retrieved, ok := store.GetOutbound(1)
	if !ok {
		t.Fatal("GetOutbound should return true for existing message")
	}
	if retrieved.Topic != msg.Topic {
		t.Errorf("topic mismatch: got %s, want %s", retrieved.Topic, msg.Topic)
	}
	if string(retrieved.Payload) != string(msg.Payload) {
		t.Error("payload mismatch")
	}

	// Get non-existent
	_, ok = store.GetOutbound(999)
	if ok {
		t.Error("GetOutbound should return false for non-existent message")
	}

	// Delete
	err = store.DeleteOutbound(1)
	if err != nil {
		t.Fatalf("DeleteOutbound failed: %v", err)
	}

	_, ok = store.GetOutbound(1)
	if ok {
		t.Error("message should be deleted")
	}
}

func TestMemoryStoreInbound(t *testing.T) {
	store := NewMemoryStore()
	msg := NewMessage("test/topic", []byte("payload"), 2, false)
	msg.PacketID = 1

	// Store
	err := store.StoreInbound(1, msg)
	if err != nil {
		t.Fatalf("StoreInbound failed: %v", err)
	}

	// Get
	retrieved, ok := store.GetInbound(1)
	if !ok {
		t.Fatal("GetInbound should return true for existing message")
	}
	if retrieved.Topic != msg.Topic {
		t.Errorf("topic mismatch: got %s, want %s", retrieved.Topic, msg.Topic)
	}

	// Get non-existent
	_, ok = store.GetInbound(999)
	if ok {
		t.Error("GetInbound should return false for non-existent message")
	}

	// Delete
	err = store.DeleteInbound(1)
	if err != nil {
		t.Fatalf("DeleteInbound failed: %v", err)
	}

	_, ok = store.GetInbound(1)
	if ok {
		t.Error("message should be deleted")
	}
}

func TestMemoryStoreGetAllOutbound(t *testing.T) {
	store := NewMemoryStore()

	msg1 := NewMessage("topic1", []byte("payload1"), 1, false)
	msg1.PacketID = 1
	msg2 := NewMessage("topic2", []byte("payload2"), 1, false)
	msg2.PacketID = 2

	store.StoreOutbound(1, msg1)
	store.StoreOutbound(2, msg2)

	msgs := store.GetAllOutbound()
	if len(msgs) != 2 {
		t.Errorf("expected 2 messages, got %d", len(msgs))
	}
}

func TestMemoryStoreReset(t *testing.T) {
	store := NewMemoryStore()

	msg := NewMessage("topic", []byte("payload"), 1, false)
	store.StoreOutbound(1, msg)
	store.StoreInbound(2, msg)

	err := store.Reset()
	if err != nil {
		t.Fatalf("Reset failed: %v", err)
	}

	msgs := store.GetAllOutbound()
	if len(msgs) != 0 {
		t.Error("outbound should be empty after reset")
	}

	_, ok := store.GetInbound(2)
	if ok {
		t.Error("inbound should be empty after reset")
	}
}

func TestMemoryStoreClose(t *testing.T) {
	store := NewMemoryStore()

	msg := NewMessage("topic", []byte("payload"), 1, false)
	store.StoreOutbound(1, msg)

	err := store.Close()
	if err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	msgs := store.GetAllOutbound()
	if len(msgs) != 0 {
		t.Error("store should be empty after close")
	}
}

func TestMemoryStoreDeepCopy(t *testing.T) {
	store := NewMemoryStore()

	original := NewMessage("topic", []byte("original"), 1, false)
	original.PacketID = 1
	original.UserProperties = map[string]string{"key": "value"}

	store.StoreOutbound(1, original)

	// Modify original
	original.Payload = []byte("modified")
	original.UserProperties["key"] = "changed"

	// Retrieved should have original values
	retrieved, _ := store.GetOutbound(1)
	if string(retrieved.Payload) != "original" {
		t.Error("stored message should not be affected by original modification")
	}
	if retrieved.UserProperties["key"] != "value" {
		t.Error("stored user properties should not be affected")
	}

	// Modify retrieved
	retrieved.Payload = []byte("also modified")

	// Get again should still have original values
	retrieved2, _ := store.GetOutbound(1)
	if string(retrieved2.Payload) != "original" {
		t.Error("stored message should not be affected by retrieved modification")
	}
}

func TestMemoryStoreConcurrency(t *testing.T) {
	store := NewMemoryStore()
	var wg sync.WaitGroup

	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(id uint16) {
			defer wg.Done()
			msg := NewMessage("topic", []byte("payload"), 1, false)
			msg.PacketID = id
			store.StoreOutbound(id, msg)
			store.GetOutbound(id)
			store.DeleteOutbound(id)
		}(uint16(i))
	}

	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(id uint16) {
			defer wg.Done()
			msg := NewMessage("topic", []byte("payload"), 2, false)
			msg.PacketID = id
			store.StoreInbound(id, msg)
			store.GetInbound(id)
			store.DeleteInbound(id)
		}(uint16(i + 1000))
	}

	wg.Wait()
}

func TestMemoryStoreNilMessage(t *testing.T) {
	store := NewMemoryStore()

	// Storing nil message should not panic
	err := store.StoreOutbound(1, nil)
	if err != nil {
		t.Fatalf("StoreOutbound nil failed: %v", err)
	}

	// Getting nil message should return nil copy
	msg, ok := store.GetOutbound(1)
	if !ok {
		t.Error("GetOutbound should return true")
	}
	if msg != nil {
		t.Error("GetOutbound should return nil for nil message")
	}
}
