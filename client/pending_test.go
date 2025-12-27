// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package client

import (
	"sync"
	"testing"
	"time"
)

func TestPendingStore(t *testing.T) {
	ps := newPendingStore(10)

	if ps.count() != 0 {
		t.Errorf("initial count should be 0, got %d", ps.count())
	}
}

func TestPendingStoreNextPacketID(t *testing.T) {
	ps := newPendingStore(10)

	id1 := ps.nextPacketID()
	id2 := ps.nextPacketID()

	if id1 == 0 {
		t.Error("first packet ID should not be 0")
	}
	if id2 == id1 {
		t.Error("packet IDs should be unique")
	}
}

func TestPendingStoreAdd(t *testing.T) {
	ps := newPendingStore(10)

	msg := NewMessage("test/topic", []byte("payload"), 1, false)
	op, err := ps.add(1, pendingPublish, msg)

	if err != nil {
		t.Fatalf("add failed: %v", err)
	}
	if op == nil {
		t.Fatal("operation should not be nil")
	}
	if ps.count() != 1 {
		t.Errorf("count should be 1, got %d", ps.count())
	}
}

func TestPendingStoreMaxInflight(t *testing.T) {
	ps := newPendingStore(2)

	_, err := ps.add(1, pendingPublish, nil)
	if err != nil {
		t.Fatalf("first add failed: %v", err)
	}

	_, err = ps.add(2, pendingPublish, nil)
	if err != nil {
		t.Fatalf("second add failed: %v", err)
	}

	_, err = ps.add(3, pendingPublish, nil)
	if err != ErrMaxInflight {
		t.Errorf("expected ErrMaxInflight, got %v", err)
	}
}

func TestPendingStoreGet(t *testing.T) {
	ps := newPendingStore(10)

	msg := NewMessage("test/topic", []byte("payload"), 1, false)
	ps.add(1, pendingPublish, msg)

	op := ps.get(1)
	if op == nil {
		t.Fatal("get should return the operation")
	}
	if op.id != 1 {
		t.Errorf("operation ID should be 1, got %d", op.id)
	}
	if op.opType != pendingPublish {
		t.Errorf("operation type should be pendingPublish")
	}

	op = ps.get(999)
	if op != nil {
		t.Error("get with non-existent ID should return nil")
	}
}

func TestPendingStoreComplete(t *testing.T) {
	ps := newPendingStore(10)
	ps.add(1, pendingPublish, nil)

	completed := ps.complete(1, nil, []byte{0x00})
	if !completed {
		t.Error("complete should return true for existing operation")
	}
	if ps.count() != 0 {
		t.Errorf("count should be 0 after complete, got %d", ps.count())
	}

	completed = ps.complete(999, nil, nil)
	if completed {
		t.Error("complete should return false for non-existent operation")
	}
}

func TestPendingStoreRemove(t *testing.T) {
	ps := newPendingStore(10)
	ps.add(1, pendingPublish, nil)

	ps.remove(1)
	if ps.count() != 0 {
		t.Errorf("count should be 0 after remove, got %d", ps.count())
	}

	// Remove non-existent should not panic
	ps.remove(999)
}

func TestPendingStoreUpdateQoS2State(t *testing.T) {
	ps := newPendingStore(10)
	ps.add(1, pendingPublish, nil)

	updated := ps.updateQoS2State(1, 1)
	if !updated {
		t.Error("updateQoS2State should return true for existing operation")
	}

	op := ps.get(1)
	if op.qos2State != 1 {
		t.Errorf("qos2State should be 1, got %d", op.qos2State)
	}

	updated = ps.updateQoS2State(999, 1)
	if updated {
		t.Error("updateQoS2State should return false for non-existent operation")
	}
}

func TestPendingStoreGetAll(t *testing.T) {
	ps := newPendingStore(10)
	ps.add(1, pendingPublish, nil)
	ps.add(2, pendingSubscribe, nil)
	ps.add(3, pendingUnsubscribe, nil)

	ops := ps.getAll()
	if len(ops) != 3 {
		t.Errorf("getAll should return 3 operations, got %d", len(ops))
	}
}

func TestPendingStoreClear(t *testing.T) {
	ps := newPendingStore(10)

	op1, _ := ps.add(1, pendingPublish, nil)
	op2, _ := ps.add(2, pendingPublish, nil)

	testErr := ErrConnectionLost
	ps.clear(testErr)

	if ps.count() != 0 {
		t.Errorf("count should be 0 after clear, got %d", ps.count())
	}

	// Operations should be completed with error
	select {
	case <-op1.done:
		if op1.err != testErr {
			t.Errorf("op1 error should be %v, got %v", testErr, op1.err)
		}
	default:
		t.Error("op1 should be done after clear")
	}

	select {
	case <-op2.done:
		if op2.err != testErr {
			t.Errorf("op2 error should be %v, got %v", testErr, op2.err)
		}
	default:
		t.Error("op2 should be done after clear")
	}
}

func TestPendingOpWait(t *testing.T) {
	ps := newPendingStore(10)
	op, _ := ps.add(1, pendingPublish, nil)

	// Complete in background
	go func() {
		time.Sleep(10 * time.Millisecond)
		ps.complete(1, nil, nil)
	}()

	err := op.wait(1 * time.Second)
	if err != nil {
		t.Errorf("wait should succeed, got error: %v", err)
	}
}

func TestPendingOpWaitTimeout(t *testing.T) {
	ps := newPendingStore(10)
	op, _ := ps.add(1, pendingPublish, nil)

	err := op.wait(10 * time.Millisecond)
	if err != ErrTimeout {
		t.Errorf("wait should timeout, got: %v", err)
	}
}

func TestPendingStoreConcurrency(t *testing.T) {
	ps := newPendingStore(1000)
	var wg sync.WaitGroup

	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			id := ps.nextPacketID()
			if id != 0 {
				ps.add(id, pendingPublish, nil)
				ps.get(id)
				ps.complete(id, nil, nil)
			}
		}()
	}

	wg.Wait()
}

func TestNextPacketIDWraparound(t *testing.T) {
	ps := newPendingStore(10)

	// Set nextID close to max
	ps.nextID = 65534

	id1 := ps.nextPacketID()
	id2 := ps.nextPacketID()
	id3 := ps.nextPacketID()

	if id1 != 65534 {
		t.Errorf("expected 65534, got %d", id1)
	}
	if id2 != 65535 {
		t.Errorf("expected 65535, got %d", id2)
	}
	if id3 != 1 {
		t.Errorf("expected 1 after wraparound, got %d", id3)
	}
}
