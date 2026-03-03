// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package memory

import "testing"

func TestStoreOutboundRoundtrip(t *testing.T) {
	s := New()

	payload := []byte("hello")
	if err := s.PutOutbound(7, payload); err != nil {
		t.Fatalf("PutOutbound() error = %v", err)
	}

	// Ensure stored value is immutable from caller mutations.
	payload[0] = 'H'

	got, ok, err := s.GetOutbound(7)
	if err != nil {
		t.Fatalf("GetOutbound() error = %v", err)
	}
	if !ok {
		t.Fatal("GetOutbound() expected ok=true")
	}
	if string(got) != "hello" {
		t.Fatalf("GetOutbound() payload = %q, want %q", string(got), "hello")
	}

	// Ensure returned value is also a copy.
	got[1] = 'X'
	got2, ok, err := s.GetOutbound(7)
	if err != nil || !ok {
		t.Fatalf("GetOutbound() second read failed: ok=%v err=%v", ok, err)
	}
	if string(got2) != "hello" {
		t.Fatalf("GetOutbound() second payload = %q, want %q", string(got2), "hello")
	}
}

func TestStoreInboundRoundtrip(t *testing.T) {
	s := New()

	if err := s.PutInbound(9, []byte("inbound")); err != nil {
		t.Fatalf("PutInbound() error = %v", err)
	}

	got, ok, err := s.GetInbound(9)
	if err != nil {
		t.Fatalf("GetInbound() error = %v", err)
	}
	if !ok {
		t.Fatal("GetInbound() expected ok=true")
	}
	if string(got) != "inbound" {
		t.Fatalf("GetInbound() payload = %q, want %q", string(got), "inbound")
	}
}

func TestStoreDeleteAndReset(t *testing.T) {
	s := New()

	if err := s.PutOutbound(1, []byte("one")); err != nil {
		t.Fatalf("PutOutbound() error = %v", err)
	}
	if err := s.PutOutbound(2, []byte("two")); err != nil {
		t.Fatalf("PutOutbound() error = %v", err)
	}
	if err := s.DeleteOutbound(1); err != nil {
		t.Fatalf("DeleteOutbound() error = %v", err)
	}

	if _, ok, err := s.GetOutbound(1); err != nil || ok {
		t.Fatalf("expected outbound key 1 to be deleted, ok=%v err=%v", ok, err)
	}

	values, err := s.ListOutbound()
	if err != nil {
		t.Fatalf("ListOutbound() error = %v", err)
	}
	if len(values) != 1 {
		t.Fatalf("ListOutbound() len = %d, want 1", len(values))
	}

	if err := s.PutInbound(3, []byte("three")); err != nil {
		t.Fatalf("PutInbound() error = %v", err)
	}
	if err := s.Reset(); err != nil {
		t.Fatalf("Reset() error = %v", err)
	}

	if values, err := s.ListOutbound(); err != nil || len(values) != 0 {
		t.Fatalf("after Reset(), ListOutbound() len=%d err=%v, want 0,nil", len(values), err)
	}
	if _, ok, err := s.GetInbound(3); err != nil || ok {
		t.Fatalf("after Reset(), expected inbound key 3 absent, ok=%v err=%v", ok, err)
	}
}
