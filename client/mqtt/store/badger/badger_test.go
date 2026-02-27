// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package badger

import (
	"errors"
	"testing"
)

func TestValidateMissingDir(t *testing.T) {
	opts := &Options{
		InMemory: false,
	}

	err := opts.Validate()
	if !errors.Is(err, ErrMissingDir) {
		t.Fatalf("Validate() error = %v, want %v", err, ErrMissingDir)
	}
}

func TestStoreOutboundInboundRoundtrip(t *testing.T) {
	s, err := New(&Options{InMemory: true})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	defer s.Close()

	if err := s.PutOutbound(1, []byte("out-1")); err != nil {
		t.Fatalf("PutOutbound() error = %v", err)
	}
	if err := s.PutInbound(2, []byte("in-2")); err != nil {
		t.Fatalf("PutInbound() error = %v", err)
	}

	outbound, ok, err := s.GetOutbound(1)
	if err != nil {
		t.Fatalf("GetOutbound() error = %v", err)
	}
	if !ok {
		t.Fatal("GetOutbound() expected ok=true")
	}
	if string(outbound) != "out-1" {
		t.Fatalf("GetOutbound() payload = %q, want %q", string(outbound), "out-1")
	}

	inbound, ok, err := s.GetInbound(2)
	if err != nil {
		t.Fatalf("GetInbound() error = %v", err)
	}
	if !ok {
		t.Fatal("GetInbound() expected ok=true")
	}
	if string(inbound) != "in-2" {
		t.Fatalf("GetInbound() payload = %q, want %q", string(inbound), "in-2")
	}
}

func TestStoreDeleteListAndReset(t *testing.T) {
	s, err := New(&Options{InMemory: true})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	defer s.Close()

	if err := s.PutOutbound(10, []byte("ten")); err != nil {
		t.Fatalf("PutOutbound() error = %v", err)
	}
	if err := s.PutOutbound(11, []byte("eleven")); err != nil {
		t.Fatalf("PutOutbound() error = %v", err)
	}

	values, err := s.ListOutbound()
	if err != nil {
		t.Fatalf("ListOutbound() error = %v", err)
	}
	if len(values) != 2 {
		t.Fatalf("ListOutbound() len = %d, want 2", len(values))
	}

	if err := s.DeleteOutbound(10); err != nil {
		t.Fatalf("DeleteOutbound() error = %v", err)
	}
	if _, ok, err := s.GetOutbound(10); err != nil || ok {
		t.Fatalf("expected deleted outbound key 10, ok=%v err=%v", ok, err)
	}

	if err := s.PutInbound(20, []byte("twenty")); err != nil {
		t.Fatalf("PutInbound() error = %v", err)
	}
	if err := s.Reset(); err != nil {
		t.Fatalf("Reset() error = %v", err)
	}

	if values, err := s.ListOutbound(); err != nil || len(values) != 0 {
		t.Fatalf("after Reset(), ListOutbound() len=%d err=%v, want 0,nil", len(values), err)
	}
	if _, ok, err := s.GetInbound(20); err != nil || ok {
		t.Fatalf("after Reset(), expected inbound key 20 absent, ok=%v err=%v", ok, err)
	}
}
