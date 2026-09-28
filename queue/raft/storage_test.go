// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package raft

import (
	"errors"
	"os"
	"os/exec"
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/hashicorp/raft"
)

func setupTestDB(t *testing.T) (*badger.DB, func()) {
	t.Helper()

	dir, err := os.MkdirTemp("", "raft-storage-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}

	opts := badger.DefaultOptions(dir)
	opts.Logger = nil
	db, err := badger.Open(opts)
	if err != nil {
		os.RemoveAll(dir)
		t.Fatalf("failed to open badger: %v", err)
	}

	cleanup := func() {
		db.Close()
		os.RemoveAll(dir)
	}

	return db, cleanup
}

func TestRaftBadgerOptionsSyncLogAndStableState(t *testing.T) {
	dir := t.TempDir()
	opts := raftBadgerOptions(dir)
	if !opts.SyncWrites {
		t.Fatal("Raft log and stable state must use synchronous Badger writes")
	}

	db, err := badger.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	logStore := NewBadgerLogStore(db, "_raft", 0)
	stableStore := NewBadgerStableStore(db, "_raft", 0)
	want := &raft.Log{Index: 7, Term: 3, Type: raft.LogCommand, Data: []byte("durable entry")}
	if err := logStore.StoreLog(want); err != nil {
		t.Fatal(err)
	}
	if err := stableStore.SetUint64([]byte("term"), want.Term); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	db, err = badger.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	got := new(raft.Log)
	if err := NewBadgerLogStore(db, "_raft", 0).GetLog(want.Index, got); err != nil {
		t.Fatal(err)
	}
	if string(got.Data) != string(want.Data) || got.Term != want.Term {
		t.Fatalf("reopened log = %#v, want %#v", got, want)
	}
	term, err := NewBadgerStableStore(db, "_raft", 0).GetUint64([]byte("term"))
	if err != nil {
		t.Fatal(err)
	}
	if term != want.Term {
		t.Fatalf("reopened term = %d, want %d", term, want.Term)
	}
}

// The child exits without closing Badger, as a broker killed after a write
// would. A graceful Close can flush buffered data and would hide a missing
// SyncWrites option.
func TestRaftBadgerWritesSurviveUncleanExit(t *testing.T) {
	const helper = "FLUXMQ_RAFT_UNCLEAN_EXIT_HELPER"
	const helperDir = "FLUXMQ_RAFT_UNCLEAN_EXIT_DIR"
	if os.Getenv(helper) == "1" {
		db, err := badger.Open(raftBadgerOptions(os.Getenv(helperDir)))
		if err != nil {
			t.Fatal(err)
		}
		if err := NewBadgerLogStore(db, "_raft", 0).StoreLog(&raft.Log{
			Index: 7, Term: 3, Type: raft.LogCommand, Data: []byte("unclean exit"),
		}); err != nil {
			t.Fatal(err)
		}
		if err := NewBadgerStableStore(db, "_raft", 0).SetUint64([]byte("term"), 3); err != nil {
			t.Fatal(err)
		}
		os.Exit(0) // Deliberately bypass Badger.Close and all deferred cleanup.
	}

	dir := t.TempDir()
	cmd := exec.Command(os.Args[0], "-test.run=^TestRaftBadgerWritesSurviveUncleanExit$")
	cmd.Env = append(os.Environ(), helper+"=1", helperDir+"="+dir)
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("unclean writer exited with error: %v\n%s", err, output)
	}

	db, err := badger.Open(raftBadgerOptions(dir))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	got := new(raft.Log)
	if err := NewBadgerLogStore(db, "_raft", 0).GetLog(7, got); err != nil {
		t.Fatal(err)
	}
	if string(got.Data) != "unclean exit" || got.Term != 3 {
		t.Fatalf("recovered log = %#v", got)
	}
	term, err := NewBadgerStableStore(db, "_raft", 0).GetUint64([]byte("term"))
	if err != nil {
		t.Fatal(err)
	}
	if term != 3 {
		t.Fatalf("recovered term = %d, want 3", term)
	}
}

func TestBadgerLogStore_FirstLastIndex(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	store := NewBadgerLogStore(db, "test-queue", 0)

	// Initially should be empty
	first, err := store.FirstIndex()
	if err != nil {
		t.Fatalf("FirstIndex failed: %v", err)
	}
	if first != 0 {
		t.Errorf("expected first index 0, got %d", first)
	}

	last, err := store.LastIndex()
	if err != nil {
		t.Fatalf("LastIndex failed: %v", err)
	}
	if last != 0 {
		t.Errorf("expected last index 0, got %d", last)
	}
}

func TestBadgerLogStore_StoreLog(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	store := NewBadgerLogStore(db, "test-queue", 0)

	log := &raft.Log{
		Index: 1,
		Term:  1,
		Type:  raft.LogCommand,
		Data:  []byte("test data"),
	}

	if err := store.StoreLog(log); err != nil {
		t.Fatalf("StoreLog failed: %v", err)
	}

	// Retrieve the log
	retrieved := &raft.Log{}
	if err := store.GetLog(1, retrieved); err != nil {
		t.Fatalf("GetLog failed: %v", err)
	}

	if retrieved.Index != log.Index {
		t.Errorf("expected index %d, got %d", log.Index, retrieved.Index)
	}
	if retrieved.Term != log.Term {
		t.Errorf("expected term %d, got %d", log.Term, retrieved.Term)
	}
	if string(retrieved.Data) != string(log.Data) {
		t.Errorf("expected data %s, got %s", log.Data, retrieved.Data)
	}
}

func TestBadgerLogStore_StoreLogs(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	store := NewBadgerLogStore(db, "test-queue", 0)

	logs := []*raft.Log{
		{Index: 1, Term: 1, Type: raft.LogCommand, Data: []byte("log1")},
		{Index: 2, Term: 1, Type: raft.LogCommand, Data: []byte("log2")},
		{Index: 3, Term: 2, Type: raft.LogCommand, Data: []byte("log3")},
	}

	if err := store.StoreLogs(logs); err != nil {
		t.Fatalf("StoreLogs failed: %v", err)
	}

	// Check first and last
	first, err := store.FirstIndex()
	if err != nil {
		t.Fatalf("FirstIndex failed: %v", err)
	}
	if first != 1 {
		t.Errorf("expected first index 1, got %d", first)
	}

	last, err := store.LastIndex()
	if err != nil {
		t.Fatalf("LastIndex failed: %v", err)
	}
	if last != 3 {
		t.Errorf("expected last index 3, got %d", last)
	}
}

func TestBadgerLogStore_DeleteRange(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	store := NewBadgerLogStore(db, "test-queue", 0)

	logs := []*raft.Log{
		{Index: 1, Term: 1, Type: raft.LogCommand, Data: []byte("log1")},
		{Index: 2, Term: 1, Type: raft.LogCommand, Data: []byte("log2")},
		{Index: 3, Term: 2, Type: raft.LogCommand, Data: []byte("log3")},
		{Index: 4, Term: 2, Type: raft.LogCommand, Data: []byte("log4")},
	}

	if err := store.StoreLogs(logs); err != nil {
		t.Fatalf("StoreLogs failed: %v", err)
	}

	// Delete range [2, 3]
	if err := store.DeleteRange(2, 3); err != nil {
		t.Fatalf("DeleteRange failed: %v", err)
	}

	// Should have 1 and 4 remaining
	first, _ := store.FirstIndex()
	if first != 1 {
		t.Errorf("expected first index 1, got %d", first)
	}

	last, _ := store.LastIndex()
	if last != 4 {
		t.Errorf("expected last index 4, got %d", last)
	}

	// Trying to get deleted logs should fail
	retrieved := &raft.Log{}
	if err := store.GetLog(2, retrieved); !errors.Is(err, raft.ErrLogNotFound) {
		t.Errorf("expected ErrLogNotFound, got %v", err)
	}
}

func TestBadgerStableStore_SetGet(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	store := NewBadgerStableStore(db, "test-queue", 0)

	key := []byte("test-key")
	val := []byte("test-value")

	if err := store.Set(key, val); err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	retrieved, err := store.Get(key)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	if string(retrieved) != string(val) {
		t.Errorf("expected %s, got %s", val, retrieved)
	}
}

func TestBadgerStableStore_SetGetUint64(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	store := NewBadgerStableStore(db, "test-queue", 0)

	key := []byte("term")
	val := uint64(42)

	if err := store.SetUint64(key, val); err != nil {
		t.Fatalf("SetUint64 failed: %v", err)
	}

	retrieved, err := store.GetUint64(key)
	if err != nil {
		t.Fatalf("GetUint64 failed: %v", err)
	}

	if retrieved != val {
		t.Errorf("expected %d, got %d", val, retrieved)
	}
}

func TestBadgerStableStore_GetNonExistent(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	store := NewBadgerStableStore(db, "test-queue", 0)

	// Per Raft StableStore contract, Get returns (nil, nil) for non-existent keys
	val, err := store.Get([]byte("non-existent"))
	if err != nil {
		t.Errorf("expected nil error, got %v", err)
	}
	if val != nil {
		t.Errorf("expected nil value, got %v", val)
	}
}
