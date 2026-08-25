// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package badger

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/absmach/fluxmq/message"
	"github.com/absmach/fluxmq/storage"
	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const closeTestFilter = "a/b"

func TestStore_OperationsAfterClose(t *testing.T) {
	store, err := New(Config{Dir: t.TempDir()})
	require.NoError(t, err)

	msg := message.NewDelivery("test/topic", []byte("payload"), 1, false)
	require.NoError(t, store.Messages().Store("client-1/queue/0", msg))

	require.NoError(t, store.Close())

	ctx := context.Background()

	cases := []struct {
		name string
		op   func() error
	}{
		{"messages/store", func() error { return store.Messages().Store("client-1/queue/1", msg) }},
		{"messages/get", func() error {
			_, err := store.Messages().Get("client-1/queue/0")
			return err
		}},
		{"messages/delete", func() error { return store.Messages().Delete("client-1/queue/0") }},
		{"messages/list", func() error {
			_, err := store.Messages().List("client-1/")
			return err
		}},
		{"messages/delete_by_prefix", func() error { return store.Messages().DeleteByPrefix("client-1/") }},
		{"sessions/get", func() error {
			_, err := store.Sessions().Get("client-1")
			return err
		}},
		{"sessions/delete", func() error { return store.Sessions().Delete("client-1") }},
		{"subscriptions/add", func() error {
			return store.Subscriptions().Add(&storage.Subscription{ClientID: "client-1", Filter: closeTestFilter})
		}},
		{"subscriptions/match", func() error {
			_, err := store.Subscriptions().Match(closeTestFilter)
			return err
		}},
		{"retained/set", func() error { return store.Retained().Set(ctx, "test/topic", msg) }},
		{"retained/get", func() error {
			_, err := store.Retained().Get(ctx, "test/topic")
			return err
		}},
		{"wills/set", func() error {
			return store.Wills().Set(ctx, "client-1", &storage.WillMessage{Topic: "will/topic"})
		}},
		{"wills/get", func() error {
			_, err := store.Wills().Get(ctx, "client-1")
			return err
		}},
		{"ping", store.Ping},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// The point is that a closed handle reports an error rather than
			// panicking the process, as BadgerDB does on its own.
			assert.ErrorIs(t, tc.op(), storage.ErrClosed)
		})
	}

	assert.NoError(t, store.Close(), "Close must be idempotent")
}

// TestDB_CloseWaitsForInFlightOperations pins the invariant that removes the
// panic window: BadgerDB checks its close state when a transaction is created
// but panics if the handle closes before the transaction reaches
// Txn.NewIterator, so the handle must not close while an operation is running.
func TestDB_CloseWaitsForInFlightOperations(t *testing.T) {
	store, err := New(Config{Dir: t.TempDir()})
	require.NoError(t, err)

	running := make(chan struct{})
	release := make(chan struct{})
	opDone := make(chan error, 1)

	go func() {
		opDone <- store.db.View(func(txn *badger.Txn) error {
			close(running)
			<-release
			return nil
		})
	}()
	<-running

	closeDone := make(chan error, 1)
	go func() {
		closeDone <- store.Close()
	}()

	select {
	case <-closeDone:
		t.Fatal("Close returned while an operation was still in flight")
	case <-time.After(50 * time.Millisecond):
	}

	close(release)
	require.NoError(t, <-opDone)
	require.NoError(t, <-closeDone)
}

// TestStore_CloseDuringOperations covers the shutdown race that used to panic:
// the broker destroys sessions from goroutines that outlive the store, so
// operations and Close overlap. BadgerDB panics with "DB Closed" when a closed
// handle reaches Txn.NewIterator, so this must stay a plain error path.
func TestStore_CloseDuringOperations(t *testing.T) {
	store, err := New(Config{Dir: t.TempDir()})
	require.NoError(t, err)

	msg := message.NewDelivery("test/topic", []byte("payload"), 1, false)
	ctx := context.Background()

	const workers = 8

	var (
		wg    sync.WaitGroup
		start = make(chan struct{})
		errMu sync.Mutex
		errs  []error
	)

	record := func(err error) {
		if err == nil {
			return
		}
		errMu.Lock()
		errs = append(errs, err)
		errMu.Unlock()
	}

	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start

			for range 50 {
				record(store.Messages().Store("client-1/queue/0", msg))
				record(store.Messages().DeleteByPrefix("client-1/"))
				record(store.Sessions().Delete("client-1"))
				record(store.Retained().Set(ctx, "test/topic", msg))
				_, err := store.Subscriptions().Match(closeTestFilter)
				record(err)
			}
		}()
	}

	close(start)
	require.NoError(t, store.Close())
	wg.Wait()

	// Losing the race is fine; anything other than ErrClosed or a not-found is
	// not.
	//
	// A conflict is not about the close race at all. DeleteByPrefix reads the
	// prefix through an iterator before deleting what it found, so a Store that
	// commits into the same prefix meanwhile puts a key it read under a newer
	// version and badger refuses the commit. Both operations are in the loop
	// above, on the same prefix, across eight goroutines, so the contention is
	// what this test manufactures rather than anything it is asserting about.
	for _, err := range errs {
		if assert.Error(t, err) {
			assert.True(t,
				assert.ObjectsAreEqual(storage.ErrClosed, err) ||
					assert.ObjectsAreEqual(storage.ErrNotFound, err) ||
					errors.Is(err, badger.ErrConflict),
				"unexpected error during close race: %v", err)
		}
	}
}
