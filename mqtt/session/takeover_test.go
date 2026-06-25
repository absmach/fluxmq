// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package session

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/absmach/fluxmq/config"
	"github.com/absmach/fluxmq/storage/messages"
	"github.com/stretchr/testify/require"
)

// recordingConn is a testConn that records whether it was closed and exposes
// the onDisconnect callback the session installed on it.
type recordingConn struct {
	testConn
	closed atomic.Bool
}

func (c *recordingConn) Close() error {
	c.closed.Store(true)
	return nil
}

// wsLikeConn models a WebSocket connection whose Close() synchronously invokes
// the onDisconnect callback (as server/websocket does), which calls back into
// the session. Used to guard against re-entrant lock deadlock on takeover.
type wsLikeConn struct {
	testConn
	closed atomic.Bool
}

func (c *wsLikeConn) Close() error {
	if c.closed.Swap(true) {
		return nil
	}
	if c.onDisconnect != nil {
		c.onDisconnect(false) // synchronous, like the WebSocket transport
	}
	return nil
}

func newTakeoverSession(t *testing.T) *Session {
	t.Helper()
	return New(
		"taker",
		4,
		Options{CleanStart: false, ReceiveMaximum: 16},
		messages.NewInflightTracker(16),
		messages.NewMessageQueue(16, true),
		config.SessionConfig{},
	)
}

// TestConnect_LocalTakeoverClosesOldConn verifies that attaching a new
// connection to a session that already has one closes the old connection
// (MQTT-3.1.4-2) and advances the epoch.
func TestConnect_LocalTakeoverClosesOldConn(t *testing.T) {
	s := newTakeoverSession(t)

	oldConn := &recordingConn{}
	oldEpoch, err := s.Connect(oldConn)
	require.NoError(t, err)
	require.False(t, oldConn.closed.Load(), "old conn must be open after first Connect")

	newConn := &recordingConn{}
	newEpoch, err := s.Connect(newConn)
	require.NoError(t, err)

	require.True(t, oldConn.closed.Load(), "takeover must close the superseded connection")
	require.False(t, newConn.closed.Load(), "new conn must remain open")
	require.Greater(t, newEpoch, oldEpoch, "epoch must advance on takeover")
	require.True(t, s.IsConnected())
	got, ok := s.Conn().(*recordingConn)
	require.True(t, ok)
	require.Same(t, newConn, got)
}

// TestDisconnectIf_StaleEpochIsNoOp is the core race guard. After a local
// takeover, a superseded runSession goroutine calling DisconnectIf with its
// old epoch must NOT tear down the session — otherwise it would kill the new
// connection. This reproduces the high-latency reconnect failure from
// propeller#241.
func TestDisconnectIf_StaleEpochIsNoOp(t *testing.T) {
	s := newTakeoverSession(t)

	oldConn := &recordingConn{}
	oldEpoch, err := s.Connect(oldConn)
	require.NoError(t, err)

	newConn := &recordingConn{}
	newEpoch, err := s.Connect(newConn)
	require.NoError(t, err)

	// Stale goroutine from the old connection tears down with its old epoch.
	require.NoError(t, s.DisconnectIf(false, oldEpoch))

	// New connection must be untouched and the session still connected.
	require.True(t, s.IsConnected(), "stale epoch teardown must not disconnect the session")
	require.False(t, newConn.closed.Load(), "stale epoch teardown must not close the new conn")

	// The current epoch's teardown does disconnect.
	require.NoError(t, s.DisconnectIf(false, newEpoch))
	require.False(t, s.IsConnected())
	require.True(t, newConn.closed.Load())
}

// TestConnect_OnDisconnectCallbackScopedToEpoch verifies the conn-level
// onDisconnect callback installed by Connect is scoped to its epoch, so the
// old socket dying after takeover cannot disconnect the new connection.
func TestConnect_OnDisconnectCallbackScopedToEpoch(t *testing.T) {
	s := newTakeoverSession(t)

	oldConn := &recordingConn{}
	_, err := s.Connect(oldConn)
	require.NoError(t, err)
	oldCallback := oldConn.onDisconnect
	require.NotNil(t, oldCallback)

	newConn := &recordingConn{}
	_, err = s.Connect(newConn)
	require.NoError(t, err)

	// Old socket's death fires the old callback. It must be a no-op.
	oldCallback(false)

	require.True(t, s.IsConnected(), "old conn callback must not disconnect after takeover")
	require.False(t, newConn.closed.Load())
}

// TestConnect_WebSocketStyleSyncOnDisconnectNoDeadlock verifies that a takeover
// of a connection whose Close() synchronously calls onDisconnect (WebSocket)
// does not deadlock by re-entering the session lock.
func TestConnect_WebSocketStyleSyncOnDisconnectNoDeadlock(t *testing.T) {
	s := newTakeoverSession(t)

	oldConn := &wsLikeConn{}
	_, err := s.Connect(oldConn)
	require.NoError(t, err)

	newConn := &wsLikeConn{}
	done := make(chan struct{})
	go func() {
		_, err := s.Connect(newConn) // would deadlock if Close re-entered s.mu
		require.NoError(t, err)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Connect deadlocked on WebSocket-style synchronous onDisconnect")
	}

	require.True(t, oldConn.closed.Load(), "old ws conn must be closed by takeover")
	require.True(t, s.IsConnected())
	require.False(t, newConn.closed.Load())
}

// TestConnect_ConcurrentReconnectKeepsLatest stress-checks that under many
// concurrent reconnects exactly one connection survives and the session is
// left in a consistent connected state.
func TestConnect_ConcurrentReconnectKeepsLatest(t *testing.T) {
	s := newTakeoverSession(t)

	const n = 64
	conns := make([]*recordingConn, n)
	errs := make([]error, n)
	var wg sync.WaitGroup

	for i := range n {
		conns[i] = &recordingConn{}
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, errs[i] = s.Connect(conns[i])
		}(i)
	}
	wg.Wait()
	require.NoError(t, errors.Join(errs...))

	// Exactly one conn (the highest epoch winner) should be open and current.
	openCount := 0
	for _, c := range conns {
		if !c.closed.Load() {
			openCount++
		}
	}
	require.Equal(t, 1, openCount, "exactly one connection must survive concurrent takeover")
	require.True(t, s.IsConnected())
}
