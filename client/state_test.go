// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package client

import (
	"sync"
	"testing"
)

func TestStateString(t *testing.T) {
	tests := []struct {
		state State
		want  string
	}{
		{StateDisconnected, "disconnected"},
		{StateConnecting, "connecting"},
		{StateConnected, "connected"},
		{StateReconnecting, "reconnecting"},
		{StateDisconnecting, "disconnecting"},
		{StateClosed, "closed"},
		{State(99), "unknown"},
	}

	for _, tt := range tests {
		got := tt.state.String()
		if got != tt.want {
			t.Errorf("State(%d).String() = %s, want %s", tt.state, got, tt.want)
		}
	}
}

func TestStateManager(t *testing.T) {
	sm := newStateManager()

	if sm.get() != StateDisconnected {
		t.Errorf("initial state should be Disconnected, got %v", sm.get())
	}

	sm.set(StateConnected)
	if sm.get() != StateConnected {
		t.Errorf("state should be Connected after set, got %v", sm.get())
	}
}

func TestStateTransition(t *testing.T) {
	sm := newStateManager()

	// Transition from Disconnected to Connecting should succeed
	if !sm.transition(StateDisconnected, StateConnecting) {
		t.Error("transition Disconnected -> Connecting should succeed")
	}
	if sm.get() != StateConnecting {
		t.Errorf("state should be Connecting, got %v", sm.get())
	}

	// Transition from wrong state should fail
	if sm.transition(StateDisconnected, StateConnected) {
		t.Error("transition from wrong state should fail")
	}
	if sm.get() != StateConnecting {
		t.Errorf("state should still be Connecting, got %v", sm.get())
	}
}

func TestStateTransitionFrom(t *testing.T) {
	sm := newStateManager()

	// transitionFrom with matching state should succeed
	if !sm.transitionFrom(StateConnecting, StateDisconnected, StateReconnecting) {
		t.Error("transitionFrom should succeed when current state matches one of the from states")
	}
	if sm.get() != StateConnecting {
		t.Errorf("state should be Connecting, got %v", sm.get())
	}

	// transitionFrom with non-matching states should fail
	sm.set(StateClosed)
	if sm.transitionFrom(StateConnecting, StateDisconnected, StateReconnecting) {
		t.Error("transitionFrom should fail when current state doesn't match any from states")
	}
}

func TestStateHelpers(t *testing.T) {
	sm := newStateManager()

	// isConnected
	if sm.isConnected() {
		t.Error("isConnected should be false initially")
	}
	sm.set(StateConnected)
	if !sm.isConnected() {
		t.Error("isConnected should be true when connected")
	}

	// canConnect
	sm.set(StateDisconnected)
	if !sm.canConnect() {
		t.Error("canConnect should be true when disconnected")
	}
	sm.set(StateReconnecting)
	if !sm.canConnect() {
		t.Error("canConnect should be true when reconnecting")
	}
	sm.set(StateConnected)
	if sm.canConnect() {
		t.Error("canConnect should be false when connected")
	}

	// isClosed
	if sm.isClosed() {
		t.Error("isClosed should be false initially")
	}
	sm.set(StateClosed)
	if !sm.isClosed() {
		t.Error("isClosed should be true when closed")
	}
}

func TestStateConcurrency(t *testing.T) {
	sm := newStateManager()
	var wg sync.WaitGroup

	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if i%2 == 0 {
				sm.set(StateConnected)
			} else {
				sm.set(StateDisconnected)
			}
			sm.get()
			sm.isConnected()
			sm.canConnect()
			sm.isClosed()
		}(i)
	}

	wg.Wait()
}
