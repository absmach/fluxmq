// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package client

import "sync/atomic"

// State represents the client connection state.
type State uint32

// Client states.
const (
	StateDisconnected State = iota
	StateConnecting
	StateConnected
	StateReconnecting
	StateDisconnecting
	StateClosed
)

// String returns the state name.
func (s State) String() string {
	switch s {
	case StateDisconnected:
		return "disconnected"
	case StateConnecting:
		return "connecting"
	case StateConnected:
		return "connected"
	case StateReconnecting:
		return "reconnecting"
	case StateDisconnecting:
		return "disconnecting"
	case StateClosed:
		return "closed"
	default:
		return "unknown"
	}
}

// stateManager handles atomic state transitions.
type stateManager struct {
	state uint32
}

// newStateManager creates a new state manager.
func newStateManager() *stateManager {
	return &stateManager{state: uint32(StateDisconnected)}
}

// get returns the current state.
func (sm *stateManager) get() State {
	return State(atomic.LoadUint32(&sm.state))
}

// set unconditionally sets the state.
func (sm *stateManager) set(s State) {
	atomic.StoreUint32(&sm.state, uint32(s))
}

// transition attempts to transition from expected to new state.
// Returns true if successful.
func (sm *stateManager) transition(from, to State) bool {
	return atomic.CompareAndSwapUint32(&sm.state, uint32(from), uint32(to))
}

// transitionFrom attempts to transition from any of the expected states.
// Returns true if successful.
func (sm *stateManager) transitionFrom(to State, from ...State) bool {
	for _, f := range from {
		if sm.transition(f, to) {
			return true
		}
	}
	return false
}

// isConnected returns true if the client is connected.
func (sm *stateManager) isConnected() bool {
	return sm.get() == StateConnected
}

// canConnect returns true if a connection attempt is allowed.
func (sm *stateManager) canConnect() bool {
	s := sm.get()
	return s == StateDisconnected || s == StateReconnecting
}

// isClosed returns true if the client has been permanently closed.
func (sm *stateManager) isClosed() bool {
	return sm.get() == StateClosed
}
