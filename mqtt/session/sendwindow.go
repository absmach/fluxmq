// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package session

import "sync"

// sendWindow is a connection-generation send quota for outbound QoS 1/2 PUBLISH
// packets. Its capacity is the negotiated Receive Maximum, already bounded by
// the configured server limit. A token is consumed for every outbound
// transmission of a packet ID — a new send or a retransmission — that does not
// already hold one on the current connection, and released when the packet is
// acknowledged (PUBACK/PUBCOMP). The window is reset on each (re)connect, which
// clears the per-connection holds and refills the quota. [MQTT-4.9]
//
// It is independent of the persistent, bidirectional inflight store: Receive
// Maximum bounds only outbound QoS 1/2 flow, so inbound QoS 2 traffic must not
// consume it.
//
// Safe for concurrent use.
type sendWindow struct {
	mu       sync.Mutex
	capacity int
	held     map[uint16]struct{} // packet IDs holding a token on this connection
	waiters  int                 // senders currently blocked in acquire
	ready    chan struct{}       // closed to wake blocked senders; recreated per broadcast
	blocking bool                // backpressure mode: acquire blocks until a token frees
}

func newSendWindow(capacity int, blocking bool) *sendWindow {
	if capacity < 1 {
		capacity = 1
	}
	return &sendWindow{
		capacity: capacity,
		held:     make(map[uint16]struct{}),
		ready:    make(chan struct{}),
		blocking: blocking,
	}
}

// broadcastLocked wakes blocked senders. It allocates a channel, so callers only
// invoke it when waiters > 0, keeping the uncontended acquire/release path
// allocation-free. Caller must hold w.mu.
func (w *sendWindow) broadcastLocked() {
	close(w.ready)
	w.ready = make(chan struct{})
}

// reset reinitialises the window to capacity, clearing the per-connection holds.
// Called on (re)connect so the quota is the new connection's Receive Maximum.
func (w *sendWindow) reset(capacity int) {
	if capacity < 1 {
		capacity = 1
	}
	w.mu.Lock()
	w.capacity = capacity
	w.held = make(map[uint16]struct{})
	if w.waiters > 0 {
		w.broadcastLocked()
	}
	w.mu.Unlock()
}

// tryAcquire consumes a token for packetID without blocking. A packet that
// already holds a token (a retransmission on this connection) succeeds without
// consuming another. Returns false if no token is available.
func (w *sendWindow) tryAcquire(packetID uint16) bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	if _, ok := w.held[packetID]; ok {
		return true
	}
	if len(w.held) >= w.capacity {
		return false
	}
	w.held[packetID] = struct{}{}
	return true
}

// acquire blocks until a token is available for packetID or stop is closed.
// Returns false only if stop fired first.
func (w *sendWindow) acquire(packetID uint16, stop <-chan struct{}) bool {
	for {
		w.mu.Lock()
		if _, ok := w.held[packetID]; ok {
			w.mu.Unlock()
			return true
		}
		if len(w.held) < w.capacity {
			w.held[packetID] = struct{}{}
			w.mu.Unlock()
			return true
		}
		w.waiters++
		ready := w.ready
		w.mu.Unlock()

		select {
		case <-ready:
		case <-stop:
			w.mu.Lock()
			w.waiters--
			w.mu.Unlock()
			return false
		}

		w.mu.Lock()
		w.waiters--
		w.mu.Unlock()
	}
}

// release returns the token held for packetID, if any, waking a blocked sender.
// It only allocates (to wake waiters) when a sender is actually blocked, so the
// uncontended PUBACK/PUBCOMP path stays allocation-free.
func (w *sendWindow) release(packetID uint16) {
	w.mu.Lock()
	if _, ok := w.held[packetID]; ok {
		delete(w.held, packetID)
		if w.waiters > 0 {
			w.broadcastLocked()
		}
	}
	w.mu.Unlock()
}
