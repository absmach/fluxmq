// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package session

import "sync"

// sendWindow is a connection-generation send quota for outbound QoS 1/2 PUBLISH
// packets. Its capacity is the negotiated Receive Maximum, already bounded by
// the configured server limit. A token is consumed for every outbound
// transmission of a packet ID — a new send or a retransmission — that does not
// already hold one for the current generation, and released when the packet is
// acknowledged (PUBACK/PUBCOMP). [MQTT-4.9]
//
// Tokens are bound to a generation (the connection epoch). reset, called on each
// (re)connect, advances the generation, drops the previous generation's holds,
// and refills the quota. Acquire and release carry the caller's generation, so a
// delivery or retry left over from a superseded connection can neither consume
// nor free the replacement generation's quota.
//
// It is independent of the persistent, bidirectional inflight store: Receive
// Maximum bounds only outbound QoS 1/2 flow, so inbound QoS 2 traffic must not
// consume it.
//
// Safe for concurrent use.
type sendWindow struct {
	mu       sync.Mutex
	capacity int
	gen      uint64              // current generation (connection epoch)
	held     map[uint16]struct{} // packet IDs holding a token in the current generation
	waiters  int                 // senders currently blocked in acquire
	ready    chan struct{}       // closed to wake blocked senders; recreated per broadcast
	blocking bool                // backpressure mode: acquire blocks until a token frees
}

func newSendWindow(capacity int, gen uint64, blocking bool) *sendWindow {
	if capacity < 1 {
		capacity = 1
	}
	return &sendWindow{
		capacity: capacity,
		gen:      gen,
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

// reset reinitialises the window to capacity for the given generation, dropping
// the previous generation's holds. Called on (re)connect.
func (w *sendWindow) reset(capacity int, gen uint64) {
	if capacity < 1 {
		capacity = 1
	}
	w.mu.Lock()
	w.capacity = capacity
	w.gen = gen
	w.held = make(map[uint16]struct{})
	if w.waiters > 0 {
		w.broadcastLocked()
	}
	w.mu.Unlock()
}

// tryAcquire consumes a token for packetID in generation gen without blocking.
// It returns false if gen is no longer current (the connection was superseded)
// or no token is available. A packet that already holds a token in the current
// generation (a retransmission) succeeds without consuming another.
func (w *sendWindow) tryAcquire(packetID uint16, gen uint64) bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.tryAcquireLocked(packetID, gen)
}

func (w *sendWindow) tryAcquireLocked(packetID uint16, gen uint64) bool {
	if gen != w.gen {
		return false
	}
	if _, ok := w.held[packetID]; ok {
		return true
	}
	if len(w.held) >= w.capacity {
		return false
	}
	w.held[packetID] = struct{}{}
	return true
}

// acquire blocks until a token is available for packetID in generation gen, the
// generation is superseded, or stop is closed. Returns false if gen is no longer
// current or stop fired first.
func (w *sendWindow) acquire(packetID uint16, gen uint64, stop <-chan struct{}) bool {
	for {
		w.mu.Lock()
		if gen != w.gen {
			w.mu.Unlock()
			return false
		}
		if w.tryAcquireLocked(packetID, gen) {
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

// release returns the token held for packetID in generation gen, if any, waking
// a blocked sender. A release carrying a superseded generation is a no-op, so a
// leftover delivery cannot free the replacement generation's quota. It only
// allocates (to wake waiters) when a sender is actually blocked, keeping the
// uncontended PUBACK/PUBCOMP path allocation-free.
func (w *sendWindow) release(packetID uint16, gen uint64) {
	w.mu.Lock()
	if gen == w.gen {
		if _, ok := w.held[packetID]; ok {
			delete(w.held, packetID)
			if w.waiters > 0 {
				w.broadcastLocked()
			}
		}
	}
	w.mu.Unlock()
}
