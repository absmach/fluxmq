// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package client

import (
	"sync"
	"time"
)

// pendingType identifies the type of pending operation.
type pendingType int

const (
	pendingPublish pendingType = iota
	pendingSubscribe
	pendingUnsubscribe
)

// pendingOp represents a pending operation waiting for acknowledgment.
type pendingOp struct {
	id       uint16
	opType   pendingType
	done     chan struct{}
	err      error
	result   interface{} // For SUBACK return codes, etc.
	created  time.Time
	message  *Message // For QoS 1/2 retransmission
	qos2State int     // 0: waiting PUBREC, 1: waiting PUBCOMP
}

// pendingStore manages pending operations.
type pendingStore struct {
	mu       sync.RWMutex
	pending  map[uint16]*pendingOp
	nextID   uint16
	maxSize  int
	inflight int
}

// newPendingStore creates a new pending operation store.
func newPendingStore(maxSize int) *pendingStore {
	return &pendingStore{
		pending: make(map[uint16]*pendingOp),
		nextID:  1,
		maxSize: maxSize,
	}
}

// nextPacketID returns the next available packet ID.
func (ps *pendingStore) nextPacketID() uint16 {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	// Find an unused ID
	startID := ps.nextID
	for {
		id := ps.nextID
		ps.nextID++
		if ps.nextID == 0 {
			ps.nextID = 1
		}

		if _, exists := ps.pending[id]; !exists {
			return id
		}

		// Wrapped around without finding free ID
		if ps.nextID == startID {
			return 0 // No available ID
		}
	}
}

// add registers a new pending operation.
func (ps *pendingStore) add(id uint16, opType pendingType, msg *Message) (*pendingOp, error) {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	if ps.inflight >= ps.maxSize {
		return nil, ErrMaxInflight
	}

	op := &pendingOp{
		id:      id,
		opType:  opType,
		done:    make(chan struct{}),
		created: time.Now(),
		message: msg,
	}

	ps.pending[id] = op
	ps.inflight++
	return op, nil
}

// get retrieves a pending operation by ID.
func (ps *pendingStore) get(id uint16) *pendingOp {
	ps.mu.RLock()
	defer ps.mu.RUnlock()
	return ps.pending[id]
}

// complete marks a pending operation as complete.
func (ps *pendingStore) complete(id uint16, err error, result interface{}) bool {
	ps.mu.Lock()
	op, exists := ps.pending[id]
	if exists {
		delete(ps.pending, id)
		ps.inflight--
	}
	ps.mu.Unlock()

	if exists && op != nil {
		op.err = err
		op.result = result
		close(op.done)
		return true
	}
	return false
}

// remove removes a pending operation without completing it.
func (ps *pendingStore) remove(id uint16) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	if _, exists := ps.pending[id]; exists {
		delete(ps.pending, id)
		ps.inflight--
	}
}

// updateQoS2State updates the QoS 2 state for a pending publish.
func (ps *pendingStore) updateQoS2State(id uint16, state int) bool {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	if op, exists := ps.pending[id]; exists {
		op.qos2State = state
		return true
	}
	return false
}

// getAll returns all pending operations (for cleanup/reconnect).
func (ps *pendingStore) getAll() []*pendingOp {
	ps.mu.RLock()
	defer ps.mu.RUnlock()

	ops := make([]*pendingOp, 0, len(ps.pending))
	for _, op := range ps.pending {
		ops = append(ops, op)
	}
	return ops
}

// clear removes all pending operations and signals them as failed.
func (ps *pendingStore) clear(err error) {
	ps.mu.Lock()
	pending := ps.pending
	ps.pending = make(map[uint16]*pendingOp)
	ps.inflight = 0
	ps.mu.Unlock()

	for _, op := range pending {
		op.err = err
		close(op.done)
	}
}

// count returns the number of inflight operations.
func (ps *pendingStore) count() int {
	ps.mu.RLock()
	defer ps.mu.RUnlock()
	return ps.inflight
}

// wait waits for a pending operation to complete with timeout.
func (op *pendingOp) wait(timeout time.Duration) error {
	select {
	case <-op.done:
		return op.err
	case <-time.After(timeout):
		return ErrTimeout
	}
}
