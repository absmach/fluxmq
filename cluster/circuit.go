// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package cluster

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

const (
	circuitClosed   = 0
	circuitOpen     = 1
	circuitHalfOpen = 2

	failureThreshold = 5
	resetTimeout     = 10 * time.Second
	maxRetries        = 5
	retryBaseDelay    = 250 * time.Millisecond
	maxPartialRetries = 5
)

// circuitBreaker implements a simple per-peer circuit breaker.
type circuitBreaker struct {
	state      atomic.Int32
	failures   atomic.Int32
	lastFailed atomic.Int64 // unix nano
}

func (cb *circuitBreaker) allow() bool {
	switch cb.state.Load() {
	case circuitOpen:
		if time.Since(time.Unix(0, cb.lastFailed.Load())) > resetTimeout {
			cb.state.CompareAndSwap(circuitOpen, circuitHalfOpen)
			return true
		}
		return false
	default:
		return true
	}
}

func (cb *circuitBreaker) recordSuccess() {
	cb.failures.Store(0)
	cb.state.Store(circuitClosed)
}

func (cb *circuitBreaker) recordFailure() {
	cb.lastFailed.Store(time.Now().UnixNano())
	if cb.failures.Add(1) >= failureThreshold {
		cb.state.Store(circuitOpen)
	}
}

// peerBreakers manages circuit breakers per peer node.
type peerBreakers struct {
	mu       sync.RWMutex
	breakers map[string]*circuitBreaker
}

func newPeerBreakers() *peerBreakers {
	return &peerBreakers{
		breakers: make(map[string]*circuitBreaker),
	}
}

func (pb *peerBreakers) get(nodeID string) *circuitBreaker {
	pb.mu.RLock()
	cb, ok := pb.breakers[nodeID]
	pb.mu.RUnlock()
	if ok {
		return cb
	}

	pb.mu.Lock()
	defer pb.mu.Unlock()
	if cb, ok = pb.breakers[nodeID]; ok {
		return cb
	}
	cb = &circuitBreaker{}
	pb.breakers[nodeID] = cb
	return cb
}

// retryWithBreaker retries fn up to maxRetries times with exponential backoff,
// respecting the circuit breaker for the given peer.
func retryWithBreaker(ctx context.Context, breakers *peerBreakers, nodeID string, fn func() error) error {
	cb := breakers.get(nodeID)

	var lastErr error
	for attempt := range maxRetries {
		if !cb.allow() {
			return fmt.Errorf("circuit open for peer %s", nodeID)
		}

		lastErr = fn()
		if lastErr == nil {
			cb.recordSuccess()
			return nil
		}

		cb.recordFailure()

		if attempt < maxRetries-1 {
			delay := retryBaseDelay << attempt
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(delay):
			}
		}
	}
	return lastErr
}
