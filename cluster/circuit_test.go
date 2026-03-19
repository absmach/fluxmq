// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package cluster

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCircuitBreaker_StartsClosedAllowsTraffic(t *testing.T) {
	cb := &circuitBreaker{}

	assert.Equal(t, int32(circuitClosed), cb.state.Load())
	assert.True(t, cb.allow())
}

func TestCircuitBreaker_OpensAfterThresholdFailures(t *testing.T) {
	cb := &circuitBreaker{}

	for i := 0; i < failureThreshold; i++ {
		cb.recordFailure()
	}

	assert.Equal(t, int32(circuitOpen), cb.state.Load())
	assert.False(t, cb.allow())
}

func TestCircuitBreaker_HalfOpenAfterResetTimeout(t *testing.T) {
	cb := &circuitBreaker{}

	for i := 0; i < failureThreshold; i++ {
		cb.recordFailure()
	}
	require.Equal(t, int32(circuitOpen), cb.state.Load())

	// Set lastFailed far enough in the past to exceed resetTimeout.
	cb.lastFailed.Store(time.Now().Add(-resetTimeout - time.Second).UnixNano())

	assert.True(t, cb.allow())
	assert.Equal(t, int32(circuitHalfOpen), cb.state.Load())
}

func TestCircuitBreaker_SuccessResetsFromHalfOpen(t *testing.T) {
	cb := &circuitBreaker{}
	cb.state.Store(circuitHalfOpen)
	cb.failures.Store(failureThreshold)

	cb.recordSuccess()

	assert.Equal(t, int32(circuitClosed), cb.state.Load())
	assert.Equal(t, int32(0), cb.failures.Load())
}

func TestCircuitBreaker_FailureInHalfOpenReopens(t *testing.T) {
	cb := &circuitBreaker{}
	cb.state.Store(circuitHalfOpen)
	// Set failures just below threshold so one more triggers open.
	cb.failures.Store(failureThreshold - 1)

	cb.recordFailure()

	assert.Equal(t, int32(circuitOpen), cb.state.Load())
}

func TestRetryWithBreaker_RetriesThenSucceeds(t *testing.T) {
	breakers := newPeerBreakers()
	var calls atomic.Int32

	fn := func() error {
		if calls.Add(1) < 3 {
			return errors.New("transient")
		}
		return nil
	}

	err := retryWithBreaker(context.Background(), breakers, "node-1", fn)

	assert.NoError(t, err)
	assert.Equal(t, int32(3), calls.Load())
}

func TestRetryWithBreaker_ExhaustsRetriesTripsCircuit(t *testing.T) {
	breakers := newPeerBreakers()
	permanent := errors.New("permanent")

	fn := func() error { return permanent }

	err := retryWithBreaker(context.Background(), breakers, "node-1", fn)
	require.Error(t, err)
	assert.Equal(t, permanent, err)

	// Circuit should now be open — next call should fail immediately.
	err = retryWithBreaker(context.Background(), breakers, "node-1", fn)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "circuit open")
}

func TestRetryWithBreaker_RespectsContextCancellation(t *testing.T) {
	breakers := newPeerBreakers()
	ctx, cancel := context.WithCancel(context.Background())

	fn := func() error {
		cancel()
		return errors.New("fail")
	}

	err := retryWithBreaker(ctx, breakers, "node-1", fn)

	assert.ErrorIs(t, err, context.Canceled)
}

func TestPeerBreakers_GetOrCreate(t *testing.T) {
	pb := newPeerBreakers()

	cb1 := pb.get("node-a")
	cb2 := pb.get("node-a")
	cb3 := pb.get("node-b")

	assert.Same(t, cb1, cb2, "same ID should return same breaker")
	assert.NotSame(t, cb1, cb3, "different ID should return different breaker")
}
