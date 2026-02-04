// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package broker

import (
	"net"
	"sync"
	"testing"

	amqpconn "github.com/absmach/fluxmq/amqp1"
	"github.com/absmach/fluxmq/amqp1/performatives"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestSession(t *testing.T) *Session {
	t.Helper()
	b := New(nil, nil, nil)
	server, client := net.Pipe()
	done := make(chan struct{})
	go func() {
		defer close(done)
		buf := make([]byte, 4096)
		for {
			if _, err := client.Read(buf); err != nil {
				return
			}
		}
	}()
	t.Cleanup(func() {
		server.Close()
		client.Close()
		<-done
	})
	c := &Connection{
		broker:  b,
		conn:    amqpconn.NewConnection(server),
		closeCh: make(chan struct{}),
		logger:  b.logger,
	}
	return newSession(c, 0, 0, 255)
}

func TestSessionInitWindows(t *testing.T) {
	s := newTestSession(t)

	begin := &performatives.Begin{
		NextOutgoingID: 42,
		IncomingWindow: 1000,
		OutgoingWindow: 2000,
	}
	s.initWindows(begin)

	assert.Equal(t, uint32(42), s.nextIncomingID)
	assert.Equal(t, uint32(1000), s.remoteIncomingWindow)
	assert.Equal(t, uint32(2000), s.remoteOutgoingWindow)
	assert.Equal(t, initialWindow, s.incomingWindow)
	assert.Equal(t, initialWindow, s.outgoingWindow)
}

func TestConsumeOutgoingWindow(t *testing.T) {
	s := newTestSession(t)
	s.initWindows(&performatives.Begin{
		NextOutgoingID: 0,
		IncomingWindow: 100,
		OutgoingWindow: 100,
	})

	id0, ok := s.consumeOutgoingWindow()
	require.True(t, ok)
	assert.Equal(t, uint32(0), id0)

	id1, ok := s.consumeOutgoingWindow()
	require.True(t, ok)
	assert.Equal(t, uint32(1), id1)
}

func TestConsumeOutgoingWindowExhaustion(t *testing.T) {
	s := newTestSession(t)
	s.initWindows(&performatives.Begin{
		NextOutgoingID: 0,
		IncomingWindow: 2,
		OutgoingWindow: 100,
	})

	_, ok := s.consumeOutgoingWindow()
	require.True(t, ok)
	_, ok = s.consumeOutgoingWindow()
	require.True(t, ok)

	// Third should fail — remote incoming window exhausted
	_, ok = s.consumeOutgoingWindow()
	assert.False(t, ok)
}

func TestConsumeOutgoingWindowOwnExhaustion(t *testing.T) {
	s := newTestSession(t)
	s.initWindows(&performatives.Begin{
		NextOutgoingID: 0,
		IncomingWindow: 100000,
		OutgoingWindow: 100000,
	})

	s.windowMu.Lock()
	s.outgoingWindow = 1
	s.windowMu.Unlock()

	_, ok := s.consumeOutgoingWindow()
	require.True(t, ok)

	_, ok = s.consumeOutgoingWindow()
	assert.False(t, ok)
}

func TestTrackIncomingTransfer(t *testing.T) {
	s := newTestSession(t)

	id := uint32(0)
	ok := s.trackIncomingTransfer(&id)
	assert.True(t, ok)
	assert.Equal(t, uint32(1), s.nextIncomingID)
	assert.Equal(t, initialWindow-1, s.incomingWindow)
}

func TestTrackIncomingTransferWindowExhausted(t *testing.T) {
	s := newTestSession(t)

	s.windowMu.Lock()
	s.incomingWindow = 0
	s.windowMu.Unlock()

	id := uint32(0)
	ok := s.trackIncomingTransfer(&id)
	assert.False(t, ok)
}

func TestTrackIncomingTransferReplenish(t *testing.T) {
	s := newTestSession(t)

	s.windowMu.Lock()
	s.incomingWindow = windowReplenishThreshold
	s.windowMu.Unlock()

	id := uint32(0)
	ok := s.trackIncomingTransfer(&id)
	assert.True(t, ok)

	// After replenish, window should be back to initialWindow
	s.windowMu.Lock()
	assert.Equal(t, initialWindow, s.incomingWindow)
	s.windowMu.Unlock()
}

func TestSessionFlowState(t *testing.T) {
	s := newTestSession(t)
	s.initWindows(&performatives.Begin{
		NextOutgoingID: 10,
		IncomingWindow: 500,
		OutgoingWindow: 600,
	})

	nextIn, inWin, nextOut, outWin := s.sessionFlowState()
	assert.Equal(t, uint32(10), nextIn)
	assert.Equal(t, initialWindow, inWin)
	assert.Equal(t, uint32(0), nextOut)
	assert.Equal(t, initialWindow, outWin)
}

func TestConsumeOutgoingWindowConcurrent(t *testing.T) {
	s := newTestSession(t)
	s.initWindows(&performatives.Begin{
		NextOutgoingID: 0,
		IncomingWindow: 1000,
		OutgoingWindow: 1000,
	})

	var wg sync.WaitGroup
	successes := make(chan uint32, 1000)

	for range 1000 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if id, ok := s.consumeOutgoingWindow(); ok {
				successes <- id
			}
		}()
	}

	wg.Wait()
	close(successes)

	ids := make(map[uint32]bool)
	for id := range successes {
		assert.False(t, ids[id], "duplicate delivery ID: %d", id)
		ids[id] = true
	}
	assert.Len(t, ids, 1000)
}
