// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package broker

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestStatsConnections(t *testing.T) {
	s := NewStats()
	assert.Equal(t, uint64(0), s.GetCurrentConnections())
	assert.Equal(t, uint64(0), s.GetTotalConnections())

	s.IncrementConnections()
	s.IncrementConnections()
	assert.Equal(t, uint64(2), s.GetTotalConnections())
	assert.Equal(t, uint64(2), s.GetCurrentConnections())

	s.DecrementConnections()
	assert.Equal(t, uint64(2), s.GetTotalConnections())
	assert.Equal(t, uint64(1), s.GetCurrentConnections())
	assert.Equal(t, uint64(1), s.GetDisconnections())
}

func TestStatsMessages(t *testing.T) {
	s := NewStats()

	s.IncrementMessagesReceived()
	s.IncrementMessagesReceived()
	s.IncrementMessagesSent()
	assert.Equal(t, uint64(2), s.GetMessagesReceived())
	assert.Equal(t, uint64(1), s.GetMessagesSent())

	s.AddBytesReceived(1024)
	s.AddBytesSent(512)
	assert.Equal(t, uint64(1024), s.GetBytesReceived())
	assert.Equal(t, uint64(512), s.GetBytesSent())
}

func TestStatsSessionsLinksSubscriptions(t *testing.T) {
	s := NewStats()

	s.IncrementSessions()
	s.IncrementSessions()
	assert.Equal(t, uint64(2), s.GetCurrentSessions())

	s.DecrementSessions()
	assert.Equal(t, uint64(1), s.GetCurrentSessions())

	s.IncrementLinks()
	assert.Equal(t, uint64(1), s.GetCurrentLinks())
	s.DecrementLinks()
	assert.Equal(t, uint64(0), s.GetCurrentLinks())

	s.IncrementSubscriptions()
	s.IncrementSubscriptions()
	assert.Equal(t, uint64(2), s.GetSubscriptions())
	s.DecrementSubscriptions()
	assert.Equal(t, uint64(1), s.GetSubscriptions())
}

func TestStatsErrors(t *testing.T) {
	s := NewStats()

	s.IncrementAuthErrors()
	s.IncrementProtocolErrors()
	s.IncrementProtocolErrors()
	assert.Equal(t, uint64(1), s.GetAuthErrors())
	assert.Equal(t, uint64(2), s.GetProtocolErrors())
}

func TestStatsConcurrency(t *testing.T) {
	s := NewStats()
	var wg sync.WaitGroup
	n := 1000

	wg.Add(n * 2)
	for range n {
		go func() {
			defer wg.Done()
			s.IncrementConnections()
		}()
		go func() {
			defer wg.Done()
			s.IncrementMessagesReceived()
		}()
	}
	wg.Wait()

	assert.Equal(t, uint64(n), s.GetTotalConnections())
	assert.Equal(t, uint64(n), s.GetMessagesReceived())
}
