// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package broker

import (
	"sync/atomic"
	"time"
)

// Stats tracks AMQP broker statistics using atomic counters.
type Stats struct {
	startTime time.Time

	totalConnections   atomic.Uint64
	currentConnections atomic.Uint64
	disconnections     atomic.Uint64

	messagesReceived atomic.Uint64
	messagesSent     atomic.Uint64

	bytesReceived atomic.Uint64
	bytesSent     atomic.Uint64

	currentSessions atomic.Uint64
	currentLinks    atomic.Uint64
	subscriptions   atomic.Uint64

	authErrors     atomic.Uint64
	protocolErrors atomic.Uint64
}

// NewStats creates a new Stats instance.
func NewStats() *Stats {
	return &Stats{
		startTime: time.Now(),
	}
}

func (s *Stats) IncrementConnections() {
	s.totalConnections.Add(1)
	s.currentConnections.Add(1)
}

func (s *Stats) DecrementConnections() {
	s.currentConnections.Add(^uint64(0))
	s.disconnections.Add(1)
}

func (s *Stats) IncrementMessagesReceived() {
	s.messagesReceived.Add(1)
}

func (s *Stats) IncrementMessagesSent() {
	s.messagesSent.Add(1)
}

func (s *Stats) AddBytesReceived(n uint64) {
	s.bytesReceived.Add(n)
}

func (s *Stats) AddBytesSent(n uint64) {
	s.bytesSent.Add(n)
}

func (s *Stats) IncrementSessions() {
	s.currentSessions.Add(1)
}

func (s *Stats) DecrementSessions() {
	s.currentSessions.Add(^uint64(0))
}

func (s *Stats) IncrementLinks() {
	s.currentLinks.Add(1)
}

func (s *Stats) DecrementLinks() {
	s.currentLinks.Add(^uint64(0))
}

func (s *Stats) IncrementSubscriptions() {
	s.subscriptions.Add(1)
}

func (s *Stats) DecrementSubscriptions() {
	s.subscriptions.Add(^uint64(0))
}

func (s *Stats) IncrementAuthErrors() {
	s.authErrors.Add(1)
}

func (s *Stats) IncrementProtocolErrors() {
	s.protocolErrors.Add(1)
}

func (s *Stats) GetTotalConnections() uint64   { return s.totalConnections.Load() }
func (s *Stats) GetCurrentConnections() uint64 { return s.currentConnections.Load() }
func (s *Stats) GetDisconnections() uint64     { return s.disconnections.Load() }
func (s *Stats) GetMessagesReceived() uint64   { return s.messagesReceived.Load() }
func (s *Stats) GetMessagesSent() uint64       { return s.messagesSent.Load() }
func (s *Stats) GetBytesReceived() uint64      { return s.bytesReceived.Load() }
func (s *Stats) GetBytesSent() uint64          { return s.bytesSent.Load() }
func (s *Stats) GetCurrentSessions() uint64    { return s.currentSessions.Load() }
func (s *Stats) GetCurrentLinks() uint64       { return s.currentLinks.Load() }
func (s *Stats) GetSubscriptions() uint64      { return s.subscriptions.Load() }
func (s *Stats) GetAuthErrors() uint64         { return s.authErrors.Load() }
func (s *Stats) GetProtocolErrors() uint64     { return s.protocolErrors.Load() }
func (s *Stats) GetUptime() time.Duration      { return time.Since(s.startTime) }
