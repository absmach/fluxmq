// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package broker

import (
	"sync/atomic"
	"time"
)

// Stats tracks AMQP 0.9.1 broker statistics using atomic counters.
type Stats struct {
	startTime time.Time

	totalConnections   atomic.Uint64
	currentConnections atomic.Uint64
	disconnections     atomic.Uint64

	messagesReceived atomic.Uint64
	messagesSent     atomic.Uint64

	bytesReceived atomic.Uint64
	bytesSent     atomic.Uint64

	currentChannels atomic.Uint64
	consumers       atomic.Uint64

	protocolErrors atomic.Uint64
}

func NewStats() *Stats {
	return &Stats{startTime: time.Now()}
}

func (s *Stats) IncrementConnections() {
	s.totalConnections.Add(1)
	s.currentConnections.Add(1)
}

func (s *Stats) DecrementConnections() {
	s.currentConnections.Add(^uint64(0))
	s.disconnections.Add(1)
}

func (s *Stats) IncrementMessagesReceived() { s.messagesReceived.Add(1) }
func (s *Stats) IncrementMessagesSent()     { s.messagesSent.Add(1) }
func (s *Stats) AddBytesReceived(n uint64)  { s.bytesReceived.Add(n) }
func (s *Stats) AddBytesSent(n uint64)      { s.bytesSent.Add(n) }
func (s *Stats) IncrementChannels()         { s.currentChannels.Add(1) }
func (s *Stats) DecrementChannels()         { s.currentChannels.Add(^uint64(0)) }
func (s *Stats) IncrementConsumers()        { s.consumers.Add(1) }
func (s *Stats) DecrementConsumers()        { s.consumers.Add(^uint64(0)) }
func (s *Stats) IncrementProtocolErrors()   { s.protocolErrors.Add(1) }

func (s *Stats) GetTotalConnections() uint64   { return s.totalConnections.Load() }
func (s *Stats) GetCurrentConnections() uint64 { return s.currentConnections.Load() }
func (s *Stats) GetDisconnections() uint64     { return s.disconnections.Load() }
func (s *Stats) GetMessagesReceived() uint64   { return s.messagesReceived.Load() }
func (s *Stats) GetMessagesSent() uint64       { return s.messagesSent.Load() }
func (s *Stats) GetBytesReceived() uint64      { return s.bytesReceived.Load() }
func (s *Stats) GetBytesSent() uint64          { return s.bytesSent.Load() }
func (s *Stats) GetCurrentChannels() uint64    { return s.currentChannels.Load() }
func (s *Stats) GetConsumers() uint64          { return s.consumers.Load() }
func (s *Stats) GetProtocolErrors() uint64     { return s.protocolErrors.Load() }
func (s *Stats) GetUptime() time.Duration      { return time.Since(s.startTime) }
