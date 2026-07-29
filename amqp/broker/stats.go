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

	localAuthSuccess       atomic.Uint64
	localAuthFailures      atomic.Uint64
	localPublishDenials    atomic.Uint64
	localOperationDenials  atomic.Uint64
	localConnections       atomic.Uint64
	localReloadSuccess     atomic.Uint64
	localReloadFailures    atomic.Uint64
	localForcedDisconnects atomic.Uint64
	localPublishTimeouts   atomic.Uint64
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
func (s *Stats) IncrementLocalAuthSuccess() { s.localAuthSuccess.Add(1) }
func (s *Stats) IncrementLocalAuthFailures() {
	s.localAuthFailures.Add(1)
}

func (s *Stats) IncrementLocalPublishDenials() {
	s.localPublishDenials.Add(1)
}

func (s *Stats) IncrementLocalOperationDenials() {
	s.localOperationDenials.Add(1)
}
func (s *Stats) IncrementLocalConnections() { s.localConnections.Add(1) }
func (s *Stats) DecrementLocalConnections() { s.localConnections.Add(^uint64(0)) }
func (s *Stats) IncrementLocalReloadSuccess() {
	s.localReloadSuccess.Add(1)
}

func (s *Stats) IncrementLocalReloadFailures() {
	s.localReloadFailures.Add(1)
}

func (s *Stats) IncrementLocalPublishTimeouts() {
	s.localPublishTimeouts.Add(1)
}

func (s *Stats) AddLocalForcedDisconnects(n uint64) {
	s.localForcedDisconnects.Add(n)
}

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
func (s *Stats) GetLocalAuthSuccess() uint64   { return s.localAuthSuccess.Load() }
func (s *Stats) GetLocalAuthFailures() uint64  { return s.localAuthFailures.Load() }
func (s *Stats) GetLocalPublishDenials() uint64 {
	return s.localPublishDenials.Load()
}

func (s *Stats) GetLocalOperationDenials() uint64 {
	return s.localOperationDenials.Load()
}
func (s *Stats) GetLocalConnections() uint64 { return s.localConnections.Load() }
func (s *Stats) GetLocalReloadSuccess() uint64 {
	return s.localReloadSuccess.Load()
}

func (s *Stats) GetLocalReloadFailures() uint64 {
	return s.localReloadFailures.Load()
}

func (s *Stats) GetLocalPublishTimeouts() uint64 {
	return s.localPublishTimeouts.Load()
}

func (s *Stats) GetLocalForcedDisconnects() uint64 {
	return s.localForcedDisconnects.Load()
}
func (s *Stats) GetUptime() time.Duration { return time.Since(s.startTime) }
