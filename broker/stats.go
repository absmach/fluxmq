// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package broker

import (
	"sync/atomic"
	"time"
)

// Stats tracks detailed broker statistics.
type Stats struct {
	startTime time.Time

	// Connection stats
	totalConnections   atomic.Uint64
	currentConnections atomic.Uint64
	disconnections     atomic.Uint64

	// Message stats
	messagesReceived atomic.Uint64
	messagesSent     atomic.Uint64
	publishReceived  atomic.Uint64
	publishSent      atomic.Uint64

	// Byte stats
	bytesReceived atomic.Uint64
	bytesSent     atomic.Uint64

	// Subscription stats
	subscriptions   atomic.Uint64
	unsubscriptions atomic.Uint64

	// Retained message stats
	retainedMessages atomic.Uint64

	// Error stats
	protocolErrors atomic.Uint64
	authErrors     atomic.Uint64
	authzErrors    atomic.Uint64
	packetErrors   atomic.Uint64
}

// NewStats creates a new Stats instance.
func NewStats() *Stats {
	return &Stats{
		startTime: time.Now(),
	}
}

// Connection tracking.
func (s *Stats) IncrementConnections() {
	s.totalConnections.Add(1)
	s.currentConnections.Add(1)
}

func (s *Stats) DecrementConnections() {
	s.currentConnections.Add(^uint64(0))
	s.disconnections.Add(1)
}

func (s *Stats) GetTotalConnections() uint64 {
	return s.totalConnections.Load()
}

func (s *Stats) GetCurrentConnections() uint64 {
	return s.currentConnections.Load()
}

func (s *Stats) GetDisconnections() uint64 {
	return s.disconnections.Load()
}

// Message tracking.
func (s *Stats) IncrementMessagesReceived() {
	s.messagesReceived.Add(1)
}

func (s *Stats) IncrementMessagesSent() {
	s.messagesSent.Add(1)
}

func (s *Stats) IncrementPublishReceived() {
	s.publishReceived.Add(1)
	s.messagesReceived.Add(1)
}

func (s *Stats) IncrementPublishSent() {
	s.publishSent.Add(1)
	s.messagesSent.Add(1)
}

func (s *Stats) GetMessagesReceived() uint64 {
	return s.messagesReceived.Load()
}

func (s *Stats) GetMessagesSent() uint64 {
	return s.messagesSent.Load()
}

func (s *Stats) GetPublishReceived() uint64 {
	return s.publishReceived.Load()
}

func (s *Stats) GetPublishSent() uint64 {
	return s.publishSent.Load()
}

// Byte tracking.
func (s *Stats) AddBytesReceived(n uint64) {
	s.bytesReceived.Add(n)
}

func (s *Stats) AddBytesSent(n uint64) {
	s.bytesSent.Add(n)
}

func (s *Stats) GetBytesReceived() uint64 {
	return s.bytesReceived.Load()
}

func (s *Stats) GetBytesSent() uint64 {
	return s.bytesSent.Load()
}

// Subscription tracking.
func (s *Stats) IncrementSubscriptions() {
	s.subscriptions.Add(1)
}

func (s *Stats) DecrementSubscriptions() {
	s.subscriptions.Add(^uint64(0))
	s.unsubscriptions.Add(1)
}

func (s *Stats) GetSubscriptions() uint64 {
	return s.subscriptions.Load()
}

// Retained message tracking

func (s *Stats) GetRetainedMessages() uint64 {
	return s.retainedMessages.Load()
}

// Error tracking.
func (s *Stats) IncrementProtocolErrors() {
	s.protocolErrors.Add(1)
}

func (s *Stats) IncrementAuthErrors() {
	s.authErrors.Add(1)
}

func (s *Stats) IncrementAuthzErrors() {
	s.authzErrors.Add(1)
}

func (s *Stats) IncrementPacketErrors() {
	s.packetErrors.Add(1)
}

func (s *Stats) GetProtocolErrors() uint64 {
	return s.protocolErrors.Load()
}

func (s *Stats) GetAuthErrors() uint64 {
	return s.authErrors.Load()
}

func (s *Stats) GetAuthzErrors() uint64 {
	return s.authzErrors.Load()
}

func (s *Stats) GetPacketErrors() uint64 {
	return s.packetErrors.Load()
}

// Uptime.
func (s *Stats) GetUptime() time.Duration {
	return time.Since(s.startTime)
}
