// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package broker

import (
	"fmt"
	"time"

	"github.com/absmach/mqtt/session"
	"github.com/absmach/mqtt/storage"
)

// expiryLoop periodically checks for expired sessions.
func (b *Broker) expiryLoop() {
	defer b.wg.Done()

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			b.expireSessions()
			b.triggerWills()
		case <-b.stopCh:
			return
		}
	}
}

// expireSessions removes expired sessions.
func (b *Broker) expireSessions() {
	b.mu.Lock()
	defer b.mu.Unlock()

	now := time.Now()
	var toDelete []string

	b.sessionsMap.ForEach(func(s *session.Session) {
		if s.IsConnected() {
			return
		}

		if s.ExpiryInterval > 0 {
			info := s.Info()
			expiryTime := info.DisconnectedAt.Add(time.Duration(s.ExpiryInterval) * time.Second)
			if now.After(expiryTime) {
				toDelete = append(toDelete, s.ID)
			}
		}
	})

	for _, clientID := range toDelete {
		s := b.sessionsMap.Get(clientID)
		b.destroySessionLocked(s)
	}
}

// statsLoop periodically publishes broker statistics.
func (b *Broker) statsLoop() {
	defer b.wg.Done()

	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			b.publishStats()
		case <-b.stopCh:
			return
		}
	}
}

// publishStats publishes current broker statistics to $SYS topics.
func (b *Broker) publishStats() {
	if b.stats == nil {
		return
	}

	stats := []struct {
		topic string
		value string
	}{
		{"$SYS/broker/version", "mqtt-broker-0.1.0"},
		{"$SYS/broker/uptime", fmt.Sprintf("%d", int64(b.stats.GetUptime().Seconds()))},
		{"$SYS/broker/clients/connected", fmt.Sprintf("%d", b.stats.GetCurrentConnections())},
		{"$SYS/broker/clients/total", fmt.Sprintf("%d", b.stats.GetTotalConnections())},
		{"$SYS/broker/clients/disconnected", fmt.Sprintf("%d", b.stats.GetDisconnections())},
		{"$SYS/broker/messages/received", fmt.Sprintf("%d", b.stats.GetMessagesReceived())},
		{"$SYS/broker/messages/sent", fmt.Sprintf("%d", b.stats.GetMessagesSent())},
		{"$SYS/broker/messages/publish/received", fmt.Sprintf("%d", b.stats.GetPublishReceived())},
		{"$SYS/broker/messages/publish/sent", fmt.Sprintf("%d", b.stats.GetPublishSent())},
		{"$SYS/broker/bytes/received", fmt.Sprintf("%d", b.stats.GetBytesReceived())},
		{"$SYS/broker/bytes/sent", fmt.Sprintf("%d", b.stats.GetBytesSent())},
		{"$SYS/broker/subscriptions/count", fmt.Sprintf("%d", b.stats.GetSubscriptions())},
		{"$SYS/broker/retained/count", fmt.Sprintf("%d", b.stats.GetRetainedMessages())},
		{"$SYS/broker/errors/protocol", fmt.Sprintf("%d", b.stats.GetProtocolErrors())},
		{"$SYS/broker/errors/auth", fmt.Sprintf("%d", b.stats.GetAuthErrors())},
		{"$SYS/broker/errors/authz", fmt.Sprintf("%d", b.stats.GetAuthzErrors())},
		{"$SYS/broker/errors/packet", fmt.Sprintf("%d", b.stats.GetPacketErrors())},
	}

	for _, s := range stats {
		msg := &storage.Message{
			Topic:  s.topic,
			QoS:    0,
			Retain: true,
		}
		msg.SetPayloadFromBytes([]byte(s.value))

		b.distribute(msg)

		// Release the message buffer after distribution
		msg.ReleasePayload()
	}
}
