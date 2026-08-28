// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package broker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/absmach/fluxmq/message"
	"github.com/absmach/fluxmq/mqtt/session"
	"github.com/absmach/fluxmq/storage"
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
	now := time.Now()
	var toDelete []string

	b.sessionsMap.ForEach(func(s *session.Session) {
		if s.IsConnected() {
			return
		}

		if s.ExpiryInterval > 0 {
			expiryTime := s.GetDisconnectedAt().Add(time.Duration(s.ExpiryInterval) * time.Second)
			if now.After(expiryTime) {
				toDelete = append(toDelete, s.ID)
			}
		}
	})

	for _, clientID := range toDelete {
		b.expireSession(context.Background(), clientID)
	}
}

// expireSession retires a session whose expiry interval has elapsed. Expiry ends
// the session, and a Will still waiting on its delay is due when the session ends
// or the delay passes, whichever comes first [MQTT-3.1.2-8], so the record is
// captured before destruction deletes it and published after the lock is
// released.
func (b *Broker) expireSession(ctx context.Context, clientID string) {
	var dueWill *storage.WillMessage

	sessionLock := b.sessionLocks.Key(clientID)
	sessionLock.Lock()
	if s := b.sessionsMap.Get(clientID); s != nil {
		if b.stores.wills != nil {
			will, err := b.stores.wills.Get(ctx, clientID)
			switch {
			case err == nil:
				dueWill = will
			case !errors.Is(err, storage.ErrNotFound):
				b.logError("load_expiring_session_will", err, slog.String("client_id", clientID))
			}
		}
		b.destroySessionLocked(ctx, s) //nolint:errcheck // best-effort session cleanup during expired session sweep
	}
	sessionLock.Unlock()

	if dueWill != nil {
		if err := b.publishWillMessage(ctx, dueWill); err != nil {
			b.logError("publish_expired_session_will", err, slog.String("client_id", clientID))
		}
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
	if b.telemetry.stats == nil {
		return
	}

	stats := []struct {
		topic string
		value string
	}{
		{"$SYS/broker/version", "mqtt-broker-0.1.0"},
		{"$SYS/broker/uptime", fmt.Sprintf("%d", int64(b.telemetry.stats.GetUptime().Seconds()))},
		{"$SYS/broker/clients/connected", fmt.Sprintf("%d", b.telemetry.stats.GetCurrentConnections())},
		{"$SYS/broker/clients/total", fmt.Sprintf("%d", b.telemetry.stats.GetTotalConnections())},
		{"$SYS/broker/clients/disconnected", fmt.Sprintf("%d", b.telemetry.stats.GetDisconnections())},
		{"$SYS/broker/messages/received", fmt.Sprintf("%d", b.telemetry.stats.GetMessagesReceived())},
		{"$SYS/broker/messages/sent", fmt.Sprintf("%d", b.telemetry.stats.GetMessagesSent())},
		{"$SYS/broker/messages/publish/received", fmt.Sprintf("%d", b.telemetry.stats.GetPublishReceived())},
		{"$SYS/broker/messages/publish/sent", fmt.Sprintf("%d", b.telemetry.stats.GetPublishSent())},
		{"$SYS/broker/bytes/received", fmt.Sprintf("%d", b.telemetry.stats.GetBytesReceived())},
		{"$SYS/broker/bytes/sent", fmt.Sprintf("%d", b.telemetry.stats.GetBytesSent())},
		{"$SYS/broker/subscriptions/count", fmt.Sprintf("%d", b.telemetry.stats.GetSubscriptions())},
		{"$SYS/broker/retained/count", fmt.Sprintf("%d", b.telemetry.stats.GetRetainedMessages())},
		{"$SYS/broker/errors/protocol", fmt.Sprintf("%d", b.telemetry.stats.GetProtocolErrors())},
		{"$SYS/broker/errors/auth", fmt.Sprintf("%d", b.telemetry.stats.GetAuthErrors())},
		{"$SYS/broker/errors/authz", fmt.Sprintf("%d", b.telemetry.stats.GetAuthzErrors())},
		{"$SYS/broker/errors/packet", fmt.Sprintf("%d", b.telemetry.stats.GetPacketErrors())},
	}

	for _, s := range stats {
		msg := message.New(s.topic, []byte(s.value))
		msg.BrokerMeta.Delivery.Retain = true

		b.distribute(context.Background(), msg) //nolint:errcheck // fire-and-forget stats distribution

		message.Release(msg)
	}
}
