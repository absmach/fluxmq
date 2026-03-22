// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package broker

import (
	"context"
	"errors"
	"io"
	"net"
	"time"

	"github.com/absmach/fluxmq/mqtt/packets"
	"github.com/absmach/fluxmq/mqtt/session"
)

// retryCheckInterval is how often we check for expired inflight messages.
const retryCheckInterval = 1 * time.Second

// runSession runs the main packet loop for a session using a Handler.
// It handles both packet reading and message retry checking in a single goroutine
// by using short read deadlines and processing retries on timeout.
func (b *Broker) runSession(handler Handler, s *session.Session) error {
	conn := s.Conn()
	if conn == nil {
		return nil
	}

	lastActivity := time.Now()
	lastRetryCheck := time.Now()

	for {
		// Calculate read deadline: minimum of keep-alive and retry check interval
		// This allows us to check retries periodically while respecting keep-alive
		readTimeout := retryCheckInterval
		if s.KeepAlive > 0 && s.KeepAlive < readTimeout {
			readTimeout = s.KeepAlive
		}
		conn.SetReadDeadline(time.Now().Add(readTimeout)) //nolint:errcheck // fails only on closed connection

		pkt, err := s.ReadPacket()
		if err != nil {
			// Check if this is a timeout (expected for retry checking)
			var netErr net.Error
			if errors.As(err, &netErr) && netErr.Timeout() {
				// Check if keep-alive has actually expired
				if s.KeepAlive > 0 {
					keepAliveDeadline := s.KeepAlive + s.KeepAlive/2
					if time.Since(lastActivity) > keepAliveDeadline {
						// Real keep-alive timeout - client is unresponsive
						b.telemetry.stats.DecrementConnections()
						s.Disconnect(false) //nolint:errcheck // disconnect during keepalive timeout; connection already dead
						return err
					}
				}
				// Just a retry check interval timeout - process retries and continue
				s.ProcessRetries()
				lastRetryCheck = time.Now()
				continue
			}

			// Real error (EOF, connection closed, etc.)
			if err != io.EOF && err != session.ErrNotConnected {
				b.telemetry.stats.IncrementPacketErrors()
			}
			b.telemetry.stats.DecrementConnections()
			s.Disconnect(false) //nolint:errcheck // disconnect on read error; connection already failed
			return err
		}

		// Packet received - update activity time
		lastActivity = time.Now()

		b.telemetry.stats.IncrementMessagesReceived()
		s.Touch()

		// Throttled retry check: under sustained traffic the read loop never
		// times out, so we check retries here but at most once per interval.
		if time.Since(lastRetryCheck) >= retryCheckInterval {
			s.ProcessRetries()
			lastRetryCheck = time.Now()
		}

		if err := dispatchPacket(handler, s, pkt); err != nil {
			if err == io.EOF {
				b.telemetry.stats.DecrementConnections()
				return nil
			}
			b.telemetry.stats.IncrementProtocolErrors()
			b.telemetry.stats.DecrementConnections()
			s.Disconnect(false) //nolint:errcheck // disconnect on protocol error; connection is being terminated
			return err
		}
	}
}

func dispatchPacket(handler Handler, s *session.Session, pkt packets.ControlPacket) error {
	switch pkt.Type() {
	case packets.PublishType:
		return handler.HandlePublish(s, pkt)
	case packets.PubAckType:
		return handler.HandlePubAck(s, pkt)
	case packets.PubRecType:
		return handler.HandlePubRec(s, pkt)
	case packets.PubRelType:
		return handler.HandlePubRel(s, pkt)
	case packets.PubCompType:
		return handler.HandlePubComp(s, pkt)
	case packets.SubscribeType:
		return handler.HandleSubscribe(s, pkt)
	case packets.UnsubscribeType:
		return handler.HandleUnsubscribe(s, pkt)
	case packets.PingReqType:
		return handler.HandlePingReq(s)
	case packets.DisconnectType:
		return handler.HandleDisconnect(s, pkt)
	case packets.AuthType:
		return handler.HandleAuth(s, pkt)
	default:
		return ErrInvalidPacketType
	}
}

// Shutdown performs a graceful shutdown of the broker.
// It waits for active sessions to disconnect or transfers them to other nodes.
func (b *Broker) Shutdown(ctx context.Context, drainTimeout time.Duration) error {
	b.shuttingDown.Store(true)

	b.telemetry.logger.Info("Starting shutdown", "drain_timeout", drainTimeout)

	// Wait for drain timeout or until all sessions disconnect
	drainDeadline := time.Now().Add(drainTimeout)
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			b.telemetry.logger.Warn("Shutdown cancelled by context")
			return b.Close()
		case <-ticker.C:
			count := b.sessionsMap.Count()
			if count == 0 {
				b.telemetry.logger.Info("All sessions disconnected")
				return b.Close()
			}
			if time.Now().After(drainDeadline) {
				b.telemetry.logger.Info("Drain timeout reached", "remaining_sessions", count)
				// Transfer remaining sessions to other nodes (if clustered)
				if b.cluster != nil {
					b.transferActiveSessions(ctx)
				}
				return b.Close()
			}
			b.telemetry.logger.Info("Waiting for sessions to drain", "remaining", count)
		}
	}
}

// transferActiveSessions transfers active sessions to other cluster nodes.
func (b *Broker) transferActiveSessions(ctx context.Context) {
	b.globalMu.Lock()
	defer b.globalMu.Unlock()

	transferred := 0
	b.sessionsMap.ForEach(func(s *session.Session) {
		if !s.IsConnected() {
			return
		}

		// Release session ownership so another node can take it
		if err := b.cluster.ReleaseSession(ctx, s.ID); err != nil {
			b.telemetry.logger.Error("Failed to release session during shutdown",
				"client_id", s.ID,
				"error", err)
			return
		}

		transferred++
		b.telemetry.logger.Info("Released session for takeover",
			"client_id", s.ID)
	})

	if transferred > 0 {
		b.telemetry.logger.Info("Released sessions for cluster takeover", "count", transferred)
		// Give clients brief moment to reconnect to other nodes
		time.Sleep(2 * time.Second)
	}
}

// Close shuts down the broker immediately.
func (b *Broker) Close() error {
	if !b.closed.CompareAndSwap(false, true) {
		return nil
	}

	close(b.stopCh)
	b.wg.Wait()

	// Close webhook notifier if enabled
	if b.telemetry.webhooks != nil {
		if err := b.telemetry.webhooks.Close(); err != nil {
			b.logError("close_webhooks", err)
		}
	}

	// Close event hook if enabled
	if b.eventHook != nil {
		if err := b.eventHook.Close(); err != nil {
			b.logError("close_event_hook", err)
		}
	}

	// Stop queue manager if enabled
	if b.queueManager != nil {
		if err := b.queueManager.Stop(); err != nil {
			b.logError("close_queue_manager", err)
		}
	}

	b.globalMu.Lock()
	defer b.globalMu.Unlock()

	b.sessionsMap.ForEach(func(s *session.Session) {
		if s.IsConnected() {
			s.Disconnect(false) //nolint:errcheck // best-effort disconnect during broker close
		} else {
			// For already-disconnected sessions, persist any queued messages
			b.persistOfflineQueue(s)
		}
	})

	// Drain and stop async fan-out workers after sessions are closed.
	if b.fanOutPool != nil {
		b.fanOutPool.Close()
	}

	return nil
}
