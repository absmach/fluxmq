// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package broker

import (
	"context"
	"log/slog"
	"sync"
	"time"

	core "github.com/absmach/fluxmq/mqtt"
	"github.com/absmach/fluxmq/mqtt/packets"
	v5 "github.com/absmach/fluxmq/mqtt/packets/v5"
	"github.com/absmach/fluxmq/mqtt/session"
)

// supersededNotifyGrace bounds how long the takeover waits to deliver the
// "session taken over" DISCONNECT before closing the old socket, so a stalled
// client cannot delay the close.
const supersededNotifyGrace = time.Second

// sessionRetirement combines protocol-neutral connection data captured by the
// session package with broker-owned lifecycle policy.
type sessionRetirement struct {
	clientID    string
	superseded  *session.Superseded
	sessionEnds bool
}

// retireSession retires a connection displaced by a takeover: it notifies an
// MQTT 5 client with a DISCONNECT (DisconnectSessionTakenOver), closes the
// socket, publishes the displaced connection's Will when required, and emits
// exactly one physical-connection disconnect event. It is safe to run in its
// own goroutine and must not block the replacement connection's setup.
//
// References: OASIS MQTT 5.0, sections 3.1.4 (session takeover) and 3.1.2.5
// (Will lifecycle).
func (b *Broker) retireSession(ctx context.Context, retired *sessionRetirement) {
	if retired == nil || retired.superseded == nil {
		return
	}
	sc := retired.superseded

	if sc.Conn != nil {
		// Notify a displaced MQTT 5 client before closing the connection. Wait for
		// the packet to actually be transmitted (the onSent callback fires after
		// the send loop writes it to the socket), not merely enqueued, so an
		// asynchronous send queue does not observe the close before flushing the
		// DISCONNECT.
		if sc.Version == core.ProtocolV5 {
			d := &v5.Disconnect{
				FixedHeader: packets.FixedHeader{PacketType: packets.DisconnectType},
				ReasonCode:  v5.DisconnectSessionTakenOver,
			}
			sent := make(chan struct{})
			writeDone := make(chan error, 1)
			var sentOnce sync.Once
			go func() {
				// WriteControlPacket may block enqueueing on a stalled client.
				writeDone <- sc.Conn.WriteControlPacket(d, func() {
					sentOnce.Do(func() { close(sent) })
				})
			}()
			timer := time.NewTimer(supersededNotifyGrace)
			waitForSent := true
			// Bounded: a stalled client must not delay the close indefinitely.
			for waitForSent {
				select {
				case <-sent:
					waitForSent = false
				case err := <-writeDone:
					if err != nil {
						waitForSent = false
						continue
					}
					// A successful enqueue is not a successful write. Keep waiting
					// for onSent, but do not select the completed result again.
					writeDone = nil
				case <-timer.C:
					waitForSent = false
				}
			}
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
		}

		// Closing unblocks any pending notify write and lets the displaced
		// connection's runSession goroutine observe the closed socket and exit.
		sc.Conn.Close() //nolint:errcheck // idempotent close of superseded connection
	}

	// A zero-delay Will is always published when the old connection closes. A
	// delayed Will is cancelled only when the same session continues; Clean Start
	// ends the old session, so its delayed Will is due immediately.
	if sc.Will != nil && (retired.sessionEnds || sc.Will.Delay == 0) {
		if err := b.publishWillMessage(ctx, sc.Will); err != nil {
			b.logError("publish_superseded_will", err, slog.String("client_id", sc.Will.ClientID))
		}
	}

	// Disconnect events describe physical connections. Every socket retired by
	// this path emits one event, whether the MQTT session ends or continues on a
	// replacement connection on this node or another one.
	if sc.Conn != nil {
		b.emitClientDisconnected(ctx, retired.clientID, "takeover")
	}
}
