// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package broker

import (
	"context"
	"log/slog"
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

// drainSuperseded retires a connection displaced by a local takeover: it
// notifies an MQTT 5 client with a DISCONNECT (DisconnectSessionTakenOver), closes the socket, and
// publishes the displaced connection's Will when required. It is safe to run in
// its own goroutine and must not block the replacement connection's setup.
//
// References: OASIS MQTT 5.0, sections 3.1.4 (session takeover) and 3.1.2.5
// (Will lifecycle).
func (b *Broker) drainSuperseded(ctx context.Context, sc *session.Superseded) {
	if sc == nil {
		return
	}

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
			go func() {
				// WriteControlPacket may block enqueueing on a stalled client.
				sc.Conn.WriteControlPacket(d, func() { close(sent) }) //nolint:errcheck // best-effort takeover notification
			}()
			// Bounded: a stalled client must not delay the close indefinitely.
			select {
			case <-sent:
			case <-time.After(supersededNotifyGrace):
			}
		}

		// Closing unblocks any pending notify write and lets the displaced
		// connection's runSession goroutine observe the closed socket and exit.
		sc.Conn.Close() //nolint:errcheck // idempotent close of superseded connection
	}

	// A zero-delay Will is always published when the old connection closes. A
	// delayed Will is cancelled only when the same session continues; Clean Start
	// ends the old session, so its delayed Will is due immediately.
	if sc.Will != nil && (sc.SessionEnds || sc.Will.Delay == 0) {
		if err := b.publishWillMessage(ctx, sc.Will); err != nil {
			b.logError("publish_superseded_will", err, slog.String("client_id", sc.Will.ClientID))
		}
	}

	// Ending the old session retires its connection for good, so the disconnect
	// is owed to hooks and webhooks. A takeover that continues the same session
	// owes nothing: the client ID stays connected under the replacement socket,
	// and the handler reports the new connection through NotifyConnect.
	if sc.SessionEnds && sc.Conn != nil {
		b.emitClientDisconnected(ctx, sc.ClientID, "takeover")
	}
}
