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

// reasonSessionTakenOver is MQTT 5 reason code 0x8E, sent in a DISCONNECT to a
// client whose session has been taken over by a new connection. [MQTT-3.1.4-3].
const reasonSessionTakenOver = 0x8E

// supersededNotifyGrace bounds how long the takeover waits to deliver the
// "session taken over" DISCONNECT before closing the old socket, so a stalled
// client cannot delay the close.
const supersededNotifyGrace = time.Second

// drainSuperseded retires a connection displaced by a local takeover: it
// notifies an MQTT 5 client with a DISCONNECT (0x8E), closes the socket, and
// publishes the displaced connection's Will when required. It is safe to run in
// its own goroutine and must not block the replacement connection's setup.
//
// References: OASIS MQTT 5.0, sections 3.1.4 (session takeover) and 3.1.2.5
// (Will lifecycle).
func (b *Broker) drainSuperseded(ctx context.Context, sc *session.Superseded) {
	if sc == nil || sc.Conn == nil {
		return
	}

	// Notify a displaced MQTT 5 client before closing the connection.
	if sc.Version == core.ProtocolV5 {
		d := &v5.Disconnect{
			FixedHeader: packets.FixedHeader{PacketType: packets.DisconnectType},
			ReasonCode:  reasonSessionTakenOver,
		}
		// Bounded best-effort: a stalled client must not delay the close.
		done := make(chan struct{})
		go func() {
			sc.Conn.WritePacket(d) //nolint:errcheck // best-effort takeover notification
			close(done)
		}()
		select {
		case <-done:
		case <-time.After(supersededNotifyGrace):
		}
	}

	// Closing unblocks any pending notify write and lets the displaced
	// connection's runSession goroutine observe the closed socket and exit.
	sc.Conn.Close() //nolint:errcheck // idempotent close of superseded connection

	// Publish the displaced connection's Will if it has no delay. A Will with a
	// delay is cancelled because the session continues under the new
	// connection (a takeover is a reconnect of the same session).
	if sc.Will != nil && sc.Will.Delay == 0 {
		if err := b.publishWillMessage(ctx, sc.Will); err != nil {
			b.logError("publish_superseded_will", err, slog.String("client_id", sc.Will.ClientID))
		}
	}
}
