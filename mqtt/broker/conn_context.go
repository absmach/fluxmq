// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package broker

import (
	core "github.com/absmach/fluxmq/mqtt"
	"github.com/absmach/fluxmq/mqtt/packets"
	"github.com/absmach/fluxmq/mqtt/session"
)

// connCtx binds a session to the specific connection generation a runSession
// goroutine owns. All client-response writes target that connection and
// teardown is scoped to its epoch.
//
// This is what makes a local session takeover generation-safe without holding
// a lock across handler work: once a connection has been superseded, conn is
// the closed old socket and epoch is stale, so a leftover goroutine's writes
// fail harmlessly and its Disconnect is a no-op. It can neither write to nor
// disconnect the replacement connection, and because no lock is held across a
// handler, a write that blocks on a stalled old connection never stalls the
// takeover.
//
// connCtx embeds *session.Session, so handlers transparently see the full
// session API; only the connection-bound operations are overridden here.
type connCtx struct {
	*session.Session
	conn  core.Connection
	epoch uint64
}

func (c *connCtx) WritePacket(pkt packets.ControlPacket) error {
	if c.conn == nil {
		return session.ErrNotConnected
	}
	return c.conn.WritePacket(pkt)
}

func (c *connCtx) WriteControlPacket(pkt packets.ControlPacket, onSent func()) error {
	if c.conn == nil {
		return session.ErrNotConnected
	}
	return c.conn.WriteControlPacket(pkt, onSent)
}

func (c *connCtx) WriteDataPacket(pkt packets.ControlPacket, onSent func()) error {
	if c.conn == nil {
		return session.ErrNotConnected
	}
	return c.conn.WriteDataPacket(pkt, onSent)
}

func (c *connCtx) TryWriteDataPacket(pkt packets.ControlPacket, onSent func()) error {
	if c.conn == nil {
		return session.ErrNotConnected
	}
	return c.conn.TryWriteDataPacket(pkt, onSent)
}

// ProcessRetries resends due inflight messages on the bound connection rather
// than the session's current connection, so a superseded goroutine cannot
// redeliver onto the replacement.
func (c *connCtx) ProcessRetries() {
	if c.conn == nil {
		return
	}
	c.Session.ProcessRetriesTo(c.conn, c.epoch)
}

// Disconnect tears down only if this is still the current connection
// generation, so a stale DISCONNECT cannot close the replacement connection.
func (c *connCtx) Disconnect(graceful bool) error {
	return c.Session.DisconnectIf(graceful, c.epoch)
}

// current reports whether this is still the active connection generation. A
// superseded goroutine reads and writes its own closed socket, so its read and
// dispatch failures are expected teardown — not packet or protocol errors —
// and must not be counted as such.
func (c *connCtx) current() bool {
	return c.Session.Epoch() == c.epoch
}
