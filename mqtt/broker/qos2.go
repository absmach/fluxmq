// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package broker

import (
	"context"
	"log/slog"

	corebroker "github.com/absmach/fluxmq/broker"
	"github.com/absmach/fluxmq/message"
)

// completeInboundQoS2 finishes the inbound half of a QoS 2 exchange for one
// packet ID: it publishes the stored message and settles the transaction.
//
// A nil return means the transaction is complete and the caller must send
// PUBCOMP; the caller builds its own, because the packet differs between
// protocol versions. An error means the message could not be published, and
// PUBCOMP must not be sent — acknowledging a transaction whose append failed
// tells the publisher a message is safe when it was lost.
//
// The policy lives here rather than in each version handler because it encodes
// two invariants that must hold identically for v3 and v5: never settle before
// the publish succeeds, and never PUBCOMP a transaction that could not be read.
// Two copies is two places for a fix to miss.
//
// op names the operation in logs, e.g. "v3_pubrel".
func (b *Broker) completeInboundQoS2(s *connCtx, packetID uint16, op string) error {
	msg, found, err := s.GetInbound(packetID)
	if err != nil {
		return err
	}
	if !found {
		// The transaction is already settled, or never arrived. PUBCOMP is the
		// correct answer either way: the publisher is waiting to release the
		// packet ID.
		b.telemetry.logger.Warn(op+"_unknown_packet",
			slog.String("client_id", s.ID),
			slog.Int("packet_id", int(packetID)))
		return nil
	}

	// Async fanout remains an opt-in pub/sub policy. Exact queue addresses run
	// synchronously so PUBCOMP cannot overtake a failed durable append.
	if b.cfg.asyncFanOut && b.routeResolver.Resolve(msg.Topic).Kind == corebroker.RoutePubSub {
		owned, err := s.AckInbound(packetID)
		if err != nil {
			return err
		}
		if b.fanOutPool == nil || !b.fanOutPool.Submit(func() { b.publishOwnedInbound(s.ID, op, owned) }) {
			// The pool is full or absent; publishing inline keeps the message
			// rather than dropping it to preserve asynchrony.
			b.publishOwnedInbound(s.ID, op, owned)
		}
		return nil
	}

	publish := msg.Clone()
	if err := b.Publish(context.Background(), publish); err != nil {
		b.logError(op+"_publish", err,
			slog.String("client_id", s.ID),
			slog.String("topic", msg.Topic))
		return err
	}

	// Settled only after the publish succeeded.
	owned, err := s.AckInbound(packetID)
	if err != nil {
		return err
	}
	message.Release(owned)

	return nil
}

// publishOwnedInbound publishes a message this path already owns, consuming it.
func (b *Broker) publishOwnedInbound(clientID, op string, owned *message.Envelope) {
	// The topic is read first: Publish consumes the envelope.
	topic := owned.Topic
	if err := b.Publish(context.Background(), owned); err != nil {
		b.logError(op+"_publish", err,
			slog.String("client_id", clientID),
			slog.String("topic", topic))
	}
}
