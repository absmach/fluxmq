// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package broker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	corebroker "github.com/absmach/fluxmq/broker"
	"github.com/absmach/fluxmq/broker/events"
	"github.com/absmach/fluxmq/cluster"
	core "github.com/absmach/fluxmq/mqtt"
	"github.com/absmach/fluxmq/mqtt/session"
	"github.com/absmach/fluxmq/storage"
	"github.com/absmach/fluxmq/storage/messages"
)

// DeliverToSession queues a message for delivery to a session.
// Returns packet ID (>0) if session is connected and QoS>0, otherwise 0.
func (b *Broker) DeliverToSession(ctx context.Context, s *session.Session, msg *storage.Message) (uint16, error) {
	// Check if message has expired
	if msg.MessageExpiry != nil && !msg.Expiry.IsZero() && time.Now().After(msg.Expiry) {
		b.logOp("message_expired",
			slog.String("client_id", s.ID),
			slog.String("topic", msg.Topic),
			slog.Time("expiry", msg.Expiry))
		msg.ReleasePayload()
		storage.ReleaseMessage(msg)
		return 0, nil
	}

	if !s.IsConnected() {
		if msg.QoS > 0 {
			// Offline queue copies the message internally, so we always release the original
			err := s.OfflineQueue().Enqueue(msg)
			msg.ReleasePayload()
			storage.ReleaseMessage(msg)
			return 0, err
		}
		msg.ReleasePayload() // Drop QoS 0 message for offline client - release buffer
		storage.ReleaseMessage(msg)
		return 0, nil
	}

	// Capture the target connection, protocol version, and generation as a
	// delivery lease. The PUBLISH is encoded for the captured version and
	// written to the captured connection, and quota is acquired for the captured
	// generation — so a takeover landing mid-delivery cannot make this delivery
	// write to, encode for, or consume the quota of, the replacement connection.
	conn, version, gen := s.DeliveryLease()
	if conn == nil {
		return b.deliverOffline(s, msg)
	}

	if msg.QoS == 0 {
		err := b.DeliverMessage(conn, version, msg, nil)
		if err == nil && b.telemetry.webhooks != nil {
			b.telemetry.webhooks.Notify(context.Background(), events.MessageDelivered{ //nolint:errcheck,contextcheck // fire-and-forget webhook notification
				ClientID:     s.ID,
				MessageTopic: msg.Topic,
				QoS:          msg.QoS,
				PayloadSize:  len(msg.GetPayload()),
			})
		}
		msg.ReleasePayload()
		storage.ReleaseMessage(msg)
		return 0, err
	}

	return b.deliverQoS(ctx, s, msg, conn, version, gen, 0)
}

// maxLeaseRetries bounds how many times a delivery re-leases against a newer
// connection generation before giving up to the offline queue, so a takeover
// storm cannot spin forever.
const maxLeaseRetries = 8

// retrySupersededLease re-leases against the current connection generation and
// retries delivery when gen was superseded by a takeover. retried reports
// whether a retry was performed (bounded by maxLeaseRetries); when it is false
// the caller handles genuine quota exhaustion or an offline session.
func (b *Broker) retrySupersededLease(ctx context.Context, s *session.Session, msg *storage.Message, gen uint64, attempt int) (packetID uint16, retried bool, err error) {
	if newConn, newVersion, curGen := s.DeliveryLease(); curGen != gen && newConn != nil && attempt < maxLeaseRetries {
		packetID, err = b.deliverQoS(ctx, s, msg, newConn, newVersion, curGen, attempt+1)
		return packetID, true, err
	}
	return 0, false, nil
}

// deliverQoS delivers a QoS 1/2 message under the lease (conn, version, gen). If
// the generation is superseded by a takeover, it re-leases against the current
// connection and retries, rather than mis-routing the message to capacity
// handling that the replacement connection may never drain.
func (b *Broker) deliverQoS(ctx context.Context, s *session.Session, msg *storage.Message, conn core.Connection, version byte, gen uint64, attempt int) (uint16, error) {
	packetID := s.NextPacketID()
	msg.PacketID = packetID

	// Acquire a send-quota (Receive Maximum) token for this outbound PUBLISH.
	// Backpressure mode blocks until a token frees; queue mode falls back to the
	// pending queue when the quota is exhausted.
	if s.SendBackpressure() {
		if !s.AcquireSendQuota(packetID, gen) {
			return b.releaseLease(ctx, s, msg, gen, attempt)
		}
	} else if !s.TryAcquireSendQuota(packetID, gen) {
		// A superseded generation must be retried against the current connection,
		// not treated as capacity exhaustion (the replacement may never produce
		// an ACK to drain the pending queue).
		if pid, retried, err := b.retrySupersededLease(ctx, s, msg, gen, attempt); retried {
			return pid, err
		}
		// Genuine quota exhaustion: defer into the pending queue.
		if s.TryEnqueuePending(msg, nil) {
			return 0, nil
		}
		msg.ReleasePayload()
		storage.ReleaseMessage(msg)
		return 0, nil
	}

	// Inflight storage takes ownership - it will release when message is ACK'd or expires
	if err := s.Inflight().Add(packetID, msg, messages.Outbound); err != nil {
		// Persistent inflight store is full (server cap, bidirectional). Release
		// the quota token and try the pending queue.
		s.ReleaseSendQuota(packetID, gen)
		if s.TryEnqueuePending(msg, nil) {
			// Deferred delivery; packet ID assigned at drain time.
			return 0, nil
		}
		// Both inflight and pending queue are full — drop with error.
		msg.ReleasePayload()
		storage.ReleaseMessage(msg)
		return 0, err
	}

	// Record that delivery was attempted so GetExpired can retry messages
	// that never reached the wire (e.g. send queue full on first attempt).
	s.Inflight().MarkDeliveryAttempted(packetID)

	onSent := func() {
		// Mark sent only while this generation is still current, atomically under
		// the session lock. If a takeover superseded the lease and the
		// asynchronously queued packet flushed on the old connection, the entry
		// stays un-sent so the replacement connection retransmits it immediately
		// rather than waiting out the retry timeout.
		s.MarkSentIfEpoch(packetID, gen)
	}
	if err := b.DeliverMessage(conn, version, msg, onSent); err != nil {
		if errors.Is(err, core.ErrSendQueueFull) {
			// Send queue full; message stays in inflight and will be retried
			// by ProcessRetries once the queue drains.
			return packetID, nil
		}
		// Other delivery failure; message is in inflight and will be retried.
		return packetID, err
	}

	// Webhook: message delivered (QoS 1/2)
	if b.telemetry.webhooks != nil {
		b.telemetry.webhooks.Notify(context.Background(), events.MessageDelivered{ //nolint:errcheck,contextcheck // fire-and-forget webhook notification
			ClientID:     s.ID,
			MessageTopic: msg.Topic,
			QoS:          msg.QoS,
			PayloadSize:  len(msg.GetPayload()),
		})
	}

	return packetID, nil
}

// releaseLease handles a backpressure acquire that returned false: a takeover
// (retry against the new generation) or a disconnect (offline queue).
func (b *Broker) releaseLease(ctx context.Context, s *session.Session, msg *storage.Message, gen uint64, attempt int) (uint16, error) {
	if pid, retried, err := b.retrySupersededLease(ctx, s, msg, gen, attempt); retried {
		return pid, err
	}
	return b.deliverOffline(s, msg)
}

// deliverOffline parks a message for an offline (or unreachable) session: QoS>0
// goes to the offline queue, QoS 0 is dropped.
func (b *Broker) deliverOffline(s *session.Session, msg *storage.Message) (uint16, error) {
	var err error
	if msg.QoS > 0 {
		err = s.OfflineQueue().Enqueue(msg)
	}
	msg.ReleasePayload()
	storage.ReleaseMessage(msg)
	return 0, err
}

// AckMessage acknowledges a message by packet ID and releases the buffer.
func (b *Broker) AckMessage(s *session.Session, packetID uint16) error {
	msg, err := s.Inflight().Ack(packetID)
	if err != nil {
		return err
	}
	if msg != nil {
		msg.ReleasePayload()
		storage.ReleaseMessage(msg)
	}

	// Release the send-quota token (PUBACK/PUBCOMP) under the current generation
	// — the live token is always held by the current connection (a retransmit
	// re-acquires under the new generation) — and pull through one pending
	// message (queue mode) to keep the pipeline full.
	s.ReleaseSendQuota(packetID, s.Epoch())
	s.DrainOnePending(func(pending *storage.Message, _ func()) error {
		_, err := b.DeliverToSession(context.Background(), s, pending)
		return err
	})

	return nil
}

// DeliverMessage encodes a message for the given protocol version and writes it
// to the given connection — both captured in the delivery lease — so a delivery
// stays bound to the generation it acquired quota for, and a cross-version
// takeover cannot encode a packet for one protocol and write it to a connection
// of the other.
func (b *Broker) DeliverMessage(conn core.Connection, version byte, msg *storage.Message, onSent func()) error {
	b.telemetry.stats.IncrementPublishSent()
	b.telemetry.stats.AddBytesSent(uint64(len(msg.GetPayload())))

	// First send (dup=false). EncodePublish is the single encoder shared with the
	// retransmission path so the two cannot drift on v5 properties.
	pub := session.EncodePublish(msg, msg.PacketID, version, false)

	// The send loop calls pub.Release() after Pack() completes.
	// onSent carries only application-level callbacks (e.g. MarkSent).
	err := conn.TryWriteDataPacket(pub, onSent)
	if err != nil {
		// TryWriteDataPacket failed without enqueuing; release immediately.
		pub.Release()
	}
	return err
}

// DeliverToClient implements cluster.MessageHandler.DeliverToClient.
func (b *Broker) DeliverToClient(ctx context.Context, clientID string, msg *cluster.Message) error {
	s := b.Get(clientID)
	if s == nil {
		return fmt.Errorf("%w: session not found: %s", corebroker.ErrClientNotConnected, clientID)
	}

	// cluster.Message comes from cluster - create storage.Message with zero-copy buffer
	storeMsg := storage.AcquireMessage()
	storeMsg.Topic = msg.Topic
	storeMsg.QoS = msg.QoS
	storeMsg.Retain = msg.Retain
	storeMsg.Properties = msg.Properties
	storeMsg.SetPayloadFromBytes(msg.Payload)

	_, err := b.DeliverToSession(ctx, s, storeMsg)
	// Note: DeliverToSession will release the message for QoS 0
	// For QoS 1/2, Inflight storage takes ownership
	return err
}

// DeliverToSessionByID delivers a message to a client by client ID.
// This implements the BrokerInterface required by the queue manager.
func (b *Broker) DeliverToSessionByID(ctx context.Context, clientID string, msg any) error {
	s := b.Get(clientID)
	if s == nil {
		return fmt.Errorf("%w: session not found: %s", corebroker.ErrClientNotConnected, clientID)
	}

	// Convert queue message to storage message
	queueMsg, ok := msg.(*storage.Message)
	if !ok {
		return fmt.Errorf("invalid message type")
	}

	_, err := b.DeliverToSession(ctx, s, queueMsg)
	return err
}
