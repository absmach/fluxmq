// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package session

import (
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/absmach/fluxmq/message"
	core "github.com/absmach/fluxmq/mqtt"
	"github.com/absmach/fluxmq/mqtt/packets"
	v3 "github.com/absmach/fluxmq/mqtt/packets/v3"
	v5 "github.com/absmach/fluxmq/mqtt/packets/v5"
	"github.com/absmach/fluxmq/storage/messages"
)

// msgHandler manages message tracking, inflight operations, and aliases.
type msgHandler struct {
	mu              sync.RWMutex
	inflight        messages.Inflight
	offlineQueue    messages.Queue
	outboundAliases map[string]uint16
	inboundAliases  map[uint16]string
	nextPacketID    uint32
	version         byte
}

// newMessageHandler creates a new message handler.
func newMessageHandler(inflight messages.Inflight, offlineQueue messages.Queue, version byte) *msgHandler {
	return &msgHandler{
		inflight:        inflight,
		offlineQueue:    offlineQueue,
		outboundAliases: make(map[string]uint16),
		inboundAliases:  make(map[uint16]string),
		version:         version,
	}
}

// RetryTimeout is the duration after which inflight messages are considered expired.
const RetryTimeout = 20 * time.Second

// ProcessRetries checks for expired inflight messages and resends them.
// This is called synchronously from the read loop instead of running in a
// separate goroutine.
//
// acquire gates each outbound retransmission against the connection's send quota
// (Receive Maximum): a message already holding a token resends, otherwise a
// token is consumed; when none is available the retransmission is deferred to a
// later cycle. This stops a reconnect from retransmitting more unacknowledged
// PUBLISH packets at once than the new connection advertised.
func (h *msgHandler) ProcessRetries(writer core.PacketWriter, version byte, acquire func(packetID uint16) bool, markRetry func(packetID uint16)) {
	h.resendOlderThan(RetryTimeout, writer, version, sendQuota{acquire: acquire}, markRetry)
}

// sendQuota gates retransmissions on the connection's send quota. release
// returns a token taken for a message that turned out to be acknowledged
// already; it may be nil where acknowledgements cannot interleave.
type sendQuota struct {
	acquire func(packetID uint16) bool
	release func(packetID uint16)
}

// ResendUnacknowledged resends every outbound message the client has not
// acknowledged, however recently it was sent. A resumed session owes these
// to the client at once [MQTT-4.4.0-1]: the connection they went out on is
// gone, and so is any chance that they arrived on it.
func (h *msgHandler) ResendUnacknowledged(writer core.PacketWriter, version byte, quota sendQuota, markRetry func(packetID uint16)) {
	h.resendOlderThan(0, writer, version, quota, markRetry)
}

func (h *msgHandler) resendOlderThan(age time.Duration, writer core.PacketWriter, version byte, quota sendQuota, markRetry func(packetID uint16)) {
	if version == 0 {
		version = h.version
	}
	if markRetry == nil {
		markRetry = func(packetID uint16) {
			if err := h.inflight.MarkRetry(packetID); err != nil {
				slog.Debug("Failed to mark retry", "packet_id", packetID, "error", err)
			}
		}
	}

	// GetExpired names what is due; its entries borrow the tracker's
	// envelopes, so only their packet IDs are used.
	for _, due := range h.inflight.GetExpired(age) {
		if due.Direction != messages.Outbound {
			continue
		}
		packetID := due.PacketID
		if quota.acquire != nil && !quota.acquire(packetID) {
			continue // no send quota available this cycle; retry later
		}
		// Waiting for quota can outlast the transaction: an acknowledgement
		// meanwhile has released its envelope, and a PUBREC has moved it on
		// to PUBREL. Read it again, with a reference of its own.
		current, ok := h.inflight.Snapshot(packetID)
		if !ok {
			if quota.release != nil {
				quota.release(packetID)
			}
			continue
		}
		if err := h.resendMessage(writer, current, version, markRetry); err != nil {
			slog.Debug("Failed to resend message", "packet_id", packetID, "error", err)
		}
		message.Release(current.Message)
	}
}

// Inflight returns the inflight tracker.
func (h *msgHandler) Inflight() messages.Inflight {
	return h.inflight
}

// OfflineQueue returns the offline queue.
func (h *msgHandler) OfflineQueue() messages.Queue {
	return h.offlineQueue
}

// NextPacketID generates the next packet ID.
func (h *msgHandler) NextPacketID() uint16 {
	for {
		id := atomic.AddUint32(&h.nextPacketID, 1)
		id16 := uint16(id & 0xFFFF)
		if id16 == 0 {
			continue // Packet ID 0 is reserved
		}
		if !h.inflight.Has(id16) {
			return id16
		}
	}
}

// SetTopicAlias sets a topic alias for outbound use.
func (h *msgHandler) SetTopicAlias(topic string, alias uint16) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.outboundAliases[topic] = alias
}

// GetTopicAlias returns the alias for a topic (outbound).
func (h *msgHandler) GetTopicAlias(topic string) (uint16, bool) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	alias, ok := h.outboundAliases[topic]
	return alias, ok
}

// SetInboundAlias sets an inbound topic alias.
func (h *msgHandler) SetInboundAlias(alias uint16, topic string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.inboundAliases[alias] = topic
}

// ResolveInboundAlias resolves an inbound alias to a topic.
func (h *msgHandler) ResolveInboundAlias(alias uint16) (string, bool) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	topic, ok := h.inboundAliases[alias]
	return topic, ok
}

// ClearAliases clears all aliases (e.g. on disconnect).
func (h *msgHandler) ClearAliases() {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.outboundAliases = make(map[string]uint16)
	h.inboundAliases = make(map[uint16]string)
}

func (h *msgHandler) resendMessage(writer core.PacketWriter, inflight *messages.InflightMessage, version byte, markRetry func(packetID uint16)) error {
	msg := inflight.Message
	if msg == nil {
		return nil
	}

	onSent := func() {
		markRetry(inflight.PacketID)
	}

	if msg.BrokerMeta.Delivery.QoS == 2 && inflight.State == messages.StatePubRecReceived {
		rel := h.newPubRelPacket(inflight.PacketID, version)
		return writer.WriteControlPacket(rel, onSent)
	}

	// dup=true: this is a retransmission. EncodePublish carries the v5 PUBLISH
	// properties so a resent message is not stripped of them.
	pub := EncodePublish(msg, inflight.PacketID, version, true)
	// TryWriteDataPacket owns pub on every path, including its error returns.
	err := writer.TryWriteDataPacket(pub, onSent)
	if errors.Is(err, core.ErrSendQueueFull) {
		h.inflight.MarkDeliveryAttempted(inflight.PacketID)
		return nil
	}
	return err
}

func (h *msgHandler) newPubRelPacket(packetID uint16, version byte) packets.ControlPacket {
	if version == packets.V5 {
		return &v5.PubRel{
			FixedHeader: packets.FixedHeader{PacketType: packets.PubRelType, QoS: 1},
			ID:          packetID,
		}
	}
	return &v3.PubRel{
		FixedHeader: packets.FixedHeader{PacketType: packets.PubRelType, QoS: 1},
		ID:          packetID,
	}
}
