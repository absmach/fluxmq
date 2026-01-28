// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package session

import (
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

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
// This is called synchronously from the read loop instead of running in a separate goroutine.
func (h *msgHandler) ProcessRetries(writer core.PacketWriter) {
	expired := h.inflight.GetExpired(RetryTimeout)
	for _, inflight := range expired {
		if err := h.resendMessage(writer, inflight); err != nil {
			slog.Debug("Failed to resend message", "packet_id", inflight.PacketID, "error", err)
			continue
		}
		h.inflight.MarkRetry(inflight.PacketID)
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

func (h *msgHandler) resendMessage(writer core.PacketWriter, inflight *messages.InflightMessage) error {
	msg := inflight.Message

	var pkt packets.ControlPacket
	if h.version == packets.V5 {
		pkt = &v5.Publish{
			FixedHeader: packets.FixedHeader{
				PacketType: packets.PublishType,
				QoS:        msg.QoS,
				Retain:     msg.Retain,
				Dup:        true,
			},
			TopicName: msg.Topic,
			Payload:   msg.Payload,
			ID:        inflight.PacketID,
		}
	} else {
		pkt = &v3.Publish{
			FixedHeader: packets.FixedHeader{
				PacketType: packets.PublishType,
				QoS:        msg.QoS,
				Retain:     msg.Retain,
				Dup:        true,
			},
			TopicName: msg.Topic,
			Payload:   msg.Payload,
			ID:        inflight.PacketID,
		}
	}

	return writer.WritePacket(pkt)
}
