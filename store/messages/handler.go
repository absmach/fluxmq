package messages

import (
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	core "github.com/dborovcanin/mqtt/core"
	"github.com/dborovcanin/mqtt/core/packets"
	v3 "github.com/dborovcanin/mqtt/core/packets/v3"
)

// MessageHandler manages message tracking, inflight operations, and aliases.
type MessageHandler struct {
	mu sync.RWMutex

	inflight     Inflight
	offlineQueue Queue

	nextPacketID uint32

	outboundAliases map[string]uint16
	inboundAliases  map[uint16]string

	stopCh chan struct{}
	wg     sync.WaitGroup
}

// NewMessageHandler creates a new message handler.
func NewMessageHandler(inflight Inflight, offlineQueue Queue) *MessageHandler {
	return &MessageHandler{
		inflight:        inflight,
		offlineQueue:    offlineQueue,
		outboundAliases: make(map[string]uint16),
		inboundAliases:  make(map[uint16]string),
		stopCh:          make(chan struct{}),
	}
}

// StartRetryLoop starts the retry loop for inflight messages.
func (h *MessageHandler) StartRetryLoop(writer core.PacketWriter) {
	h.wg.Add(1)
	go h.retryLoop(writer)
}

// Stop stops the message handler background tasks.
func (h *MessageHandler) Stop() {
	close(h.stopCh)
	h.wg.Wait()
	h.stopCh = make(chan struct{}) // Reset for reuse if needed, though usually new session
}

// Inflight returns the inflight tracker.
func (h *MessageHandler) Inflight() Inflight {
	return h.inflight
}

// OfflineQueue returns the offline queue.
func (h *MessageHandler) OfflineQueue() Queue {
	return h.offlineQueue
}

// NextPacketID generates the next packet ID.
func (h *MessageHandler) NextPacketID() uint16 {
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
func (h *MessageHandler) SetTopicAlias(topic string, alias uint16) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.outboundAliases[topic] = alias
}

// GetTopicAlias returns the alias for a topic (outbound).
func (h *MessageHandler) GetTopicAlias(topic string) (uint16, bool) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	alias, ok := h.outboundAliases[topic]
	return alias, ok
}

// SetInboundAlias sets an inbound topic alias.
func (h *MessageHandler) SetInboundAlias(alias uint16, topic string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.inboundAliases[alias] = topic
}

// ResolveInboundAlias resolves an inbound alias to a topic.
func (h *MessageHandler) ResolveInboundAlias(alias uint16) (string, bool) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	topic, ok := h.inboundAliases[alias]
	return topic, ok
}

// ClearAliases clears all aliases (e.g. on disconnect).
func (h *MessageHandler) ClearAliases() {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.outboundAliases = make(map[string]uint16)
	h.inboundAliases = make(map[uint16]string)
}

func (h *MessageHandler) retryLoop(writer core.PacketWriter) {
	defer h.wg.Done()

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			expired := h.inflight.GetExpired(20 * time.Second)
			for _, inflight := range expired {
				if err := h.resendMessage(writer, inflight); err != nil {
					slog.Debug("Failed to resend message", "packet_id", inflight.PacketID, "error", err)
					continue
				}
				h.inflight.MarkRetry(inflight.PacketID)
			}
		case <-h.stopCh:
			return
		}
	}
}

func (h *MessageHandler) resendMessage(writer core.PacketWriter, inflight *InflightMessage) error {
	msg := inflight.Message

	pub := &v3.Publish{
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

	return writer.WritePacket(pub)
}
