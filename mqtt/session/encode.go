// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package session

import (
	"encoding/base64"
	"strconv"
	"time"

	"github.com/absmach/fluxmq/mqtt/packets"
	v3 "github.com/absmach/fluxmq/mqtt/packets/v3"
	v5 "github.com/absmach/fluxmq/mqtt/packets/v5"
	"github.com/absmach/fluxmq/storage"
)

// EncodePublish builds a PUBLISH control packet for the given protocol version
// from msg. The packet is taken from the version's pool and the caller must
// return it via pkt.Release() once written. dup sets the DUP flag (true for a
// retransmission). For v5 it applies the message's PUBLISH properties and the
// remaining message-expiry interval, so a retransmission carries the same
// properties as the original send.
//
// This is the single encoder shared by the first-send path (broker delivery)
// and the retransmission path (ProcessRetries), so the two cannot diverge.
func EncodePublish(msg *storage.Message, packetID uint16, version byte, dup bool) packets.ControlPacket {
	if version == packets.V5 {
		p := v5.AcquirePublish()
		p.FixedHeader = packets.FixedHeader{
			PacketType: packets.PublishType,
			QoS:        msg.QoS,
			Retain:     msg.Retain,
			Dup:        dup,
		}
		p.TopicName = msg.Topic
		p.Payload = msg.GetPayload()
		p.ID = packetID

		// Send the remaining message-expiry interval, not the original.
		if msg.MessageExpiry != nil && !msg.Expiry.IsZero() {
			if remaining := time.Until(msg.Expiry); remaining > 0 {
				remainingSec := uint32(remaining.Seconds())
				p.Properties.MessageExpiry = &remainingSec
			}
		}
		applyPublishProperties(p.Properties, msg)
		return p
	}

	p := v3.AcquirePublish()
	p.FixedHeader = packets.FixedHeader{
		PacketType: packets.PublishType,
		QoS:        msg.QoS,
		Retain:     msg.Retain,
		Dup:        dup,
	}
	p.TopicName = msg.Topic
	p.Payload = msg.GetPayload()
	p.ID = packetID
	return p
}

func applyPublishProperties(props *v5.PublishProperties, msg *storage.Message) {
	if props == nil || msg == nil {
		return
	}

	// Prefer explicit fields; fall back to mapped properties.
	if msg.ContentType != "" {
		props.ContentType = msg.ContentType
	} else if v := msg.Properties["content-type"]; v != "" {
		props.ContentType = v
	}

	if msg.ResponseTopic != "" {
		props.ResponseTopic = msg.ResponseTopic
	} else if v := msg.Properties["response-topic"]; v != "" {
		props.ResponseTopic = v
	}

	if len(msg.CorrelationData) > 0 {
		props.CorrelationData = msg.CorrelationData
	} else if v := msg.Properties["correlation-id"]; v != "" {
		if decoded, err := base64.StdEncoding.DecodeString(v); err == nil {
			props.CorrelationData = decoded
		} else {
			props.CorrelationData = []byte(v)
		}
	}

	if msg.PayloadFormat != nil {
		props.PayloadFormat = msg.PayloadFormat
	} else if v := msg.Properties["payload-format"]; v != "" {
		if n, err := strconv.ParseUint(v, 10, 8); err == nil {
			pf := byte(n)
			props.PayloadFormat = &pf
		}
	}

	// Build User properties slice directly without intermediate map.
	// Skip entirely when no properties exist (common case).
	var userCount int
	if msg.Properties != nil {
		for k := range msg.Properties {
			if !isReservedUserPropertyKey(k) {
				userCount++
			}
		}
	}
	userCount += len(msg.UserProperties)

	if userCount > 0 {
		props.User = make([]v5.User, 0, userCount)
		if msg.Properties != nil {
			for k, v := range msg.Properties {
				if isReservedUserPropertyKey(k) {
					continue
				}
				props.User = append(props.User, v5.User{Key: k, Value: v})
			}
		}
		for k, v := range msg.UserProperties {
			props.User = append(props.User, v5.User{Key: k, Value: v})
		}
	}
}

func isReservedUserPropertyKey(key string) bool {
	switch key {
	case "content-type", "response-topic", "correlation-id", "payload-format":
		return true
	default:
		return false
	}
}
