// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package broker

import (
	"sync/atomic"

	"github.com/absmach/fluxmq/message"
)

type ingressCounter struct {
	publishes    atomic.Uint64
	payloadBytes atomic.Uint64
}

// IngressCounters counts incoming application publishes, excluding forwarded
// deliveries and MQTT control packets. Bytes count application payloads.
type IngressCounters struct {
	Publishes    uint64 `json:"publishes"`
	PayloadBytes uint64 `json:"payload_bytes"`
}

// IngressStats separates MQTT TCP, MQTT WebSocket and the publish-only bridges.
// These counters are a breakdown, not additional traffic to add to broker totals.
func (s *Stats) IngressStats() map[string]IngressCounters {
	out := make(map[string]IngressCounters, 4)
	for i, name := range []string{"mqtt", "websocket", "http", "coap"} {
		out[name] = IngressCounters{Publishes: s.ingress[i].publishes.Load(), PayloadBytes: s.ingress[i].payloadBytes.Load()}
	}
	return out
}

func (b *Broker) recordIngress(msg *message.Envelope) {
	index := 0
	switch msg.BrokerMeta.Source.Protocol {
	case message.ProtocolHTTP:
		index = 2
	case message.ProtocolCoAP:
		index = 3
	case "", message.ProtocolMQTT:
		if s := b.sessionsMap.Get(msg.BrokerMeta.Source.ClientID); s != nil {
			if transport, ok := s.Conn().(interface{ Transport() string }); ok && transport.Transport() == "websocket" {
				index = 1
			}
		}
	default:
		// Other protocol brokers own their ingress counters.
		return
	}
	b.telemetry.stats.ingress[index].publishes.Add(1)
	b.telemetry.stats.ingress[index].payloadBytes.Add(uint64(len(msg.PayloadBytes())))
}
