// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package broker

import (
	"context"
	"testing"

	"github.com/absmach/fluxmq/message"
	"github.com/absmach/fluxmq/mqtt/session"
	"github.com/absmach/fluxmq/storage/memory"
)

type ingressWebSocket struct{ mockConnection }

func (*ingressWebSocket) Transport() string { return "websocket" }

func TestIngressSeparatesBridgesAndWebSocketFromMQTT(t *testing.T) {
	b := NewBroker(memory.New(), nil)
	defer b.Close()
	s, _, err := b.CreateSession("ws-client", 5, session.Options{CleanStart: true})
	if err != nil {
		t.Fatal(err)
	}
	_, conn, cancel := bindConnectionContext(context.Background(), &ingressWebSocket{})
	defer cancel()
	if _, err := s.Connect(conn); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		protocol message.Protocol
		client   string
		payload  string
	}{
		{message.ProtocolMQTT, "tcp-client", "tcp"},
		{message.ProtocolMQTT, "ws-client", "websocket"},
		{message.ProtocolHTTP, "http-client", "http"},
		{message.ProtocolCoAP, "coap-client", "coap"},
	} {
		msg := message.New("metrics/test", []byte(tc.payload))
		msg.BrokerMeta.Source.Protocol = tc.protocol
		msg.BrokerMeta.Source.ClientID = tc.client
		if err := b.Publish(context.Background(), msg); err != nil {
			t.Fatal(err)
		}
	}
	got := b.Stats().IngressStats()
	for name, size := range map[string]uint64{"mqtt": 3, "websocket": 9, "http": 4, "coap": 4} {
		if got[name].Publishes != 1 || got[name].PayloadBytes != size {
			t.Fatalf("%s: %+v", name, got[name])
		}
	}
	// Local cross-protocol/cluster deliveries must not become new ingress.
	forwarded := message.New("metrics/test", []byte("forwarded"))
	defer message.Release(forwarded)
	if err := b.ForwardPublish(context.Background(), forwarded); err != nil {
		t.Fatal(err)
	}
	for name, counters := range b.Stats().IngressStats() {
		if counters != got[name] {
			t.Fatalf("forwarded publish counted as %s ingress", name)
		}
	}
}
