// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package faults

import (
	"net"
	"testing"
	"time"

	"github.com/absmach/fluxmq/mqtt/packets"
	v5 "github.com/absmach/fluxmq/mqtt/packets/v5"
	"github.com/absmach/fluxmq/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// v5Client is a bare MQTT 5 client that does only what a test tells it to, so
// a test controls exactly when acknowledgements reach the broker.
type v5Client struct {
	t    *testing.T
	conn net.Conn
}

func dialV5(t *testing.T, addr, clientID string, receiveMaximum uint16) *v5Client {
	t.Helper()

	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	c := &v5Client{t: t, conn: conn}

	expiry := uint32(300)
	c.send(&v5.Connect{
		FixedHeader:     packets.FixedHeader{PacketType: packets.ConnectType},
		ProtocolName:    "MQTT",
		ProtocolVersion: packets.V5,
		KeepAlive:       60,
		ClientID:        clientID,
		Properties:      &v5.ConnectProperties{SessionExpiryInterval: &expiry, ReceiveMaximum: &receiveMaximum},
	})
	connack, ok := c.next().(*v5.ConnAck)
	require.True(t, ok, "expected CONNACK")
	require.Equal(t, byte(v5.ConnAckSuccess), connack.ReasonCode)
	return c
}

func (c *v5Client) send(pkt packets.ControlPacket) {
	c.t.Helper()
	require.NoError(c.t, pkt.Pack(c.conn))
}

// next reads one packet, failing the test if none arrives in time.
func (c *v5Client) next() packets.ControlPacket {
	c.t.Helper()
	require.NoError(c.t, c.conn.SetReadDeadline(time.Now().Add(3*time.Second)))
	pkt, _, _, err := v5.ReadPacket(c.conn)
	require.NoError(c.t, err, "no packet from the broker")
	return pkt
}

func (c *v5Client) subscribe(filter string) {
	c.t.Helper()
	c.send(&v5.Subscribe{
		FixedHeader: packets.FixedHeader{PacketType: packets.SubscribeType, QoS: 1},
		ID:          1,
		Opts:        []v5.SubOption{{Topic: filter, MaxQoS: 1}},
	})
	_, ok := c.next().(*v5.SubAck)
	require.True(c.t, ok, "expected SUBACK")
}

// receive acknowledges QoS 1 messages as they arrive until it has count of
// them, and returns their payloads.
func (c *v5Client) receive(count int) []string {
	c.t.Helper()
	var payloads []string
	for len(payloads) < count {
		pub, ok := c.next().(*v5.Publish)
		require.True(c.t, ok, "expected PUBLISH after %d messages", len(payloads))
		payloads = append(payloads, string(pub.Payload))
		c.send(&v5.PubAck{FixedHeader: packets.FixedHeader{PacketType: packets.PubAckType}, ID: pub.ID})
	}
	return payloads
}

// ping proves the broker is reading this connection.
func (c *v5Client) ping() {
	c.t.Helper()
	c.send(&v5.PingReq{FixedHeader: packets.FixedHeader{PacketType: packets.PingReqType}})
	_, ok := c.next().(*v5.PingResp)
	require.True(c.t, ok, "expected PINGRESP")
}

// A client that resumes its session with a Receive Maximum smaller than its
// backlog gets the backlog one message at a time, each released by its
// acknowledgement. Those acknowledgements arrive on the connection being
// resumed, so the broker must already be reading it while it delivers.
func TestResumedSessionBacklogBeyondReceiveMaximum(t *testing.T) {
	tests := []struct {
		name string
		// unacked messages were sent on the previous connection and lost;
		// queued ones were published while the client was away.
		unacked, queued int
	}{
		{"unacked_and_queued", 1, 1},
		{"queued_beyond_quota", 0, 3},
		{"unacked_beyond_quota", 3, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lb := testutil.StartLocalBroker(t)
			proxy := testutil.NewFaultProxy(t, lb.TCPAddr)
			const clientID, topic = "rm-one", "rm/t"

			// Room for every unacknowledged message on the first connection;
			// with less, default backpressure would hold the publisher instead.
			first := dialV5(t, proxy.Addr(), clientID, 10)
			first.subscribe(topic)

			publisher := connect(t, lb.TCPAddr, "rm-pub", testutil.ConnectOptions{CleanSession: true, KeepAlive: 60})
			proxy.Blackhole()
			sent := publishNumbered(t, publisher, topic, tt.unacked, 1)
			proxy.Cut()
			require.Eventually(t, func() bool {
				s := lb.Broker.Get(clientID)
				return s != nil && !s.IsConnected()
			}, 5*time.Second, 10*time.Millisecond, "the broker did not notice the connection end")
			for i := range tt.queued {
				payload := []byte("queued-" + string(rune('a'+i)))
				require.NoError(t, publisher.Publish(topic, 1, payload, false))
				sent = append(sent, string(payload))
			}

			resumed := dialV5(t, lb.TCPAddr, clientID, 1)
			assert.ElementsMatch(t, sent, resumed.receive(len(sent)))
			resumed.ping()
		})
	}
}
