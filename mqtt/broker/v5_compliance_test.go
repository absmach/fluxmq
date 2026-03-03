// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package broker

import (
	"io"
	"net"
	"testing"
	"time"

	"github.com/absmach/fluxmq/mqtt/packets"
	v5 "github.com/absmach/fluxmq/mqtt/packets/v5"
	"github.com/absmach/fluxmq/mqtt/session"
	"github.com/absmach/fluxmq/storage"
	"github.com/absmach/fluxmq/storage/memory"
	"github.com/stretchr/testify/require"
)

type captureConnection struct {
	net.Conn
	packets []packets.ControlPacket
}

func (m *captureConnection) WritePacket(p packets.ControlPacket) error {
	return m.WriteControlPacket(p, nil)
}

func (m *captureConnection) WriteControlPacket(p packets.ControlPacket, onSent func()) error {
	m.packets = append(m.packets, p)
	return nil
}

func (m *captureConnection) WriteDataPacket(p packets.ControlPacket, onSent func()) error {
	m.packets = append(m.packets, p)
	return nil
}

func (m *captureConnection) TryWriteDataPacket(p packets.ControlPacket, onSent func()) error {
	return m.WriteDataPacket(p, onSent)
}

func (m *captureConnection) ReadPacket() (packets.ControlPacket, error) {
	return nil, io.EOF
}

func (m *captureConnection) Close() error {
	return nil
}

func (m *captureConnection) SetReadDeadline(t time.Time) error {
	return nil
}

func (m *captureConnection) SetKeepAlive(d time.Duration) error {
	return nil
}

func (m *captureConnection) SetOnDisconnect(fn func(graceful bool)) {}
func (m *captureConnection) Touch()                                 {}
func (m *captureConnection) RemoteAddr() net.Addr                   { return &mockAddr{} }
func (m *captureConnection) LocalAddr() net.Addr                    { return &mockAddr{} }

func newComplianceTestBroker(t *testing.T) *Broker {
	t.Helper()

	store := memory.New()
	b := NewBroker(store, nil, WithLogger(testLogger()))
	t.Cleanup(func() { _ = b.Close() })
	return b
}

func TestV5NoLocalPreventsSelfDelivery(t *testing.T) {
	b := newComplianceTestBroker(t)

	s, _, err := b.CreateSession("client-1", 5, session.Options{CleanStart: true})
	require.NoError(t, err)

	conn := &captureConnection{}
	require.NoError(t, s.Connect(conn))
	require.NoError(t, b.subscribe(s, "devices/one", 1, storage.SubscribeOptions{NoLocal: true}))

	msg := &storage.Message{
		Topic:       "devices/one",
		PublisherID: "client-1",
		QoS:         1,
	}
	msg.SetPayloadFromBytes([]byte("payload"))
	require.NoError(t, b.Publish(msg))

	require.Len(t, conn.packets, 0)
}

func TestV5RetainAsPublishedAffectsDeliveryFlag(t *testing.T) {
	b := newComplianceTestBroker(t)

	s1, _, err := b.CreateSession("client-rap-false", 5, session.Options{CleanStart: true})
	require.NoError(t, err)
	s2, _, err := b.CreateSession("client-rap-true", 5, session.Options{CleanStart: true})
	require.NoError(t, err)

	conn1 := &captureConnection{}
	conn2 := &captureConnection{}
	require.NoError(t, s1.Connect(conn1))
	require.NoError(t, s2.Connect(conn2))

	require.NoError(t, b.subscribe(s1, "devices/two", 1, storage.SubscribeOptions{RetainAsPublished: false}))
	require.NoError(t, b.subscribe(s2, "devices/two", 1, storage.SubscribeOptions{RetainAsPublished: true}))

	msg := &storage.Message{
		Topic:       "devices/two",
		PublisherID: "publisher",
		QoS:         1,
		Retain:      true,
	}
	msg.SetPayloadFromBytes([]byte("payload"))
	require.NoError(t, b.Publish(msg))

	require.Len(t, conn1.packets, 1)
	require.Len(t, conn2.packets, 1)

	p1, ok := conn1.packets[0].(*v5.Publish)
	require.True(t, ok)
	require.False(t, p1.Retain)

	p2, ok := conn2.packets[0].(*v5.Publish)
	require.True(t, ok)
	require.True(t, p2.Retain)
}

func TestV5RetainHandlingRespected(t *testing.T) {
	b := newComplianceTestBroker(t)
	s, _, err := b.CreateSession("sub-client", 5, session.Options{CleanStart: true})
	require.NoError(t, err)

	conn := &captureConnection{}
	require.NoError(t, s.Connect(conn))

	retained := &storage.Message{
		Topic:       "retain/topic",
		PublisherID: "publisher",
		QoS:         1,
		Retain:      true,
	}
	retained.SetPayloadFromBytes([]byte("retained"))
	require.NoError(t, b.Publish(retained))
	matchedRetained, err := b.GetRetainedMatching("retain/topic")
	require.NoError(t, err)
	require.Len(t, matchedRetained, 1)

	handler := NewV5Handler(b)

	rh0 := byte(0)
	sub1 := &v5.Subscribe{
		FixedHeader: packets.FixedHeader{PacketType: packets.SubscribeType, QoS: 1},
		ID:          1,
		Opts: []v5.SubOption{
			{Topic: "retain/topic", MaxQoS: 1, RetainHandling: &rh0},
		},
	}
	require.NoError(t, handler.HandleSubscribe(s, sub1))
	require.Len(t, conn.packets, 2)

	_, ok := conn.packets[0].(*v5.Publish)
	require.True(t, ok)
	_, ok = conn.packets[1].(*v5.SubAck)
	require.True(t, ok)

	conn.packets = nil

	rh1 := byte(1)
	sub2 := &v5.Subscribe{
		FixedHeader: packets.FixedHeader{PacketType: packets.SubscribeType, QoS: 1},
		ID:          2,
		Opts: []v5.SubOption{
			{Topic: "retain/topic", MaxQoS: 1, RetainHandling: &rh1},
		},
	}
	require.NoError(t, handler.HandleSubscribe(s, sub2))
	require.Len(t, conn.packets, 1)
	_, ok = conn.packets[0].(*v5.SubAck)
	require.True(t, ok)
}

func TestV5PublishErrorResponseMatchesQoS(t *testing.T) {
	b := newComplianceTestBroker(t)

	s, _, err := b.CreateSession("publisher", 5, session.Options{CleanStart: true})
	require.NoError(t, err)
	s.TopicAliasMax = 1

	conn := &captureConnection{}
	require.NoError(t, s.Connect(conn))

	handler := NewV5Handler(b)

	alias2 := uint16(2)
	pub0 := &v5.Publish{
		FixedHeader: packets.FixedHeader{PacketType: packets.PublishType, QoS: 0},
		TopicName:   "topic/one",
		Payload:     []byte("x"),
		Properties: &v5.PublishProperties{
			TopicAlias: &alias2,
		},
	}
	err = handler.HandlePublish(s, pub0)
	require.ErrorIs(t, err, ErrTopicInvalid)
	require.Len(t, conn.packets, 0)

	pub2 := &v5.Publish{
		FixedHeader: packets.FixedHeader{PacketType: packets.PublishType, QoS: 2},
		TopicName:   "topic/two",
		ID:          10,
		Payload:     []byte("x"),
		Properties: &v5.PublishProperties{
			TopicAlias: &alias2,
		},
	}
	require.NoError(t, handler.HandlePublish(s, pub2))
	require.Len(t, conn.packets, 1)
	_, ok := conn.packets[0].(*v5.PubRec)
	require.True(t, ok)
}
