// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package broker

import (
	"context"
	"errors"
	"testing"

	"github.com/absmach/fluxmq/mqtt/packets"
	v3 "github.com/absmach/fluxmq/mqtt/packets/v3"
	v5 "github.com/absmach/fluxmq/mqtt/packets/v5"
	"github.com/absmach/fluxmq/mqtt/session"
	queuepkg "github.com/absmach/fluxmq/queue"
	"github.com/absmach/fluxmq/queue/storage"
	qtypes "github.com/absmach/fluxmq/queue/types"
	"github.com/stretchr/testify/require"
)

type failingMQTTQueueManager struct {
	mockQueueManager
	publishErr error
}

func TestMQTT5DurabilityUnconfirmedIsUnavailable(t *testing.T) {
	code, _ := mqtt5QueuePublishError(storage.ErrDurabilityUnconfirmed)
	require.Equal(t, byte(v5.PubAckUnspecifiedError), code)
}

func (m *failingMQTTQueueManager) Publish(context.Context, qtypes.PublishRequest) error {
	return m.publishErr
}

func TestMQTT5QueueErrorContract(t *testing.T) {
	tests := []struct {
		name string
		code queuepkg.ErrorCode
		want byte
	}{
		{name: "invalid", code: queuepkg.ErrorCodeInvalidArgument, want: v5.PubAckImplementationSpecificError},
		{name: "missing", code: queuepkg.ErrorCodeNotFound, want: v5.PubAckImplementationSpecificError},
		{name: "precondition", code: queuepkg.ErrorCodeFailedPrecondition, want: v5.PubAckImplementationSpecificError},
		{name: "resource exhausted", code: queuepkg.ErrorCodeResourceExhausted, want: v5.PubAckQuotaExceeded},
		{name: "unavailable", code: queuepkg.ErrorCodeUnavailable, want: v5.PubAckUnspecifiedError},
		{name: "internal", code: queuepkg.ErrorCodeInternal, want: v5.PubAckUnspecifiedError},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := queuepkg.WithFailure(errors.New("detail"), queuepkg.Failure{Code: tt.code})
			got, _ := mqtt5QueuePublishError(err)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestMQTT5QueueFailureUsesNativePubAck(t *testing.T) {
	b := NewBroker(nil, nil)
	defer b.Close()
	b.queueManager = &failingMQTTQueueManager{publishErr: queuepkg.WithFailure(
		errors.New("backend detail"),
		queuepkg.Failure{Code: queuepkg.ErrorCodeResourceExhausted, Durability: queuepkg.DurabilityNotAttempted},
	)}

	s, _, err := b.CreateSession("queue-publisher", 5, session.Options{CleanStart: true})
	require.NoError(t, err)
	conn := &captureConnection{}
	_, err = s.Connect(conn)
	require.NoError(t, err)

	err = newV5Handler(b).HandlePublish(bindConn(s), &v5.Publish{
		FixedHeader: packets.FixedHeader{PacketType: packets.PublishType, QoS: 1},
		ID:          7,
		TopicName:   testQueueOrdersProcess,
		Payload:     []byte("payload"),
	})
	require.NoError(t, err)
	require.Len(t, conn.packets, 1)
	ack, ok := conn.packets[0].(*v5.PubAck)
	require.True(t, ok)
	require.NotNil(t, ack.ReasonCode)
	require.Equal(t, byte(v5.PubAckQuotaExceeded), *ack.ReasonCode)
}

func TestMQTTQoS2QueueFailurePreservesInboundUntilRetry(t *testing.T) {
	tests := []struct {
		name    string
		version byte
		publish func(*Broker, *connCtx) error
		pubrel  func(*Broker, *connCtx) error
		isComp  func(packets.ControlPacket) bool
	}{
		{
			name:    "v3",
			version: packets.V311,
			publish: func(b *Broker, conn *connCtx) error {
				return newV3Handler(b).HandlePublish(conn, &v3.Publish{
					FixedHeader: packets.FixedHeader{PacketType: packets.PublishType, QoS: 2},
					ID:          7,
					TopicName:   testQueueOrdersProcess,
					Payload:     []byte("payload"),
				})
			},
			pubrel: func(b *Broker, conn *connCtx) error {
				return newV3Handler(b).HandlePubRel(conn, &v3.PubRel{
					FixedHeader: packets.FixedHeader{PacketType: packets.PubRelType, QoS: 1},
					ID:          7,
				})
			},
			isComp: func(packet packets.ControlPacket) bool { _, ok := packet.(*v3.PubComp); return ok },
		},
		{
			name:    "v5",
			version: packets.V5,
			publish: func(b *Broker, conn *connCtx) error {
				return newV5Handler(b).HandlePublish(conn, &v5.Publish{
					FixedHeader: packets.FixedHeader{PacketType: packets.PublishType, QoS: 2},
					ID:          7,
					TopicName:   testQueueOrdersProcess,
					Payload:     []byte("payload"),
				})
			},
			pubrel: func(b *Broker, conn *connCtx) error {
				return newV5Handler(b).HandlePubRel(conn, &v5.PubRel{
					FixedHeader: packets.FixedHeader{PacketType: packets.PubRelType, QoS: 1},
					ID:          7,
				})
			},
			isComp: func(packet packets.ControlPacket) bool { _, ok := packet.(*v5.PubComp); return ok },
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			publishErr := queuepkg.WithFailure(
				errors.New("queue unavailable"),
				queuepkg.Failure{Code: queuepkg.ErrorCodeUnavailable, Retryable: true},
			)
			queueManager := &failingMQTTQueueManager{publishErr: publishErr}
			b := NewBroker(nil, nil)
			defer b.Close()
			b.queueManager = queueManager
			b.cfg.asyncFanOut = true

			s, _, err := b.CreateSession("queue-publisher-"+tt.name, tt.version, session.Options{CleanStart: true})
			require.NoError(t, err)
			conn := &captureConnection{}
			_, err = s.Connect(conn)
			require.NoError(t, err)
			bound := bindConn(s)

			require.NoError(t, tt.publish(b, bound))
			require.Len(t, conn.packets, 1)
			conn.packets = nil

			err = tt.pubrel(b, bound)
			require.ErrorIs(t, err, publishErr)
			require.Empty(t, conn.packets, "PUBCOMP acknowledged a failed queue append")
			inbound, found, getErr := s.GetInbound(7)
			require.NoError(t, getErr)
			require.True(t, found)
			require.Equal(t, []byte("payload"), inbound.PayloadBytes())

			queueManager.publishErr = nil
			require.NoError(t, tt.pubrel(b, bound))
			require.Len(t, conn.packets, 1)
			require.True(t, tt.isComp(conn.packets[0]))
			_, found, getErr = s.GetInbound(7)
			require.NoError(t, getErr)
			require.False(t, found)
		})
	}
}

// A PUBREL naming a packet identifier the session does not hold must be
// answered with the reason code the protocol defines for it.
//
// Both versions send PUBCOMP — the publisher is waiting to release the packet
// ID either way — but MQTT 5.0 §3.7.2.1 defines 0x92 for this case, and
// answering 0x00 tells the publisher a transaction it never had was completed.
// v3 has no reason codes, so its PUBCOMP is unchanged.
func TestUnknownPubRelReasonCode(t *testing.T) {
	tests := []struct {
		name    string
		version byte
		pubrel  func(*Broker, *connCtx) error
		check   func(*testing.T, packets.ControlPacket)
	}{
		{
			name:    "v3 answers a plain PUBCOMP",
			version: packets.V311,
			pubrel: func(b *Broker, conn *connCtx) error {
				return newV3Handler(b).HandlePubRel(conn, &v3.PubRel{
					FixedHeader: packets.FixedHeader{PacketType: packets.PubRelType, QoS: 1},
					ID:          42,
				})
			},
			check: func(t *testing.T, packet packets.ControlPacket) {
				comp, ok := packet.(*v3.PubComp)
				require.True(t, ok, "expected a v3 PUBCOMP, got %T", packet)
				require.Equal(t, uint16(42), comp.ID)
			},
		},
		{
			name:    "v5 answers packet identifier not found",
			version: packets.V5,
			pubrel: func(b *Broker, conn *connCtx) error {
				return newV5Handler(b).HandlePubRel(conn, &v5.PubRel{
					FixedHeader: packets.FixedHeader{PacketType: packets.PubRelType, QoS: 1},
					ID:          42,
				})
			},
			check: func(t *testing.T, packet packets.ControlPacket) {
				comp, ok := packet.(*v5.PubComp)
				require.True(t, ok, "expected a v5 PUBCOMP, got %T", packet)
				require.NotNil(t, comp.ReasonCode)
				require.Equal(t, v5.PubCompPacketIdentifierNotFound, *comp.ReasonCode,
					"an unknown packet identifier must not be reported as success")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := NewBroker(nil, nil)
			defer b.Close()

			s, _, err := b.CreateSession("unknown-pubrel-"+tt.name, tt.version, session.Options{CleanStart: true})
			require.NoError(t, err)
			conn := &captureConnection{}
			_, err = s.Connect(conn)
			require.NoError(t, err)

			// No PUBLISH preceded this PUBREL, so the session holds nothing.
			require.NoError(t, tt.pubrel(b, bindConn(s)))
			require.Len(t, conn.packets, 1)
			tt.check(t, conn.packets[0])
		})
	}
}
