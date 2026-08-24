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
	qtypes "github.com/absmach/fluxmq/queue/types"
	"github.com/stretchr/testify/require"
)

type failingMQTTQueueManager struct {
	mockQueueManager
	publishErr error
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
			require.Equal(t, []byte("payload"), inbound.GetPayload())

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
