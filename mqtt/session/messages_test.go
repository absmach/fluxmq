// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package session

import (
	"testing"
	"time"

	core "github.com/absmach/fluxmq/mqtt"
	"github.com/absmach/fluxmq/mqtt/packets"
	v3 "github.com/absmach/fluxmq/mqtt/packets/v3"
	v5 "github.com/absmach/fluxmq/mqtt/packets/v5"
	"github.com/absmach/fluxmq/storage"
	"github.com/absmach/fluxmq/storage/messages"
	"github.com/stretchr/testify/require"
)

func TestProcessRetriesSkipsInbound(t *testing.T) {
	f := &fakeInflight{
		expired: []*messages.InflightMessage{
			{
				PacketID:  1,
				Direction: messages.Inbound,
				State:     messages.StatePublishSent,
				Message:   &storage.Message{Topic: "t", QoS: 1},
			},
		},
	}
	h := newMessageHandler(f, nil, 4)
	w := &mockWriter{}

	h.ProcessRetries(w)

	require.Empty(t, w.data)
	require.Empty(t, w.control)
	require.Empty(t, f.retryCalls)
}

func TestProcessRetriesOutboundQoS1UsesPublish(t *testing.T) {
	f := &fakeInflight{
		expired: []*messages.InflightMessage{
			{
				PacketID:  10,
				Direction: messages.Outbound,
				State:     messages.StatePublishSent,
				Message:   &storage.Message{Topic: "t", QoS: 1, Payload: []byte("a")},
			},
		},
	}
	h := newMessageHandler(f, nil, 4)
	w := &mockWriter{}

	h.ProcessRetries(w)

	require.Len(t, w.data, 1)
	require.IsType(t, &v3.Publish{}, w.data[0])
	require.Empty(t, w.control)
	require.Equal(t, []uint16{10}, f.retryCalls)
}

func TestProcessRetriesOutboundQoS2PublishSentUsesPublish(t *testing.T) {
	f := &fakeInflight{
		expired: []*messages.InflightMessage{
			{
				PacketID:  20,
				Direction: messages.Outbound,
				State:     messages.StatePublishSent,
				Message:   &storage.Message{Topic: "t", QoS: 2, Payload: []byte("a")},
			},
		},
	}
	h := newMessageHandler(f, nil, 4)
	w := &mockWriter{}

	h.ProcessRetries(w)

	require.Len(t, w.data, 1)
	require.IsType(t, &v3.Publish{}, w.data[0])
	require.Empty(t, w.control)
	require.Equal(t, []uint16{20}, f.retryCalls)
}

func TestProcessRetriesOutboundQoS2PubRecReceivedUsesPubRelV3(t *testing.T) {
	f := &fakeInflight{
		expired: []*messages.InflightMessage{
			{
				PacketID:  30,
				Direction: messages.Outbound,
				State:     messages.StatePubRecReceived,
				Message:   &storage.Message{Topic: "t", QoS: 2, Payload: []byte("a")},
			},
		},
	}
	h := newMessageHandler(f, nil, 4)
	w := &mockWriter{}

	h.ProcessRetries(w)

	require.Empty(t, w.data)
	require.Len(t, w.control, 1)
	require.IsType(t, &v3.PubRel{}, w.control[0])
	require.Equal(t, []uint16{30}, f.retryCalls)
}

func TestProcessRetriesOutboundQoS2PubRecReceivedUsesPubRelV5(t *testing.T) {
	f := &fakeInflight{
		expired: []*messages.InflightMessage{
			{
				PacketID:  40,
				Direction: messages.Outbound,
				State:     messages.StatePubRecReceived,
				Message:   &storage.Message{Topic: "t", QoS: 2, Payload: []byte("a")},
			},
		},
	}
	h := newMessageHandler(f, nil, packets.V5)
	w := &mockWriter{}

	h.ProcessRetries(w)

	require.Empty(t, w.data)
	require.Len(t, w.control, 1)
	require.IsType(t, &v5.PubRel{}, w.control[0])
	require.Equal(t, []uint16{40}, f.retryCalls)
}

type mockWriter struct {
	control []packets.ControlPacket
	data    []packets.ControlPacket
}

func (w *mockWriter) WritePacket(pkt packets.ControlPacket) error {
	return w.WriteControlPacket(pkt, nil)
}

func (w *mockWriter) WriteControlPacket(pkt packets.ControlPacket, onSent func()) error {
	w.control = append(w.control, pkt)
	if onSent != nil {
		onSent()
	}
	return nil
}

func (w *mockWriter) WriteDataPacket(pkt packets.ControlPacket, onSent func()) error {
	w.data = append(w.data, pkt)
	if onSent != nil {
		onSent()
	}
	return nil
}

func (w *mockWriter) TryWriteDataPacket(pkt packets.ControlPacket, onSent func()) error {
	return w.WriteDataPacket(pkt, onSent)
}

type fakeInflight struct {
	expired                []*messages.InflightMessage
	retryCalls             []uint16
	deliveryAttemptedCalls []uint16
}

func (f *fakeInflight) Add(packetID uint16, msg *storage.Message, direction messages.Direction) error {
	return nil
}

func (f *fakeInflight) Ack(packetID uint16) (*storage.Message, error) { return nil, nil }

func (f *fakeInflight) Get(packetID uint16) (*messages.InflightMessage, bool) { return nil, false }

func (f *fakeInflight) Has(packetID uint16) bool { return false }

func (f *fakeInflight) WasReceived(packetID uint16) bool { return false }

func (f *fakeInflight) MarkReceived(packetID uint16) {}

func (f *fakeInflight) UpdateState(packetID uint16, state messages.InflightState) error { return nil }

func (f *fakeInflight) ClearReceived(packetID uint16) {}

func (f *fakeInflight) GetExpired(expiry time.Duration) []*messages.InflightMessage {
	return f.expired
}

func (f *fakeInflight) MarkSent(packetID uint16) {}

func (f *fakeInflight) MarkDeliveryAttempted(packetID uint16) {
	f.deliveryAttemptedCalls = append(f.deliveryAttemptedCalls, packetID)
}

func (f *fakeInflight) MarkRetry(packetID uint16) error {
	f.retryCalls = append(f.retryCalls, packetID)
	for _, inf := range f.expired {
		if inf.PacketID == packetID {
			inf.Retries++
			inf.SentAt = time.Now()
		}
	}
	return nil
}

func (f *fakeInflight) GetAll() []*messages.InflightMessage { return nil }

func (f *fakeInflight) CleanupExpiredReceived(olderThan time.Duration) {}

// queueFullWriter returns ErrSendQueueFull for every TryWriteDataPacket call.
type queueFullWriter struct{}

func (w *queueFullWriter) WritePacket(_ packets.ControlPacket) error { return nil }
func (w *queueFullWriter) WriteControlPacket(_ packets.ControlPacket, onSent func()) error {
	if onSent != nil {
		onSent()
	}
	return nil
}
func (w *queueFullWriter) WriteDataPacket(_ packets.ControlPacket, onSent func()) error {
	return core.ErrSendQueueFull
}
func (w *queueFullWriter) TryWriteDataPacket(_ packets.ControlPacket, _ func()) error {
	return core.ErrSendQueueFull
}

func TestProcessRetries_ResetsBackoffOnQueueFull(t *testing.T) {
	f := &fakeInflight{
		expired: []*messages.InflightMessage{
			{
				PacketID:  1,
				Direction: messages.Outbound,
				State:     messages.StatePublishSent,
				Message:   &storage.Message{Topic: "t", QoS: 1, Payload: []byte("a")},
			},
		},
	}
	h := newMessageHandler(f, nil, 4)

	h.ProcessRetries(&queueFullWriter{})

	// MarkRetry must NOT be called — the message never reached the wire.
	require.Empty(t, f.retryCalls)
	// MarkDeliveryAttempted must be called to reset the 500ms backoff timer.
	require.Equal(t, []uint16{1}, f.deliveryAttemptedCalls)
}
