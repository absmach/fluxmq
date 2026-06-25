// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package broker

import (
	"testing"
	"time"

	"github.com/absmach/fluxmq/config"
	"github.com/absmach/fluxmq/mqtt/packets"
	v3 "github.com/absmach/fluxmq/mqtt/packets/v3"
	v5 "github.com/absmach/fluxmq/mqtt/packets/v5"
	"github.com/absmach/fluxmq/mqtt/session"
	"github.com/absmach/fluxmq/storage"
	"github.com/absmach/fluxmq/storage/messages"
	"github.com/stretchr/testify/require"
)

type duplicateRejectingInflight struct {
	rejected     *storage.Message
	baseAddCalls int
	addErr       error
}

func (f *duplicateRejectingInflight) Add(_ uint16, _ *storage.Message, _ messages.Direction) error {
	f.baseAddCalls++
	return nil
}

func (f *duplicateRejectingInflight) AddInbound(_ uint16, msg *storage.Message) (bool, error) {
	f.rejected = msg
	return false, f.addErr
}

func (f *duplicateRejectingInflight) Ack(uint16) (*storage.Message, error) {
	return nil, messages.ErrPacketNotFound
}

func (f *duplicateRejectingInflight) Get(uint16) (*messages.InflightMessage, bool) {
	return nil, false
}

func (f *duplicateRejectingInflight) Has(uint16) bool {
	return false
}

func (f *duplicateRejectingInflight) WasReceived(uint16) bool {
	return false
}

func (f *duplicateRejectingInflight) MarkReceived(uint16) {}

func (f *duplicateRejectingInflight) UpdateState(uint16, messages.InflightState) error {
	return nil
}

func (f *duplicateRejectingInflight) ClearReceived(uint16) {}

func (f *duplicateRejectingInflight) GetExpired(time.Duration) []*messages.InflightMessage {
	return nil
}

func (f *duplicateRejectingInflight) MarkSent(uint16) {}

func (f *duplicateRejectingInflight) MarkDeliveryAttempted(uint16) {}

func (f *duplicateRejectingInflight) MarkRetry(uint16) error {
	return nil
}

func (f *duplicateRejectingInflight) GetAll() []*messages.InflightMessage {
	return nil
}

func (f *duplicateRejectingInflight) CleanupExpiredReceived(time.Duration) {}

func newDuplicateReleaseSession(t *testing.T, version byte, inflight messages.Inflight) (*session.Session, *captureConnection) {
	t.Helper()

	s := session.New(
		"publisher",
		version,
		session.Options{CleanStart: true},
		inflight,
		messages.NewMessageQueue(8, true),
		config.SessionConfig{MaxInflightMessages: 8},
	)
	conn := &captureConnection{}
	_, err := s.Connect(conn)
	require.NoError(t, err)
	return s, conn
}

func TestV3QoS2DuplicateReleasesUnacceptedMessage(t *testing.T) {
	b := newComplianceTestBroker(t)
	inflight := &duplicateRejectingInflight{}
	s, conn := newDuplicateReleaseSession(t, packets.V311, inflight)

	err := newV3Handler(b).HandlePublish(bindConn(s), &v3.Publish{
		FixedHeader: packets.FixedHeader{PacketType: packets.PublishType, QoS: 2, Dup: true},
		TopicName:   "duplicate/v3",
		ID:          7,
		Payload:     []byte("duplicate"),
	})
	require.NoError(t, err)
	require.Zero(t, inflight.baseAddCalls)
	require.NotNil(t, inflight.rejected)
	require.Empty(t, inflight.rejected.Topic)
	require.Nil(t, inflight.rejected.PayloadBuf)
	require.Len(t, conn.packets, 1)
	require.IsType(t, &v3.PubRec{}, conn.packets[0])
}

func TestV5QoS2DuplicateReleasesUnacceptedMessage(t *testing.T) {
	b := newComplianceTestBroker(t)
	inflight := &duplicateRejectingInflight{}
	s, conn := newDuplicateReleaseSession(t, packets.V5, inflight)

	err := newV5Handler(b).HandlePublish(bindConn(s), &v5.Publish{
		FixedHeader: packets.FixedHeader{PacketType: packets.PublishType, QoS: 2, Dup: true},
		TopicName:   "duplicate/v5",
		ID:          7,
		Payload:     []byte("duplicate"),
	})
	require.NoError(t, err)
	require.Zero(t, inflight.baseAddCalls)
	require.NotNil(t, inflight.rejected)
	require.Empty(t, inflight.rejected.Topic)
	require.Nil(t, inflight.rejected.PayloadBuf)
	require.Len(t, conn.packets, 1)
	require.IsType(t, &v5.PubRec{}, conn.packets[0])
}

func TestV5QoS2AdmissionFailureReleasesMessage(t *testing.T) {
	b := newComplianceTestBroker(t)
	inflight := &duplicateRejectingInflight{addErr: messages.ErrInflightFull}
	s, conn := newDuplicateReleaseSession(t, packets.V5, inflight)

	err := newV5Handler(b).HandlePublish(bindConn(s), &v5.Publish{
		FixedHeader: packets.FixedHeader{PacketType: packets.PublishType, QoS: 2},
		TopicName:   "full/v5",
		ID:          8,
		Payload:     []byte("rejected"),
	})
	require.ErrorIs(t, err, messages.ErrInflightFull)
	require.Zero(t, inflight.baseAddCalls)
	require.NotNil(t, inflight.rejected)
	require.Empty(t, inflight.rejected.Topic)
	require.Nil(t, inflight.rejected.PayloadBuf)
	require.Empty(t, conn.packets)
}
