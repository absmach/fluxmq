// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package session

import (
	"testing"
	"time"

	"github.com/absmach/fluxmq/mqtt/packets"
	v3 "github.com/absmach/fluxmq/mqtt/packets/v3"
	v5 "github.com/absmach/fluxmq/mqtt/packets/v5"
	"github.com/absmach/fluxmq/storage"
	"github.com/stretchr/testify/require"
)

// TestEncodePublish_V5RetransmitCarriesProperties guards against the
// retransmission path stripping v5 PUBLISH properties: a resent message
// (dup=true) must carry the same ContentType, ResponseTopic, CorrelationData,
// UserProperties, PayloadFormat, and a positive remaining MessageExpiry as the
// first send.
func TestEncodePublish_V5RetransmitCarriesProperties(t *testing.T) {
	pf := byte(1)
	expiry := uint32(120)
	msg := &storage.Message{
		Topic:           "sensors/temp",
		QoS:             1,
		Retain:          true,
		ContentType:     "application/json",
		ResponseTopic:   "responses/123",
		CorrelationData: []byte("corr-1"),
		PayloadFormat:   &pf,
		MessageExpiry:   &expiry,
		Expiry:          time.Now().Add(60 * time.Second),
		UserProperties:  map[string]string{"trace": "abc"},
	}
	msg.SetPayloadFromBytes([]byte("payload"))

	pkt := EncodePublish(msg, 42, packets.V5, true)
	pub, ok := pkt.(*v5.Publish)
	require.True(t, ok, "v5 version must produce a *v5.Publish")
	defer pub.Release()

	require.Equal(t, uint16(42), pub.ID)
	require.Equal(t, "sensors/temp", pub.TopicName)
	require.True(t, pub.FixedHeader.Dup, "retransmission must set the DUP flag")
	require.Equal(t, byte(1), pub.FixedHeader.QoS)
	require.True(t, pub.FixedHeader.Retain)

	require.NotNil(t, pub.Properties)
	require.Equal(t, "application/json", pub.Properties.ContentType)
	require.Equal(t, "responses/123", pub.Properties.ResponseTopic)
	require.Equal(t, []byte("corr-1"), pub.Properties.CorrelationData)
	require.NotNil(t, pub.Properties.PayloadFormat)
	require.Equal(t, byte(1), *pub.Properties.PayloadFormat)
	require.NotNil(t, pub.Properties.MessageExpiry, "remaining message expiry must be set")
	require.Greater(t, *pub.Properties.MessageExpiry, uint32(0))
	require.LessOrEqual(t, *pub.Properties.MessageExpiry, uint32(60))
	require.Contains(t, pub.Properties.User, v5.User{Key: "trace", Value: "abc"})
}

// TestEncodePublish_V3FirstSendNoDup verifies the v3 path encodes a *v3.Publish
// and honours the dup flag.
func TestEncodePublish_V3FirstSendNoDup(t *testing.T) {
	msg := &storage.Message{Topic: "t", QoS: 2}
	msg.SetPayloadFromBytes([]byte("p"))

	pkt := EncodePublish(msg, 7, packets.V311, false)
	pub, ok := pkt.(*v3.Publish)
	require.True(t, ok, "v3 version must produce a *v3.Publish")

	require.Equal(t, uint16(7), pub.ID)
	require.Equal(t, "t", pub.TopicName)
	require.False(t, pub.FixedHeader.Dup, "first send must not set the DUP flag")
	require.Equal(t, byte(2), pub.FixedHeader.QoS)
}
