// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package session

import (
	"testing"
	"time"

	"github.com/absmach/fluxmq/message"
	"github.com/absmach/fluxmq/mqtt/packets"
	v3 "github.com/absmach/fluxmq/mqtt/packets/v3"
	v5 "github.com/absmach/fluxmq/mqtt/packets/v5"
	"github.com/stretchr/testify/assert"
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
	msg := message.NewDelivery(testTopic, []byte("payload"), 1, true)
	msg.User.ContentType = "application/json"
	msg.User.ResponseTopic = "responses/123"
	msg.User.CorrelationData = []byte("corr-1")
	msg.User.PayloadFormat = &pf
	msg.User.MessageExpiry = &expiry
	msg.User.Properties = map[string]string{testTraceKey: testTraceVal}
	msg.Broker.Delivery.ExpiresAt = time.Now().Add(60 * time.Second)

	pkt := EncodePublish(msg, 42, packets.V5, true)
	pub, ok := pkt.(*v5.Publish)
	require.True(t, ok, "v5 version must produce a *v5.Publish")
	defer pub.Release()

	require.Equal(t, uint16(42), pub.ID)
	require.Equal(t, testTopic, pub.TopicName)
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
	msg := message.NewDelivery("t", []byte("p"), 2, false)

	pkt := EncodePublish(msg, 7, packets.V311, false)
	pub, ok := pkt.(*v3.Publish)
	require.True(t, ok, "v3 version must produce a *v3.Publish")

	require.Equal(t, uint16(7), pub.ID)
	require.Equal(t, "t", pub.TopicName)
	require.False(t, pub.FixedHeader.Dup, "first send must not set the DUP flag")
	require.Equal(t, byte(2), pub.FixedHeader.QoS)
}

func TestEncodePublishDeliveryOverridesMutableDeliveryFlags(t *testing.T) {
	msg := message.NewDelivery("t", []byte("p"), 2, true)
	defer message.Release(msg)

	pkt := EncodePublishDelivery(msg, 0, packets.V311, false, 0, false)
	pub, ok := pkt.(*v3.Publish)
	require.True(t, ok, "v3 version must produce a *v3.Publish")
	defer pub.Release()

	require.Equal(t, byte(0), pub.FixedHeader.QoS)
	require.False(t, pub.FixedHeader.Retain)
	require.Equal(t, byte(2), msg.Broker.Delivery.QoS, "borrowed envelope must not be mutated")
	require.True(t, msg.Broker.Delivery.Retain, "borrowed envelope must not be mutated")
}

const (
	testRuleTrace = `["rule-a"]`
	testTraceVal  = "abc"
	testTraceKey  = "trace"
)

// Broker-internal properties travel between services in the property bag. They
// must never be encoded into a v5 PUBLISH, or every subscribing device would
// read the internal state services pass to one another.
func TestEncodePublish_V5OmitsReservedProperties(t *testing.T) {
	reserved := message.ReservedPropertyPrefix + "re.trace"

	tests := []struct {
		name           string
		properties     map[string]string
		userProperties map[string]string
		want           map[string]string
	}{
		{
			name:       "reserved mapped property is omitted",
			properties: map[string]string{reserved: testRuleTrace, "tenant": "acme"},
			want:       map[string]string{"tenant": "acme"},
		},
		{
			name:           "reserved user property is omitted",
			userProperties: map[string]string{reserved: testRuleTrace, testTraceKey: testTraceVal},
			want:           map[string]string{testTraceKey: testTraceVal},
		},
		{
			name:       "only reserved properties yields none",
			properties: map[string]string{reserved: testRuleTrace},
			want:       map[string]string{},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			msg := message.NewDelivery(testTopic, []byte("payload"), 1, false)
			msg.User.Properties = make(map[string]string, len(tc.properties)+len(tc.userProperties))
			for key, value := range tc.properties {
				msg.User.Properties[key] = value
			}
			for key, value := range tc.userProperties {
				msg.User.Properties[key] = value
			}

			pkt := EncodePublish(msg, 1, packets.V5, false)
			pub, ok := pkt.(*v5.Publish)
			require.True(t, ok, "expected a v5 publish packet")
			t.Cleanup(pkt.Release)

			got := map[string]string{}
			if pub.Properties != nil {
				for _, user := range pub.Properties.User {
					got[user.Key] = user.Value
				}
			}
			assert.Equal(t, tc.want, got)
			assert.NotContains(t, got, reserved)
		})
	}
}
