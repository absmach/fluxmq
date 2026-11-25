// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package messages

import (
	"testing"

	"github.com/absmach/fluxmq/message"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Retransmissions follow the order the messages were first sent
// [MQTT-4.6.0-1]; packet IDs wrap, so the tracker keeps that order itself.
func TestInflightListsOutboundInSendOrder(t *testing.T) {
	tracker := NewInflightTracker(1000)
	var want []uint16
	for i := range 200 {
		id := uint16((i*7919)%65535 + 1) // distinct, out of numeric order
		msg := message.New("t", nil)
		require.NoError(t, tracker.Add(id, msg, Outbound))
		tracker.MarkSent(id)
		want = append(want, id)
	}
	require.NoError(t, tracker.Add(1, message.New("in", nil), Inbound))

	assert.Equal(t, want, tracker.OutboundIDs())

	var expired []uint16
	for _, entry := range tracker.GetExpired(0) {
		expired = append(expired, entry.PacketID)
	}
	assert.Equal(t, want, expired)

	// Replacing an entry keeps its place.
	require.NoError(t, tracker.Add(want[0], message.New("t", nil), Outbound))
	assert.Equal(t, want, tracker.OutboundIDs())
	tracker.Clear()
}
