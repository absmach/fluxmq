// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package broker

import (
	"fmt"
	"testing"

	"github.com/absmach/fluxmq/message"
	"github.com/absmach/fluxmq/storage/memory"
	"github.com/absmach/fluxmq/storage/messages"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Stored inflight keys carry decimal packet IDs and list in byte order. A
// restore must still put the messages back in packet-ID order, the closest a
// stored entry has to the order they must be resent in.
func TestRestoredInflightKeepsPacketIDOrder(t *testing.T) {
	store := memory.New()
	b := NewBroker(store, nil)
	t.Cleanup(func() { _ = b.Close() })

	const clientID = "restore-order"
	for _, id := range []uint16{10, 2, 1, 33} {
		msg := message.NewDelivery("t", []byte("p"), 1, false)
		msg.BrokerMeta.Delivery.PacketID = id
		msg.BrokerMeta.Delivery.InflightDirection = byte(messages.Outbound)
		key := fmt.Sprintf("%s%s%d/%d", clientID, inflightPrefix, messages.Outbound, id)
		require.NoError(t, store.Messages().Store(key, msg))
		message.Release(msg)
	}

	tracker := messages.NewInflightTracker(16)
	require.NoError(t, b.restoreInflightFromStorage(clientID, tracker))
	assert.Equal(t, []uint16{1, 2, 10, 33}, tracker.OutboundIDs())
	tracker.Clear()
}
