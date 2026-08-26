// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package message

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Adapters that receive a delivery as a property map resolve the offset once,
// here, rather than parsing a textual identifier back when the settlement
// arrives. Offset 0 is a real offset, so "absent" and "zero" must stay distinct.
const testOrdersQueue = "orders"

func TestQueueOffsetFromProperties(t *testing.T) {
	tests := []struct {
		name       string
		properties map[string]string
		wantOffset uint64
		wantOK     bool
	}{
		{name: "nil map", properties: nil},
		{name: "absent", properties: map[string]string{"other": "1"}},
		{name: "empty value", properties: map[string]string{PropertyOffset: ""}},
		{name: "not a number", properties: map[string]string{PropertyOffset: "seven"}},
		{name: "negative", properties: map[string]string{PropertyOffset: "-1"}},
		{name: "overflows uint64", properties: map[string]string{PropertyOffset: "18446744073709551616"}},
		{name: "composite identifier", properties: map[string]string{PropertyOffset: testOrdersQueue + ":42"}},
		{name: "zero", properties: map[string]string{PropertyOffset: "0"}, wantOffset: 0, wantOK: true},
		{name: "small", properties: map[string]string{PropertyOffset: "42"}, wantOffset: 42, wantOK: true},
		{
			name:       "max uint64",
			properties: map[string]string{PropertyOffset: strconv.FormatUint(^uint64(0), 10)},
			wantOffset: ^uint64(0),
			wantOK:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			offset, ok := QueueOffsetFromProperties(tt.properties)
			assert.Equal(t, tt.wantOK, ok)
			assert.Equal(t, tt.wantOffset, offset)
		})
	}
}

// A queue delivery always projects its offset, so the resolver can rely on it
// being present rather than falling back to a parsed message identifier.
func TestQueueDeliveryAlwaysProjectsItsOffset(t *testing.T) {
	for _, offset := range []uint64{0, 1, 42} {
		envelope := New("devices/1", []byte("payload"))
		envelope.BrokerMeta.Queue = QueueMetadata{
			Name:    testOrdersQueue,
			GroupID: testGroupID,
			Offset:  offset,
		}

		projected := ProjectProperties(envelope, PublicProjection)
		require.NotNil(t, projected)

		resolved, ok := QueueOffsetFromProperties(projected)
		require.True(t, ok, "a queue delivery must carry a resolvable offset")
		assert.Equal(t, offset, resolved)

		Release(envelope)
	}
}

// An ordinary pub/sub message carries no queue metadata, so an adapter must be
// able to tell it apart from a queue delivery and never settle on its behalf.
func TestOrdinaryDeliveryCarriesNoQueueOffset(t *testing.T) {
	envelope := New("devices/1", []byte("payload"))
	defer Release(envelope)

	_, ok := QueueOffsetFromProperties(ProjectProperties(envelope, PublicProjection))
	assert.False(t, ok)
}

// The delivery handle and a publisher's own message-id used to share one stored
// field, so whichever was written last won. A queue delivery names its record;
// an ordinary message carries whatever the publisher set; neither can become
// the other.
func TestMessageIDNamespacesDoNotCollide(t *testing.T) {
	t.Run("queue delivery names its record", func(t *testing.T) {
		envelope := New("$queue/orders/new", []byte("payload"))
		defer Release(envelope)
		envelope.PublisherMeta.MessageID = "publisher-chose-this"
		envelope.BrokerMeta.Queue = QueueMetadata{Name: testOrdersQueue, GroupID: testGroupID, Offset: 42}

		projected := ProjectProperties(envelope, PublicProjection)
		assert.Equal(t, testOrdersQueue+":42", projected[PropertyMessageID],
			"the broker's handle must win for a durable delivery")
	})

	t.Run("ordinary message keeps the publisher value", func(t *testing.T) {
		envelope := New("devices/1", []byte("payload"))
		defer Release(envelope)
		envelope.PublisherMeta.MessageID = "publisher-chose-this"

		projected := ProjectProperties(envelope, PublicProjection)
		assert.Equal(t, "publisher-chose-this", projected[PropertyMessageID])
	})

	t.Run("a peer cannot forge the delivery handle", func(t *testing.T) {
		envelope := New("devices/1", []byte("payload"))
		defer Release(envelope)

		require.NoError(t, ApplyTrustedProperties(envelope, map[string]string{
			PropertyMessageID: testOrdersQueue + ":9999",
			PropertyQueueName: testOrdersQueue,
			PropertyOffset:    "42",
		}))
		assert.Equal(t, testOrdersQueue+":9999", envelope.PublisherMeta.MessageID,
			"a wire message-id is the publisher's, and stays user metadata")
		assert.Equal(t, testOrdersQueue+":42", envelope.BrokerMeta.Queue.DeliveryID(),
			"the handle is derived from the queue and offset, not read from the peer")
	})

	t.Run("a record with no queue has no handle", func(t *testing.T) {
		assert.Equal(t, "", QueueMetadata{Offset: 7}.DeliveryID())
	})
}

// An empty source topic used to be projected as an empty property, which said
// only that the broker had nothing to say. Offset 0 is the opposite case and
// must stay unconditional: it is the first record in a queue, and omitting it
// would make it indistinguishable from a delivery carrying no offset.
func TestQueueProjectionOmitsAnEmptySourceTopicButKeepsOffsetZero(t *testing.T) {
	envelope := New("$queue/orders/new", []byte("payload"))
	defer Release(envelope)
	envelope.BrokerMeta.Queue = QueueMetadata{Name: testOrdersQueue, GroupID: testGroupID, Offset: 0}

	projected := ProjectProperties(envelope, PublicProjection)
	require.NotNil(t, projected)

	_, present := projected[PropertySourceTopic]
	assert.False(t, present, "an absent source topic must not be projected as an empty one")
	assert.Equal(t, "0", projected[PropertyOffset], "offset 0 is a real offset")

	envelope.BrokerMeta.Source.Topic = "orders/new"
	withSource := ProjectProperties(envelope, PublicProjection)
	assert.Equal(t, "orders/new", withSource[PropertySourceTopic])
}
