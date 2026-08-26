// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package message

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protowire"
)

// Subscription IDs are written packed, and records written before that carry one
// varint-tagged entry each. Proto3 requires a decoder to accept both, and the
// records already on disk require it of this one.
func TestSubscriptionIDsDecodeFromBothWireForms(t *testing.T) {
	want := []uint32{7, 9, 4096}

	t.Run("packed, as written now", func(t *testing.T) {
		envelope := NewDelivery("devices/1", []byte("payload"), 1, false)
		defer Release(envelope)
		envelope.BrokerMeta.Delivery.SubscriptionIDs = want

		encoded, err := MarshalBinary(envelope)
		require.NoError(t, err)

		decoded, err := UnmarshalBinary(encoded)
		require.NoError(t, err)
		defer Release(decoded)
		assert.Equal(t, want, decoded.BrokerMeta.Delivery.SubscriptionIDs)
	})

	t.Run("unpacked, as written before", func(t *testing.T) {
		var delivery []byte
		for _, id := range want {
			delivery = protowire.AppendTag(delivery, 3, protowire.VarintType)
			delivery = protowire.AppendVarint(delivery, uint64(id))
		}

		decoded, err := UnmarshalBinary(legacyEnvelope(delivery))
		require.NoError(t, err)
		defer Release(decoded)
		assert.Equal(t, want, decoded.BrokerMeta.Delivery.SubscriptionIDs)
	})
}

func TestSubscriptionIDOverflowIsRejectedInBothForms(t *testing.T) {
	tooLarge := uint64(^uint32(0)) + 1

	t.Run("packed", func(t *testing.T) {
		var delivery []byte
		delivery = protowire.AppendTag(delivery, 3, protowire.BytesType)
		delivery = protowire.AppendBytes(delivery, protowire.AppendVarint(nil, tooLarge))

		_, err := UnmarshalBinary(legacyEnvelope(delivery))
		require.Error(t, err)
	})

	t.Run("unpacked", func(t *testing.T) {
		var delivery []byte
		delivery = protowire.AppendTag(delivery, 3, protowire.VarintType)
		delivery = protowire.AppendVarint(delivery, tooLarge)

		_, err := UnmarshalBinary(legacyEnvelope(delivery))
		require.Error(t, err)
	})
}

// A metadata blob describes a record whose value and key the record owns.
// Carrying either inside the blob leaves two sources for one field, and the
// embedded one used to lose silently.
func TestMetadataRejectsTheRecordsOwnValueAndKey(t *testing.T) {
	envelope := NewDelivery("devices/1", []byte("payload"), 1, false)
	defer Release(envelope)
	envelope.PublisherMeta.Key = []byte("partition-key")

	// A full encoding carries both, which is exactly what must not be read back
	// as metadata.
	full, err := MarshalBinary(envelope)
	require.NoError(t, err)

	_, err = UnmarshalMetadata(full, []byte("record-value"), []byte("record-key"))
	require.ErrorIs(t, err, ErrMetadataCarriesPayload)

	// Now with only the key embedded, to reach the second guard.
	withKey := protowire.AppendTag(nil, 1, protowire.VarintType)
	withKey = protowire.AppendVarint(withKey, uint64(Version1))
	user := protowire.AppendTag(nil, 1, protowire.BytesType)
	user = protowire.AppendBytes(user, []byte("partition-key"))
	withKey = protowire.AppendTag(withKey, 4, protowire.BytesType)
	withKey = protowire.AppendBytes(withKey, user)

	_, err = UnmarshalMetadata(withKey, []byte("record-value"), []byte("record-key"))
	require.ErrorIs(t, err, ErrMetadataCarriesKey)
}

// A metadata blob that carries neither is read normally, so the guards above
// assert an invariant rather than rejecting what MarshalMetadata writes.
func TestMetadataWithoutValueOrKeyDecodes(t *testing.T) {
	envelope := NewDelivery("devices/1", []byte("payload"), 1, false)
	defer Release(envelope)
	envelope.PublisherMeta.Key = []byte("partition-key")
	envelope.BrokerMeta.Queue.Name = "telemetry"

	metadata, err := MarshalMetadata(envelope)
	require.NoError(t, err)

	decoded, err := UnmarshalMetadata(metadata, []byte("record-value"), []byte("record-key"))
	require.NoError(t, err)
	defer Release(decoded)

	assert.Equal(t, "record-value", string(decoded.PayloadBytes()))
	assert.Equal(t, "record-key", string(decoded.PublisherMeta.Key))
	assert.Equal(t, "telemetry", decoded.BrokerMeta.Queue.Name)
}

// legacyEnvelope frames a hand-built delivery submessage as a complete envelope.
func legacyEnvelope(delivery []byte) []byte {
	broker := protowire.AppendTag(nil, 2, protowire.BytesType)
	broker = protowire.AppendBytes(broker, delivery)

	encoded := protowire.AppendTag(nil, 1, protowire.VarintType)
	encoded = protowire.AppendVarint(encoded, uint64(Version1))
	encoded = protowire.AppendTag(encoded, 2, protowire.BytesType)
	encoded = protowire.AppendBytes(encoded, []byte("devices/1"))
	encoded = protowire.AppendTag(encoded, 5, protowire.BytesType)
	return protowire.AppendBytes(encoded, broker)
}
